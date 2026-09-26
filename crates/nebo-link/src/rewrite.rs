//! URL rewriting between a runtime and the owner's browser, so a runtime UI
//! works under `https://neboai.com/t/<botId>` without knowing it is there.
//!
//! The runtime is told its public host (`X-Forwarded-Host`, `Origin`), and
//! may or may not know the `/t/<botId>` prefix. Whatever it writes, the link
//! makes every URL of the runtime's own surface point through the tunnel.
//!
//! # Outbound (runtime to browser)
//!
//! An absolute URL is rewritten when its authority names the runtime:
//!
//! - the public host: `https://` or `wss://` + the host of
//!   [`WEB_ORIGIN`](crate::endpoints::WEB_ORIGIN), with or without `:443`.
//!   The runtime only learns that host from the link, so a URL on it is a URL
//!   of the runtime's own surface.
//! - the runtime's loopback address: `http://` or `ws://` +
//!   `127.0.0.1:<port>`, `localhost:<port>` or `[::1]:<port>`, where `port`
//!   is the runtime UI's port. The origin becomes the public one (`https`,
//!   `wss`).
//!
//! Then the path: one that already starts with `/t/<botId>` (followed by
//! `/`, `?`, `#` or the end of the URL) is left as it is; any other path gets
//! `/t/<botId>` in front. A bare origin (no path) stays an origin: an origin
//! is compared, not fetched, so `https://neboai.com` is never given a path.
//! Every other URL (other hosts, other ports, the hub's own subdomains) is
//! untouched.
//!
//! Both slash spellings are matched and kept: `https://…/x` and the
//! JSON-escaped `https:\/\/…\/x`.
//!
//! A path-absolute `Location` or `Content-Location` (`/login`) gets the
//! prefix the same way, and a `Set-Cookie` `Path` is scoped under it.
//!
//! # Inbound (browser to runtime)
//!
//! A public URL of this bot (`https://neboai.com/t/<botId>/…`) in a
//! WebSocket text frame or `Referer` is mapped the way the request path is
//! ([`PathMode`]): a runtime that serves under the prefix
//! ([`PathMode::ReaddPrefix`]) gets it unchanged; a runtime that serves at
//! its root ([`PathMode::StripWithForwardedPrefix`]) gets it without the
//! prefix.
//!
//! # Bodies
//!
//! Text bodies (HTML, CSS, JavaScript, JSON, event streams) are rewritten as
//! they stream: each chunk is forwarded at once, holding back only the few
//! bytes at its end that could begin a URL still being received. A gzip or
//! brotli body is decoded, rewritten and encoded again with the same coding,
//! flushed per chunk, so the tunnel still carries it compressed and an event
//! stream is never held up by the encoder.

use std::borrow::Cow;
use std::io::Write;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use bytes::Bytes;
use hyper::body::Frame;
use hyper::header::{self, HeaderMap, HeaderValue};
use nebo_runtimes::PathMode;

use crate::proxy::{Body, BoxError};

/// Which way a URL is travelling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Runtime to browser.
    Outbound,
    /// Browser to runtime.
    Inbound,
}

/// The rewrite rules for one linked runtime.
#[derive(Debug, Clone)]
pub struct Rewriter {
    /// The public host, e.g. `neboai.com`.
    host: String,
    /// `/t/<botId>`.
    base_path: String,
    /// `/t/<botId>` in the JSON-escaped spelling, `\/t\/<botId>`.
    base_path_escaped: String,
    /// The runtime UI's loopback authorities.
    loopback: [String; 3],
    path_mode: PathMode,
}

/// The longest authority the rewriter compares. Anything longer is some
/// other host.
const MAX_AUTHORITY: usize = 64;

/// Content decoded at a time, which bounds the decoded bytes held at once.
const DECODE_SLICE: usize = 4096;

/// Brotli settings for re-encoding: a quality that keeps up with a stream.
const BROTLI_QUALITY: u32 = 5;
const BROTLI_WINDOW: u32 = 22;

/// One change to a buffer: replace `len` bytes at `at` with `with`.
#[derive(Debug, PartialEq, Eq)]
struct Edit {
    at: usize,
    len: usize,
    with: String,
}

/// What a scan found.
struct Scan {
    edits: Vec<Edit>,
    /// Bytes before this are final; bytes from here may begin a URL that
    /// continues in the next chunk.
    done: usize,
}

/// What starts at one position.
enum Found {
    Nothing,
    /// Not enough bytes to decide.
    More,
    /// A URL that ends its authority at `resume`, with the edit it needs.
    Url {
        edit: Option<Edit>,
        resume: usize,
    },
}

/// How one literal compares at a position.
enum Lit {
    Yes(usize),
    No,
    More,
}

impl Rewriter {
    /// `public_origin` is the browser's origin (`https://neboai.com`),
    /// `base_path` the tunnel prefix (`/t/<botId>`), `port` the runtime UI's
    /// loopback port.
    pub fn new(public_origin: &str, base_path: &str, port: u16, path_mode: PathMode) -> Self {
        let host = public_origin
            .split_once("://")
            .map_or(public_origin, |(_, host)| host)
            .trim_end_matches('/')
            .to_ascii_lowercase();
        let base_path = base_path.trim_end_matches('/').to_string();
        Self {
            host,
            base_path_escaped: base_path.replace('/', "\\/"),
            base_path,
            loopback: [
                format!("127.0.0.1:{port}"),
                format!("localhost:{port}"),
                format!("[::1]:{port}"),
            ],
            path_mode,
        }
    }

    /// Rewrites every URL in `text`.
    pub fn rewrite<'a>(&self, direction: Direction, text: &'a str) -> Cow<'a, str> {
        let scan = self.scan(direction, text.as_bytes(), None, true);
        if scan.edits.is_empty() {
            return Cow::Borrowed(text);
        }
        // Every edit starts and ends at an ASCII byte of a URL, so it falls
        // on a character boundary.
        let mut out = String::with_capacity(text.len() + 64);
        let mut from = 0;
        for edit in &scan.edits {
            out.push_str(&text[from..edit.at]);
            out.push_str(&edit.with);
            from = edit.at + edit.len;
        }
        out.push_str(&text[from..]);
        Cow::Owned(out)
    }

    /// Rewrites a `Location` or `Content-Location` value: a path-absolute
    /// reference gets the prefix, an absolute URL the outbound rule.
    pub fn location<'a>(&self, value: &'a str) -> Cow<'a, str> {
        if value.starts_with('/') && !value.starts_with("//") {
            if self.is_prefixed(value) {
                Cow::Borrowed(value)
            } else {
                Cow::Owned(format!("{}{value}", self.base_path))
            }
        } else {
            self.rewrite(Direction::Outbound, value)
        }
    }

    /// Rewrites the URL of a `Refresh` value (`5; url=/next`).
    pub fn refresh<'a>(&self, value: &'a str) -> Cow<'a, str> {
        let Some(at) = value.to_ascii_lowercase().find("url=") else {
            return Cow::Borrowed(value);
        };
        let (head, url) = value.split_at(at + 4);
        let quote = url.chars().next().filter(|c| *c == '\'' || *c == '"');
        let (open, url) = url.split_at(quote.map_or(0, char::len_utf8));
        match self.location(url) {
            Cow::Borrowed(_) => Cow::Borrowed(value),
            Cow::Owned(url) => Cow::Owned(format!("{head}{open}{url}")),
        }
    }

    /// Scopes a `Set-Cookie` `Path` under the prefix.
    pub fn set_cookie<'a>(&self, value: &'a str) -> Cow<'a, str> {
        let mut changed = false;
        let attributes: Vec<Cow<str>> = value
            .split(';')
            .enumerate()
            .map(|(n, attribute)| {
                let Some((name, path)) = attribute.split_once('=') else {
                    return Cow::Borrowed(attribute);
                };
                if n == 0 || !name.trim().eq_ignore_ascii_case("path") {
                    return Cow::Borrowed(attribute);
                }
                let path = path.trim();
                if !path.starts_with('/') || self.is_prefixed(path) {
                    return Cow::Borrowed(attribute);
                }
                changed = true;
                let path = if path == "/" { "" } else { path };
                Cow::Owned(format!("{}={}{path}", name, self.base_path))
            })
            .collect();
        if changed {
            Cow::Owned(attributes.join(";"))
        } else {
            Cow::Borrowed(value)
        }
    }

    /// Rewrites the URL-bearing headers of a runtime's response.
    pub fn response_headers(&self, headers: &mut HeaderMap) {
        for name in [
            header::LOCATION,
            header::CONTENT_LOCATION,
            header::REFRESH,
            header::SET_COOKIE,
        ] {
            let values: Vec<HeaderValue> = headers.get_all(&name).iter().cloned().collect();
            if values.is_empty() {
                continue;
            }
            let rewritten: Vec<HeaderValue> = values
                .into_iter()
                .map(|value| {
                    let Ok(text) = value.to_str() else { return value };
                    let text = if name == header::SET_COOKIE {
                        self.set_cookie(text)
                    } else if name == header::REFRESH {
                        self.refresh(text)
                    } else {
                        self.location(text)
                    };
                    match text {
                        Cow::Borrowed(_) => value,
                        Cow::Owned(text) => HeaderValue::try_from(text).unwrap_or(value),
                    }
                })
                .collect();
            headers.remove(&name);
            for value in rewritten {
                headers.append(&name, value);
            }
        }
    }

    /// Rewrites the URL-bearing headers of a browser's request.
    pub fn request_headers(&self, headers: &mut HeaderMap) {
        let Some(referer) = headers.get(header::REFERER).and_then(|v| v.to_str().ok()) else {
            return;
        };
        if let Cow::Owned(referer) = self.rewrite(Direction::Inbound, referer)
            && let Ok(value) = HeaderValue::try_from(referer)
        {
            headers.insert(header::REFERER, value);
        }
    }

    /// Whether a path-absolute `path` is already under the prefix.
    fn is_prefixed(&self, path: &str) -> bool {
        path.strip_prefix(&self.base_path)
            .is_some_and(|rest| rest.is_empty() || rest.starts_with(['/', '?', '#']))
    }

    /// Finds the edits for `buf`. `before` is the byte that preceded it in
    /// the stream; `end` says no more bytes follow.
    fn scan(&self, direction: Direction, buf: &[u8], before: Option<u8>, end: bool) -> Scan {
        let mut edits = Vec::new();
        let mut i = 0;
        while let Some(offset) = buf[i..].iter().position(|b| matches!(b, b'h' | b'H' | b'w' | b'W')) {
            i += offset;
            let previous = if i == 0 { before } else { Some(buf[i - 1]) };
            if previous.is_some_and(is_scheme_byte) {
                i += 1;
                continue;
            }
            match self.url_at(direction, buf, i, end) {
                Found::Nothing => i += 1,
                Found::More => return Scan { edits, done: i },
                Found::Url { edit, resume } => {
                    edits.extend(edit);
                    i = resume;
                }
            }
        }
        Scan { edits, done: buf.len() }
    }

    /// Reads the URL starting at `i`, if it is one the rules touch.
    fn url_at(&self, direction: Direction, buf: &[u8], i: usize, end: bool) -> Found {
        let scheme_len = buf[i..].iter().take(6).take_while(|b| b.is_ascii_alphabetic()).count();
        let colon = i + scheme_len;
        if colon == buf.len() {
            return if end { Found::Nothing } else { Found::More };
        }
        if buf[colon] != b':' {
            return Found::Nothing;
        }
        let scheme = buf[i..colon].to_ascii_lowercase();
        let (public, loopback) = match scheme.as_slice() {
            b"https" | b"wss" => (true, false),
            b"http" | b"ws" => (false, direction == Direction::Outbound),
            _ => return Found::Nothing,
        };
        if !public && !loopback {
            return Found::Nothing;
        }

        let (slash, authority_start) = match lit(buf, colon + 1, b"//", end) {
            Lit::Yes(at) => ("/", at),
            Lit::More => return Found::More,
            Lit::No => match lit(buf, colon + 1, b"\\/\\/", end) {
                Lit::Yes(at) => ("\\/", at),
                Lit::More => return Found::More,
                Lit::No => return Found::Nothing,
            },
        };

        let authority_len = buf[authority_start..]
            .iter()
            .take(MAX_AUTHORITY + 1)
            .take_while(|b| is_authority_byte(**b))
            .count();
        if authority_len > MAX_AUTHORITY {
            return Found::Nothing;
        }
        let j = authority_start + authority_len;
        if j == buf.len() && !end {
            return Found::More;
        }
        let authority = buf[authority_start..j].to_ascii_lowercase();
        let ours = if public {
            authority == self.host.as_bytes() || authority == format!("{}:443", self.host).as_bytes()
        } else {
            self.loopback.iter().any(|a| a.as_bytes() == authority.as_slice())
        };
        if !ours {
            return Found::Nothing;
        }

        let base_path = if slash == "/" {
            &self.base_path
        } else {
            &self.base_path_escaped
        };
        let prefixed = match lit(buf, j, base_path.as_bytes(), end) {
            Lit::More => return Found::More,
            Lit::No => false,
            Lit::Yes(after) => match buf.get(after) {
                None if !end => return Found::More,
                None => true,
                Some(b) => !is_segment_byte(*b),
            },
        };
        let has_path = match lit(buf, j, slash.as_bytes(), end) {
            Lit::More => return Found::More,
            Lit::Yes(_) => true,
            Lit::No => false,
        };

        let edit = match direction {
            Direction::Outbound => {
                let insert = if has_path && !prefixed { base_path.as_str() } else { "" };
                if loopback {
                    let scheme = if scheme.as_slice() == b"http" { "https" } else { "wss" };
                    Some(Edit {
                        at: i,
                        len: j - i,
                        with: format!("{scheme}:{slash}{slash}{}{insert}", self.host),
                    })
                } else {
                    (!insert.is_empty()).then(|| Edit {
                        at: j,
                        len: 0,
                        with: insert.to_string(),
                    })
                }
            }
            Direction::Inbound => (prefixed && self.path_mode == PathMode::StripWithForwardedPrefix).then(|| {
                let rest_has_path = buf[j + base_path.len()..].starts_with(slash.as_bytes());
                Edit {
                    at: j,
                    len: base_path.len(),
                    with: if rest_has_path {
                        String::new()
                    } else {
                        slash.to_string()
                    },
                }
            }),
        };
        Found::Url { edit, resume: j }
    }
}

/// Compares `literal` (ASCII, case-insensitively) at `at`.
fn lit(buf: &[u8], at: usize, literal: &[u8], end: bool) -> Lit {
    let have = &buf[at.min(buf.len())..];
    let n = have.len().min(literal.len());
    if !have[..n].eq_ignore_ascii_case(&literal[..n]) {
        Lit::No
    } else if n == literal.len() {
        Lit::Yes(at + n)
    } else if end {
        Lit::No
    } else {
        Lit::More
    }
}

/// A byte that can precede a scheme within the same token.
fn is_scheme_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'+' | b'-' | b'.')
}

/// A byte of a host, port or userinfo.
fn is_authority_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b':' | b'[' | b']' | b'@' | b'_' | b'~' | b'%')
}

/// A byte that continues a path segment (so `/t/<botId>x` is another path).
fn is_segment_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~' | b'%')
}

/// Whether a response of this content type is rewritten.
pub fn is_text(headers: &HeaderMap) -> bool {
    let Some(content_type) = headers.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()) else {
        return false;
    };
    let essence = content_type.split(';').next().unwrap_or("").trim().to_ascii_lowercase();
    matches!(
        essence.as_str(),
        "text/html"
            | "text/css"
            | "text/javascript"
            | "application/javascript"
            | "application/json"
            | "text/event-stream"
    ) || essence.ends_with("+json")
}

/// The content codings the link can decode to rewrite a body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Coding {
    Identity,
    Gzip,
    Brotli,
}

impl Coding {
    /// The coding of a response, when the link can read it.
    pub fn of(headers: &HeaderMap) -> Option<Self> {
        let Some(value) = headers.get(header::CONTENT_ENCODING) else {
            return Some(Self::Identity);
        };
        match value.to_str().ok()?.trim().to_ascii_lowercase().as_str() {
            "" | "identity" => Some(Self::Identity),
            "gzip" | "x-gzip" => Some(Self::Gzip),
            "br" => Some(Self::Brotli),
            _ => None,
        }
    }

    /// Narrows a request's `Accept-Encoding` to the codings the link can
    /// decode, so a text response always arrives in one it can rewrite.
    pub fn restrict_accept(headers: &mut HeaderMap) {
        let accepted: Vec<String> = headers
            .get_all(header::ACCEPT_ENCODING)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(','))
            .map(str::trim)
            .filter(|coding| {
                let name = coding.split(';').next().unwrap_or("").trim().to_ascii_lowercase();
                matches!(name.as_str(), "gzip" | "x-gzip" | "br" | "identity")
            })
            .map(str::to_string)
            .collect();
        headers.remove(header::ACCEPT_ENCODING);
        if !accepted.is_empty()
            && let Ok(value) = HeaderValue::try_from(accepted.join(", "))
        {
            headers.insert(header::ACCEPT_ENCODING, value);
        }
    }
}

/// Rewrites a stream of text, one chunk at a time.
pub struct TextStream {
    rewriter: std::sync::Arc<Rewriter>,
    direction: Direction,
    /// The tail of the last chunk that may begin a URL.
    held: Vec<u8>,
    /// The last byte passed on.
    before: Option<u8>,
}

impl TextStream {
    pub fn new(rewriter: std::sync::Arc<Rewriter>, direction: Direction) -> Self {
        Self {
            rewriter,
            direction,
            held: Vec::new(),
            before: None,
        }
    }

    /// Rewrites the next chunk, returning what can be passed on now.
    pub fn push(&mut self, chunk: Bytes) -> Bytes {
        self.process(chunk, false)
    }

    /// Passes on whatever was held back.
    pub fn finish(&mut self) -> Bytes {
        self.process(Bytes::new(), true)
    }

    fn process(&mut self, chunk: Bytes, end: bool) -> Bytes {
        let buf = if self.held.is_empty() {
            chunk
        } else {
            let mut joined = std::mem::take(&mut self.held);
            joined.extend_from_slice(&chunk);
            Bytes::from(joined)
        };
        let scan = self.rewriter.scan(self.direction, &buf, self.before, end);
        self.held = buf[scan.done..].to_vec();
        if scan.done > 0 {
            self.before = Some(buf[scan.done - 1]);
        }
        if scan.edits.is_empty() {
            return buf.slice(..scan.done);
        }
        let mut out = Vec::with_capacity(scan.done + 64);
        let mut from = 0;
        for edit in &scan.edits {
            out.extend_from_slice(&buf[from..edit.at]);
            out.extend_from_slice(edit.with.as_bytes());
            from = edit.at + edit.len;
        }
        out.extend_from_slice(&buf[from..scan.done]);
        Bytes::from(out)
    }
}

/// A decoder and an encoder for one compressed body.
enum Codec {
    Gzip {
        decoder: Box<flate2::write::GzDecoder<Vec<u8>>>,
        encoder: Box<flate2::write::GzEncoder<Vec<u8>>>,
    },
    Brotli {
        decoder: Box<brotli::DecompressorWriter<Vec<u8>>>,
        encoder: Box<brotli::CompressorWriter<Vec<u8>>>,
    },
}

impl Codec {
    fn new(coding: Coding) -> Option<Self> {
        match coding {
            Coding::Identity => None,
            Coding::Gzip => Some(Self::Gzip {
                decoder: Box::new(flate2::write::GzDecoder::new(Vec::new())),
                encoder: Box::new(flate2::write::GzEncoder::new(
                    Vec::new(),
                    flate2::Compression::default(),
                )),
            }),
            Coding::Brotli => Some(Self::Brotli {
                decoder: Box::new(brotli::DecompressorWriter::new(Vec::new(), DECODE_SLICE)),
                encoder: Box::new(brotli::CompressorWriter::new(
                    Vec::new(),
                    DECODE_SLICE,
                    BROTLI_QUALITY,
                    BROTLI_WINDOW,
                )),
            }),
        }
    }

    fn decode(&mut self, input: &[u8]) -> std::io::Result<Vec<u8>> {
        match self {
            Self::Gzip { decoder, .. } => {
                decoder.write_all(input)?;
                decoder.flush()?;
                Ok(std::mem::take(decoder.get_mut()))
            }
            Self::Brotli { decoder, .. } => {
                decoder.write_all(input)?;
                decoder.flush()?;
                Ok(std::mem::take(decoder.get_mut()))
            }
        }
    }

    fn encode(&mut self, input: &[u8]) -> std::io::Result<()> {
        match self {
            Self::Gzip { encoder, .. } => encoder.write_all(input),
            Self::Brotli { encoder, .. } => encoder.write_all(input),
        }
    }

    /// Flushes the encoder, returning the encoded bytes so far.
    fn flush(&mut self) -> std::io::Result<Vec<u8>> {
        match self {
            Self::Gzip { encoder, .. } => {
                encoder.flush()?;
                Ok(std::mem::take(encoder.get_mut()))
            }
            Self::Brotli { encoder, .. } => {
                encoder.flush()?;
                Ok(std::mem::take(encoder.get_mut()))
            }
        }
    }

    /// Ends the decoder, returning the last decoded bytes.
    fn end_decoding(&mut self) -> std::io::Result<Vec<u8>> {
        match self {
            Self::Gzip { decoder, .. } => {
                decoder.try_finish()?;
                Ok(std::mem::take(decoder.get_mut()))
            }
            Self::Brotli { decoder, .. } => {
                decoder.close()?;
                Ok(std::mem::take(decoder.get_mut()))
            }
        }
    }

    /// Ends the encoder, returning the last encoded bytes.
    fn end_encoding(self) -> std::io::Result<Vec<u8>> {
        match self {
            Self::Gzip { encoder, .. } => encoder.finish(),
            Self::Brotli { encoder, .. } => Ok(encoder.into_inner()),
        }
    }
}

/// A response body with its URLs rewritten as it streams.
pub struct RewrittenBody {
    inner: Body,
    text: TextStream,
    codec: Option<Codec>,
    state: BodyState,
}

enum BodyState {
    Streaming,
    /// The inner body ended; these trailers (if any) go out last.
    Trailers(Option<HeaderMap>),
    Done,
}

impl RewrittenBody {
    pub fn new(inner: Body, rewriter: std::sync::Arc<Rewriter>, coding: Coding) -> Self {
        Self {
            inner,
            text: TextStream::new(rewriter, Direction::Outbound),
            codec: Codec::new(coding),
            state: BodyState::Streaming,
        }
    }

    fn chunk(&mut self, data: Bytes) -> std::io::Result<Bytes> {
        let Some(codec) = &mut self.codec else {
            return Ok(self.text.push(data));
        };
        for slice in data.chunks(DECODE_SLICE) {
            let plain = codec.decode(slice)?;
            if !plain.is_empty() {
                codec.encode(&self.text.push(Bytes::from(plain)))?;
            }
        }
        codec.flush().map(Bytes::from)
    }

    fn end(&mut self) -> std::io::Result<Bytes> {
        let Some(mut codec) = self.codec.take() else {
            return Ok(self.text.finish());
        };
        let plain = codec.end_decoding()?;
        codec.encode(&self.text.push(Bytes::from(plain)))?;
        codec.encode(&self.text.finish())?;
        let mut out = codec.flush()?;
        out.extend(codec.end_encoding()?);
        Ok(Bytes::from(out))
    }
}

impl hyper::body::Body for RewrittenBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                BodyState::Done => return Poll::Ready(None),
                BodyState::Trailers(trailers) => {
                    let trailers = trailers.take();
                    this.state = BodyState::Done;
                    if let Some(trailers) = trailers {
                        return Poll::Ready(Some(Ok(Frame::trailers(trailers))));
                    }
                }
                BodyState::Streaming => {
                    let frame = ready!(Pin::new(&mut this.inner).poll_frame(cx));
                    let (out, trailers) = match frame {
                        Some(Err(error)) => return Poll::Ready(Some(Err(error))),
                        Some(Ok(frame)) => match frame.into_data() {
                            Ok(data) => (this.chunk(data), None),
                            Err(frame) => (this.end(), Some(frame.into_trailers().ok())),
                        },
                        None => (this.end(), Some(None)),
                    };
                    if let Some(trailers) = trailers {
                        this.state = BodyState::Trailers(trailers);
                    }
                    match out {
                        Err(error) => {
                            this.state = BodyState::Done;
                            return Poll::Ready(Some(Err(error.into())));
                        }
                        Ok(out) if !out.is_empty() => return Poll::Ready(Some(Ok(Frame::data(out)))),
                        Ok(_) => {}
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BOT: &str = "/t/5a137883-49df-4063-b52e-00c1eea1f535";

    fn rewriter(path_mode: PathMode) -> Rewriter {
        Rewriter::new("https://neboai.com", BOT, 28790, path_mode)
    }

    fn out(text: &str) -> String {
        rewriter(PathMode::ReaddPrefix)
            .rewrite(Direction::Outbound, text)
            .into_owned()
    }

    #[test]
    fn public_urls_get_the_prefix() {
        for (from, to) in [
            (
                r#"{"canvas":"https://neboai.com:443/__openclaw__/cap/tok"}"#,
                r#"{"canvas":"https://neboai.com:443/t/5a137883-49df-4063-b52e-00c1eea1f535/__openclaw__/cap/tok"}"#,
            ),
            (
                "<a href=\"https://neboai.com/chat?x=1\">",
                "<a href=\"https://neboai.com/t/5a137883-49df-4063-b52e-00c1eea1f535/chat?x=1\">",
            ),
            (
                "wss://NeboAI.com/ws",
                "wss://NeboAI.com/t/5a137883-49df-4063-b52e-00c1eea1f535/ws",
            ),
            (
                "https://neboai.com/",
                "https://neboai.com/t/5a137883-49df-4063-b52e-00c1eea1f535/",
            ),
        ] {
            assert_eq!(out(from), to);
        }
    }

    #[test]
    fn prefixed_bare_and_foreign_urls_are_untouched() {
        for text in [
            "https://neboai.com/t/5a137883-49df-4063-b52e-00c1eea1f535/chat",
            "https://neboai.com/t/5a137883-49df-4063-b52e-00c1eea1f535",
            "https://neboai.com/t/5a137883-49df-4063-b52e-00c1eea1f535?x",
            r#"{"allowedOrigins":["https://neboai.com"]}"#,
            "https://api.neboai.com/v1/x",
            "https://neboai.com.evil.example/x",
            "https://neboai.com@evil.example/x",
            "https://neboai.com:8443/x",
            "http://neboai.com/x",
            "https://example.com/neboai.com/x",
            "xhttps://neboai.com/x",
            "http://127.0.0.1:18789/x",
            "https://127.0.0.1:28790/x",
            "no urls at all",
        ] {
            assert_eq!(out(text), text, "{text}");
        }
        // Another path under /t/ that is not this bot's prefix.
        assert_eq!(
            out("https://neboai.com/t/5a137883-49df-4063-b52e-00c1eea1f535x/y"),
            "https://neboai.com/t/5a137883-49df-4063-b52e-00c1eea1f535/t/5a137883-49df-4063-b52e-00c1eea1f535x/y"
        );
    }

    #[test]
    fn loopback_urls_become_public() {
        for (from, to) in [
            (
                "http://127.0.0.1:28790/__openclaw__/cap/tok",
                "https://neboai.com/t/5a137883-49df-4063-b52e-00c1eea1f535/__openclaw__/cap/tok",
            ),
            (
                "ws://localhost:28790/t/5a137883-49df-4063-b52e-00c1eea1f535/",
                "wss://neboai.com/t/5a137883-49df-4063-b52e-00c1eea1f535/",
            ),
            (
                "http://[::1]:28790/x",
                "https://neboai.com/t/5a137883-49df-4063-b52e-00c1eea1f535/x",
            ),
            ("\"http://127.0.0.1:28790\"", "\"https://neboai.com\""),
        ] {
            assert_eq!(out(from), to);
        }
    }

    #[test]
    fn json_escaped_slashes_are_matched_and_kept() {
        assert_eq!(
            out(r#"{"u":"https:\/\/neboai.com\/__openclaw__\/cap\/tok"}"#),
            r#"{"u":"https:\/\/neboai.com\/t\/5a137883-49df-4063-b52e-00c1eea1f535\/__openclaw__\/cap\/tok"}"#
        );
        assert_eq!(
            out(r#""http:\/\/127.0.0.1:28790\/x""#),
            r#""https:\/\/neboai.com\/t\/5a137883-49df-4063-b52e-00c1eea1f535\/x""#
        );
        let prefixed = r#""https:\/\/neboai.com\/t\/5a137883-49df-4063-b52e-00c1eea1f535\/x""#;
        assert_eq!(out(prefixed), prefixed);
    }

    #[test]
    fn inbound_follows_the_path_mode() {
        let url = "go to https://neboai.com/t/5a137883-49df-4063-b52e-00c1eea1f535/x?y and https://neboai.com/t/5a137883-49df-4063-b52e-00c1eea1f535";
        let readd = rewriter(PathMode::ReaddPrefix);
        assert_eq!(readd.rewrite(Direction::Inbound, url), url);
        let strip = rewriter(PathMode::StripWithForwardedPrefix);
        assert_eq!(
            strip.rewrite(Direction::Inbound, url),
            "go to https://neboai.com/x?y and https://neboai.com/"
        );
        // Inbound never touches unprefixed or loopback URLs.
        for text in ["https://neboai.com/x", "http://127.0.0.1:28790/x"] {
            assert_eq!(strip.rewrite(Direction::Inbound, text), text);
        }
    }

    /// Every split point of a text gives the same result as the whole text,
    /// and bytes are only held back while they could begin a URL.
    #[test]
    fn chunk_boundaries_never_change_the_result() {
        let rw = std::sync::Arc::new(rewriter(PathMode::ReaddPrefix));
        let text = concat!(
            "data: {\"a\":\"https://neboai.com:443/__openclaw__/cap/tok\",",
            "\"b\":\"https:\\/\\/neboai.com\\/x\",\"c\":\"http://127.0.0.1:28790/y\",",
            "\"d\":\"https://neboai.com/t/5a137883-49df-4063-b52e-00c1eea1f535/z\",\"e\":\"https://neboai.com\"}\n\n"
        );
        let whole = rw.rewrite(Direction::Outbound, text).into_owned();
        for a in 0..text.len() {
            for b in [a, (a + 7).min(text.len()), (a + 40).min(text.len())] {
                let mut stream = TextStream::new(rw.clone(), Direction::Outbound);
                let mut got = Vec::new();
                for part in [&text[..a], &text[a..b], &text[b..]] {
                    got.extend_from_slice(&stream.push(Bytes::copy_from_slice(part.as_bytes())));
                }
                got.extend_from_slice(&stream.finish());
                assert_eq!(String::from_utf8(got).unwrap(), whole, "split at {a}, {b}");
            }
        }
    }

    #[test]
    fn only_a_possible_url_is_held_back() {
        let rw = std::sync::Arc::new(rewriter(PathMode::ReaddPrefix));
        let mut stream = TextStream::new(rw, Direction::Outbound);
        assert_eq!(stream.push(Bytes::from_static(b"data: hello\n\n")), "data: hello\n\n");
        assert_eq!(stream.push(Bytes::from_static(b"data: https://nebo")), "data: ");
        assert_eq!(
            stream.push(Bytes::from_static(b"ai.com/x\n\n")),
            "https://neboai.com/t/5a137883-49df-4063-b52e-00c1eea1f535/x\n\n"
        );
        assert_eq!(
            stream.push(Bytes::from_static(b"https://example.com/")),
            "https://example.com/"
        );
        assert_eq!(stream.finish(), "");
    }

    #[test]
    fn locations_cookies_and_refresh() {
        let rw = rewriter(PathMode::StripWithForwardedPrefix);
        assert_eq!(rw.location("/login?next=/"), format!("{BOT}/login?next=/"));
        assert_eq!(rw.location(&format!("{BOT}/login")), format!("{BOT}/login"));
        assert_eq!(rw.location("//cdn.example/x"), "//cdn.example/x");
        assert_eq!(
            rw.location("https://neboai.com/login"),
            format!("https://neboai.com{BOT}/login")
        );
        assert_eq!(
            rw.location("http://127.0.0.1:28790/"),
            format!("https://neboai.com{BOT}/")
        );
        assert_eq!(rw.refresh("0; url=/next"), format!("0; url={BOT}/next"));
        assert_eq!(rw.refresh("0; URL='/next'"), format!("0; URL='{BOT}/next'"));
        assert_eq!(rw.refresh("5"), "5");
        assert_eq!(
            rw.set_cookie("sid=a; Path=/; HttpOnly"),
            format!("sid=a; Path={BOT}; HttpOnly")
        );
        assert_eq!(rw.set_cookie("sid=a; path=/api"), format!("sid=a; path={BOT}/api"));
        assert_eq!(
            rw.set_cookie(&format!("sid=a; Path={BOT}")),
            format!("sid=a; Path={BOT}")
        );
        assert_eq!(rw.set_cookie("path=/; Secure"), "path=/; Secure");
        assert_eq!(rw.set_cookie("sid=a"), "sid=a");
    }

    #[test]
    fn accept_encoding_is_narrowed_to_what_the_link_reads() {
        let mut h = HeaderMap::new();
        h.insert(
            header::ACCEPT_ENCODING,
            HeaderValue::from_static("gzip, deflate, br;q=0.9, zstd"),
        );
        Coding::restrict_accept(&mut h);
        assert_eq!(h.get(header::ACCEPT_ENCODING).unwrap(), "gzip, br;q=0.9");
        h.insert(header::ACCEPT_ENCODING, HeaderValue::from_static("zstd"));
        Coding::restrict_accept(&mut h);
        assert!(h.get(header::ACCEPT_ENCODING).is_none());
    }

    fn body_of(chunks: Vec<&'static [u8]>) -> Body {
        use http_body_util::BodyExt;
        let frames = futures::stream::iter(
            chunks
                .into_iter()
                .map(|c| Ok::<_, BoxError>(Frame::data(Bytes::from_static(c)))),
        );
        http_body_util::StreamBody::new(frames).boxed()
    }

    async fn collect(body: RewrittenBody) -> Vec<u8> {
        use http_body_util::BodyExt;
        body.collect().await.unwrap().to_bytes().to_vec()
    }

    const PAGE: &str = "<a href=\"https://neboai.com/a\">a</a><img src=\"http://localhost:28790/b.png\">";
    const PAGE_OUT: &str = "<a href=\"https://neboai.com/t/5a137883-49df-4063-b52e-00c1eea1f535/a\">a</a><img src=\"https://neboai.com/t/5a137883-49df-4063-b52e-00c1eea1f535/b.png\">";

    #[tokio::test]
    async fn compressed_bodies_are_decoded_rewritten_and_encoded_again() {
        let rw = std::sync::Arc::new(rewriter(PathMode::ReaddPrefix));

        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(PAGE.as_bytes()).unwrap();
        let gz: &'static [u8] = gz.finish().unwrap().leak();
        let (a, b) = gz.split_at(gz.len() / 2);
        let got = collect(RewrittenBody::new(body_of(vec![a, b]), rw.clone(), Coding::Gzip)).await;
        let mut plain = String::new();
        std::io::Read::read_to_string(&mut flate2::read::GzDecoder::new(got.as_slice()), &mut plain).unwrap();
        assert_eq!(plain, PAGE_OUT);

        let mut br = brotli::CompressorWriter::new(Vec::new(), 4096, 9, 22);
        br.write_all(PAGE.as_bytes()).unwrap();
        let br: &'static [u8] = br.into_inner().leak();
        let (a, b) = br.split_at(br.len() / 3);
        let got = collect(RewrittenBody::new(body_of(vec![a, b]), rw.clone(), Coding::Brotli)).await;
        let mut plain = String::new();
        std::io::Read::read_to_string(&mut brotli::Decompressor::new(got.as_slice(), 4096), &mut plain).unwrap();
        assert_eq!(plain, PAGE_OUT);

        let got = collect(RewrittenBody::new(body_of(vec![PAGE.as_bytes()]), rw, Coding::Identity)).await;
        assert_eq!(String::from_utf8(got).unwrap(), PAGE_OUT);
    }
}
