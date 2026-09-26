//! Server-sent events as the Hermes API server writes them.
//!
//! One frame is `id: <seq>\ndata: <json>\n\n` (`api_server.py` `_sse_frame`;
//! the run stream never sets an `event:` line, the event name is the JSON's
//! `event` field). Comment frames carry the stream state: `: open` once the
//! head is flushed, `: keepalive` every idle interval, and `: stream closed`
//! after the terminal event (`api_server_runs.py` `_handle_run_events`).

use bytes::BytesMut;

/// One parsed frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Frame {
    /// A `data:` frame with its `id:` when the server sent one.
    Data { id: Option<u64>, data: String },
    /// A comment line (`: keepalive`), without the leading colon and space.
    Comment(String),
}

/// Splits a byte stream into frames at blank lines.
#[derive(Debug, Default)]
pub(super) struct Parser {
    buffer: BytesMut,
}

impl Parser {
    pub fn push(&mut self, chunk: &[u8]) {
        self.buffer.extend_from_slice(chunk);
    }

    /// The next complete frame, if the buffer holds one.
    pub fn next_frame(&mut self) -> Option<Frame> {
        loop {
            let end = find_blank_line(&self.buffer)?;
            let raw = self.buffer.split_to(end.end);
            let text = String::from_utf8_lossy(&raw[..end.start]);
            if let Some(frame) = parse_frame(&text) {
                return Some(frame);
            }
        }
    }
}

/// The byte range of the first blank line (`\n\n` or `\r\n\r\n`): where the
/// frame's text ends and where the next frame starts.
fn find_blank_line(buffer: &[u8]) -> Option<std::ops::Range<usize>> {
    let mut i = 0;
    while i < buffer.len() {
        if buffer[i] != b'\n' {
            i += 1;
            continue;
        }
        let rest = &buffer[i + 1..];
        if rest.starts_with(b"\n") {
            return Some(i..i + 2);
        }
        if rest.starts_with(b"\r\n") {
            let start = if i > 0 && buffer[i - 1] == b'\r' { i - 1 } else { i };
            return Some(start..i + 3);
        }
        i += 1;
    }
    None
}

fn parse_frame(text: &str) -> Option<Frame> {
    let mut id = None;
    let mut data: Option<String> = None;
    let mut comment = None;
    for line in text.lines() {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if let Some(rest) = line.strip_prefix(':') {
            comment = Some(rest.strip_prefix(' ').unwrap_or(rest).to_owned());
            continue;
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "id" => id = value.trim().parse().ok(),
            "data" => {
                let data = data.get_or_insert_with(String::new);
                if !data.is_empty() {
                    data.push('\n');
                }
                data.push_str(value);
            }
            _ => {}
        }
    }
    match (data, comment) {
        (Some(data), _) => Some(Frame::Data { id, data }),
        (None, Some(comment)) => Some(Frame::Comment(comment)),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_arrive_in_any_chunking() {
        let text = b": open\n\nid: 0\ndata: {\"a\":1}\n\nid: 1\r\ndata: {\"b\":2}\r\n\r\n: keepalive\n\n";
        for size in [1, 3, 7, 64] {
            let mut parser = Parser::default();
            let mut frames = Vec::new();
            for chunk in text.chunks(size) {
                parser.push(chunk);
                while let Some(frame) = parser.next_frame() {
                    frames.push(frame);
                }
            }
            assert_eq!(
                frames,
                vec![
                    Frame::Comment("open".into()),
                    Frame::Data {
                        id: Some(0),
                        data: "{\"a\":1}".into()
                    },
                    Frame::Data {
                        id: Some(1),
                        data: "{\"b\":2}".into()
                    },
                    Frame::Comment("keepalive".into()),
                ],
                "chunk size {size}"
            );
        }
    }

    #[test]
    fn multi_line_data_and_missing_id() {
        let mut parser = Parser::default();
        parser.push(b"data: a\ndata: b\n\ndata:{}\n\n");
        assert_eq!(
            parser.next_frame(),
            Some(Frame::Data {
                id: None,
                data: "a\nb".into()
            })
        );
        assert_eq!(
            parser.next_frame(),
            Some(Frame::Data {
                id: None,
                data: "{}".into()
            })
        );
        assert_eq!(parser.next_frame(), None);
    }
}
