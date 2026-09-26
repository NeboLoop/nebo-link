// The transport, in two layers, so that end-to-end encryption (spec section
// 17) drops in without changing the client API:
//
//   Dialer         opens a Socket: WebSocket messages, text or binary.
//   SecureChannel  turns a Socket into a FrameChannel: whole OAL frames.
//
// `plaintext` sends each frame as one JSON text message (OAL 0.1) and leaves
// authentication to `host/hello`. An end-to-end channel (`encrypted` in
// e2e.ts, Noise IK, section 17.2) does its handshake in `open`,
// authenticates the device by its static key, and reports that in
// `FrameChannel.authenticated`; the client then skips `host/hello`.

import { OALError } from './errors.js';
import type { ClientInfo, Frame, VersionRange } from './types.js';

/** How a socket or channel closed. */
export interface CloseInfo {
  code: number;
  reason: string;
}

/** Thrown by `recv()` once the socket or channel has closed. */
export class ChannelClosed extends Error {
  override readonly name = 'ChannelClosed';
  constructor(readonly code: number, readonly reason: string) {
    super(`Closed with ${code}${reason ? `: ${reason}` : ''}`);
  }
}

/** An open WebSocket, message by message. */
export interface Socket {
  send(data: string | Uint8Array): void;
  /** The next message. Rejects with `ChannelClosed` once the socket has closed. */
  recv(): Promise<string | Uint8Array>;
  close(code?: number, reason?: string): void;
}

/** Opens a socket to `url`, offering `protocols` (OAL offers `oal`). */
export type Dialer = (url: string, protocols: string[]) => Promise<Socket>;

/** What a secure channel knows when it opens. */
export interface ChannelContext {
  protocol: VersionRange;
  client: ClientInfo;
  /** The host's id and static public key, known once paired. */
  host?: { id: string; publicKey: string };
  /** This device's X25519 key pair (base64url), and its id once paired. */
  device?: { id?: string; publicKey: string; privateKey: string };
  /** The pairing code, when this connection pairs. */
  code?: string;
}

/** A connection that carries whole OAL frames. */
export interface FrameChannel {
  send(frame: Frame): void;
  /** The next frame. Rejects with `ChannelClosed` once the channel has closed. */
  recv(): Promise<Frame>;
  close(code?: number, reason?: string): void;
  /**
   * Set when the channel authenticated the device itself (an end-to-end
   * handshake). The client then sends no `host/hello`.
   */
  readonly authenticated?: { protocol: string; device: { id: string; name?: string } };
  /**
   * The peer's static public key (base64url), when the channel's handshake
   * authenticated it. A pairing checks it against `info.host.publicKey`.
   */
  readonly peerKey?: string;
}

/** Turns a socket into a frame channel. See `plaintext`. */
export interface SecureChannel {
  open(socket: Socket, context: ChannelContext): Promise<FrameChannel>;
}

/** OAL 0.1: one frame per JSON text message, authenticated by `host/hello`. */
export const plaintext: SecureChannel = {
  async open(socket) {
    return {
      send: (frame) => socket.send(JSON.stringify(frame)),
      async recv() {
        for (;;) {
          const data = await socket.recv();
          if (typeof data !== 'string') continue; // Binary is reserved for encryption.
          try {
            const frame: unknown = JSON.parse(data);
            if (frame && typeof frame === 'object' && !Array.isArray(frame)) return frame as Frame;
          } catch {
            // Not JSON: the spec says to drop it.
          }
        }
      },
      close: (code, reason) => socket.close(code, reason),
    };
  },
};

/** Dials with the platform `WebSocket` (browsers, Node 22 and later). */
export const webSocketDialer: Dialer = (url, protocols) =>
  new Promise((resolve, reject) => {
    let ws: WebSocket;
    try {
      ws = new WebSocket(url, protocols);
    } catch {
      reject(new OALError('invalid_params', `That isn't a WebSocket URL: ${url}`));
      return;
    }
    ws.binaryType = 'arraybuffer';
    const inbox: (string | Uint8Array)[] = [];
    const waiters: { resolve: (m: string | Uint8Array) => void; reject: (e: Error) => void }[] = [];
    let closed: ChannelClosed | undefined;
    let opened = false;

    ws.addEventListener('open', () => {
      opened = true;
      resolve({
        send: (data) => ws.send(data),
        recv: () => {
          const next = inbox.shift();
          if (next !== undefined) return Promise.resolve(next);
          if (closed) return Promise.reject(closed);
          return new Promise((res, rej) => waiters.push({ resolve: res, reject: rej }));
        },
        close: (code = 1000, reason = '') => {
          if (ws.readyState === WebSocket.CLOSING || ws.readyState === WebSocket.CLOSED) return;
          // The platform WebSocket sends only 1000 and 3000-4999; any other
          // code (1002, 1009) goes as 1000 with its reason.
          ws.close(code === 1000 || (code >= 3000 && code <= 4999) ? code : 1000, reason);
        },
      });
    });
    ws.addEventListener('message', (event: MessageEvent) => {
      const data = typeof event.data === 'string' ? event.data : new Uint8Array(event.data as ArrayBuffer);
      const waiter = waiters.shift();
      if (waiter) waiter.resolve(data);
      else inbox.push(data);
    });
    ws.addEventListener('close', (event: CloseEvent) => {
      closed = new ChannelClosed(event.code, event.reason);
      for (const waiter of waiters.splice(0)) waiter.reject(closed);
      if (!opened) reject(new OALError('host_offline', `Couldn't reach ${new URL(url).host}.`));
    });
  });
