// One host's connection: dial, handshake, heartbeat, reconnect, and routing
// JSON-RPC between the host channel and the agent channels (spec sections
// 4, 5, 11 and 12).

import { ChannelClosed, type ChannelContext, type Dialer, type FrameChannel, type SecureChannel } from './channel.js';
import { OALError } from './errors.js';
import type { ClientInfo, Frame, HostInfo, RpcError } from './types.js';
import { PROTOCOL } from './types.js';

/** A client sends `host/ping` after this long without sending anything. */
const PING_AFTER = 20_000;
/** ...and gives up on the connection when the ping has no answer by then. */
const PING_TIMEOUT = 10_000;
/** Requests wait this long for a reconnect before failing with `host_offline`. */
const RECONNECT_GRACE = 10_000;
const BACKOFF_START = 1_000;
const BACKOFF_MAX = 30_000;

type Json = Record<string, any>;

export interface LinkHandlers {
  /** A notification on the host channel. */
  hostNotification(method: string, params: Json): void;
  /** A request or notification on an agent channel. */
  agentMessage(agent: string, message: Json): void;
  /**
   * The connection is authenticated. Runs before the link reports ready;
   * requests made here pass `{ early: true }`.
   */
  connected(): Promise<void>;
  disconnected(): void;
}

export interface LinkOptions {
  url: string;
  hostName: string;
  context: ChannelContext;
  auth: Json;
  secure: SecureChannel;
  dialer: Dialer;
}

interface Waiter {
  resolve(result: any): void;
  reject(error: OALError): void;
}

interface Deferred<T> {
  promise: Promise<T>;
  resolve(value: T): void;
  reject(error: unknown): void;
}

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  promise.catch(() => {});
  return { promise, resolve, reject };
}

/** Errors that no reconnect can fix. */
function permanent(error: unknown): error is OALError {
  return (
    error instanceof OALError &&
    ['unauthenticated', 'version_mismatch', 'unpaired', 'pairing_refused', 'invalid_params', 'closed'].includes(error.code)
  );
}

export function closeError(close: { code: number; reason: string }, hostName: string): OALError {
  switch (close.code) {
    case 4001:
      return new OALError('unauthenticated', `This device isn't paired with ${hostName}. Pair it again.`);
    case 4002:
      return new OALError('version_mismatch', `This app and ${hostName} don't speak the same OAL version.`);
    case 4003:
      return new OALError('unpaired', `This device was unpaired from ${hostName}. Pair it again.`);
    default:
      return new OALError('connection_lost', `Lost the connection to ${hostName}.`);
  }
}

/**
 * Sends one request on a channel that is not being pumped yet and reads
 * frames until its answer arrives. Used for the first request on a
 * connection (`host/hello`, `host/pair`).
 */
export async function exchange(channel: FrameChannel, method: string, params: Json, hostName: string): Promise<Json> {
  channel.send({ jsonrpc: '2.0', id: 0, method, params });
  for (;;) {
    let frame: Frame;
    try {
      frame = await channel.recv();
    } catch (error) {
      if (error instanceof ChannelClosed) throw closeError(error, hostName);
      throw error;
    }
    if (frame.id === 0 && !('method' in frame) && !('agent' in frame)) {
      if (frame.error) throw OALError.fromRpc(frame.error as RpcError);
      return frame.result as Json;
    }
  }
}

export class Link {
  info?: HostInfo;
  device?: { id: string; name: string };
  online = false;
  handlers!: LinkHandlers;

  private channel?: FrameChannel;
  private waiters = new Map<string, Waiter>();
  private nextId = 0;
  private lastSent = 0;
  private ready = deferred<void>();
  private failure?: OALError;
  private closing = false;
  private wake?: () => void;
  private heartbeat?: ReturnType<typeof setInterval>;

  constructor(private readonly options: LinkOptions) {}

  /** Starts connecting. Resolves after the first attempt; rejects if it can never succeed. */
  start(): Promise<void> {
    const first = deferred<void>();
    void this.run(first);
    return first.promise;
  }

  close(): void {
    this.closing = true;
    this.fail(new OALError('closed', 'The client was closed.'));
    this.channel?.close(1000, '');
    this.wake?.();
  }

  /** A request on the host channel, or on `agent`'s channel. */
  async request(method: string, params: Json, options: { agent?: string; early?: boolean } = {}): Promise<any> {
    if (!options.early) await this.whenReady();
    const channel = this.channel;
    if (!channel) throw this.lost();
    const id = ++this.nextId;
    const acp = { jsonrpc: '2.0', id, method, params };
    const answer = new Promise<any>((resolve, reject) => this.waiters.set(`${options.agent ?? ''}#${id}`, { resolve, reject }));
    this.send(channel, options.agent === undefined ? acp : { agent: options.agent, acp });
    return answer;
  }

  /** A notification on `agent`'s channel. */
  async notify(agent: string, method: string, params: Json): Promise<void> {
    await this.whenReady();
    const channel = this.channel;
    if (!channel) throw this.lost();
    this.send(channel, { agent, acp: { jsonrpc: '2.0', method, params } });
  }

  /** Answers a request the host sent on `agent`'s channel. */
  respond(agent: string, id: unknown, answer: { result: unknown } | { error: RpcError }): void {
    if (this.channel) this.send(this.channel, { agent, acp: { jsonrpc: '2.0', id, ...answer } });
  }

  /** Waits for the connection, up to the reconnect grace period. */
  async whenReady(): Promise<void> {
    if (this.failure) throw this.failure;
    if (this.online) return;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new OALError('host_offline', `${this.options.hostName} is offline.`)), RECONNECT_GRACE);
    });
    try {
      await Promise.race([this.ready.promise, timeout]);
    } finally {
      clearTimeout(timer);
    }
  }

  private lost(): OALError {
    return this.failure ?? new OALError('connection_lost', `Lost the connection to ${this.options.hostName}.`);
  }

  private send(channel: FrameChannel, frame: Frame): void {
    this.lastSent = Date.now();
    channel.send(frame);
  }

  private fail(error: OALError): void {
    this.failure ??= error;
    this.ready.reject(this.failure);
    this.rejectWaiters(this.failure);
  }

  private rejectWaiters(error: OALError): void {
    const waiters = [...this.waiters.values()];
    this.waiters.clear();
    for (const waiter of waiters) waiter.reject(error);
  }

  private async run(first: Deferred<void>): Promise<void> {
    let delay = BACKOFF_START;
    while (!this.closing) {
      let channel: FrameChannel;
      try {
        channel = await this.open();
      } catch (error) {
        if (permanent(error)) {
          this.fail(error);
          first.reject(error);
          return;
        }
        first.resolve();
        await this.sleep(delay);
        delay = Math.min(delay * 2, BACKOFF_MAX);
        continue;
      }
      delay = BACKOFF_START;
      this.channel = channel;
      const closed = this.pump(channel);
      this.startHeartbeat(channel);
      try {
        await this.handlers.connected();
      } catch {
        // A failed resume shows up on the sessions it concerns.
      }
      if (this.channel === channel) {
        this.online = true;
        this.ready.resolve();
      }
      first.resolve();

      const close = await closed;
      clearInterval(this.heartbeat);
      this.channel = undefined;
      this.online = false;
      this.ready = deferred<void>();
      const error = closeError(close, this.options.hostName);
      this.rejectWaiters(error);
      this.handlers.disconnected();
      if (permanent(error)) {
        this.fail(error);
        return;
      }
      await this.sleep(delay);
    }
  }

  private async open(): Promise<FrameChannel> {
    const { url, hostName, context, auth, secure, dialer } = this.options;
    const socket = await dialer(url, ['oal']).catch((error: unknown) => {
      throw error instanceof OALError ? new OALError(error.code, `Couldn't reach ${hostName}.`) : error;
    });
    const channel = await secure.open(socket, context).catch((error: unknown) => {
      throw error instanceof ChannelClosed ? closeError(error, hostName) : error;
    });
    try {
      if (channel.authenticated) {
        const device = channel.authenticated.device;
        this.device = { id: device.id, name: device.name ?? '' };
        this.info = (await exchange(channel, 'host/info', {}, hostName)) as HostInfo;
      } else {
        const result = await exchange(channel, 'host/hello', { protocol: PROTOCOL, client: context.client, auth }, hostName);
        this.device = result.device;
        this.info = result.info as HostInfo;
      }
    } catch (error) {
      channel.close(1000, '');
      throw error;
    }
    return channel;
  }

  private async pump(channel: FrameChannel): Promise<{ code: number; reason: string }> {
    for (;;) {
      let frame: Frame;
      try {
        frame = await channel.recv();
      } catch (error) {
        return error instanceof ChannelClosed ? { code: error.code, reason: error.reason } : { code: 1006, reason: String(error) };
      }
      try {
        this.dispatch(frame);
      } catch {
        // One bad frame never takes the connection down.
      }
    }
  }

  private dispatch(frame: Frame): void {
    if (typeof frame.agent === 'string') {
      const message = frame.acp as Json | undefined;
      if (!message || typeof message !== 'object') return;
      if ('method' in message) this.handlers.agentMessage(frame.agent, message);
      else this.settle(`${frame.agent}#${message.id}`, message);
    } else if ('method' in frame) {
      if (!('id' in frame)) this.handlers.hostNotification(frame.method as string, (frame.params as Json) ?? {});
    } else if ('id' in frame) {
      this.settle(`#${frame.id}`, frame);
    }
  }

  private settle(key: string, message: Json): void {
    const waiter = this.waiters.get(key);
    if (!waiter) return;
    this.waiters.delete(key);
    if (message.error) waiter.reject(OALError.fromRpc(message.error));
    else waiter.resolve(message.result);
  }

  private startHeartbeat(channel: FrameChannel): void {
    clearInterval(this.heartbeat);
    this.lastSent = Date.now();
    let pinging = false;
    this.heartbeat = setInterval(() => {
      if (pinging || Date.now() - this.lastSent < PING_AFTER) return;
      pinging = true;
      const timer = setTimeout(() => channel.close(4000, 'No answer to ping.'), PING_TIMEOUT);
      this.request('host/ping', {}, { early: true })
        .catch(() => {})
        .finally(() => {
          clearTimeout(timer);
          pinging = false;
        });
    }, 1_000);
    (this.heartbeat as { unref?: () => void }).unref?.();
  }

  /** Waits `ms` with jitter, or until the link is closed. */
  private sleep(ms: number): Promise<void> {
    const jittered = ms / 2 + Math.random() * (ms / 2);
    return new Promise((resolve) => {
      const timer = setTimeout(resolve, jittered);
      this.wake = () => {
        clearTimeout(timer);
        resolve();
      };
    });
  }
}
