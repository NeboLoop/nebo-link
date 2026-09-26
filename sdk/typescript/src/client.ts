// The client API: Client → Host → Agent → Session → Turn.

import { webSocketDialer, type Dialer, type SecureChannel } from './channel.js';
import { Link } from './connection.js';
import { encrypted } from './e2e.js';
import { OALError } from './errors.js';
import { DEFAULT_CLIENT, checkEndpoint, relayUrl, type Endpoint, type Identity } from './identity.js';
import { Queue } from './queue.js';
import { relayDialer } from './relay.js';
import {
  PROTOCOL,
  type AgentInfo,
  type Attachment,
  type ClientInfo,
  type Device,
  type DeviceRef,
  type HostInfo,
  type PendingRequestInfo,
  type PermissionOption,
  type PermissionOutcome,
  type PlanEntry,
  type SessionModeState,
  type SessionSummary,
  type StopReason,
  type ToolCall,
  type Usage,
} from './types.js';

type Json = Record<string, any>;

/** What a turn streams, in order. A turn ends with `done` or `error`. */
export type TurnEvent =
  | { type: 'text'; text: string }
  | { type: 'thinking'; text: string }
  /** A prompt from another device (or, in `history`, any earlier prompt). */
  | { type: 'user'; text: string }
  | { type: 'tool_start'; tool: ToolCall }
  | { type: 'tool_update'; tool: ToolCall }
  /** The tool call finished: `tool.status` is `completed` or `failed`. `output` is its text. */
  | { type: 'tool_result'; tool: ToolCall; output: string }
  | { type: 'permission'; request: PermissionRequest }
  /** A permission request was answered, here or on another device. */
  | { type: 'permission_resolved'; request: PermissionRequest; outcome: PermissionOutcome | null; answeredBy: DeviceRef | null }
  | { type: 'plan'; entries: PlanEntry[] }
  | { type: 'mode'; modeId: string }
  | { type: 'usage'; usage: Usage }
  /**
   * The turn finished. `stopReason` is null when it finished while this
   * client was disconnected, so its outcome wasn't seen.
   */
  | { type: 'done'; stopReason: StopReason | null }
  | { type: 'error'; error: OALError }
  /** An ACP session update this SDK has no event for, unchanged. */
  | { type: 'update'; update: Json };

/** What `Host.updates()` streams. */
export type HostUpdate =
  | { type: 'presence'; online: boolean }
  | { type: 'agent'; change: 'added' | 'updated' | 'removed'; agent: Agent }
  | { type: 'permission'; request: PermissionRequest }
  | { type: 'permission_resolved'; request: PermissionRequest; outcome: PermissionOutcome | null; answeredBy: DeviceRef | null };

export type ConnectOptions = Endpoint & {
  /** One identity from `pair`, or several (one per host) with a relay. */
  credentials: Identity | Identity[];
  client?: ClientInfo;
  /** The secure-channel layer. Default: `encrypted` (end-to-end, OAL section 17). */
  secure?: SecureChannel;
  /** Opens sockets. Default: the platform `WebSocket`. */
  dialer?: Dialer;
};

/** Connects to every host in `credentials`. Hosts that are offline keep retrying in the background. */
export async function connect(options: ConnectOptions): Promise<Client> {
  checkEndpoint(options);
  const identities = Array.isArray(options.credentials) ? options.credentials : [options.credentials];
  if (options.url && identities.length !== 1) {
    throw new OALError('invalid_params', 'A url reaches one host. Pass a relay to reach several.');
  }
  const dialer = options.dialer ?? webSocketDialer;
  const client = new Client(
    identities.map(
      (identity) =>
        new Host(identity, {
          url: options.url ?? relayUrl(options.relay!, `/oal/hosts/${encodeURIComponent(identity.host.id)}`),
          client: options.client ?? DEFAULT_CLIENT,
          secure: options.secure ?? encrypted,
          // Through a relay, every connection first proves this device's key.
          dialer: options.relay ? relayDialer(options.relay, identity.device, dialer) : dialer,
        }),
    ),
  );
  try {
    await Promise.all(client.hosts().map((host) => host._link.start()));
  } catch (error) {
    client.close();
    throw error;
  }
  return client;
}

export class Client {
  constructor(private readonly hostList: Host[]) {}

  /** The paired hosts. `host.online` says which are reachable now. */
  hosts(): Host[] {
    return [...this.hostList];
  }

  close(): void {
    for (const host of this.hostList) host._close();
  }
}

const key = (...parts: string[]) => parts.join('\u0000');

export class Host {
  readonly id: string;
  readonly name: string;
  /** @internal */
  readonly _link: Link;
  /** @internal */
  readonly _client: ClientInfo;

  private agentsById = new Map<string, Agent>();
  private sessions = new Map<string, Session>();
  private requests = new Map<string, PermissionRequest>();
  private initialized = new Map<string, Promise<void>>();
  private subscribers = new Set<Queue<HostUpdate>>();

  /** @internal */
  constructor(identity: Identity, options: { url: string; client: ClientInfo; secure: SecureChannel; dialer: Dialer }) {
    this.id = identity.host.id;
    this.name = identity.host.name;
    this._client = options.client;
    this._link = new Link({
      url: options.url,
      hostName: identity.host.name,
      context: { protocol: PROTOCOL, client: options.client, host: identity.host, device: identity.device },
      auth: { type: 'device', deviceId: identity.device.id, token: identity.device.token },
      secure: options.secure,
      dialer: options.dialer,
    });
    this._link.handlers = {
      hostNotification: (method, params) => this.hostNotification(method, params),
      agentMessage: (agent, message) => this.agentMessage(agent, message),
      connected: () => this.connected(),
      disconnected: () => this.disconnected(),
    };
  }

  get online(): boolean {
    return this._link.online;
  }

  /** `host/info`, as of the last connection. */
  get info(): HostInfo | undefined {
    return this._link.info;
  }

  /** The device this connection is authenticated as. */
  get device(): { id: string; name: string } | undefined {
    return this._link.device;
  }

  /** The agents on this host. */
  async agents(): Promise<Agent[]> {
    const { agents } = (await this._link.request('host/agents', {})) as { agents: AgentInfo[] };
    return agents.map((info) => this.agentFrom(info));
  }

  /** Permission requests waiting for an answer, across every agent (the inbox). */
  async notifications(): Promise<PermissionRequest[]> {
    const { requests } = (await this._link.request('host/pending', {})) as { requests: PendingRequestInfo[] };
    return requests.map((info) => this.requestFrom(info));
  }

  async devices(): Promise<Device[]> {
    const { devices } = (await this._link.request('host/devices', {})) as { devices: Device[] };
    return devices;
  }

  /** Revokes a paired device; by default this one. */
  async unpair(deviceId?: string): Promise<void> {
    await this._link.request('host/unpair', { deviceId: deviceId ?? this.device?.id });
  }

  /** Presence, agent changes and permission requests, as they happen. */
  updates(): AsyncIterable<HostUpdate> {
    const queue: Queue<HostUpdate> = new Queue(() => this.subscribers.delete(queue));
    this.subscribers.add(queue);
    return queue;
  }

  /** @internal */
  _close(): void {
    this._link.close();
    for (const queue of this.subscribers) queue.end();
    for (const session of this.sessions.values()) session._closed();
  }

  /** @internal Sends `initialize` on an agent's channel once per connection. */
  _initialize(agent: string, early = false): Promise<void> {
    let done = this.initialized.get(agent);
    if (!done) {
      done = this._link
        .request(
          'initialize',
          {
            protocolVersion: 1,
            clientCapabilities: { fs: { readTextFile: false, writeTextFile: false }, terminal: false },
            clientInfo: this._client,
          },
          { agent, early },
        )
        .then(() => {});
      this.initialized.set(agent, done);
      done.catch(() => this.initialized.delete(agent));
    }
    return done;
  }

  /** @internal */
  _session(agent: string, sessionId: string): Session | undefined {
    return this.sessions.get(key(agent, sessionId));
  }

  /** @internal */
  _track(session: Session): void {
    this.sessions.set(key(session.agent.id, session.id), session);
  }

  /** @internal */
  _untrack(session: Session): void {
    this.sessions.delete(key(session.agent.id, session.id));
  }

  /** @internal The host's pending id for a request, looked up when the host hasn't announced it yet. */
  async _pendingId(request: PermissionRequest): Promise<string | undefined> {
    if (request.id) return request.id;
    await this.notifications();
    return request.id;
  }

  private emit(update: HostUpdate): void {
    for (const queue of this.subscribers) queue.push(update);
  }

  private agentFrom(info: AgentInfo): Agent {
    let agent = this.agentsById.get(info.id);
    if (agent) agent._set(info);
    else {
      agent = new Agent(this, info);
      this.agentsById.set(info.id, agent);
    }
    return agent;
  }

  private requestFrom(info: PendingRequestInfo): PermissionRequest {
    const k = key(info.agent, info.sessionId, info.toolCall.toolCallId);
    let request = this.requests.get(k);
    if (!request) {
      request = new PermissionRequest(this, info.agent, info.sessionId, info.toolCall, info.options);
      this.requests.set(k, request);
    }
    request.id = info.id;
    request.turnId = info.turnId;
    request.createdAt = info.createdAt;
    return request;
  }

  private hostNotification(method: string, params: Json): void {
    switch (method) {
      case 'host/turn':
        this._session(params.agent, params.sessionId)?._turnNotice(params);
        break;
      case 'host/pending_update': {
        const info = params.request as PendingRequestInfo;
        if (params.change === 'added') {
          this.emit({ type: 'permission', request: this.requestFrom(info) });
        } else if (params.change === 'resolved') {
          const request = this.requestFrom(info);
          this.resolved(request, params.outcome ?? null, params.answeredBy ?? null);
        }
        break;
      }
      case 'host/agent_update': {
        const agent = this.agentFrom(params.agent);
        if (params.change === 'removed') this.agentsById.delete(agent.id);
        this.emit({ type: 'agent', change: params.change, agent });
        break;
      }
    }
  }

  private resolved(request: PermissionRequest, outcome: PermissionOutcome | null, answeredBy: DeviceRef | null): void {
    if (request.resolved) return;
    request.resolved = { outcome, answeredBy };
    request._rpcId = undefined;
    this.requests.delete(key(request.agent, request.sessionId, request.toolCall.toolCallId));
    this._session(request.agent, request.sessionId)?._resolved(request);
    this.emit({ type: 'permission_resolved', request, outcome, answeredBy });
  }

  private agentMessage(agent: string, message: Json): void {
    const params = (message.params ?? {}) as Json;
    const hasId = 'id' in message;
    switch (message.method) {
      case 'session/update':
        this._session(agent, params.sessionId)?._update(params.update);
        return;
      case 'session/request_permission': {
        if (!hasId) return;
        const k = key(agent, params.sessionId, params.toolCall.toolCallId);
        const known = this.requests.get(k);
        if (known && !known.resolved) {
          // Announced on the host channel first, or the same request again after a reconnect or load.
          known._rpcId = message.id;
          this._session(agent, params.sessionId)?._permission(known);
          return;
        }
        const request = new PermissionRequest(this, agent, params.sessionId, params.toolCall, params.options);
        request._rpcId = message.id;
        this.requests.set(k, request);
        this._session(agent, params.sessionId)?._permission(request);
        return;
      }
      case '$/cancel_request': {
        for (const request of this.requests.values()) {
          if (request.agent === agent && request._rpcId === params.requestId) request._rpcId = undefined;
        }
        this._link.respond(agent, params.requestId, { error: { code: -32800, message: 'Request cancelled' } });
        return;
      }
      default:
        // This client offers no fs, terminal or elicitation.
        if (hasId) this._link.respond(agent, message.id, { error: { code: -32601, message: 'Method not found' } });
    }
  }

  private async connected(): Promise<void> {
    this.initialized.clear();
    await Promise.all([...this.sessions.values()].map((session) => session._resume()));
    if (this.requests.size > 0) {
      // Requests answered while away are no longer pending.
      const { requests } = (await this._link.request('host/pending', {}, { early: true })) as { requests: PendingRequestInfo[] };
      const waiting = new Set(requests.map((info) => this.requestFrom(info)));
      for (const request of [...this.requests.values()]) {
        if (!waiting.has(request)) this.resolved(request, null, null);
      }
    }
    this.emit({ type: 'presence', online: true });
  }

  private disconnected(): void {
    this.initialized.clear();
    for (const request of this.requests.values()) request._rpcId = undefined;
    for (const session of this.sessions.values()) session._disconnected();
    this.emit({ type: 'presence', online: false });
  }
}

export class Agent {
  readonly id: string;
  label!: string;
  runtime!: string;
  folder?: string;
  online!: boolean;
  offlineReason?: string;
  capabilities!: Record<string, unknown>;
  /** The modes a new session starts in, when the agent has modes. */
  modes?: SessionModeState;

  /** @internal */
  constructor(readonly host: Host, info: AgentInfo) {
    this.id = info.id;
    this._set(info);
  }

  /** @internal */
  _set(info: AgentInfo): void {
    this.label = info.label;
    this.runtime = info.runtime;
    this.folder = info.folder;
    this.online = info.online;
    this.offlineReason = info.offlineReason;
    this.capabilities = info.capabilities;
    this.modes = info.modes ?? undefined;
  }

  /** The agent's sessions (ACP `session/list`). */
  async sessions(): Promise<SessionSummary[]> {
    await this.host._initialize(this.id);
    const result = await this.host._link.request('session/list', { cwd: this.folder }, { agent: this.id });
    return result.sessions as SessionSummary[];
  }

  /**
   * A new session, or with `id` an existing one, loaded: its `history`, and
   * its running `turn` with any permission request still waiting.
   */
  async session(id?: string): Promise<Session> {
    await this.host._initialize(this.id);
    const params = { cwd: this.folder ?? '/', mcpServers: [] };
    if (id === undefined) {
      const result = await this.host._link.request('session/new', params, { agent: this.id });
      const session = new Session(this, result.sessionId, result.modes ?? this.modes);
      this.host._track(session);
      return session;
    }
    const known = this.host._session(this.id, id);
    if (known) return known;
    const session = new Session(this, id, undefined);
    this.host._track(session);
    session._replay = { first: true, index: 0, sawRunning: false };
    try {
      const result = await this.host._link.request('session/load', { ...params, sessionId: id }, { agent: this.id });
      session._loaded(result);
    } catch (error) {
      this.host._untrack(session);
      throw error;
    }
    return session;
  }
}

export class Session {
  /** The session's modes, kept current. */
  modes?: SessionModeState;
  /** The conversation as it was when this session was loaded. */
  readonly history: TurnEvent[] = [];
  /** The turn running now, whoever started it; null between turns. */
  turn: Turn | null = null;

  /** @internal Every update seen, to skip what the host replays after a reconnect. */
  _log: Json[] = [];
  /** @internal Set while a `session/load` is replaying. */
  _replay?: { first: boolean; index: number; sawRunning: boolean };

  /** @internal */
  constructor(readonly agent: Agent, readonly id: string, modes: SessionModeState | undefined) {
    this.modes = modes;
  }

  /** Sends a prompt. The returned turn streams its events. */
  prompt(text: string, attachments: Attachment[] = []): Turn {
    const turn = new Turn(this, true);
    if (this.turn) {
      turn._fail(new OALError('turn_in_progress', `${this.agent.label} is still working on the last message. Wait for it or stop it.`));
      return turn;
    }
    this.turn = turn;
    const prompt: Json[] = [{ type: 'text', text }];
    for (const file of attachments) {
      prompt.push({ type: 'resource_link', uri: file.url, name: file.name, mimeType: file.mimeType, size: file.size });
    }
    this.agent.host._link.request('session/prompt', { sessionId: this.id, prompt }, { agent: this.agent.id }).then(
      (result: Json) => turn._responded({ stopReason: result.stopReason, usage: result.usage }),
      (error: OALError) => {
        if (error.code !== 'connection_lost') turn._responded({ error });
        // A lost connection is settled by the resume (section 12).
      },
    );
    return turn;
  }

  /** Stops the running turn. It ends with `done` and stop reason `cancelled`. */
  async cancel(): Promise<void> {
    await this.agent.host._link.notify(this.agent.id, 'session/cancel', { sessionId: this.id });
  }

  /** Switches the session's mode (one of `modes.availableModes`). */
  async setMode(modeId: string): Promise<void> {
    if (this.modes && !this.modes.availableModes.some((mode) => mode.id === modeId)) {
      const names = this.modes.availableModes.map((mode) => mode.id).join(', ');
      throw new OALError('not_offered', `${this.agent.label} has no mode called ${modeId}. Its modes are ${names}.`);
    }
    await this.agent.host._link.request('session/set_mode', { sessionId: this.id, modeId }, { agent: this.agent.id });
    if (this.modes) this.modes = { ...this.modes, currentModeId: modeId };
  }

  /** @internal */
  _update(update: Json): void {
    const replay = this._replay;
    if (replay && !replay.first && replay.index < this._log.length) {
      if (deepEqual(update, this._log[replay.index])) {
        replay.index++;
        return;
      }
      // The echo of our own prompt: recorded by the host, never sent to us.
      if (update.sessionUpdate === 'user_message_chunk') return;
    }
    this._log.push(update);
    if (update.sessionUpdate === 'current_mode_update' && this.modes) {
      this.modes = { ...this.modes, currentModeId: update.currentModeId };
    }
    const events = toEvents(update);
    if (replay?.first) this.history.push(...events);
    else this.turn?._push(...events);
  }

  /** @internal `host/turn`. */
  _turnNotice(params: Json): void {
    const turn = this.turn;
    const mine = (t: Turn) => t.id === params.turnId || (t.id === undefined && t._local);
    if (params.state === 'running') {
      if (this._replay) this._replay.sawRunning = true;
      if (turn && mine(turn)) turn.id = params.turnId;
      else {
        this.turn = new Turn(this, false);
        this.turn.id = params.turnId;
      }
    } else if (params.state === 'ended' && turn && mine(turn)) {
      turn.id = params.turnId;
      turn._ended({
        stopReason: params.stopReason,
        usage: params.usage,
        error: params.error ? OALError.fromRpc(params.error) : undefined,
      });
    }
  }

  /** @internal */
  _permission(request: PermissionRequest): void {
    if (!this.turn || request._turn === this.turn) return;
    request._turn = this.turn;
    this.turn._push({ type: 'permission', request });
  }

  /** @internal */
  _resolved(request: PermissionRequest): void {
    const { outcome, answeredBy } = request.resolved!;
    this.turn?._push({ type: 'permission_resolved', request, outcome, answeredBy });
  }

  /** @internal The `session/load` answer. */
  _loaded(result: Json): void {
    if (result?.modes) this.modes = result.modes;
    const replay = this._replay;
    this._replay = undefined;
    const turn = this.turn;
    if (replay && !replay.first && turn && !replay.sawRunning) {
      // No turn running on the host: ours ended while we were away, or never got there.
      if (turn.id) turn._ended({ stopReason: null });
      else turn._fail(new OALError('connection_lost', `Lost the connection before ${this.agent.host.name} got the message. Send it again.`));
    }
  }

  /** @internal Loads the session again on a new connection (section 12). */
  async _resume(): Promise<void> {
    try {
      await this.agent.host._initialize(this.agent.id, true);
      this._replay = { first: false, index: 0, sawRunning: false };
      const result = await this.agent.host._link.request(
        'session/load',
        { sessionId: this.id, cwd: this.agent.folder ?? '/', mcpServers: [] },
        { agent: this.agent.id, early: true },
      );
      this._loaded(result);
    } catch (error) {
      this._replay = undefined;
      if ((error as OALError).code !== 'connection_lost') this.turn?._fail(error as OALError);
    }
  }

  /** @internal */
  _disconnected(): void {
    this.turn?._lost();
  }

  /** @internal */
  _closed(): void {
    this.turn?._fail(new OALError('closed', 'The client was closed.'));
  }
}

/** One prompt and everything it streams. Iterate it once with `for await`. */
export class Turn implements AsyncIterable<TurnEvent> {
  /** The host's id for the turn, once it has started. */
  id?: string;
  finished = false;
  /** @internal True when this client sent the prompt. */
  readonly _local: boolean;

  private queue = new Queue<TurnEvent>();
  private awaitingResponse: boolean;
  private endedWith?: { stopReason?: StopReason | null; usage?: Usage; error?: OALError };
  private response?: { stopReason?: StopReason; usage?: Usage; error?: OALError };

  /** @internal */
  constructor(readonly session: Session, local: boolean) {
    this._local = local;
    this.awaitingResponse = local;
  }

  [Symbol.asyncIterator](): AsyncIterator<TurnEvent> {
    return this.queue[Symbol.asyncIterator]();
  }

  /** Stops the turn (`session.cancel()`). */
  cancel(): Promise<void> {
    return this.session.cancel();
  }

  /** @internal */
  _push(...events: TurnEvent[]): void {
    if (!this.finished) for (const event of events) this.queue.push(event);
  }

  /** @internal `host/turn` ended. */
  _ended(outcome: { stopReason?: StopReason | null; usage?: Usage; error?: OALError }): void {
    this.endedWith = outcome;
    this.settle();
  }

  /** @internal The `session/prompt` answer. */
  _responded(response: { stopReason?: StopReason; usage?: Usage; error?: OALError }): void {
    this.response = response;
    this.awaitingResponse = false;
    this.settle();
  }

  /** @internal The connection dropped; the prompt's answer won't come. */
  _lost(): void {
    this.awaitingResponse = false;
  }

  /** @internal */
  _fail(error: OALError): void {
    this.finish({ error });
  }

  private settle(): void {
    if (this.response?.error && this.id === undefined) return this.finish({ error: this.response.error });
    if (!this.endedWith || this.awaitingResponse) return;
    const { stopReason, usage, error } = this.endedWith;
    this.finish({
      stopReason: stopReason === undefined ? (this.response?.stopReason ?? null) : stopReason,
      usage: usage ?? this.response?.usage,
      error: error ?? this.response?.error,
    });
  }

  private finish(outcome: { stopReason?: StopReason | null; usage?: Usage; error?: OALError }): void {
    if (this.finished) return;
    if (outcome.usage) this.queue.push({ type: 'usage', usage: outcome.usage });
    this.queue.push(outcome.error ? { type: 'error', error: outcome.error } : { type: 'done', stopReason: outcome.stopReason ?? null });
    this.finished = true;
    this.queue.end();
    if (this.session.turn === this) this.session.turn = null;
  }
}

/**
 * An agent asking permission to use a tool. Answer it with `allowOnce`,
 * `allowAlways`, `deny` or `choose`. The first answer from any device wins.
 */
export class PermissionRequest {
  /** The host's id for the request, once known. */
  id?: string;
  turnId?: string;
  createdAt?: string;
  /** Set once the request has been answered, here or elsewhere. */
  resolved: { outcome: PermissionOutcome | null; answeredBy: DeviceRef | null } | null = null;
  /** @internal The id of the `session/request_permission` open on this connection. */
  _rpcId?: unknown;
  /** @internal The turn this request was delivered to. */
  _turn?: Turn;

  /** @internal */
  constructor(
    private readonly host: Host,
    readonly agent: string,
    readonly sessionId: string,
    readonly toolCall: ToolCall,
    readonly options: PermissionOption[],
  ) {}

  allowOnce(): Promise<void> {
    return this.chooseKind(['allow_once'], 'Allow once');
  }

  allowAlways(): Promise<void> {
    return this.chooseKind(['allow_always'], 'Always allow');
  }

  deny(): Promise<void> {
    return this.chooseKind(['reject_once', 'reject_always'], 'Deny');
  }

  /** Answers with one of `options` by its `optionId`. */
  async choose(optionId: string): Promise<void> {
    if (!this.options.some((option) => option.optionId === optionId)) {
      throw new OALError('not_offered', "That isn't one of this request's choices.");
    }
    if (this.resolved) throw new OALError('already_answered', 'This was already answered on another device.');
    for (let attempt = 0; ; attempt++) {
      const id = await this.host._pendingId(this);
      if (!id) {
        if (attempt > 0) return; // Answered while the connection was down.
        throw new OALError('unknown_request', 'That request is no longer waiting.');
      }
      try {
        await this.host._link.request('host/answer', { id, optionId });
        return;
      } catch (error) {
        if ((error as OALError).code !== 'connection_lost' || attempt > 0) throw error;
        this.id = undefined; // Look it up again once reconnected.
      }
    }
  }

  private chooseKind(kinds: PermissionOption['kind'][], label: string): Promise<void> {
    const option = kinds.map((kind) => this.options.find((o) => o.kind === kind)).find(Boolean);
    if (!option) return Promise.reject(new OALError('not_offered', `This request doesn't offer ${label}.`));
    return this.choose(option.optionId);
  }
}

function text(content: Json | undefined): string | undefined {
  return content?.type === 'text' ? content.text : undefined;
}

function toolOutput(content: unknown[] | undefined): string {
  return (content ?? [])
    .map((item) => {
      const c = item as Json;
      return c.type === 'content' ? (text(c.content) ?? '') : '';
    })
    .join('');
}

/** An ACP session update as events. */
function toEvents(update: Json): TurnEvent[] {
  const { sessionUpdate, ...rest } = update;
  switch (sessionUpdate) {
    case 'agent_message_chunk':
    case 'agent_thought_chunk':
    case 'user_message_chunk': {
      const value = text(update.content);
      if (value === undefined) break;
      const type = sessionUpdate === 'agent_message_chunk' ? 'text' : sessionUpdate === 'agent_thought_chunk' ? 'thinking' : 'user';
      return [{ type, text: value }];
    }
    case 'tool_call':
      return [{ type: 'tool_start', tool: rest as ToolCall }];
    case 'tool_call_update': {
      const tool = rest as ToolCall;
      if (tool.status === 'completed' || tool.status === 'failed') return [{ type: 'tool_result', tool, output: toolOutput(tool.content) }];
      return [{ type: 'tool_update', tool }];
    }
    case 'plan':
      return [{ type: 'plan', entries: update.entries as PlanEntry[] }];
    case 'current_mode_update':
      return [{ type: 'mode', modeId: update.currentModeId }];
  }
  return [{ type: 'update', update }];
}

function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (typeof a !== 'object' || typeof b !== 'object' || a === null || b === null) return false;
  if (Array.isArray(a) !== Array.isArray(b)) return false;
  const ka = Object.keys(a);
  const kb = Object.keys(b);
  return ka.length === kb.length && ka.every((k) => deepEqual((a as Json)[k], (b as Json)[k]));
}
