// Shapes of the messages OAL 0.1 defines, written from spec/schemas/ (the
// JSON Schemas of spec/oal-0.1.md), and the ACP
// types a client sees on an agent channel. ACP types keep ACP's own field
// names, because OAL carries ACP unchanged.

/** The OAL versions this SDK speaks. */
export const PROTOCOL = { min: '0.1', max: '0.1' } as const;

export interface VersionRange {
  min: string;
  max: string;
}

export interface ClientInfo {
  name: string;
  version: string;
}

/** `host/info` (section 7.1). */
export interface HostInfo {
  host: { id: string; name: string; publicKey: string; tlsFingerprint?: string };
  software: { name: string; version: string };
  protocol: VersionRange;
  acp: { protocolVersion: number };
  runtimes: Runtime[];
  maxFrameBytes: number;
  attachments: { schemes: string[]; maxBytes: number };
}

export interface Runtime {
  id: string;
  name: string;
  /** `acp`, or the adapter name (`openclaw`, `hermes`). */
  kind: string;
  version?: string;
}

/** ACP `SessionMode`. */
export interface SessionMode {
  id: string;
  name: string;
  description?: string;
}

/** ACP `SessionModeState`. */
export interface SessionModeState {
  currentModeId: string;
  availableModes: SessionMode[];
}

/** An agent as `host/agents` describes it (section 7.2). */
export interface AgentInfo {
  id: string;
  label: string;
  runtime: string;
  folder?: string;
  online: boolean;
  offlineReason?: string;
  /** ACP `AgentCapabilities`. */
  capabilities: Record<string, unknown>;
  modes?: SessionModeState | null;
}

/** ACP `PermissionOption`. */
export interface PermissionOption {
  optionId: string;
  name: string;
  kind: 'allow_once' | 'allow_always' | 'reject_once' | 'reject_always';
}

/** ACP `RequestPermissionOutcome`. */
export type PermissionOutcome = { outcome: 'selected'; optionId: string } | { outcome: 'cancelled' };

/** ACP `ToolCall` / `ToolCallUpdate`. Every field but the id is optional on an update. */
export interface ToolCall {
  toolCallId: string;
  title?: string;
  kind?: string;
  status?: 'pending' | 'in_progress' | 'completed' | 'failed';
  content?: unknown[];
  locations?: unknown[];
  rawInput?: unknown;
  rawOutput?: unknown;
}

/** ACP `PlanEntry`. */
export interface PlanEntry {
  content: string;
  priority: 'high' | 'medium' | 'low';
  status: 'pending' | 'in_progress' | 'completed';
}

/** ACP `StopReason`. */
export type StopReason = 'end_turn' | 'max_tokens' | 'max_turn_requests' | 'refusal' | 'cancelled';

/** The tokens and cost of one turn (section 9). */
export interface Usage {
  inputTokens?: number;
  outputTokens?: number;
  thoughtTokens?: number;
  cachedReadTokens?: number;
  cachedWriteTokens?: number;
  totalTokens?: number;
  cost?: { amount: number; currency: string };
}

/** Who did something: a paired device. */
export interface DeviceRef {
  deviceId: string;
  name: string;
}

/** A paired device, from `host/devices` (section 6.4). */
export interface Device {
  id: string;
  name: string;
  pairedAt: string;
  lastSeenAt?: string;
  current: boolean;
}

/** ACP `SessionInfo`, from `session/list`. */
export interface SessionSummary {
  sessionId: string;
  cwd: string;
  title?: string;
  updatedAt?: string;
}

/** A file sent by reference (section 14): a URL the host can fetch. */
export interface Attachment {
  url: string;
  name: string;
  mimeType?: string;
  size?: number;
}

/** A pending permission request as the host lists it (section 10). */
export interface PendingRequestInfo {
  id: string;
  agent: string;
  sessionId: string;
  turnId?: string;
  toolCall: ToolCall;
  options: PermissionOption[];
  createdAt: string;
}

/** A JSON-RPC 2.0 error object. */
export interface RpcError {
  code: number;
  message: string;
  data?: unknown;
}

/** One OAL frame: a host-channel JSON-RPC message, or `{agent, acp}`. */
export type Frame = Record<string, unknown>;
