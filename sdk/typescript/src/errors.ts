import type { RpcError } from './types.js';

/**
 * Stable error codes. Branch on `code`, never on `message`.
 *
 * The first eleven are the OAL codes (spec section 15). The rest come from
 * this SDK or from JSON-RPC:
 * - `unpaired`: the host revoked this device (close 4003). Pair again.
 * - `connection_lost`: the connection dropped during a request.
 * - `host_offline`: the host couldn't be reached.
 * - `not_offered`: the choice or mode isn't one the request or session offers.
 * - `not_found`: ACP -32002 (no such session on this connection or agent).
 * - `invalid_params`: JSON-RPC -32602.
 * - `closed`: the client was closed.
 * - `agent_error`: any other error from the agent, passed through.
 */
export type ErrorCode =
  | 'version_mismatch'
  | 'unauthenticated'
  | 'pairing_refused'
  | 'unknown_agent'
  | 'agent_unavailable'
  | 'turn_in_progress'
  | 'already_answered'
  | 'unknown_request'
  | 'attachment_failed'
  | 'not_permitted'
  | 'turn_failed'
  | 'unpaired'
  | 'connection_lost'
  | 'host_offline'
  | 'not_offered'
  | 'not_found'
  | 'invalid_params'
  | 'closed'
  | 'agent_error';

const RPC_CODES: Record<number, ErrorCode> = {
  [-33001]: 'version_mismatch',
  [-33002]: 'unauthenticated',
  [-33003]: 'pairing_refused',
  [-33004]: 'unknown_agent',
  [-33005]: 'agent_unavailable',
  [-33006]: 'turn_in_progress',
  [-33007]: 'already_answered',
  [-33008]: 'unknown_request',
  [-33009]: 'attachment_failed',
  [-33010]: 'not_permitted',
  [-33011]: 'turn_failed',
  [-32002]: 'not_found',
  [-32602]: 'invalid_params',
};

/** Every error this SDK throws or yields. `message` is one plain sentence for people. */
export class OALError extends Error {
  override readonly name = 'OALError';
  readonly code: ErrorCode;
  /** The JSON-RPC error code, when the error came from the host or agent. */
  readonly rpcCode?: number;
  readonly data?: unknown;

  constructor(code: ErrorCode, message: string, options: { rpcCode?: number; data?: unknown } = {}) {
    super(message);
    this.code = code;
    this.rpcCode = options.rpcCode;
    this.data = options.data;
  }

  /** The error for a JSON-RPC error object from a host or agent. */
  static fromRpc(error: RpcError): OALError {
    return new OALError(RPC_CODES[error.code] ?? 'agent_error', error.message, { rpcCode: error.code, data: error.data });
  }
}
