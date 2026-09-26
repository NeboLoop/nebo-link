// @openagentlink/client: drive any agent linked over Open Agent Link (OAL).
// Spec: https://openagent.link (spec/oal-0.1.md in NeboLoop/nebo-link).

export { connect, Client, Host, Agent, Session, Turn, PermissionRequest } from './client.js';
export type { ConnectOptions, TurnEvent, HostUpdate } from './client.js';
export { pair } from './identity.js';
export type { Identity, PairOptions, Endpoint } from './identity.js';
export { OALError } from './errors.js';
export type { ErrorCode } from './errors.js';
export { plaintext, webSocketDialer, ChannelClosed } from './channel.js';
export { encrypted } from './e2e.js';
export type { Socket, Dialer, SecureChannel, FrameChannel, ChannelContext, CloseInfo } from './channel.js';
export * from './types.js';
