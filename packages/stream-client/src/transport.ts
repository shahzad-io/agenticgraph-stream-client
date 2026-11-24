import type { StreamEvent } from './types/events.js';

export type StreamTransportConnectionState =
  | 'disconnected'
  | 'connecting'
  | 'connected'
  | 'reconnecting'
  /** Gave up after `maxAttempts` consecutive failed reconnects (SSETransport). */
  | 'failed';

/** Why a transport stopped (SSETransport, BroadcastTransport). `null` while it is running or before it starts. */
export type StreamTransportStopReason =
  /** A `completed` or `failed` execution terminal. */
  | 'terminal'
  /** Control `replay_unavailable` (SSE) or replay `gone` (broadcast): the cursor is older than anything the server stores. */
  | 'replay_unavailable'
  /** `resolveUrl` / `resolveReplayUrl` returned `null`: the app ended the stream. Not an error. */
  | 'resolver'
  /** `disconnect()` was called. */
  | 'manual'
  /** Gave up after `maxAttempts` consecutive failures. */
  | 'failed'
  /** 403 from a stream route or channel auth: invalid token, `refreshToken` gave none, or the app authorizer said no. */
  | 'unauthorized';

/**
 * Transport abstraction - core depends on this interface, not on Laravel Echo.
 * Implementations: SSETransport, BroadcastTransport, MockTransport; EchoTransport (deprecated).
 */
export interface StreamTransport {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  reconnect(): Promise<void>;
  onEvent(callback: (event: StreamEvent) => void): void;
  getConnectionState(): StreamTransportConnectionState;
  /** Optional: why the transport last stopped, `null` while running. */
  getStopReason?(): StreamTransportStopReason | null;
  /**
   * Optional: invoked when connection state changes (e.g. drop detection). Transports that
   * report a stop reason pass it with `disconnected` and `failed`.
   */
  onConnectionStateChange?(
    callback: (
      state: StreamTransportConnectionState,
      stopReason?: StreamTransportStopReason | null,
    ) => void,
  ): () => void;
}
