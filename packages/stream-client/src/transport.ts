import type { StreamEvent } from './types/events.js';

export type StreamTransportConnectionState =
  | 'disconnected'
  | 'connecting'
  | 'connected'
  | 'reconnecting';

/**
 * Transport abstraction — core depends on this interface, not on Laravel Echo.
 * Implementations: EchoTransport, SSETransport, MockTransport (Phase 1).
 */
export interface StreamTransport {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  reconnect(): Promise<void>;
  onEvent(callback: (event: StreamEvent) => void): void;
  getConnectionState(): StreamTransportConnectionState;
  /** Optional — invoked when connection state changes (e.g. drop detection). */
  onConnectionStateChange?(
    callback: (state: StreamTransportConnectionState) => void,
  ): () => void;
}
