import type { StreamEvent } from '../types/events.js';
import type {
  StreamTransport,
  StreamTransportConnectionState,
} from '../transport.js';

/**
 * Programmatic transport for tests and zero-backend evaluation.
 */
export class MockTransport implements StreamTransport {
  private eventCallback: ((event: StreamEvent) => void) | null = null;

  private connectionState: StreamTransportConnectionState = 'disconnected';

  private connectionStateListeners = new Set<(state: StreamTransportConnectionState) => void>();

  private connectDelayMs = 0;

  /** Defer reaching `connected` until after connect() resolves (for connect-await tests). */
  setConnectDelay(ms: number): void {
    this.connectDelayMs = ms;
  }

  async connect(): Promise<void> {
    this.setConnectionState('connecting');

    if (this.connectDelayMs > 0) {
      await new Promise((resolve) => {
        setTimeout(resolve, this.connectDelayMs);
      });
    }

    this.setConnectionState('connected');
  }

  async disconnect(): Promise<void> {
    this.teardown(true);
  }

  async reconnect(): Promise<void> {
    if (this.connectionState === 'connected') {
      return;
    }

    this.teardown(false);
    await this.connect();
  }

  onEvent(callback: (event: StreamEvent) => void): void {
    this.eventCallback = callback;
  }

  getConnectionState(): StreamTransportConnectionState {
    return this.connectionState;
  }

  onConnectionStateChange(
    callback: (state: StreamTransportConnectionState) => void,
  ): () => void {
    this.connectionStateListeners.add(callback);

    return () => {
      this.connectionStateListeners.delete(callback);
    };
  }

  /** Push a single event to the registered consumer. */
  emit(event: StreamEvent): void {
    this.eventCallback?.(event);
  }

  /** Push events in order. */
  emitSequence(events: StreamEvent[]): void {
    for (const event of events) {
      this.emit(event);
    }
  }

  /** Simulate a connection drop (surfaces connection error on the stream). */
  simulateDrop(): void {
    this.setConnectionState('disconnected');
  }

  private teardown(clearCallback: boolean): void {
    this.setConnectionState('disconnected');

    if (clearCallback) {
      this.eventCallback = null;
    }
  }

  private setConnectionState(state: StreamTransportConnectionState): void {
    this.connectionState = state;

    for (const listener of this.connectionStateListeners) {
      listener(state);
    }
  }
}
