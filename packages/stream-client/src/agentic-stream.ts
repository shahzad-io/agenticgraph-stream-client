import { AsyncEventStream } from './stream/async-event-stream.js';
import type { KnownEventHandler, StreamEventHandler } from './stream/async-event-stream.js';
import {
  applyConnectionDrop,
  applyEventToStreamState,
  createInitialStreamState,
  type StreamStateSnapshot,
} from './stream/stream-state.js';
import type { StreamErrorDetail } from './stream/stream-error.js';
import { parseReplayCursor } from './stream/stream-error.js';
import type { KnownStreamEvent } from './types/events.js';
import type { StreamEvent } from './types/events.js';
import type { StreamInterrupt } from './types/interrupt.js';
import type { StreamStatus } from './types/status.js';
import type { StreamTransport, StreamTransportConnectionState } from './transport.js';

export interface AgenticStreamConnectOptions {
  transport: StreamTransport;
}

export type StatusChangeHandler = (status: StreamStatus, snapshot: StreamStateSnapshot) => void;

/**
 * Framework-agnostic streaming client. One internal {@link AsyncEventStream} feeds all APIs.
 */
export class AgenticStream {
  private readonly transport: StreamTransport;

  private readonly eventStream = new AsyncEventStream();

  private state: StreamStateSnapshot = createInitialStreamState();

  private statusListeners = new Map<symbol, StatusChangeHandler>();

  private transportEventHandler: ((event: StreamEvent) => void) | null = null;

  private connectionStateUnsubscribe: (() => void) | null = null;

  private connected = false;

  private reconnecting = false;

  private constructor(transport: StreamTransport) {
    this.transport = transport;
  }

  /**
   * Connect a transport and return the stream.
   *
   * Resolves only once the transport reports `connected` — use this before kicking off
   * a client-initiated broadcast flow so the subscription is live first.
   *
   * Broadcast is fire-and-forget (no replay). Events emitted before subscription are lost;
   * SSE with cursor/seq is the gap-free-from-start alternative.
   */
  static async connect(options: AgenticStreamConnectOptions): Promise<AgenticStream> {
    const stream = new AgenticStream(options.transport);
    await stream.initialize();

    return stream;
  }

  get status(): StreamStatus {
    return this.state.status;
  }

  get interrupt(): StreamInterrupt | null {
    return this.state.interrupt;
  }

  get error(): StreamErrorDetail | null {
    return this.state.error;
  }

  /**
   * Observe status transitions (for framework adapters). Returns unsubscribe.
   */
  onStatusChange(handler: StatusChangeHandler): () => void {
    const id = Symbol('status');
    this.statusListeners.set(id, handler);

    return () => {
      this.statusListeners.delete(id);
    };
  }

  /**
   * Fresh async iterable over the internal event stream (no replay).
   * Multiple concurrent iterators each receive events from creation forward.
   */
  events(): AsyncIterable<StreamEvent> {
    return this.eventStream.createIterable();
  }

  on(handler: StreamEventHandler): () => void;

  on<T extends KnownStreamEvent['eventType']>(
    eventType: T,
    handler: KnownEventHandler<T>,
  ): () => void;

  on(
    eventTypeOrHandler: unknown,
    maybeHandler?: unknown,
  ): () => void {
    if (typeof eventTypeOrHandler === 'function') {
      return this.eventStream.addListener(undefined, eventTypeOrHandler as StreamEventHandler);
    }

    return this.eventStream.addListener(
      eventTypeOrHandler as KnownStreamEvent['eventType'],
      maybeHandler as StreamEventHandler,
    );
  }

  off(eventType: string | undefined, handler: StreamEventHandler): void {
    this.eventStream.removeListener(eventType, handler);
  }

  subscribe(handler: StreamEventHandler): () => void;

  subscribe<T extends KnownStreamEvent['eventType']>(
    eventType: T,
    handler: KnownEventHandler<T>,
  ): () => void;

  subscribe(
    eventTypeOrHandler: unknown,
    maybeHandler?: unknown,
  ): () => void {
    if (typeof eventTypeOrHandler === 'function') {
      return this.on(eventTypeOrHandler as StreamEventHandler);
    }

    return this.on(
      eventTypeOrHandler as KnownStreamEvent['eventType'],
      maybeHandler as StreamEventHandler,
    );
  }

  async disconnect(): Promise<void> {
    this.connectionStateUnsubscribe?.();
    this.connectionStateUnsubscribe = null;
    this.transportEventHandler = null;
    this.eventStream.close();
    await this.transport.disconnect();
    this.connected = false;
  }

  /**
   * Manual reconnect. No-op when already connected. Tears down transport subscription first.
   */
  async reconnect(): Promise<void> {
    if (this.reconnecting) {
      return;
    }

    if (this.transport.getConnectionState() === 'connected') {
      return;
    }

    this.reconnecting = true;

    try {
      await this.transport.reconnect();
    } finally {
      this.reconnecting = false;
    }
  }

  private async initialize(): Promise<void> {
    this.bindTransport();
    await this.transport.connect();
    await this.waitForConnectionState('connected');
    this.connected = true;
  }

  private waitForConnectionState(target: StreamTransportConnectionState): Promise<void> {
    if (this.transport.getConnectionState() === target) {
      return Promise.resolve();
    }

    const onStateChange = this.transport.onConnectionStateChange;

    if (onStateChange === undefined) {
      return Promise.resolve();
    }

    return new Promise((resolve) => {
      const unsubscribe = onStateChange((state) => {
        if (state === target) {
          unsubscribe();
          resolve();
        }
      });

      if (this.transport.getConnectionState() === target) {
        unsubscribe();
        resolve();
      }
    });
  }

  private bindTransport(): void {
    this.transportEventHandler = (event: StreamEvent) => {
      this.handleEvent(event);
    };

    this.transport.onEvent(this.transportEventHandler);

    if (this.transport.onConnectionStateChange) {
      this.connectionStateUnsubscribe = this.transport.onConnectionStateChange((state) => {
        this.handleConnectionStateChange(state);
      });
    }
  }

  private handleEvent(event: StreamEvent): void {
    if (event.eventType === 'stream_replay_unavailable') {
      const payload = event.payload as { cursor?: number; final_state?: unknown };

      this.updateState({
        ...this.state,
        status: 'error',
        error: {
          kind: 'replay_unavailable',
          cursor: parseReplayCursor(payload),
          final_state: payload.final_state,
        },
      });

      return;
    }

    if (event.eventType === 'stream_heartbeat') {
      return;
    }

    const next = applyEventToStreamState(event, this.state);
    this.updateState(next);
    this.eventStream.dispatch(event);
  }

  private handleConnectionStateChange(state: StreamTransportConnectionState): void {
    if (state === 'disconnected' && this.connected) {
      this.updateState(applyConnectionDrop(this.state));
    }
  }

  private updateState(next: StreamStateSnapshot): void {
    const prevStatus = this.state.status;
    this.state = next;

    if (prevStatus !== next.status) {
      for (const listener of this.statusListeners.values()) {
        listener(next.status, next);
      }
    }
  }
}
