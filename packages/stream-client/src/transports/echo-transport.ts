import type { StreamEvent } from '../types/events.js';
import type {
  StreamTransport,
  StreamTransportConnectionState,
} from '../transport.js';
import type { EchoChannel, EchoInstance, EchoTransportOptions } from './echo-types.js';
import { normalizeEventName, wireToStreamEvent } from './wire-event.js';

const DEFAULT_EVENT_TYPES = [
  'graph_start',
  'graph_complete',
  'graph_failed',
  'node_start',
  'node_complete',
  'node_error',
  'node_interrupt',
  'stream_resume',
  'execution_terminal',
  'stream_error',
  'stream_close',
  'text_start',
  'text_delta',
  'text_complete',
  'tool_call',
  'tool_result',
  'thinking_start',
  'thinking_delta',
  'thinking_complete',
  'stream_start',
  'step_start',
  'step_finish',
  'stream_end',
  'provider_tool_event',
  'artifact',
  'error',
] as const;

type WireHandler = (eventName: string, body: unknown) => void;

/**
 * Broadcast transport - wraps a consumer-provided Echo instance (never creates Echo).
 * Events during a connection drop are lost (fire-and-forget broadcast).
 */
export class EchoTransport implements StreamTransport {
  private readonly echo: EchoInstance;

  private readonly channelName: string;

  private readonly eventTypes: readonly string[];

  private channel: EchoChannel | null = null;

  private eventCallback: ((event: StreamEvent) => void) | null = null;

  private connectionState: StreamTransportConnectionState = 'disconnected';

  private connectionStateListeners = new Set<(state: StreamTransportConnectionState) => void>();

  private boundHandlers = new Map<string, WireHandler>();

  private listenToAllHandler: ((eventName: string, payload: unknown) => void) | null = null;

  constructor(options: EchoTransportOptions) {
    this.echo = options.echo;
    this.channelName = options.channel;
    this.eventTypes = options.eventTypes ?? DEFAULT_EVENT_TYPES;
  }

  async connect(): Promise<void> {
    if (this.connectionState === 'connected') {
      return;
    }

    this.setConnectionState('connecting');
    this.channel = this.echo.channel(this.channelName);
    this.attachListeners();
    this.setConnectionState('connected');
  }

  async disconnect(): Promise<void> {
    this.teardown(true);
  }

  async reconnect(): Promise<void> {
    if (this.connectionState === 'connected') {
      return;
    }

    this.setConnectionState('reconnecting');
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

  /** @internal Test hook - handle a raw broadcast delivery. */
  _handleBroadcast(eventName: string, body: unknown): void {
    this.deliverWireEvent(eventName, body);
  }

  private teardown(clearCallback: boolean): void {
    this.detachListeners();

    if (this.channel) {
      this.channel.unsubscribe();
      this.channel = null;
    }

    this.echo.leave(this.channelName);
    this.setConnectionState('disconnected');

    if (clearCallback) {
      this.eventCallback = null;
    }
  }

  private attachListeners(): void {
    if (!this.channel) {
      return;
    }

    if (this.channel.listenToAll) {
      this.listenToAllHandler = (eventName: string, payload: unknown) => {
        this.deliverWireEvent(eventName, payload);
      };
      this.channel.listenToAll(this.listenToAllHandler);

      return;
    }

    for (const eventType of this.eventTypes) {
      const handler: WireHandler = (_eventName, body) => {
        this.deliverWireEvent(eventType, body);
      };
      this.boundHandlers.set(eventType, handler);
      this.channel.listen(`.${eventType}`, (body) => handler(eventType, body));
    }
  }

  private detachListeners(): void {
    if (!this.channel) {
      return;
    }

    if (this.listenToAllHandler && this.channel.listenToAll) {
      this.channel.stopListening('', this.listenToAllHandler as (payload: unknown) => void);
      this.listenToAllHandler = null;
    }

    for (const [eventType, handler] of this.boundHandlers) {
      this.channel.stopListening(`.${eventType}`, (body) => handler(eventType, body));
    }

    this.boundHandlers.clear();
  }

  private deliverWireEvent(eventName: string, body: unknown): void {
    const normalized = normalizeEventName(eventName);
    const event = wireToStreamEvent(normalized, body);

    if (event !== null) {
      this.eventCallback?.(event);
    }
  }

  private setConnectionState(state: StreamTransportConnectionState): void {
    this.connectionState = state;

    for (const listener of this.connectionStateListeners) {
      listener(state);
    }
  }
}
