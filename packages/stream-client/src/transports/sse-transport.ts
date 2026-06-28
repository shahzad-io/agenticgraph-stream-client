import type { StreamEvent } from '../types/events.js';
import type {
  StreamTransport,
  StreamTransportConnectionState,
} from '../transport.js';
import type { EventSourceFactory, EventSourceLike, SSETransportOptions } from './sse-types.js';
import { EVENT_SOURCE_CLOSED } from './sse-types.js';
import { wireToStreamEvent } from './wire-event.js';

const SSE_EVENT_TYPES = [
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
  'stream_heartbeat',
  'stream_replay_unavailable',
  'text_start',
  'text_delta',
  'text_complete',
  'tool_call',
  'tool_result',
  'thinking_start',
  'thinking_delta',
  'thinking_complete',
  'step_start',
  'step_finish',
  'provider_tool_event',
  'artifact',
  'error',
] as const;

function defaultEventSourceFactory(url: string): EventSourceLike {
  if (typeof EventSource === 'undefined') {
    throw new Error('EventSource is not available in this environment');
  }

  return new EventSource(url) as unknown as EventSourceLike;
}

function buildReconnectUrl(baseUrl: string, cursor: number | null, useCursorQuery: boolean): string {
  if (cursor === null || !useCursorQuery) {
    return baseUrl;
  }

  const url = new URL(baseUrl, typeof window !== 'undefined' ? window.location.href : 'http://localhost');

  url.searchParams.set('cursor', String(cursor));

  return url.toString();
}

/**
 * Store-backed SSE transport with gap-free reconnect via last-seen `seq` cursor.
 */
export class SSETransport implements StreamTransport {
  private readonly baseUrl: string;

  private readonly eventSourceFactory: EventSourceFactory;

  private readonly useCursorQuery: boolean;

  private eventSource: EventSourceLike | null = null;

  private eventCallback: ((event: StreamEvent) => void) | null = null;

  private connectionState: StreamTransportConnectionState = 'disconnected';

  private connectionStateListeners = new Set<(state: StreamTransportConnectionState) => void>();

  private lastSeq: number | null = null;

  private readonly listeners = new Map<string, (event: MessageEvent) => void>();

  private readonly errorListener: (event: Event) => void;

  constructor(options: SSETransportOptions) {
    this.baseUrl = options.url;
    this.eventSourceFactory = options.eventSourceFactory ?? defaultEventSourceFactory;
    this.useCursorQuery = options.useCursorQuery ?? true;

    this.errorListener = () => {
      if (this.eventSource?.readyState === EVENT_SOURCE_CLOSED) {
        this.setConnectionState('disconnected');
      }
    };
  }

  async connect(): Promise<void> {
    if (this.connectionState === 'connected') {
      return;
    }

    await this.openEventSource(this.baseUrl);
  }

  async disconnect(): Promise<void> {
    this.teardown(true);
    this.eventCallback = null;
    this.lastSeq = null;
  }

  async reconnect(): Promise<void> {
    if (this.connectionState === 'connected') {
      return;
    }

    this.setConnectionState('reconnecting');
    this.teardown(false);

    const url = buildReconnectUrl(this.baseUrl, this.lastSeq, this.useCursorQuery);
    await this.openEventSource(url);
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

  getLastSeq(): number | null {
    return this.lastSeq;
  }

  /** @internal Test hook — deliver a parsed SSE message. */
  _handleSseMessage(eventType: string, data: unknown, id: string | null): void {
    this.deliverParsed(eventType, data, id);
  }

  private teardown(clearListeners: boolean): void {
    this.closeEventSource();

    if (clearListeners) {
      this.setConnectionState('disconnected');
    }
  }

  private async openEventSource(url: string): Promise<void> {
    this.setConnectionState('connecting');
    this.eventSource = this.eventSourceFactory(url);

    for (const eventType of SSE_EVENT_TYPES) {
      const listener = (event: MessageEvent) => {
        let data: unknown;

        try {
          data = JSON.parse(String(event.data)) as unknown;
        } catch {
          return;
        }

        this.deliverParsed(eventType, data, event.lastEventId || null);
      };

      this.listeners.set(eventType, listener);
      this.eventSource.addEventListener(eventType, listener);
    }

    this.eventSource.onerror = this.errorListener;
    this.setConnectionState('connected');
  }

  private closeEventSource(): void {
    if (!this.eventSource) {
      return;
    }

    for (const [eventType, listener] of this.listeners) {
      this.eventSource.removeEventListener(eventType, listener);
    }

    this.listeners.clear();
    this.eventSource.onerror = null;
    this.eventSource.close();
    this.eventSource = null;
  }

  private deliverParsed(eventType: string, data: unknown, id: string | null): void {
    if (id !== null && id !== '') {
      const parsed = Number.parseInt(id, 10);

      if (!Number.isNaN(parsed)) {
        this.lastSeq = parsed;
      }
    }

    if (typeof data === 'object' && data !== null && 'seq' in data) {
      const seq = (data as { seq: unknown }).seq;

      if (typeof seq === 'number') {
        this.lastSeq = seq;
      }
    }

    const event = wireToStreamEvent(eventType, data);

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
