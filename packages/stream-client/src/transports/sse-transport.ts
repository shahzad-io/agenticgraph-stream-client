import type { StreamEvent } from '../types/events.js';
import type {
  StreamTransport,
  StreamTransportConnectionState,
  StreamTransportStopReason,
} from '../transport.js';
import type { EventSourceFactory, EventSourceLike, SSETransportOptions } from './sse-types.js';
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
  'stream_reconnect',
  'stream_replay_unavailable',
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
  'custom_event',
] as const;

function defaultEventSourceFactory(url: string): EventSourceLike {
  if (typeof EventSource === 'undefined') {
    throw new Error('EventSource is not available in this environment');
  }

  return new EventSource(url) as unknown as EventSourceLike;
}

/**
 * Adds `custom_events=wrap` (author-defined events arrive as `custom_event`, a name the browser
 * can listen for; a server before core 0.2.1 ignores it) and the resume cursor. A relative URL stays relative.
 */
function withParams(baseUrl: string, cursor: number | null): string {
  const relative = !/^[a-z][a-z0-9+.-]*:/i.test(baseUrl);
  const url = new URL(baseUrl, typeof window !== 'undefined' ? window.location.href : 'http://localhost');

  url.searchParams.set('custom_events', 'wrap');

  if (cursor !== null) {
    url.searchParams.set('cursor', String(cursor));
  }

  return relative ? `${url.pathname}${url.search}${url.hash}` : url.toString();
}

function controlName(data: unknown): string | null {
  if (typeof data !== 'object' || data === null || !('control' in data)) {
    return null;
  }

  const control = (data as { control: unknown }).control;

  return typeof control === 'string' ? control : null;
}

/**
 * Store-backed SSE transport. Owns reconnects: on an error or the end of a response it closes
 * the EventSource (no native browser retry) and reopens with `?cursor=<lastSeq>`, with
 * exponential backoff and jitter. Stops on a `completed`/`failed` terminal, on control
 * `replay_unavailable`, when `resolveUrl` returns `null`, on `disconnect()`, or after
 * `maxAttempts` consecutive failures (state `failed`). `getStopReason()` says which.
 */
export class SSETransport implements StreamTransport {
  private readonly baseUrl: string;

  private readonly eventSourceFactory: EventSourceFactory;

  private readonly maxAttempts: number;

  private readonly baseDelayMs: number;

  private readonly maxDelayMs: number;

  private readonly resolveUrl: SSETransportOptions['resolveUrl'];

  private eventSource: EventSourceLike | null = null;

  private eventCallback: ((event: StreamEvent) => void) | null = null;

  private connectionState: StreamTransportConnectionState = 'disconnected';

  private connectionStateListeners = new Set<
    (state: StreamTransportConnectionState, stopReason: StreamTransportStopReason | null) => void
  >();

  private stopReason: StreamTransportStopReason | null = null;

  private lastSeq: number | null = null;

  private failures = 0;

  private stopped = false;

  private retryTimer: ReturnType<typeof setTimeout> | null = null;

  private readonly listeners = new Map<string, (event: MessageEvent) => void>();

  constructor(options: SSETransportOptions) {
    this.baseUrl = options.url;
    this.eventSourceFactory = options.eventSourceFactory ?? defaultEventSourceFactory;
    this.maxAttempts = options.maxAttempts ?? 5;
    this.baseDelayMs = options.baseDelayMs ?? 500;
    this.maxDelayMs = options.maxDelayMs ?? 10000;
    this.resolveUrl = options.resolveUrl;
  }

  async connect(): Promise<void> {
    if (this.connectionState === 'connected') {
      return;
    }

    this.clearRetryTimer();
    this.closeEventSource();
    this.stopped = false;
    this.stopReason = null;
    this.failures = 0;
    this.openEventSource(withParams(this.baseUrl, this.lastSeq), 'connecting');
  }

  async disconnect(): Promise<void> {
    this.stop('disconnected', 'manual');
    this.eventCallback = null;
    this.lastSeq = null;
  }

  /** Manual reconnect now (also after `failed`). No-op when connected. */
  async reconnect(): Promise<void> {
    if (this.connectionState === 'connected') {
      return;
    }

    this.clearRetryTimer();
    this.closeEventSource();
    this.stopped = false;
    this.stopReason = null;
    this.failures = 0;
    this.setConnectionState('reconnecting');
    await this.reopen();
  }

  onEvent(callback: (event: StreamEvent) => void): void {
    this.eventCallback = callback;
  }

  getConnectionState(): StreamTransportConnectionState {
    return this.connectionState;
  }

  /** Why the transport stopped (first stop wins until connect()/reconnect()); `null` while running or before the first connect. */
  getStopReason(): StreamTransportStopReason | null {
    return this.stopReason;
  }

  onConnectionStateChange(
    callback: (state: StreamTransportConnectionState, stopReason: StreamTransportStopReason | null) => void,
  ): () => void {
    this.connectionStateListeners.add(callback);

    return () => {
      this.connectionStateListeners.delete(callback);
    };
  }

  getLastSeq(): number | null {
    return this.lastSeq;
  }

  /** @internal Test hook - deliver a parsed SSE message. */
  _handleSseMessage(eventType: string, data: unknown): void {
    this.deliverParsed(eventType, data);
  }

  private openEventSource(url: string, state: 'connecting' | 'reconnecting'): void {
    this.setConnectionState(state);

    const eventSource = this.eventSourceFactory(url);
    this.eventSource = eventSource;

    for (const eventType of SSE_EVENT_TYPES) {
      const listener = (event: MessageEvent) => {
        let data: unknown;

        try {
          data = JSON.parse(String(event.data)) as unknown;
        } catch {
          return;
        }

        this.deliverParsed(eventType, data);
      };

      this.listeners.set(eventType, listener);
      eventSource.addEventListener(eventType, listener);
    }

    eventSource.onopen = () => {
      this.failures = 0;
      this.setConnectionState('connected');
    };
    eventSource.onerror = () => {
      this.handleFailure();
    };
  }

  private async reopen(): Promise<void> {
    let url: string | null;

    try {
      url = this.resolveUrl ? await this.resolveUrl(this.lastSeq) : this.baseUrl;
    } catch {
      this.handleFailure();

      return;
    }

    if (this.stopped) {
      return;
    }

    if (url === null) {
      this.stop('disconnected', 'resolver');

      return;
    }

    this.openEventSource(withParams(url, this.lastSeq), 'reconnecting');
  }

  /** Error or end of response: close at once (no native retry), then back off or give up. */
  private handleFailure(): void {
    this.closeEventSource();

    if (this.stopped) {
      return;
    }

    this.failures += 1;

    if (this.failures > this.maxAttempts) {
      this.stop('failed', 'failed');

      return;
    }

    const exponential = Math.min(this.maxDelayMs, this.baseDelayMs * 2 ** (this.failures - 1));
    this.scheduleReopen(exponential / 2 + Math.random() * (exponential / 2));
  }

  private scheduleReopen(delayMs: number): void {
    this.clearRetryTimer();
    this.setConnectionState('reconnecting');
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      void this.reopen();
    }, delayMs);
  }

  /** The first stop holds until the next connect()/reconnect(): later stops keep its reason and state. */
  private stop(state: 'disconnected' | 'failed', reason: StreamTransportStopReason): void {
    this.clearRetryTimer();
    this.closeEventSource();

    if (this.stopped) {
      return;
    }

    this.stopped = true;
    this.stopReason = reason;
    this.setConnectionState(state);
  }

  private clearRetryTimer(): void {
    if (this.retryTimer !== null) {
      clearTimeout(this.retryTimer);
      this.retryTimer = null;
    }
  }

  private closeEventSource(): void {
    if (!this.eventSource) {
      return;
    }

    for (const [eventType, listener] of this.listeners) {
      this.eventSource.removeEventListener(eventType, listener);
    }

    this.listeners.clear();
    this.eventSource.onopen = null;
    this.eventSource.onerror = null;
    this.eventSource.close();
    this.eventSource = null;
  }

  private deliverParsed(eventType: string, data: unknown): void {
    if (eventType === 'custom_event') {
      // Wrapped author event: the author's type is in `event_type`, not part of the wire body.
      if (typeof data !== 'object' || data === null) {
        return;
      }

      const { event_type: authorType, ...body } = data as Record<string, unknown>;

      if (typeof authorType !== 'string' || authorType === '') {
        return;
      }

      eventType = authorType;
      data = body;
    }

    const control = controlName(data);

    if (control !== null) {
      this.handleControl(control, data as Record<string, unknown>);

      return;
    }

    const event = wireToStreamEvent(eventType, data);

    if (event === null) {
      return;
    }

    if ('seq' in event && typeof event.seq === 'number') {
      if (this.lastSeq !== null && event.seq <= this.lastSeq) {
        return;
      }

      this.lastSeq = event.seq;
    }

    this.eventCallback?.(event);

    if (event.eventType === 'execution_terminal') {
      const status = (event.payload as { status?: unknown } | null)?.status;

      if (status === 'completed' || status === 'failed') {
        this.stop('disconnected', 'terminal');
      }
    }
  }

  private handleControl(control: string, body: Record<string, unknown>): void {
    if (control === 'reconnect') {
      this.closeEventSource();
      this.failures = 0;
      this.scheduleReopen(typeof body.after_ms === 'number' ? body.after_ms : this.baseDelayMs);

      return;
    }

    if (control === 'replay_unavailable') {
      this.eventCallback?.({
        eventType: 'stream_replay_unavailable',
        payload: {
          cursor: body.cursor as number | string,
          ...('final_state' in body ? { final_state: body.final_state as Record<string, unknown> | null } : {}),
        },
      });
      this.stop('disconnected', 'replay_unavailable');
    }
  }

  private setConnectionState(state: StreamTransportConnectionState): void {
    if (this.connectionState === state) {
      return;
    }

    this.connectionState = state;

    for (const listener of this.connectionStateListeners) {
      listener(state, this.stopReason);
    }
  }
}
