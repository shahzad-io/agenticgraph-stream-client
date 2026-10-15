/**
 * Injectable EventSource surface for SSETransport (browser EventSource or test double).
 */
export interface EventSourceLike {
  readonly url: string;
  onopen: ((event: Event) => void) | null;
  onerror: ((event: Event) => void) | null;
  addEventListener(
    type: string,
    listener: (event: MessageEvent) => void,
    options?: AddEventListenerOptions | boolean,
  ): void;
  removeEventListener(
    type: string,
    listener: (event: MessageEvent) => void,
    options?: EventListenerOptions | boolean,
  ): void;
  close(): void;
  readonly readyState: number;
}

export type EventSourceFactory = (url: string) => EventSourceLike;

export interface SSETransportOptions {
  url: string;
  eventSourceFactory?: EventSourceFactory;
  /** Consecutive failed attempts before the transport gives up with state `failed` (default 5). */
  maxAttempts?: number;
  /** Delay before the first retry; doubles per consecutive failure (default 500). */
  baseDelayMs?: number;
  /** Upper bound for one retry delay (default 10000). */
  maxDelayMs?: number;
  /**
   * Called before each reconnect with the last seen seq. Return the URL to use (for signed
   * or expiring URLs). `?cursor=` is added to whatever it returns. Return `null` to stop
   * reconnecting (state `disconnected`, stop reason `resolver`, not a failure). A throw or
   * rejection counts as a failed attempt.
   */
  resolveUrl?: (lastSeq: number | null) => Promise<string | null> | string | null;
}

export const EVENT_SOURCE_CONNECTING = 0;
export const EVENT_SOURCE_OPEN = 1;
export const EVENT_SOURCE_CLOSED = 2;
