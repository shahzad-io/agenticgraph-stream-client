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
  /** Append `?cursor=` on reconnect (default true). */
  useCursorQuery?: boolean;
}

export const EVENT_SOURCE_CONNECTING = 0;
export const EVENT_SOURCE_OPEN = 1;
export const EVENT_SOURCE_CLOSED = 2;
