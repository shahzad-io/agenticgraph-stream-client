/**
 * Minimal Echo surface - consumer provides a configured Laravel Echo instance.
 * Echo is NOT bundled; this is type-only structural typing.
 */
export interface EchoChannel {
  listen(event: string, callback: (payload: unknown) => void): EchoChannel;
  stopListening(event: string, callback?: (payload: unknown) => void): EchoChannel;
  listenToAll?(callback: (eventName: string, payload: unknown) => void): EchoChannel;
  unsubscribe(): void;
}

export interface EchoInstance {
  channel(name: string): EchoChannel;
  leave(name: string): void;
  disconnect(): void;
}

export interface EchoTransportOptions {
  echo: EchoInstance;
  channel: string;
  /**
   * Event types to `.listen()` when `listenToAll` is unavailable.
   * Defaults to the closed framework + Prism set plus `stream_close`.
   */
  eventTypes?: string[];
}
