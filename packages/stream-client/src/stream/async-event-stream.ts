import type { KnownStreamEvent } from '../types/events.js';
import type { StreamEvent } from '../types/events.js';

export type StreamEventHandler = (event: StreamEvent) => void;

interface ListenerEntry {
  filter: string | undefined;
  handler: StreamEventHandler;
}

interface IteratorSink {
  push(event: StreamEvent): void;
  close(): void;
}

/**
 * Single internal async event stream - the source of truth for all consumption APIs.
 * Transport events enter via {@link dispatch}; `.on`, `.subscribe`, and `.events()` all read from here.
 */
export class AsyncEventStream {
  private listeners = new Map<symbol, ListenerEntry>();

  private iteratorSinks = new Set<IteratorSink>();

  private closed = false;

  dispatch(event: StreamEvent): void {
    if (this.closed) {
      return;
    }

    for (const entry of this.listeners.values()) {
      if (entry.filter === undefined || event.eventType === entry.filter) {
        entry.handler(event);
      }
    }

    for (const sink of this.iteratorSinks) {
      sink.push(event);
    }
  }

  addListener(filter: string | undefined, handler: StreamEventHandler): () => void {
    const id = Symbol('listener');
    this.listeners.set(id, { filter, handler });

    return () => {
      this.listeners.delete(id);
    };
  }

  removeListener(filter: string | undefined, handler: StreamEventHandler): boolean {
    for (const [id, entry] of this.listeners) {
      if (entry.filter === filter && entry.handler === handler) {
        this.listeners.delete(id);

        return true;
      }
    }

    return false;
  }

  /**
   * Returns a fresh async iterable. Each iterator receives events from the moment it is created
   * forward (no replay). Multiple concurrent `.events()` iterators are supported - each gets
   * every event dispatched after its creation.
   */
  createIterable(): AsyncIterable<StreamEvent> {
    const stream = this;
    const queue: StreamEvent[] = [];
    let wakeup: (() => void) | null = null;
    let done = false;

    const sink: IteratorSink = {
      push(event: StreamEvent) {
        queue.push(event);
        wakeup?.();
      },
      close() {
        done = true;
        wakeup?.();
      },
    };

    this.iteratorSinks.add(sink);

    return {
      [Symbol.asyncIterator]() {
        return {
          async next(): Promise<IteratorResult<StreamEvent>> {
            while (queue.length === 0 && !done) {
              await new Promise<void>((resolve) => {
                wakeup = resolve;
              });
            }

            if (queue.length > 0) {
              return { value: queue.shift()!, done: false };
            }

            return { value: undefined as unknown as StreamEvent, done: true };
          },
          async return(): Promise<IteratorResult<StreamEvent>> {
            stream.iteratorSinks.delete(sink);
            done = true;
            wakeup?.();

            return { value: undefined as unknown as StreamEvent, done: true };
          },
        };
      },
    };
  }

  close(): void {
    this.closed = true;

    for (const sink of this.iteratorSinks) {
      sink.close();
    }

    this.listeners.clear();
  }
}

export type KnownEventHandler<T extends KnownStreamEvent['eventType']> = (
  event: Extract<KnownStreamEvent, { eventType: T }>,
) => void;
