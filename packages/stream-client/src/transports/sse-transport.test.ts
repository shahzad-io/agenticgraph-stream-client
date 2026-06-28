import { describe, expect, it, vi } from 'vitest';
import { SSETransport } from './sse-transport.js';
import type { EventSourceLike } from './sse-types.js';
import { EVENT_SOURCE_CLOSED, EVENT_SOURCE_OPEN } from './sse-types.js';

interface FakeEventSource extends EventSourceLike {
  dispatch(eventType: string, data: unknown, lastEventId?: string): void;
}

function createFakeEventSource(url: string): FakeEventSource {
  const listeners = new Map<string, (event: MessageEvent) => void>();

  return {
    url,
    onopen: null,
    onerror: null,
    readyState: EVENT_SOURCE_OPEN,
    addEventListener(type, listener) {
      listeners.set(type, listener);
    },
    removeEventListener(type, listener) {
      if (listeners.get(type) === listener) {
        listeners.delete(type);
      }
    },
    close: vi.fn(),
    dispatch(eventType: string, data: unknown, lastEventId = '10') {
      const listener = listeners.get(eventType);

      if (!listener) {
        return;
      }

      listener({
        type: eventType,
        data: JSON.stringify(data),
        lastEventId,
      } as MessageEvent);
    },
  };
}

describe('SSETransport', () => {
  it('reconstructs StreamEvent from SSE event type + JSON data', async () => {
    let fake: FakeEventSource | null = null;
    const transport = new SSETransport({
      url: '/agentic-graph/streams/s1',
      eventSourceFactory: (url) => {
        fake = createFakeEventSource(url);

        return fake;
      },
    });

    const received: unknown[] = [];
    transport.onEvent((event) => received.push(event));

    await transport.connect();

    fake!.dispatch('text_delta', {
      stream_id: 's',
      execution_id: 'e',
      node_name: null,
      span_id: null,
      parent_span_id: null,
      seq: 10,
      node_seq: 1,
      occurred_at: '2024-01-01T00:00:00+00:00',
      payload: { delta: 'sse', message_id: 'm', timestamp: 1 },
    });

    expect(received).toHaveLength(1);
    expect(received[0]).toMatchObject({ eventType: 'text_delta', seq: 10 });
    expect(transport.getLastSeq()).toBe(10);

    await transport.disconnect();
    expect(transport.getLastSeq()).toBeNull();
  });

  it('disconnect resets lastSeq so a subsequent connect sends no cursor', async () => {
    const urls: string[] = [];
    const fakes: FakeEventSource[] = [];

    const transport = new SSETransport({
      url: 'http://localhost/streams/s1',
      eventSourceFactory: (url) => {
        urls.push(url);
        const fake = createFakeEventSource(url);
        fakes.push(fake);

        return fake;
      },
    });

    transport.onEvent(() => {});
    await transport.connect();

    fakes[0]!.dispatch(
      'text_delta',
      {
        stream_id: 's',
        execution_id: 'e',
        node_name: null,
        span_id: null,
        parent_span_id: null,
        seq: 0,
        node_seq: 1,
        occurred_at: '2024-01-01T00:00:00+00:00',
        payload: { delta: 'x', message_id: 'm', timestamp: 1 },
      },
      '0',
    );

    expect(transport.getLastSeq()).toBe(0);

    await transport.disconnect();
    expect(transport.getLastSeq()).toBeNull();

    await transport.connect();

    expect(urls).toHaveLength(2);
    expect(urls[1]).toBe('http://localhost/streams/s1');
    expect(urls[1]).not.toContain('cursor=');
  });

  it('reconnect appends cursor query for gap-free replay without clearing lastSeq', async () => {
    const urls: string[] = [];
    const fakes: FakeEventSource[] = [];

    const transport = new SSETransport({
      url: 'http://localhost/streams/s1',
      eventSourceFactory: (url) => {
        urls.push(url);
        const fake = createFakeEventSource(url);
        fakes.push(fake);

        return fake;
      },
    });

    transport.onEvent(() => {});
    await transport.connect();

    fakes[0]!.dispatch(
      'text_delta',
      {
        stream_id: 's',
        execution_id: 'e',
        node_name: null,
        span_id: null,
        parent_span_id: null,
        seq: 5,
        node_seq: 1,
        occurred_at: '2024-01-01T00:00:00+00:00',
        payload: { delta: 'x', message_id: 'm', timestamp: 1 },
      },
      '5',
    );

    fakes[0]!.readyState = EVENT_SOURCE_CLOSED;
    fakes[0]!.onerror?.(new Event('error'));
    expect(transport.getConnectionState()).toBe('disconnected');
    expect(transport.getLastSeq()).toBe(5);

    await transport.reconnect();

    expect(urls[1]).toContain('cursor=5');
  });
});
