import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { SSETransport } from './sse-transport.js';
import type { EventSourceLike, SSETransportOptions } from './sse-types.js';
import { EVENT_SOURCE_CONNECTING } from './sse-types.js';
import type { StreamEvent } from '../types/events.js';

interface FakeEventSource extends EventSourceLike {
  closed: boolean;
  open(): void;
  fail(): void;
  send(eventType: string, data: unknown): void;
}

function createFakeEventSource(url: string): FakeEventSource {
  const listeners = new Map<string, (event: MessageEvent) => void>();

  const fake: FakeEventSource = {
    url,
    onopen: null,
    onerror: null,
    readyState: EVENT_SOURCE_CONNECTING,
    closed: false,
    addEventListener(type, listener) {
      listeners.set(type, listener);
    },
    removeEventListener(type, listener) {
      if (listeners.get(type) === listener) {
        listeners.delete(type);
      }
    },
    close() {
      fake.closed = true;
    },
    open() {
      fake.onopen?.(new Event('open'));
    },
    fail() {
      fake.onerror?.(new Event('error'));
    },
    send(eventType, data) {
      listeners.get(eventType)?.({ type: eventType, data: JSON.stringify(data) } as MessageEvent);
    },
  };

  return fake;
}

function wire(seq: number, eventType = 'text_delta', payload: unknown = { delta: String(seq), message_id: 'm', timestamp: 1 }) {
  return {
    eventType,
    body: {
      stream_id: 's',
      execution_id: 'e',
      node_name: null,
      span_id: null,
      parent_span_id: null,
      seq,
      node_seq: null,
      occurred_at: '2024-01-01T00:00:00+00:00',
      payload,
    },
  };
}

function setup(options: Partial<SSETransportOptions> = {}) {
  const fakes: FakeEventSource[] = [];
  const received: StreamEvent[] = [];
  const states: string[] = [];
  const stops: [string, string | null | undefined][] = [];
  const transport = new SSETransport({
    url: 'http://localhost/streams/s1',
    eventSourceFactory: (url) => {
      const fake = createFakeEventSource(url);
      fakes.push(fake);

      return fake;
    },
    ...options,
  });

  transport.onEvent((event) => received.push(event));
  transport.onConnectionStateChange((state, stopReason) => {
    states.push(state);

    if (state === 'disconnected' || state === 'failed') {
      stops.push([state, stopReason]);
    }
  });

  const send = (fake: FakeEventSource, seq: number, eventType?: string, payload?: unknown) => {
    const { eventType: type, body } = wire(seq, eventType, payload);
    fake.send(type, body);
  };

  return { transport, fakes, received, states, stops, send, last: () => fakes[fakes.length - 1]! };
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.spyOn(Math, 'random').mockReturnValue(0.5);
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe('SSETransport', () => {
  it('reconstructs StreamEvent from SSE event type + JSON data and tracks seq', async () => {
    const { transport, received, send, last } = setup();

    await transport.connect();
    last().open();
    send(last(), 10);

    expect(received).toHaveLength(1);
    expect(received[0]).toMatchObject({ eventType: 'text_delta', seq: 10 });
    expect(transport.getLastSeq()).toBe(10);

    await transport.disconnect();
    expect(transport.getLastSeq()).toBeNull();
  });

  it('delivers stream_start and stream_end around the step events of a multi-step run', async () => {
    const { transport, received, send, last } = setup();

    await transport.connect();
    last().open();
    ['stream_start', 'step_start', 'step_finish', 'step_start', 'step_finish', 'stream_end'].forEach((type, i) => send(last(), i + 1, type, { id: String(i), timestamp: 1 }));

    expect(received.map((event) => event.eventType)).toEqual(['stream_start', 'step_start', 'step_finish', 'step_start', 'step_finish', 'stream_end']);
  });

  it('disconnect resets lastSeq so a subsequent connect sends no cursor', async () => {
    const { transport, fakes, send, last } = setup();

    await transport.connect();
    last().open();
    send(last(), 3);
    await transport.disconnect();
    await transport.connect();

    expect(fakes).toHaveLength(2);
    expect(fakes[1]!.url).toBe('http://localhost/streams/s1?custom_events=wrap');
  });

  it('reports connected only on the real open event', async () => {
    const { transport, last, states } = setup();

    await transport.connect();
    expect(transport.getConnectionState()).toBe('connecting');

    last().open();
    expect(transport.getConnectionState()).toBe('connected');
    expect(states).toEqual(['connecting', 'connected']);
  });

  it('closes the EventSource at once on an error and reconnects itself with the cursor', async () => {
    const { transport, fakes, send, last } = setup();

    await transport.connect();
    last().open();
    send(last(), 4);
    last().fail();

    expect(fakes[0]!.closed).toBe(true);
    expect(transport.getConnectionState()).toBe('reconnecting');

    await vi.advanceTimersByTimeAsync(375);

    expect(fakes).toHaveLength(2);
    expect(fakes[1]!.url).toBe('http://localhost/streams/s1?custom_events=wrap&cursor=4');
  });

  it('backs off exponentially with jitter up to maxDelayMs', async () => {
    const { transport, fakes, last } = setup({ baseDelayMs: 100, maxDelayMs: 300, maxAttempts: 10 });
    const random = vi.mocked(Math.random);

    await transport.connect();

    // delay = exp / 2 + random * exp / 2, exp = min(maxDelayMs, base * 2^(n-1))
    const expected = [
      { random: 0, delay: 50 },
      { random: 1, delay: 200 },
      { random: 0, delay: 150 },
      { random: 0.5, delay: 225 },
    ];

    for (const [index, step] of expected.entries()) {
      random.mockReturnValue(step.random);
      last().fail();
      await vi.advanceTimersByTimeAsync(step.delay - 1);
      expect(fakes).toHaveLength(index + 1);
      await vi.advanceTimersByTimeAsync(1);
      expect(fakes).toHaveLength(index + 2);
    }
  });

  it('gives up with state failed after maxAttempts consecutive failures', async () => {
    const { transport, fakes, last, states } = setup({ maxAttempts: 2 });

    await transport.connect();
    last().fail();
    await vi.advanceTimersByTimeAsync(10_000);
    last().fail();
    await vi.advanceTimersByTimeAsync(10_000);
    last().fail();
    await vi.advanceTimersByTimeAsync(10_000);

    expect(fakes).toHaveLength(3);
    expect(transport.getConnectionState()).toBe('failed');
    expect(states.at(-1)).toBe('failed');
  });

  it('resets the failure counter on a successful open', async () => {
    const { transport, fakes, last } = setup({ maxAttempts: 1 });

    await transport.connect();
    last().fail();
    await vi.advanceTimersByTimeAsync(10_000);
    last().open();
    last().fail();
    await vi.advanceTimersByTimeAsync(10_000);

    expect(fakes).toHaveLength(3);
    expect(transport.getConnectionState()).toBe('reconnecting');
  });

  it('treats control reconnect as planned: resets the counter and waits after_ms', async () => {
    const { transport, fakes, received, send, last } = setup({ maxAttempts: 1 });

    await transport.connect();
    last().fail();
    await vi.advanceTimersByTimeAsync(10_000);
    send(last(), 7);
    last().send('stream_reconnect', { control: 'reconnect', after_ms: 1500 });

    expect(fakes[1]!.closed).toBe(true);
    expect(transport.getLastSeq()).toBe(7);
    expect(received).toHaveLength(1);

    await vi.advanceTimersByTimeAsync(1499);
    expect(fakes).toHaveLength(2);
    await vi.advanceTimersByTimeAsync(1);
    expect(fakes[2]!.url).toBe('http://localhost/streams/s1?custom_events=wrap&cursor=7');

    last().fail();
    await vi.advanceTimersByTimeAsync(10_000);
    expect(fakes).toHaveLength(4);
  });

  it.each(['completed', 'failed'])('stops on a %s terminal', async (status) => {
    const { transport, fakes, send, last } = setup();

    await transport.connect();
    last().open();
    send(last(), 9, 'execution_terminal', { status, final_state: null });

    expect(fakes[0]!.closed).toBe(true);
    expect(transport.getConnectionState()).toBe('disconnected');

    last().fail();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(fakes).toHaveLength(1);
  });

  it('guard: keeps the connection on an interrupted terminal', async () => {
    const { transport, fakes, received, send, last } = setup();

    await transport.connect();
    last().open();
    send(last(), 9, 'execution_terminal', { status: 'interrupted', final_state: null });
    send(last(), 10, 'stream_resume', { from_seq: 9, resumed_at_seq: 10, gap_events: [], checkpoint_id: 'cp' });

    expect(fakes[0]!.closed).toBe(false);
    expect(transport.getConnectionState()).toBe('connected');
    expect(received.map((event) => event.eventType)).toEqual(['execution_terminal', 'stream_resume']);
  });

  it('delivers control replay_unavailable without moving lastSeq, then stops', async () => {
    const { transport, fakes, received, send, last } = setup();

    await transport.connect();
    last().open();
    send(last(), 3);
    last().send('stream_replay_unavailable', { control: 'replay_unavailable', cursor: 3, final_state: null });

    expect(received.at(-1)).toEqual({
      eventType: 'stream_replay_unavailable',
      payload: { cursor: 3, final_state: null },
    });
    expect(transport.getLastSeq()).toBe(3);
    expect(transport.getConnectionState()).toBe('disconnected');

    await vi.advanceTimersByTimeAsync(60_000);
    expect(fakes).toHaveLength(1);
  });

  it('guard: swallows heartbeats without moving lastSeq', async () => {
    const { transport, received, send, last } = setup();

    await transport.connect();
    last().open();
    send(last(), 2);
    last().send('stream_heartbeat', { control: 'heartbeat', timestamp: '2024-01-01T00:00:00Z' });

    expect(received).toHaveLength(1);
    expect(transport.getLastSeq()).toBe(2);
  });

  it('drops stored events at or below the last seen seq', async () => {
    const { transport, received, send, last } = setup();

    await transport.connect();
    last().open();

    for (const seq of [1, 2, 2, 1, 3]) {
      send(last(), seq);
    }

    expect(received.map((event) => ('seq' in event ? event.seq : null))).toEqual([1, 2, 3]);
  });

  it('calls resolveUrl with lastSeq before each reconnect and adds the cursor to its URL', async () => {
    const resolveUrl = vi.fn(async (lastSeq: number | null) => `http://localhost/streams/s1?signature=sig-${lastSeq}`);
    const { transport, fakes, send, last } = setup({ resolveUrl });

    await transport.connect();
    last().open();
    send(last(), 4);
    last().fail();
    await vi.advanceTimersByTimeAsync(10_000);

    expect(resolveUrl).toHaveBeenCalledWith(4);
    expect(fakes[0]!.url).toBe('http://localhost/streams/s1?custom_events=wrap');
    expect(fakes[1]!.url).toBe('http://localhost/streams/s1?signature=sig-4&custom_events=wrap&cursor=4');
  });

  it('counts a rejected resolveUrl as a failed attempt', async () => {
    const resolveUrl = vi.fn(async () => {
      throw new Error('signing failed');
    });
    const { transport, fakes, last } = setup({ resolveUrl, maxAttempts: 2 });

    await transport.connect();
    last().fail();
    await vi.advanceTimersByTimeAsync(60_000);

    expect(resolveUrl).toHaveBeenCalledTimes(2);
    expect(fakes).toHaveLength(1);
    expect(transport.getConnectionState()).toBe('failed');
    expect(transport.getStopReason()).toBe('failed');
  });

  it('stops cleanly when resolveUrl returns null: no failure, no further attempts', async () => {
    const resolveUrl = vi.fn(async () => null);
    const { transport, fakes, send, stops, last } = setup({ resolveUrl, maxAttempts: 5 });

    await transport.connect();
    last().open();
    send(last(), 4);
    last().fail();
    await vi.advanceTimersByTimeAsync(60_000);

    expect(resolveUrl).toHaveBeenCalledTimes(1);
    expect(resolveUrl).toHaveBeenCalledWith(4);
    expect(fakes).toHaveLength(1);
    expect(fakes[0]!.closed).toBe(true);
    expect(transport.getConnectionState()).toBe('disconnected');
    expect(transport.getStopReason()).toBe('resolver');
    expect(stops).toEqual([['disconnected', 'resolver']]);
  });

  it('reports the stop reason for each stop path', async () => {
    const terminal = setup();
    await terminal.transport.connect();
    terminal.last().open();
    send(terminal, 1, 'execution_terminal', { status: 'completed', final_state: null });

    const replay = setup();
    await replay.transport.connect();
    replay.last().open();
    replay.last().send('stream_replay_unavailable', { control: 'replay_unavailable', cursor: 3, final_state: null });

    const failed = setup({ maxAttempts: 0 });
    await failed.transport.connect();
    failed.last().fail();

    const manual = setup();
    await manual.transport.connect();
    manual.last().open();
    await manual.transport.disconnect();

    expect([terminal, replay, failed, manual].map(({ transport, stops }) => [transport.getStopReason(), stops])).toEqual([
      ['terminal', [['disconnected', 'terminal']]],
      ['replay_unavailable', [['disconnected', 'replay_unavailable']]],
      ['failed', [['failed', 'failed']]],
      ['manual', [['disconnected', 'manual']]],
    ]);

    function send(ctx: ReturnType<typeof setup>, seq: number, eventType: string, payload: unknown) {
      ctx.send(ctx.last(), seq, eventType, payload);
    }
  });

  it('keeps the first stop reason when disconnect() is called on a stopped transport', async () => {
    const terminal = setup();
    await terminal.transport.connect();
    terminal.last().open();
    terminal.send(terminal.last(), 1, 'execution_terminal', { status: 'completed', final_state: null });
    await terminal.transport.disconnect();

    const failed = setup({ maxAttempts: 0 });
    await failed.transport.connect();
    failed.last().fail();
    await failed.transport.disconnect();

    expect(terminal.transport.getStopReason()).toBe('terminal');
    expect(terminal.stops).toEqual([['disconnected', 'terminal']]);
    expect(failed.transport.getStopReason()).toBe('failed');
    expect(failed.stops).toEqual([['failed', 'failed']]);
  });

  it('clears the stop reason when the transport starts again', async () => {
    const { transport, states, last } = setup({ maxAttempts: 0 });

    expect(transport.getStopReason()).toBeNull();

    await transport.connect();
    last().fail();
    expect(transport.getStopReason()).toBe('failed');

    await transport.reconnect();
    expect(transport.getStopReason()).toBeNull();
    expect(states.at(-1)).toBe('reconnecting');
  });

  it('manual reconnect after failed opens again with the cursor', async () => {
    const { transport, fakes, send, last } = setup({ maxAttempts: 0 });

    await transport.connect();
    last().open();
    send(last(), 5);
    last().fail();
    expect(transport.getConnectionState()).toBe('failed');

    await transport.reconnect();

    expect(fakes[1]!.url).toBe('http://localhost/streams/s1?custom_events=wrap&cursor=5');
    expect(transport.getConnectionState()).toBe('reconnecting');
  });
});

describe('SSETransport custom events', () => {
  it('asks for wrapped custom events on the first request', async () => {
    const { transport, fakes } = setup();

    await transport.connect();

    expect(new URL(fakes[0]!.url).searchParams.get('custom_events')).toBe('wrap');
  });

  it('keeps the parameter on a cursor reconnect', async () => {
    const { transport, fakes, send, last } = setup();

    await transport.connect();
    last().open();
    send(last(), 7);
    last().fail();
    await vi.advanceTimersByTimeAsync(10_000);

    const url = new URL(fakes[1]!.url);
    expect(url.searchParams.get('cursor')).toBe('7');
    expect(url.searchParams.get('custom_events')).toBe('wrap');
  });

  it('adds the parameter to a resolveUrl URL and keeps its query', async () => {
    const { transport, fakes, last } = setup({ resolveUrl: () => 'http://localhost/s?sig=abc' });

    await transport.connect();
    last().open();
    last().fail();
    await vi.advanceTimersByTimeAsync(10_000);

    const url = new URL(fakes[1]!.url);
    expect(url.searchParams.get('sig')).toBe('abc');
    expect(url.searchParams.get('custom_events')).toBe('wrap');
  });

  it('keeps the parameter on a manual reconnect', async () => {
    const { transport, fakes } = setup({ maxAttempts: 0 });

    await transport.connect();
    fakes[0]!.fail();
    await transport.reconnect();

    expect(new URL(fakes[1]!.url).searchParams.get('custom_events')).toBe('wrap');
  });

  it('keeps a relative URL relative', async () => {
    const { transport, fakes } = setup({ url: '/streams/s1?a=1' });

    await transport.connect();

    expect(fakes[0]!.url).toBe('/streams/s1?a=1&custom_events=wrap');
  });

  it('turns a custom_event frame back into the author event type', async () => {
    const { transport, received, last } = setup();

    await transport.connect();
    last().open();
    const { body } = wire(3, 'x', { sources: [1] });
    last().send('custom_event', { ...body, event_type: 'sources' });

    expect(received).toHaveLength(1);
    expect(received[0]).toMatchObject({ eventType: 'sources', seq: 3, payload: { sources: [1] } });
    expect(received[0]).not.toHaveProperty('event_type');
    expect(transport.getLastSeq()).toBe(3);
  });

  it('unwraps an author type that is itself called custom_event', async () => {
    const { transport, received, last } = setup();

    await transport.connect();
    last().open();
    const { body } = wire(1);
    last().send('custom_event', { ...body, event_type: 'custom_event' });

    expect(received[0]).toMatchObject({ eventType: 'custom_event' });
  });

  it('drops a custom_event frame without an event_type', async () => {
    const { transport, received, last } = setup();

    await transport.connect();
    last().open();
    last().send('custom_event', wire(1).body);

    expect(received).toHaveLength(0);
  });

  it('ignores custom events under their own name (server without the parameter)', async () => {
    const { transport, received, send, last } = setup();

    await transport.connect();
    last().open();
    send(last(), 1, 'sources');
    send(last(), 2);

    expect(received.map((event) => event.eventType)).toEqual(['text_delta']);
  });
});
