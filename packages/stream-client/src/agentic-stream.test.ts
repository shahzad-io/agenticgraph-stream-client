import { describe, expect, it, vi } from 'vitest';
import { AgenticStream } from './agentic-stream.js';
import { MockTransport } from './transports/mock-transport.js';
import {
  makeExecutionTerminalEvent,
  makeNodeInterruptEvent,
  makeStreamCloseEvent,
  makeStreamErrorEvent,
  makeTextDeltaEvent,
  resetSeqCounter,
} from './testing/event-fixtures.js';
import type { StreamEvent } from './types/events.js';
import type { StreamStatus } from './types/status.js';

describe('AgenticStream', () => {
  resetSeqCounter();

  it('connects and receives events via .on()', async () => {
    const transport = new MockTransport();
    const stream = await AgenticStream.connect({ transport });
    const received: string[] = [];

    stream.on((event: StreamEvent) => {
      received.push(event.eventType);
    });

    transport.emit(makeTextDeltaEvent('hi'));
    transport.emit(makeTextDeltaEvent(' there'));

    expect(received).toEqual(['text_delta', 'text_delta']);

    await stream.disconnect();
  });

  it('filters by event type with typed .on()', async () => {
    const transport = new MockTransport();
    const stream = await AgenticStream.connect({ transport });
    const deltas: string[] = [];

    stream.on('text_delta', (event) => {
      deltas.push(event.payload.delta);
    });

    transport.emit(makeTextDeltaEvent('a'));
    transport.emit(makeNodeInterruptEvent());

    expect(deltas).toEqual(['a']);

    await stream.disconnect();
  });

  it('removes listeners with .off()', async () => {
    const transport = new MockTransport();
    const stream = await AgenticStream.connect({ transport });
    const handler = vi.fn();

    stream.on(handler);
    transport.emit(makeTextDeltaEvent('x'));
    stream.off(undefined, handler);
    transport.emit(makeTextDeltaEvent('y'));

    expect(handler).toHaveBeenCalledTimes(1);

    await stream.disconnect();
  });

  it('subscribe returns working unsubscribe', async () => {
    const transport = new MockTransport();
    const stream = await AgenticStream.connect({ transport });
    const handler = vi.fn();
    const unsub = stream.subscribe('text_delta', handler);

    transport.emit(makeTextDeltaEvent('one'));
    unsub();
    transport.emit(makeTextDeltaEvent('two'));

    expect(handler).toHaveBeenCalledTimes(1);

    await stream.disconnect();
  });

  it('.events() async-iterates the same events as .on()', async () => {
    const transport = new MockTransport();
    const stream = await AgenticStream.connect({ transport });
    const fromOn: string[] = [];
    const fromIterator: string[] = [];

    stream.on((event: StreamEvent) => {
      fromOn.push(event.eventType);
    });

    const iter = stream.events()[Symbol.asyncIterator]();

    transport.emit(makeTextDeltaEvent('a'));
    transport.emit(makeTextDeltaEvent('b'));

    const first = await iter.next();
    const second = await iter.next();

    fromIterator.push(first.value!.eventType, second.value!.eventType);

    expect(fromOn).toEqual(['text_delta', 'text_delta']);
    expect(fromIterator).toEqual(['text_delta', 'text_delta']);

    await stream.disconnect();
  });

  it('proves one source of truth — one emit dispatches once to multiple consumers', async () => {
    const transport = new MockTransport();
    const stream = await AgenticStream.connect({ transport });
    let onAllCount = 0;
    let onTypedCount = 0;
    let iterCount = 0;

    stream.on(() => {
      onAllCount += 1;
    });
    stream.on('text_delta', () => {
      onTypedCount += 1;
    });

    const iter = stream.events()[Symbol.asyncIterator]();

    transport.emit(makeTextDeltaEvent('once'));

    await iter.next();

    iterCount = 1;

    expect(onAllCount).toBe(1);
    expect(onTypedCount).toBe(1);
    expect(iterCount).toBe(1);

    await stream.disconnect();
  });
});

describe('status normalization', () => {
  it('maps stream_close.success to complete', async () => {
    const transport = new MockTransport();
    const stream = await AgenticStream.connect({ transport });

    transport.emit(makeStreamCloseEvent('success'));

    expect(stream.status).toBe('complete');

    await stream.disconnect();
  });

  it('maps execution_terminal.completed to complete', async () => {
    const transport = new MockTransport();
    const stream = await AgenticStream.connect({ transport });

    transport.emit(makeExecutionTerminalEvent('completed'));

    expect(stream.status).toBe('complete');

    await stream.disconnect();
  });

  it('maps stream_close.cancelled to interrupted', async () => {
    const transport = new MockTransport();
    const stream = await AgenticStream.connect({ transport });

    transport.emit(makeStreamCloseEvent('cancelled'));

    expect(stream.status).toBe('interrupted');

    await stream.disconnect();
  });

  it('maps execution_terminal.interrupted to interrupted', async () => {
    const transport = new MockTransport();
    const stream = await AgenticStream.connect({ transport });

    transport.emit(makeExecutionTerminalEvent('interrupted'));

    expect(stream.status).toBe('interrupted');

    await stream.disconnect();
  });

  it('maps stream_close.error and execution_terminal.failed to error', async () => {
    const t1 = new MockTransport();
    const t2 = new MockTransport();
    const s1 = await AgenticStream.connect({ transport: t1 });
    const s2 = await AgenticStream.connect({ transport: t2 });

    t1.emit(makeStreamCloseEvent('error'));
    t2.emit(makeExecutionTerminalEvent('failed'));

    expect(s1.status).toBe('error');
    expect(s2.status).toBe('error');
    expect(s1.error?.kind).toBe('terminal_error');
    expect(s2.error?.kind).toBe('terminal_error');

    await s1.disconnect();
    await s2.disconnect();
  });

  it('maps node_interrupt to interrupted with interrupt populated', async () => {
    const transport = new MockTransport();
    const stream = await AgenticStream.connect({ transport });

    transport.emit(makeNodeInterruptEvent());

    expect(stream.status).toBe('interrupted');
    expect(stream.interrupt).toEqual({
      checkpointId: 'cp-123',
      reason: 'Needs approval',
      node: 'alpha',
      payload: {
        node_name: 'alpha',
        reason: 'Needs approval',
        checkpoint_id: 'cp-123',
      },
    });

    await stream.disconnect();
  });

  it('maps stream_error to error status', async () => {
    const transport = new MockTransport();
    const stream = await AgenticStream.connect({ transport });

    transport.emit(makeStreamErrorEvent());

    expect(stream.status).toBe('error');
    expect(stream.error?.kind).toBe('stream_error');

    await stream.disconnect();
  });

  it('notifies status change listeners', async () => {
    const transport = new MockTransport();
    const stream = await AgenticStream.connect({ transport });
    const statuses: string[] = [];

    stream.onStatusChange((status: StreamStatus) => {
      statuses.push(status);
    });

    transport.emit(makeTextDeltaEvent('x'));
    transport.emit(makeStreamCloseEvent('success'));

    expect(statuses).toContain('streaming');
    expect(statuses).toContain('complete');

    await stream.disconnect();
  });
});

describe('reconnect idempotency', () => {
  it('reconnect while connected is a no-op', async () => {
    const transport = new MockTransport();
    const stream = await AgenticStream.connect({ transport });
    const handler = vi.fn();

    stream.on(handler);
    await stream.reconnect();

    transport.emit(makeTextDeltaEvent('a'));

    expect(handler).toHaveBeenCalledTimes(1);

    await stream.disconnect();
  });

  it('reconnect after drop re-establishes and events arrive once', async () => {
    const transport = new MockTransport();
    const stream = await AgenticStream.connect({ transport });
    const handler = vi.fn();

    stream.on(handler);
    transport.simulateDrop();
    await stream.reconnect();

    transport.emit(makeTextDeltaEvent('after'));
    transport.emit(makeTextDeltaEvent('reconnect'));

    expect(handler).toHaveBeenCalledTimes(2);

    await stream.disconnect();
  });

  it('transport reconnect does not duplicate listeners', async () => {
    const transport = new MockTransport();
    await transport.connect();
    const handler = vi.fn();
    transport.onEvent(handler);

    await transport.reconnect();
    transport.emit(makeTextDeltaEvent('single'));

    expect(handler).toHaveBeenCalledTimes(1);
  });
});

describe('StreamInterrupt.payload', () => {
  it('surfaces the full node_interrupt payload including author-defined fields', async () => {
    const transport = new MockTransport();
    const stream = await AgenticStream.connect({ transport });

    transport.emit(
      makeNodeInterruptEvent({
        payload: {
          node_name: 'alpha',
          reason: 'Needs approval',
          checkpoint_id: 'cp-123',
          interrupt_payload: {
            draft: 'Review this text',
            custom: { records: [1, 2] },
          },
        },
      }),
    );

    expect(stream.interrupt?.payload).toEqual({
      node_name: 'alpha',
      reason: 'Needs approval',
      checkpoint_id: 'cp-123',
      interrupt_payload: {
        draft: 'Review this text',
        custom: { records: [1, 2] },
      },
    });
    expect(stream.interrupt?.checkpointId).toBe('cp-123');
    expect(stream.interrupt?.reason).toBe('Needs approval');
    expect(stream.interrupt?.node).toBe('alpha');

    await stream.disconnect();
  });

  it('surfaces nested interrupt_payload from wire through StreamInterrupt.payload', async () => {
    const transport = new MockTransport();
    const stream = await AgenticStream.connect({ transport });

    transport.emit(
      makeNodeInterruptEvent({
        payload: {
          node_name: 'await_review',
          reason: 'Review draft',
          checkpoint_id: 'cp-789',
          interrupt_payload: {
            topic: 'SSE',
            draft: 'Wire draft',
          },
        },
      }),
    );

    const nested = stream.interrupt?.payload.interrupt_payload as Record<string, unknown>;

    expect(nested).toMatchObject({ topic: 'SSE', draft: 'Wire draft' });

    await stream.disconnect();
  });

  it('surfaces execution_terminal interrupted meta as payload', async () => {
    const transport = new MockTransport();
    const stream = await AgenticStream.connect({ transport });

    transport.emit(
      makeExecutionTerminalEvent('interrupted', {
        checkpoint_id: 'cp-456',
        reason: 'Human gate',
        node_name: 'await_review',
        draft: 'from terminal',
      }),
    );

    expect(stream.interrupt?.payload).toMatchObject({
      checkpoint_id: 'cp-456',
      reason: 'Human gate',
      node_name: 'await_review',
      draft: 'from terminal',
    });

    await stream.disconnect();
  });

  it('surfaces replay_unavailable with string cursor from PHP wire payloads', async () => {
    const transport = new MockTransport();
    const stream = await AgenticStream.connect({ transport });

    transport.emit({
      eventType: 'stream_replay_unavailable',
      stream_id: 's',
      execution_id: 'e',
      node_name: null,
      span_id: null,
      parent_span_id: null,
      seq: 1,
      node_seq: null,
      occurred_at: '2024-01-01T00:00:00+00:00',
      payload: { cursor: '7' },
    } as StreamEvent);

    expect(stream.status).toBe('error');
    expect(stream.error).toMatchObject({
      kind: 'replay_unavailable',
      cursor: 7,
    });

    await stream.disconnect();
  });
});

describe('connect() awaits connected state', () => {
  it('resolves only after the transport reports connected', async () => {
    const transport = new MockTransport();
    transport.setConnectDelay(25);

    let resolved = false;
    const connectPromise = AgenticStream.connect({ transport }).then((stream) => {
      resolved = true;

      return stream;
    });

    await new Promise((resolve) => {
      setTimeout(resolve, 5);
    });
    expect(resolved).toBe(false);
    expect(transport.getConnectionState()).toBe('connecting');

    const stream = await connectPromise;

    expect(resolved).toBe(true);
    expect(transport.getConnectionState()).toBe('connected');

    await stream.disconnect();
  });

  it('kickoff-after-await receives events emitted after connect resolves', async () => {
    const transport = new MockTransport();
    transport.setConnectDelay(10);
    const received: string[] = [];

    const stream = await AgenticStream.connect({ transport });
    stream.on('text_delta', (event) => {
      received.push(event.payload.delta);
    });

    transport.emit(makeTextDeltaEvent('after-connect'));

    expect(received).toEqual(['after-connect']);

    await stream.disconnect();
  });
});
