import { act, renderHook, waitFor } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { MockTransport } from '@agenticgraph/stream-client';
import {
  makeExecutionTerminalEvent,
  makeNodeCompleteEvent,
  makeNodeInterruptEvent,
  makeNodeStartEvent,
  makeStreamCloseEvent,
  makeStreamResumeEvent,
  makeThinkingDeltaEvent,
  makeTextDeltaEvent,
  resetSeqCounter,
} from '../../stream-client/src/testing/event-fixtures.js';
import { activeNodeSelector, thinkingSelector } from '@agenticgraph/stream-client';
import { useAgenticStream } from './useAgenticStream.js';
import { useStreamSelector } from './useStreamSelector.js';

async function setup(onResume?: (decision: unknown, checkpoint: unknown) => Promise<void>) {
  const transport = new MockTransport();
  const hook = renderHook(() => useAgenticStream({ transport, onResume }));

  await waitFor(() => {
    expect(hook.result.current.client).not.toBeNull();
  });

  return { transport, hook };
}

describe('useAgenticStream (React)', () => {
  resetSeqCounter();

  it('accumulates text_delta only, not thinking_delta', async () => {
    const { transport, hook } = await setup();

    act(() => {
      transport.emit(makeTextDeltaEvent('hello'));
      transport.emit(makeThinkingDeltaEvent('secret'));
      transport.emit(makeTextDeltaEvent(' world'));
    });

    await waitFor(() => {
      expect(hook.result.current.text).toBe('hello world');
    });
  });

  it('reflects status for both terminal vocabularies', async () => {
    const broadcast = await setup();
    act(() => broadcast.transport.emit(makeStreamCloseEvent('success')));
    await waitFor(() => expect(broadcast.hook.result.current.status).toBe('complete'));

    const sse = await setup();
    act(() => sse.transport.emit(makeExecutionTerminalEvent('completed')));
    await waitFor(() => expect(sse.hook.result.current.status).toBe('complete'));
  });

  it('populates interrupt on node_interrupt', async () => {
    const { transport, hook } = await setup();

    act(() => transport.emit(makeNodeInterruptEvent()));

    await waitFor(() => {
      expect(hook.result.current.status).toBe('interrupted');
      expect(hook.result.current.interrupt).toEqual({
        checkpointId: 'cp-123',
        reason: 'Needs approval',
        node: 'alpha',
        payload: {
          node_name: 'alpha',
          reason: 'Needs approval',
          checkpoint_id: 'cp-123',
        },
      });
    });
  });

  it('updates lastEvent', async () => {
    const { transport, hook } = await setup();
    const event = makeTextDeltaEvent('x');

    act(() => transport.emit(event));

    await waitFor(() => {
      expect(hook.result.current.lastEvent?.eventType).toBe('text_delta');
    });
  });

  it('cleans up on unmount', async () => {
    const transport = new MockTransport();
    const disconnectSpy = vi.spyOn(transport, 'disconnect');
    const { unmount } = renderHook(() => useAgenticStream({ transport }));

    await waitFor(() => expect(disconnectSpy).not.toHaveBeenCalled());
    unmount();
    expect(disconnectSpy).toHaveBeenCalled();
  });

  it('resume calls onResume and continued events flow', async () => {
    const onResume = vi.fn().mockResolvedValue(undefined);
    const { transport, hook } = await setup(onResume);

    act(() => transport.emit(makeNodeInterruptEvent()));
    await waitFor(() => expect(hook.result.current.status).toBe('interrupted'));

    await act(async () => {
      await hook.result.current.resume('approve');
    });

    expect(onResume).toHaveBeenCalledWith('approve', {
      checkpointId: 'cp-123',
      reason: 'Needs approval',
      node: 'alpha',
      payload: {
        node_name: 'alpha',
        reason: 'Needs approval',
        checkpoint_id: 'cp-123',
      },
    });

    act(() => {
      transport.emit(makeStreamResumeEvent());
      transport.emit(makeTextDeltaEvent('after'));
      transport.emit(makeExecutionTerminalEvent('completed'));
    });

    await waitFor(() => {
      expect(hook.result.current.text).toBe('after');
      expect(hook.result.current.status).toBe('complete');
    });
  });

  it('resume without onResume throws', async () => {
    const { hook } = await setup();

    await expect(hook.result.current.resume('x')).rejects.toThrow(/onResume/);
  });

  it('accumulates 200 text_delta events correctly', async () => {
    const { transport, hook } = await setup();

    act(() => {
      for (let i = 0; i < 200; i += 1) {
        transport.emit(makeTextDeltaEvent('.'));
      }
    });

    await waitFor(() => {
      expect(hook.result.current.text).toHaveLength(200);
    });
  });
});

describe('mirror canonical sequence', () => {
  it('react: text=ab, status=interrupted for shared event sequence', async () => {
    const { transport, hook } = await setup();

    act(() => {
      transport.emit(makeTextDeltaEvent('a'));
      transport.emit(makeThinkingDeltaEvent('ignored'));
      transport.emit(makeTextDeltaEvent('b'));
      transport.emit(makeNodeInterruptEvent());
    });

    await waitFor(() => {
      expect(hook.result.current.text).toBe('ab');
      expect(hook.result.current.status).toBe('interrupted');
      expect(hook.result.current.interrupt?.checkpointId).toBe('cp-123');
    });
  });
});

describe('useStreamSelector (React)', () => {
  it('activeNodeSelector reflects running nodes', async () => {
    const transport = new MockTransport();
    const { result } = renderHook(() => {
      const stream = useAgenticStream({ transport });
      const activeNodes = useStreamSelector(stream.client, activeNodeSelector);

      return { stream, activeNodes };
    });

    await waitFor(() => expect(result.current.stream.client).not.toBeNull());

    act(() => {
      transport.emit(makeNodeStartEvent('alpha'));
      transport.emit(makeNodeStartEvent('beta'));
      transport.emit(makeNodeCompleteEvent('alpha'));
    });

    await waitFor(() => {
      expect(result.current.activeNodes).toEqual(['beta']);
    });
  });

  it('thinkingSelector accumulates thinking separately from text', async () => {
    const transport = new MockTransport();
    const { result } = renderHook(() => {
      const stream = useAgenticStream({ transport });
      const thinking = useStreamSelector(stream.client, thinkingSelector);

      return { stream, thinking };
    });

    await waitFor(() => expect(result.current.stream.client).not.toBeNull());

    act(() => {
      transport.emit(makeThinkingDeltaEvent('think'));
      transport.emit(makeTextDeltaEvent('say'));
    });

    await waitFor(() => {
      expect(result.current.thinking).toBe('think');
      expect(result.current.stream.text).toBe('say');
    });
  });

  it('custom selector works', async () => {
    const transport = new MockTransport();
    const { result } = renderHook(() => {
      const stream = useAgenticStream({ transport });
      const count = useStreamSelector(stream.client, (events) => events.length);

      return { stream, count };
    });

    await waitFor(() => expect(result.current.stream.client).not.toBeNull());

    act(() => {
      transport.emit(makeTextDeltaEvent('a'));
      transport.emit(makeTextDeltaEvent('b'));
    });

    await waitFor(() => expect(result.current.count).toBe(2));
  });
});

import { SSETransport } from '@agenticgraph/stream-client';
import type { EventSourceLike } from '@agenticgraph/stream-client';
import { EVENT_SOURCE_OPEN } from '../../stream-client/src/transports/sse-types.js';
import { useEffect, useRef } from 'react';

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
    dispatch(eventType: string, data: unknown, lastEventId = '1') {
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

const wireBody = (seq: number, payload: Record<string, unknown>) => ({
  stream_id: 'stream-sse-react',
  execution_id: 'exec-sse-react',
  node_name: 'generate_draft',
  span_id: null,
  parent_span_id: null,
  seq,
  node_seq: seq,
  occurred_at: '2024-01-01T00:00:00+00:00',
  payload,
});

describe('useAgenticStream + SSETransport (React)', () => {
  resetSeqCounter();

  it('accumulates text_delta and maps execution_terminal.completed to complete', async () => {
    let fake: FakeEventSource | null = null;
    const transport = new SSETransport({
      url: '/agentic-graph/streams/stream-sse-react',
      eventSourceFactory: (url) => {
        fake = createFakeEventSource(url);

        return fake;
      },
    });

    const { result } = renderHook(() => useAgenticStream({ transport }));

    await waitFor(() => expect(result.current.client).not.toBeNull());

    act(() => {
      fake!.dispatch(
        'text_delta',
        wireBody(1, { delta: 'Hello ', message_id: 'm1', timestamp: 1 }),
        '1',
      );
      fake!.dispatch(
        'text_delta',
        wireBody(2, { delta: 'SSE', message_id: 'm1', timestamp: 2 }),
        '2',
      );
      fake!.dispatch(
        'node_interrupt',
        wireBody(3, {
          node_name: 'await_review',
          reason: 'Review',
          checkpoint_id: 'cp-sse',
          interrupt_payload: { draft: 'Hello SSE', topic: 'test' },
        }),
        '3',
      );
    });

    await waitFor(() => {
      expect(result.current.text).toBe('Hello SSE');
      expect(result.current.status).toBe('interrupted');
      expect(result.current.interrupt?.payload.interrupt_payload).toMatchObject({
        draft: 'Hello SSE',
      });
    });

    act(() => {
      fake!.dispatch(
        'execution_terminal',
        {
          stream_id: 'stream-sse-react',
          execution_id: 'exec-sse-react',
          node_name: null,
          span_id: null,
          parent_span_id: null,
          seq: 4,
          node_seq: null,
          occurred_at: '2024-01-01T00:00:00+00:00',
          payload: { status: 'completed', final_state: null },
        },
        '4',
      );
    });

    await waitFor(() => expect(result.current.status).toBe('complete'));
  });
});

describe('stream text render diagnostic (React)', () => {
  it('delivers N incremental deltas to hook text but may batch React renders synchronously', async () => {
    const transport = new MockTransport();
    const textRenderLog: string[] = [];

    const { result } = renderHook(() => {
      const stream = useAgenticStream({ transport });
      const deltaCount = useStreamSelector(
        stream.client,
        (events) =>
          events.filter((event) => event.eventType === 'text_delta').length,
      );
      const prevText = useRef('');

      useEffect(() => {
        if (stream.text !== prevText.current) {
          textRenderLog.push(stream.text);
          prevText.current = stream.text;
        }
      }, [stream.text]);

      return { stream, deltaCount };
    });

    await waitFor(() => expect(result.current.stream.client).not.toBeNull());

    act(() => {
      for (let index = 0; index < 12; index += 1) {
        transport.emit(makeTextDeltaEvent(`t${index}`));
      }
    });

    await waitFor(() =>
      expect(result.current.stream.text).toBe('t0t1t2t3t4t5t6t7t8t9t10t11'),
    );

    expect(result.current.deltaCount).toBe(12);
    expect(textRenderLog.at(-1)).toBe('t0t1t2t3t4t5t6t7t8t9t10t11');
    expect(textRenderLog.length).toBeLessThan(12);
  });

  it('renders more incrementally when deltas arrive in separate async turns', async () => {
    const transport = new MockTransport();
    const textRenderLog: string[] = [];

    const { result } = renderHook(() => {
      const stream = useAgenticStream({ transport });
      const prevText = useRef('');

      useEffect(() => {
        if (stream.text !== prevText.current) {
          textRenderLog.push(stream.text);
          prevText.current = stream.text;
        }
      }, [stream.text]);

      return stream;
    });

    await waitFor(() => expect(result.current.client).not.toBeNull());

    for (let index = 0; index < 6; index += 1) {
      await act(async () => {
        await new Promise((resolve) => {
          setTimeout(resolve, 5);
        });
        transport.emit(makeTextDeltaEvent(`d${index}`));
      });
    }

    await waitFor(() =>
      expect(result.current.text).toBe('d0d1d2d3d4d5'),
    );

    expect(textRenderLog.length).toBeGreaterThan(1);
  });
});
