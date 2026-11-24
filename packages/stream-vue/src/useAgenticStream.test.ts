import { mount, flushPromises } from '@vue/test-utils';
import { defineComponent, nextTick, shallowRef } from 'vue';
import { describe, expect, it, vi } from 'vitest';
import { BroadcastTransport, MockTransport, SSETransport, type EventSourceLike, type StreamTransport } from '@agenticgraph/stream-client';
import { fakeEcho, fakeRoutes, loadRuns } from '../../stream-client/src/testing/broadcast-fakes.js';
import {
  makeExecutionTerminalEvent,
  makeNodeCompleteEvent,
  makeNodeInterruptEvent,
  makeNodeStartEvent,
  makeStreamCloseEvent,
  makeStreamResumeEvent,
  makeThinkingDeltaEvent,
  makeMultiStepToolRunEvents,
  makeTextDeltaEvent,
  resetSeqCounter,
} from '../../stream-client/src/testing/event-fixtures.js';
import { activeNodeSelector, thinkingSelector, useAgenticStream, useStreamSelector } from './index.js';

function mountComposable<T>(composable: () => T): { result: T; unmount: () => void } {
  let state!: T;
  const Comp = defineComponent({
    setup() {
      state = composable();

      return () => null;
    },
  });
  const wrapper = mount(Comp);

  return {
    get result() {
      return state;
    },
    unmount: () => wrapper.unmount(),
  };
}

async function waitForClient(result: { client: { value: unknown } }): Promise<void> {
  for (let i = 0; i < 50; i += 1) {
    await flushPromises();
    await nextTick();

    if (result.client.value !== null) {
      return;
    }
  }

  throw new Error('client not connected');
}

describe('useAgenticStream (Vue)', () => {
  resetSeqCounter();

  it('accumulates text_delta only, not thinking_delta', async () => {
    const transport = new MockTransport();
    const { result } = mountComposable(() => useAgenticStream({ transport }));

    await waitForClient(result);

    transport.emit(makeTextDeltaEvent('hello'));
    transport.emit(makeThinkingDeltaEvent('secret'));
    transport.emit(makeTextDeltaEvent(' world'));
    await flushPromises();
    await nextTick();

    expect(result.text.value).toBe('hello world');
  });

  it('reflects status for both terminal vocabularies', async () => {
    const broadcastTransport = new MockTransport();
    const broadcast = mountComposable(() => useAgenticStream({ transport: broadcastTransport }));
    await waitForClient(broadcast.result);
    broadcastTransport.emit(makeStreamCloseEvent('success'));
    await flushPromises();
    await nextTick();
    expect(broadcast.result.status.value).toBe('complete');
    broadcast.unmount();

    const sseTransport = new MockTransport();
    const sse = mountComposable(() => useAgenticStream({ transport: sseTransport }));
    await waitForClient(sse.result);
    sseTransport.emit(makeExecutionTerminalEvent('completed'));
    await flushPromises();
    await nextTick();
    expect(sse.result.status.value).toBe('complete');
  });

  it('populates interrupt on node_interrupt', async () => {
    const transport = new MockTransport();
    const { result } = mountComposable(() => useAgenticStream({ transport }));
    await waitForClient(result);

    transport.emit(makeNodeInterruptEvent());
    await flushPromises();
    await nextTick();

    expect(result.status.value).toBe('interrupted');
    expect(result.interrupt.value).toEqual({
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

  it('updates lastEvent', async () => {
    const transport = new MockTransport();
    const { result } = mountComposable(() => useAgenticStream({ transport }));
    await waitForClient(result);

    transport.emit(makeTextDeltaEvent('x'));
    await flushPromises();
    await nextTick();

    expect(result.lastEvent.value?.eventType).toBe('text_delta');
  });

  it('cleans up on unmount', async () => {
    const transport = new MockTransport();
    const disconnectSpy = vi.spyOn(transport, 'disconnect');
    const mounted = mountComposable(() => useAgenticStream({ transport }));
    await waitForClient(mounted.result);
    mounted.unmount();
    expect(disconnectSpy).toHaveBeenCalled();
  });

  it('resume calls onResume and continued events flow', async () => {
    const onResume = vi.fn().mockResolvedValue(undefined);
    const transport = new MockTransport();
    const { result } = mountComposable(() => useAgenticStream({ transport, onResume }));
    await waitForClient(result);

    transport.emit(makeNodeInterruptEvent());
    await flushPromises();
    await nextTick();
    expect(result.status.value).toBe('interrupted');

    await result.resume('approve');
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

    transport.emit(makeStreamResumeEvent());
    transport.emit(makeTextDeltaEvent('after'));
    transport.emit(makeExecutionTerminalEvent('completed'));
    await flushPromises();
    await nextTick();

    expect(result.text.value).toBe('after');
    expect(result.status.value).toBe('complete');
  });

  it('resume without onResume throws', async () => {
    const transport = new MockTransport();
    const { result } = mountComposable(() => useAgenticStream({ transport }));
    await waitForClient(result);

    await expect(result.resume('x')).rejects.toThrow(/onResume/);
  });

  it('accumulates 200 text_delta events correctly', async () => {
    const transport = new MockTransport();
    const { result } = mountComposable(() => useAgenticStream({ transport }));
    await waitForClient(result);

    for (let i = 0; i < 200; i += 1) {
      transport.emit(makeTextDeltaEvent('.'));
    }

    await flushPromises();
    await nextTick();

    expect(result.text.value).toHaveLength(200);
  });

  it('hook state tracks core getters (thin adapter)', async () => {
    const transport = new MockTransport();
    const { result } = mountComposable(() => useAgenticStream({ transport }));
    await waitForClient(result);

    transport.emit(makeTextDeltaEvent('z'));
    transport.emit(makeNodeInterruptEvent());
    await flushPromises();
    await nextTick();

    expect(result.status.value).toBe(result.client.value!.status);
    expect(result.interrupt.value).toEqual(result.client.value!.interrupt);
    expect(result.error.value).toBe(result.client.value!.error);
  });

  it('mirror canonical sequence: text=ab, status=interrupted', async () => {
    const transport = new MockTransport();
    const { result } = mountComposable(() => useAgenticStream({ transport }));
    await waitForClient(result);

    transport.emit(makeTextDeltaEvent('a'));
    transport.emit(makeThinkingDeltaEvent('ignored'));
    transport.emit(makeTextDeltaEvent('b'));
    transport.emit(makeNodeInterruptEvent());
    await flushPromises();
    await nextTick();

    expect(result.text.value).toBe('ab');
    expect(result.status.value).toBe('interrupted');
    expect(result.interrupt.value?.checkpointId).toBe('cp-123');
  });

  it('a two-step tool run: text holds every step, finalText the last; streaming after stream_end', async () => {
    const transport = new MockTransport();
    const { result } = mountComposable(() => useAgenticStream({ transport }));
    await waitForClient(result);

    makeMultiStepToolRunEvents().forEach((event) => transport.emit(event));
    await flushPromises();
    await nextTick();

    expect(result.text.value).toBe('Checking.Final.');
    expect(result.finalText.value).toBe('Final.');
    expect(result.status.value).toBe('streaming');
    expect(result.lastEvent.value?.eventType).toBe('stream_end');
  });
});

describe('useAgenticStream when connect() rejects (Vue)', () => {
  it('sets status error and a connection error instead of an unhandled rejection', async () => {
    const sources: EventSourceLike[] = [];
    const transport = new SSETransport({
      url: '/agentic-graph/streams/stream-fail',
      maxAttempts: 0,
      eventSourceFactory: (url) => {
        const source: EventSourceLike = {
          url,
          onopen: null,
          onerror: null,
          readyState: 0,
          addEventListener() {},
          removeEventListener() {},
          close() {},
        };
        sources.push(source);

        return source;
      },
    });
    const { result } = mountComposable(() => useAgenticStream({ transport }));

    await flushPromises();
    expect(sources).toHaveLength(1);
    sources[0]!.onerror?.(new Event('error'));
    await flushPromises();
    await nextTick();

    expect(result.status.value).toBe('error');
    expect(result.error.value).toEqual({
      kind: 'connection',
      message: 'Stream transport failed to connect',
      canReconnect: false,
    });
    expect(result.client.value).toBeNull();
  });
});

describe('useStreamSelector (Vue)', () => {
  it('activeNodeSelector reflects running nodes', async () => {
    const transport = new MockTransport();
    const { result } = mountComposable(() => {
      const stream = useAgenticStream({ transport });
      const activeNodes = useStreamSelector(stream.client, activeNodeSelector);

      return { stream, activeNodes };
    });

    await waitForClient(result.stream);

    transport.emit(makeNodeStartEvent('alpha'));
    transport.emit(makeNodeStartEvent('beta'));
    transport.emit(makeNodeCompleteEvent('alpha'));
    await flushPromises();
    await nextTick();

    expect(result.activeNodes.value).toEqual(['beta']);
  });

  it('thinkingSelector accumulates thinking separately from text', async () => {
    const transport = new MockTransport();
    const { result } = mountComposable(() => {
      const stream = useAgenticStream({ transport });
      const thinking = useStreamSelector(stream.client, thinkingSelector);

      return { stream, thinking };
    });

    await waitForClient(result.stream);

    transport.emit(makeThinkingDeltaEvent('think'));
    transport.emit(makeTextDeltaEvent('say'));
    await flushPromises();
    await nextTick();

    expect(result.thinking.value).toBe('think');
    expect(result.stream.text.value).toBe('say');
  });

  it('custom selector works', async () => {
    const transport = new MockTransport();
    const { result } = mountComposable(() => {
      const stream = useAgenticStream({ transport });
      const count = useStreamSelector(stream.client, (events) => events.length);

      return { stream, count };
    });

    await waitForClient(result.stream);

    transport.emit(makeTextDeltaEvent('a'));
    transport.emit(makeTextDeltaEvent('b'));
    await flushPromises();
    await nextTick();

    expect(result.count.value).toBe(2);
  });

  it('reconnect() recovers from exhausted attempts and keeps the text', async () => {
    const transport = new MockTransport();
    const { result } = mountComposable(() => useAgenticStream({ transport }));

    await waitForClient(result);
    transport.emit(makeTextDeltaEvent('hi'));
    transport.simulateFailure();
    await nextTick();

    expect(result.status.value).toBe('error');
    expect(result.error.value).toMatchObject({ kind: 'connection', canReconnect: true });

    await result.reconnect();
    await nextTick();

    expect(result.status.value).toBe('streaming');
    expect(result.error.value).toBeNull();
    expect(result.text.value).toBe('hi');
  });

  it('streams a recorded core 0.3 broadcast run through BroadcastTransport: lost frames come from the store', async () => {
    const run = loadRuns().streamed!;
    const routes = fakeRoutes(run, { visible: 0 });
    vi.stubGlobal('fetch', routes.fetch);
    const echo = fakeEcho();
    const id = run.stored[0]!.body.stream_id as string;
    const transport = new BroadcastTransport({ echo: echo.echo, channel: `private-agenticgraph.stream.${id}`, replayUrl: `/s/${id}/events`, token: 't1' });
    const { result } = mountComposable(() => useAgenticStream({ transport }));

    await waitForClient(result);
    routes.visible = run.stored.length;
    run.broadcast.filter((_, i) => i < 11 || i > 14).forEach((frame) => echo.push(frame));

    await vi.waitFor(() => expect(result.status.value).toBe('complete'));
    expect(result.text.value).toBe(run.text);
    expect(result.finalText.value).toBe(run.text);
    vi.unstubAllGlobals();
  });

  it('unmount during connect stops the transport by hand, with no unhandled rejection', async () => {
    const rejections: unknown[] = [];
    const onRejection = (reason: unknown) => rejections.push(reason);
    process.on('unhandledRejection', onRejection);
    const echo = fakeEcho({ manualConfirm: true });
    const transport = new BroadcastTransport({ echo: echo.echo, channel: 'private-agenticgraph.stream.s1', replayUrl: '/s/s1/events', token: 't1' });
    const { result, unmount } = mountComposable(() => useAgenticStream({ transport }));

    await new Promise((done) => setTimeout(done, 5));
    expect(transport.getConnectionState()).toBe('connecting');
    unmount();
    await new Promise((done) => setTimeout(done, 20));
    process.off('unhandledRejection', onRejection);

    expect(transport.getStopReason()).toBe('manual');
    expect(echo.log).toContain('leave agenticgraph.stream.s1');
    expect(result.client.value).toBeNull();
    expect(rejections).toEqual([]);
  });

  it('a transport change during connect stops the old transport; its late result does not become the client', async () => {
    const echo = fakeEcho({ manualConfirm: true });
    const old = new BroadcastTransport({ echo: echo.echo, channel: 'private-agenticgraph.stream.s1', replayUrl: '/s/s1/events', token: 't1' });
    const next = new MockTransport();
    const transport = shallowRef<StreamTransport>(old);
    const { result } = mountComposable(() => useAgenticStream({ get transport() { return transport.value; } }));

    await new Promise((done) => setTimeout(done, 5));
    transport.value = next;
    await waitForClient(result);
    await new Promise((done) => setTimeout(done, 20));

    expect(old.getStopReason()).toBe('manual');
    expect(next.getConnectionState()).toBe('connected');
    expect(result.status.value).toBe('idle');
    expect(result.error.value).toBeNull();
  });
});

describe('useAgenticStream (Vue) remount', () => {
  it('unmount during connect and a remount with the same transport ends with one connected stream that completes', async () => {
    const run = loadRuns().streamed!;
    const routes = fakeRoutes(run, { visible: 0 });
    vi.stubGlobal('fetch', routes.fetch);
    const echo = fakeEcho();
    const id = run.stored[0]!.body.stream_id as string;
    const transport = new BroadcastTransport({ echo: echo.echo, channel: `private-agenticgraph.stream.${id}`, replayUrl: `/s/${id}/events`, token: 't1' });

    mountComposable(() => useAgenticStream({ transport })).unmount();
    const second = mountComposable(() => useAgenticStream({ transport }));
    await waitForClient(second.result);
    await new Promise((done) => setTimeout(done, 20));

    expect(transport.getStopReason()).toBeNull();
    expect(transport.getConnectionState()).toBe('connected');
    expect(echo.log.at(-1)).toBe(`subscribe private-agenticgraph.stream.${id}`);

    routes.visible = run.stored.length;
    run.broadcast.forEach((frame) => echo.push(frame));
    for (let i = 0; i < 50 && second.result.status.value !== 'complete'; i += 1) {
      await new Promise((done) => setTimeout(done, 5));
    }
    expect(second.result.status.value).toBe('complete');
    expect(second.result.text.value).toBe(run.text);
    vi.unstubAllGlobals();
  });
});
