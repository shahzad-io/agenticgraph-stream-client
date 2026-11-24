// Spec 8.3: one scenario set over a transport factory, on runs recorded from core 0.3 (src/testing/frames-0.3.json).
// Default factory: BroadcastTransport. BROADCAST_TEST_TRANSPORT=echo runs the same cases on EchoTransport 0.1
// (the fail-first evidence of session 4).
import { afterEach, describe, expect, it, vi } from 'vitest';
import { AgenticStream } from '../agentic-stream.js';
import { streamChannelAuthorizer } from '../create-transport.js';
import {
  fakeEcho,
  fakeRoutes,
  frameSeq,
  loadRuns,
  type FakeEchoOptions,
  type FakeRoutesOptions,
  type Frame,
  type RecordedRun,
} from '../testing/broadcast-fakes.js';
import type { StreamTransport } from '../transport.js';
import type { StreamEvent } from '../types/events.js';
import { BroadcastTransport } from './broadcast-transport.js';
import type { BroadcastTransportOptions } from './broadcast-types.js';
import { EchoTransport } from './echo-transport.js';

type Factory = (options: BroadcastTransportOptions) => StreamTransport;

const factories: Record<string, Factory> = {
  broadcast: (options) => new BroadcastTransport(options),
  echo: (options) => new EchoTransport({ echo: options.echo, channel: options.channel }),
};
const factory = factories[process.env.BROADCAST_TEST_TRANSPORT ?? 'broadcast'] as Factory;

const runs = loadRuns();
const sleep = (ms: number) => new Promise((done) => setTimeout(done, ms));

async function until(predicate: () => boolean, timeoutMs = 2000): Promise<void> {
  const deadline = Date.now() + timeoutMs;

  while (!predicate()) {
    if (Date.now() > deadline) {
      throw new Error('timed out');
    }

    await sleep(5);
  }
}

interface Setup {
  routes?: FakeRoutesOptions;
  echo?: FakeEchoOptions;
  transport?: Partial<BroadcastTransportOptions>;
}

/** Connects the transport through AgenticStream, as an app does. The store is empty until `begin()`. */
async function follow(run: RecordedRun, setup: Setup = {}) {
  const routes = fakeRoutes(run, { visible: 0, ...setup.routes });
  const echo = fakeEcho(setup.echo);
  // A real fetch answers on a later task; so does the fake (handlers are bound after connect resolves, as in the hooks).
  vi.stubGlobal('fetch', async (input: string, init?: RequestInit) => {
    await sleep(0);

    return routes.fetch(input, init);
  });
  const streamId = run.stored[0]?.body.stream_id as string;
  const transport = factory({
    echo: echo.echo,
    channel: `private-agenticgraph.stream.${streamId}`,
    replayUrl: `/agentic-graph/streams/${streamId}/events?token=t1`,
    authUrl: `/agentic-graph/streams/${streamId}/auth`,
    token: 't1',
    idleMs: 60_000,
    ...setup.transport,
  });
  const events: StreamEvent[] = [];
  const connecting = AgenticStream.connect({ transport });

  const view = {
    routes,
    echo,
    transport,
    events,
    stream: null as AgenticStream | null,
    connecting,
    async connected() {
      view.stream = await connecting;
      view.stream.on((event) => {
        events.push(event);
      });
      await sleep(20);

      return view;
    },
    /** The run starts: from now on the store holds every event (it is written before the frame is sent). */
    begin(count = run.stored.length) {
      routes.visible = count;
    },
    push(frames: Frame[]) {
      frames.forEach((frame) => echo.push(frame));
    },
    get status() {
      return view.stream?.status;
    },
  };

  return view;
}

function textOf(events: StreamEvent[]): string {
  return events.filter((e) => e.eventType === 'text_delta').map((e) => (e.payload as { delta: string }).delta).join('');
}

/** The SSE contract: the store's events, each once, in seq order, the store's text, and the end status. */
function expectContract(view: Awaited<ReturnType<typeof follow>>, run: RecordedRun, status = 'complete') {
  expect(view.events.map((e) => `${e.eventType}:${(e as { seq?: number }).seq}`)).toEqual(
    run.stored.map((e) => `${e.type}:${e.body.seq}`),
  );
  expect(textOf(view.events)).toBe(run.text);
  expect(view.status).toBe(status);
}

const streamed = runs.streamed as RecordedRun;
const frames = streamed.broadcast;
const terminalAt = frames.findIndex((f) => f.event === 'execution_terminal');

afterEach(() => {
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

describe(`broadcast contract (spec 8.3) on ${process.env.BROADCAST_TEST_TRANSPORT ?? 'broadcast'}`, () => {
  it('1a. drop mid-stream (G2): frames lost on the channel come from the store', async () => {
    const view = await (await follow(streamed)).connected();
    view.begin();
    view.push(frames.filter((_, i) => i < 11 || i > 14));
    await until(() => view.status !== 'streaming' && view.status !== 'idle').catch(() => undefined);

    expectContract(view, streamed);
  });

  it('1b. late join (G3): the events before the subscribe come from the store', async () => {
    const sentBefore = frames.slice(0, 12);
    const storedBefore = streamed.stored.filter((e) => e.body.seq <= (frameSeq(sentBefore.at(-1) as Frame) as number) + 2).length;
    const view = await (await follow(streamed, { routes: { visible: storedBefore } })).connected();
    view.begin();
    view.push(frames.slice(12));
    await until(() => view.status === 'complete').catch(() => undefined);

    expectContract(view, streamed);
  });

  it('1c. swapped frames (G5): applied in seq order', async () => {
    const view = await (await follow(streamed)).connected();
    const out = [...frames];

    for (let i = 5; i + 1 < out.length - 2; i += 5) {
      [out[i], out[i + 1]] = [out[i + 1] as Frame, out[i] as Frame];
    }

    view.begin();
    view.push(out);
    await until(() => view.status === 'complete').catch(() => undefined);

    expectContract(view, streamed);
  });

  it('1d. duplicated frames (G5): each event once', async () => {
    const view = await (await follow(streamed)).connected();
    view.begin();
    view.push(frames.flatMap((f, i) => (i % 7 === 6 ? [f, f] : [f])));
    await until(() => view.status === 'complete').catch(() => undefined);

    expectContract(view, streamed);
  });

  it('1e. a run that ends while the socket is down completes after the resubscribe, no duplicate text', async () => {
    const view = await (await follow(streamed)).connected();
    view.begin();
    view.push(frames.slice(0, 13));
    view.echo.drop();
    view.push(frames.slice(13));
    view.echo.confirm();
    await until(() => view.status === 'complete').catch(() => undefined);

    expectContract(view, streamed);
  });

  it('2. batch frames are unpacked into their events', async () => {
    expect(frames.filter((f) => f.event === 'batch').length).toBeGreaterThan(3);
    const view = await (await follow(streamed)).connected();
    view.begin();
    view.push(frames);
    await until(() => view.status === 'complete').catch(() => undefined);

    expectContract(view, streamed);
    expect(view.routes.replays).toBe(1);
  });

  it('3. a filtered run (seq holes that exist nowhere) completes with exactly one replay after the start replay', async () => {
    const run = runs.filter as RecordedRun;
    const view = await (await follow(run)).connected();
    view.begin();
    view.push(run.broadcast);
    await until(() => view.status === 'complete').catch(() => undefined);

    expectContract(view, run);
    expect(view.routes.replays).toBe(2);
  });

  it('4a. a lost terminal is found by the idle replay', async () => {
    const view = await (await follow(streamed, { transport: { idleMs: 100 } })).connected();
    view.begin();
    view.push(frames.slice(0, terminalAt));
    await sleep(50);
    expect(view.status).toBe('streaming');
    await until(() => view.status === 'complete', 1000).catch(() => undefined);

    expectContract(view, streamed);
  });

  it('4b. stream_close with no terminal replays at once, not at the idle timer', async () => {
    const view = await (await follow(streamed)).connected();
    view.begin();
    view.push(frames.filter((f) => f.event !== 'execution_terminal'));
    await until(() => view.status === 'complete', 300).catch(() => undefined);

    expectContract(view, streamed);
  });

  it('5. idle replay backs off (1x, 2x, 4x, then 6x) and resets on a frame', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] });
    const t0 = Date.now();
    const view = await follow(streamed, { transport: { idleMs: 1000 } });
    await vi.advanceTimersByTimeAsync(1);
    await view.connecting;
    const at = () => view.routes.requests.map((r) => r.at);
    await vi.advanceTimersByTimeAsync(1000 + 2000 + 4000 + 6000 + 6000);
    // Idle replays only (request 0 is the start replay); the idle timer starts at the subscribe, t0.
    const idle = at().slice(1).map((t, i, all) => t - (i === 0 ? t0 : (all[i - 1] as number)));

    const before = at().length;
    view.begin(1);
    view.push(frames.slice(0, 1));
    await vi.advanceTimersByTimeAsync(999);
    const justBefore = at().length;
    await vi.advanceTimersByTimeAsync(2);

    expect(idle.map((ms) => Math.round(ms / 10) * 10)).toEqual([1000, 2000, 4000, 6000, 6000]);
    expect(justBefore).toBe(before);
    expect(at().length).toBe(before + 1);
  });

  it('6. interrupted keeps the transport connected, with no idle replay, and the resumed segment completes', async () => {
    const run = runs.interrupted as RecordedRun;
    const split = run.interrupt_at as number;
    const view = await (await follow(run, { transport: { idleMs: 40 } })).connected();
    const interruptSeq = run.broadcast[split - 1]?.payload.seq as number;
    view.begin(run.stored.filter((e) => e.body.seq <= interruptSeq).length);
    view.push(run.broadcast.slice(0, split));
    await until(() => view.status === 'interrupted').catch(() => undefined);
    const replays = view.routes.replays;
    await sleep(200);

    expect(view.status).toBe('interrupted');
    expect(view.transport.getConnectionState()).toBe('connected');
    expect(view.routes.replays).toBe(replays);

    view.begin();
    view.push(run.broadcast.slice(split));
    await until(() => view.status === 'complete').catch(() => undefined);

    expectContract(view, run);
  });

  it('7. replay gone stops with replay_unavailable', async () => {
    const view = await (await follow(streamed, { routes: { gone: true } })).connected();
    view.begin();
    view.push(frames.filter((_, i) => i < 11 || i > 14));
    await until(() => view.status === 'error').catch(() => undefined);

    expect(view.transport.getStopReason?.()).toBe('replay_unavailable');
    expect(view.stream?.error?.kind).toBe('replay_unavailable');
  });

  it('8. channel auth failure stops with unauthorized', async () => {
    const view = await follow(streamed, { echo: { authorizer: streamChannelAuthorizer() }, routes: { authForbidden: true } });
    const outcome = await view.connecting.then(() => 'connected', (error: Error) => error.message);

    expect(outcome).toMatch(/unauthorized/);
    expect(view.transport.getStopReason?.()).toBe('unauthorized');
    expect(view.routes.requests.filter((r) => r.method === 'POST').map((r) => r.token)).toEqual(['t1']);
  });

  it('9a. expired token: refreshToken gives a new one and the stream continues', async () => {
    const refreshToken = vi.fn(async () => 't2');
    const view = await (
      await follow(streamed, { routes: { expiredTokens: ['t1'], validTokens: ['t2'] }, transport: { refreshToken } })
    ).connected();
    view.begin();
    view.push(frames.filter((_, i) => i < 11 || i > 14));
    await until(() => view.status === 'complete').catch(() => undefined);

    expectContract(view, streamed);
    expect(refreshToken).toHaveBeenCalledTimes(1);
    expect(view.routes.requests.map((r) => r.token).slice(0, 2)).toEqual(['t1', 't2']);
  });

  it('9b. expired token: refreshToken returns null and the transport stops with unauthorized', async () => {
    const view = await (
      await follow(streamed, { routes: { expiredTokens: ['t1'] }, transport: { refreshToken: async () => null } })
    ).connected();
    await until(() => view.status === 'error').catch(() => undefined);

    expect(view.transport.getStopReason?.()).toBe('unauthorized');
    expect(view.stream?.error).toMatchObject({ kind: 'connection', canReconnect: false });
  });

  it('10. a frame sent between the subscribe and the replay answer is kept; no replay before the subscription is confirmed', async () => {
    const sentBefore = frames.slice(0, 12);
    const storedAtRead = streamed.stored.filter((e) => e.body.seq <= (frameSeq(sentBefore.at(-1) as Frame) as number) + 2).length;
    const view = await follow(streamed, { echo: { manualConfirm: true }, routes: { visible: storedAtRead } });
    view.push(sentBefore);
    await sleep(20);
    const replaysBeforeConfirm = view.routes.replays;
    view.routes.hold();
    view.echo.confirm();
    await view.connected();
    view.begin();
    view.push(frames.slice(12));
    view.routes.release();
    await until(() => view.status === 'complete').catch(() => undefined);

    expect(replaysBeforeConfirm).toBe(0);
    expectContract(view, streamed);
  });

  it('11. resolveReplayUrl returning null stops with resolver, not an error', async () => {
    const view = await (await follow(streamed, { transport: { resolveReplayUrl: () => null } })).connected();
    await until(() => view.transport.getConnectionState() === 'disconnected').catch(() => undefined);

    expect(view.transport.getStopReason?.()).toBe('resolver');
    expect(view.status).toBe('idle');
  });

  it('replays page by page while more is true', async () => {
    const view = await (await follow(streamed, { routes: { visible: streamed.stored.length, pageSize: 10 } })).connected();
    await until(() => view.status === 'complete').catch(() => undefined);

    expectContract(view, streamed);
    expect(view.routes.requests.map((r) => r.url.searchParams.get('after'))).toEqual([null, '10', '20', '30']);
  });

  it('keeps the tool, held, failed and custom-event runs equal to the store', async () => {
    for (const name of ['tool', 'held', 'failed']) {
      const run = runs[name] as RecordedRun;
      const view = await (await follow(run)).connected();
      view.begin();
      view.push(run.broadcast);
      await until(() => view.status === 'complete' || view.status === 'error').catch(() => undefined);

      expectContract(view, run, name === 'failed' ? 'error' : 'complete');
    }
  });
});
