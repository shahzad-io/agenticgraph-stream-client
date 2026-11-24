#!/usr/bin/env node
/**
 * Parity harness (spec 8.4, CI part): one scenario set over SSETransport and over BroadcastTransport, results
 * compared after normalization. No PHP and no Reverb: runs recorded from core 0.3
 * (packages/stream-client/src/testing/frames-0.3.json) are served by a Node fake server (the SSE route with the
 * bytes the core sent, the replay route, the channel auth route), and a fake Echo sends the broadcast frames.
 *
 * A clock sends one frame per tick. The store moves first (the core stores an event before it broadcasts it), so
 * the SSE route and the replay route see what a real server would see at that moment.
 *
 * Scenarios: streamed with a drop, tool with text reset, held answer, interrupted and resumed, failed, late join,
 * event filter. Conditions (broadcast): plain (batch_ms = 0: one frame per event), batched (as recorded), chaos
 * (batched, every 7th frame twice, every 5th pair swapped). "Two Reverb servers" needs real Reverb (session 5).
 *
 * Compared: event type sequence, seq, text, finalText, terminal statuses, AgenticStream status, stop reason,
 * custom events. The SSE run is the baseline; it must also equal the store.
 *
 *   npm run integration:parity
 */
import http from 'node:http';
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  AgenticStream,
  BroadcastTransport,
  SSETransport,
  appendFinalTextDelta,
  parseSseFrame,
  wireToStreamEvent,
} from '@agenticgraph/stream-client';

const __dirname = dirname(fileURLToPath(import.meta.url));
const { runs } = JSON.parse(readFileSync(resolve(__dirname, '../packages/stream-client/src/testing/frames-0.3.json'), 'utf8'));
const TICK_MS = 8;
const PAUSE_MS = 400; // interrupted: longer than the idle timer, so a wrong idle replay would show
const IDLE_MS = 150;
const sleep = (ms) => new Promise((done) => setTimeout(done, ms));

// --- frames per condition --------------------------------------------------------------------------------------

const unpack = (frames) => frames.flatMap((f) => (f.event === 'batch'
  ? f.payload.events.map(({ type, ...body }) => ({ event: type, payload: body }))
  : [f]));

function chaos(frames) {
  const out = [...frames];
  const fixed = (f) => f.event === 'execution_terminal' || f.event === 'stream_close';

  for (let i = 5; i + 1 < out.length; i += 5) {
    if (!fixed(out[i]) && !fixed(out[i + 1])) {
      [out[i], out[i + 1]] = [out[i + 1], out[i]];
    }
  }

  return out.flatMap((f, i) => (i % 7 === 6 ? [f, f] : [f]));
}

const CONDITIONS = { plain: unpack, batched: (frames) => frames, chaos };

// --- one run on the server side: store, SSE connections, channel ------------------------------------------------

const maxSeq = (frame) => Math.max(0, ...(frame.event === 'batch' ? frame.payload.events : [frame.payload]).map((b) => b.seq ?? 0));

function serverRun(run) {
  const sseBlocks = new Map();

  for (const block of run.sse.split('\n\n')) {
    const id = /^id: (\d+)$/m.exec(block)?.[1];

    if (id) {
      sseBlocks.set(Number(id), `${block}\n\n`);
    }
  }

  return { run, sseBlocks, storeMax: 0, sse: new Set(), sseConnections: 0, replays: 0, subscribers: new Set() };
}

const visible = (state) => state.run.stored.filter((e) => e.body.seq <= state.storeMax);

function writeSse(state, connection) {
  for (const event of visible(state).filter((e) => e.body.seq > connection.cursor)) {
    if (connection.closed) {
      return;
    }

    connection.res.write(state.sseBlocks.get(event.body.seq));
    connection.cursor = event.body.seq;
    connection.sent += 1;

    if (connection.dropAfter !== null && connection.sent >= connection.dropAfter) {
      connection.closed = true;
      connection.res.destroy();

      return;
    }

    // As the core: a completed or failed terminal ends the response; interrupted keeps it open.
    if (event.type === 'execution_terminal' && event.body.payload.status !== 'interrupted') {
      connection.closed = true;
      connection.res.end();

      return;
    }
  }
}

/** The server sends one frame: the store first, then the channel, then the open SSE responses. */
function serverSend(state, frame) {
  state.storeMax = frame.event === 'stream_close' ? Infinity : Math.max(state.storeMax, maxSeq(frame));

  for (const subscriber of state.subscribers) {
    subscriber.deliver(frame);
  }

  for (const connection of state.sse) {
    writeSse(state, connection);
  }
}

// --- fake HTTP server: SSE route, replay route, channel auth route ----------------------------------------------

let current = null;
let sseDropPlan = null;

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://127.0.0.1');
  const state = current;

  if (url.pathname.endsWith('/events')) {
    state.replays += 1;
    const after = url.searchParams.has('after') ? Number(url.searchParams.get('after')) : null;
    const page = visible(state).filter((e) => e.body.seq > (after ?? 0)).slice(0, 200);
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify({ events: page, last_seq: page.at(-1)?.body.seq ?? after ?? 0, more: page.length === 200, gone: false }));

    return;
  }

  if (url.pathname.endsWith('/auth')) {
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify({ auth: 'key:signature' }));

    return;
  }

  res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' });
  res.write('retry: 2000\n\n');
  const cursor = url.searchParams.has('cursor') ? Number(url.searchParams.get('cursor')) : 0;
  const connection = { res, cursor, sent: 0, closed: false, dropAfter: sseDropPlan?.shift() ?? null };
  state.sseConnections += 1;
  state.sse.add(connection);
  req.on('close', () => {
    connection.closed = true;
    state.sse.delete(connection);
  });
  writeSse(state, connection);
});

// --- fake Echo: frames reach a channel only while it is subscribed ----------------------------------------------

function fakeEcho(state) {
  const subscriber = { live: false, onAll: null, onSubscribed: null, deliver: (frame) => subscriber.live && subscriber.onAll?.(`.${frame.event}`, frame.payload) };
  const channel = {
    listen: () => channel,
    stopListening: () => channel,
    listenToAll: (callback) => ((subscriber.onAll = callback), channel),
    subscribed: (callback) => ((subscriber.onSubscribed = callback), channel),
    error: () => channel,
    unsubscribe: () => (subscriber.live = false),
  };
  const confirm = () => {
    subscriber.live = true;
    subscriber.onSubscribed?.();
  };
  const subscribe = () => {
    state.subscribers.add(subscriber);
    setTimeout(confirm, 3); // the auth round trip
    return channel;
  };

  return {
    echo: { channel: subscribe, private: subscribe, leave: () => state.subscribers.delete(subscriber), disconnect: () => undefined },
    /** The socket drops: frames are lost until the resubscribe. */
    drop: () => (subscriber.live = false),
    resubscribe: confirm,
  };
}

// --- Node EventSource with browser semantics for what SSETransport uses -----------------------------------------

function nodeEventSource(url) {
  const listeners = new Map();
  const controller = new AbortController();
  let closed = false;
  const source = {
    url,
    onopen: null,
    onerror: null,
    readyState: 0,
    addEventListener: (type, listener) => listeners.set(type, [...(listeners.get(type) ?? []), listener]),
    removeEventListener: (type, listener) => listeners.set(type, (listeners.get(type) ?? []).filter((l) => l !== listener)),
    close: () => {
      closed = true;
      controller.abort();
    },
  };

  void (async () => {
    try {
      const response = await fetch(url, { signal: controller.signal });
      source.onopen?.(new Event('open'));
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';

      while (!closed) {
        const { done, value } = await reader.read();

        if (done) {
          throw new Error('response ended');
        }

        buffer += decoder.decode(value, { stream: true });

        for (let at = buffer.indexOf('\n\n'); at !== -1 && !closed; at = buffer.indexOf('\n\n')) {
          const frame = parseSseFrame(buffer.slice(0, at));
          buffer = buffer.slice(at + 2);

          for (const listener of (frame && listeners.get(frame.eventType)) ?? []) {
            listener({ type: frame.eventType, data: JSON.stringify(frame.data) });
          }
        }
      }
    } catch {
      if (!closed) {
        source.onerror?.(new Event('error'));
      }
    }
  })();

  return source;
}

// --- scenarios -----------------------------------------------------------------------------------------------------

const firstIndex = (frames, test) => frames.findIndex(test);
const deltasBefore = (frames, n) => {
  let seen = 0;

  return frames.findIndex((f) => (seen += f.event === 'batch' ? f.payload.events.length : f.event === 'text_delta' ? 1 : 0) >= n);
};

/** drop: [index in the server frame list, ticks down] for broadcast; sseDropAfter: events on the first SSE connection. */
const SCENARIOS = {
  'streamed with a drop': { run: 'streamed', drop: (f) => [deltasBefore(f, 6), 6], sseDropAfter: 14 },
  'tool with text reset': { run: 'tool', drop: (f) => [deltasBefore(f, 2), 4], sseDropAfter: 12 },
  'held answer': { run: 'held', drop: (f) => [firstIndex(f, (x) => x.event === 'status' && x.payload.seq > 3), 3], sseDropAfter: 7 },
  'interrupted and resumed': { run: 'interrupted', pause: true },
  failed: { run: 'failed', drop: (f) => [firstIndex(f, (x) => x.event === 'node_start' && x.payload.seq > 2), 3], sseDropAfter: 5 },
  'late join': { run: 'streamed', joinAt: (f) => Math.floor(f.length / 2) },
  'event filter': { run: 'filter' },
};

const FRAMEWORK = new Set(['graph_start', 'graph_complete', 'graph_failed', 'node_start', 'node_complete', 'node_error', 'node_interrupt',
  'stream_resume', 'execution_terminal', 'stream_error', 'text_start', 'text_delta', 'text_complete', 'tool_call', 'tool_result',
  'thinking_start', 'thinking_delta', 'thinking_complete', 'stream_start', 'step_start', 'step_finish', 'stream_end',
  'provider_tool_event', 'artifact', 'error', 'stream_replay_unavailable']);

function normalize(events, stream, stopReason) {
  return {
    types: events.map((e) => e.eventType).join(' '),
    seq: events.map((e) => e.seq).join(','),
    text: events.filter((e) => e.eventType === 'text_delta').map((e) => e.payload.delta).join(''),
    finalText: events.reduce(appendFinalTextDelta, ''),
    terminals: events.filter((e) => e.eventType === 'execution_terminal').map((e) => e.payload.status).join(','),
    status: stream.status,
    stopReason,
    custom: events.filter((e) => !FRAMEWORK.has(e.eventType)).map((e) => `${e.eventType}:${JSON.stringify(e.payload)}`).join(' | '),
  };
}

/** Runs the server clock over `frames` and follows it with one transport. */
async function play(scenario, frames, makeTransport, onTick = () => undefined) {
  const run = runs[scenario.run];
  const state = serverRun(run);
  current = state;
  const events = [];
  let stream = null;
  let transport = null;
  const join = async () => {
    transport = makeTransport(state);
    stream = await AgenticStream.connect({ transport });
    stream.on((event) => events.push(event));
  };
  const joinAt = scenario.joinAt?.(frames) ?? 0;
  const resumeSeq = run.interrupt_at === null ? null : run.broadcast[run.interrupt_at].payload.seq;

  if (joinAt === 0) {
    await join();
    await sleep(20);
  }

  for (let i = 0; i < frames.length; i += 1) {
    if (i === joinAt && joinAt > 0) {
      await join();
    }

    if (scenario.pause && maxSeq(frames[i]) === resumeSeq) {
      await sleep(PAUSE_MS);
    }

    onTick({ i, state, frames });
    serverSend(state, frames[i]);
    await sleep(TICK_MS);
  }

  const deadline = Date.now() + 5000;

  while (Date.now() < deadline && !(transport.getStopReason() !== null && ['complete', 'error'].includes(stream.status))) {
    await sleep(10);
  }

  await sleep(50);
  const result = { ...normalize(events, stream, transport.getStopReason()), replays: state.replays, sse: state.sseConnections };
  await stream.disconnect();
  current = null;

  return result;
}

function sseRun(scenario) {
  const run = runs[scenario.run];
  sseDropPlan = scenario.sseDropAfter ? [scenario.sseDropAfter] : [];

  return play(scenario, run.broadcast, () => new SSETransport({
    url: `http://127.0.0.1:${server.address().port}/streams/${run.name}?token=t1`,
    baseDelayMs: 20,
    eventSourceFactory: nodeEventSource,
  }));
}

function broadcastRun(scenario, condition) {
  const run = runs[scenario.run];
  const frames = CONDITIONS[condition](run.broadcast);
  const [dropAt, down] = scenario.drop?.(frames) ?? [-1, 0];
  let echo = null;
  sseDropPlan = [];

  return play(scenario, frames, (state) => {
    echo = fakeEcho(state);

    return new BroadcastTransport({
      echo: echo.echo,
      channel: `private-agenticgraph.stream.${run.stored[0].body.stream_id}`,
      replayUrl: `http://127.0.0.1:${server.address().port}/streams/${run.name}/events?token=t1`,
      authUrl: `http://127.0.0.1:${server.address().port}/streams/${run.name}/auth`,
      token: 't1',
      idleMs: IDLE_MS,
    });
  }, ({ i }) => {
    if (i === dropAt) {
      echo.drop();
    } else if (i === dropAt + down) {
      echo.resubscribe();
    }
  });
}

/** The store, read as the SSE contract says a client must see it. */
function storeView(run) {
  const events = run.stored.map((e) => wireToStreamEvent(e.type, e.body));

  return normalize(events, { status: events.some((e) => e.eventType === 'execution_terminal' && e.payload.status === 'failed') ? 'error' : 'complete' }, 'terminal');
}

// --- main ----------------------------------------------------------------------------------------------------------

const COMPARED = ['types', 'seq', 'text', 'finalText', 'terminals', 'status', 'stopReason', 'custom'];
const diff = (a, b) => COMPARED.filter((key) => a[key] !== b[key]);

await new Promise((done) => server.listen(0, '127.0.0.1', done));
const rows = [];
let failures = 0;

for (const [name, scenario] of Object.entries(SCENARIOS)) {
  const sse = await sseRun(scenario);
  const storeDiff = diff(sse, storeView(runs[scenario.run]));
  rows.push({ scenario: name, condition: 'SSE (baseline)', equal: storeDiff.length === 0 ? 'store: yes' : `store: NO (${storeDiff.join(', ')})`, ...sse });
  failures += storeDiff.length > 0 ? 1 : 0;

  for (const condition of Object.keys(CONDITIONS)) {
    const broadcast = await broadcastRun(scenario, condition);
    const keys = diff(broadcast, sse);
    failures += keys.length > 0 ? 1 : 0;
    rows.push({ scenario: name, condition, equal: keys.length === 0 ? 'SSE: yes' : `SSE: NO (${keys.join(', ')})`, ...broadcast });
  }

  rows.push({ scenario: name, condition: 'two Reverb servers', equal: 'skipped: live only (session 5)' });
}

server.close();
const short = (text) => (text === undefined ? '' : text.length > 24 ? `${text.slice(0, 21)}...` : text);
console.log('| Scenario | Condition | Equal | Events | Text | finalText | Terminals | Status | Stop | Custom | Replays / SSE connections |');
console.log('|---|---|---|---|---|---|---|---|---|---|---|');

for (const r of rows) {
  const count = r.types === undefined ? '' : r.types.split(' ').length;
  const custom = r.custom === undefined ? '' : r.custom === '' ? '-' : r.custom.split(' | ').map((c) => c.split(':')[0]).join(', ');
  console.log(`| ${r.scenario} | ${r.condition} | ${r.equal} | ${count} | ${short(r.text)} | ${short(r.finalText)} | ${r.terminals ?? ''} | ${r.status ?? ''} | ${r.stopReason ?? ''} | ${custom} | ${r.condition.startsWith('SSE') ? r.sse : r.replays ?? ''} |`);
}

console.log(`\nVERDICT: ${failures === 0 ? 'every scenario and condition equal (fakes)' : `${failures} MISMATCH(ES)`}`);
process.exit(failures === 0 ? 0 : 1);
