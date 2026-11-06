#!/usr/bin/env node
/**
 * Live SSE harness: the stream client against a real Laravel app with AgenticGraph, checks 1 to 5.
 *
 * Starts the app itself: two `php -S` servers from its public/ (normal and short
 * max_duration), one real queue worker, Redis DB 5/6 flushed first. Then reads real runs
 * of the app's linear SSE demo graph through SSETransport + AgenticStream and compares every
 * client run with the stored stream (seq by seq, and the text). The app must serve
 * POST /demo/linear-sse/start, POST /demo/linear-sse/{execution}/resume and GET /up.
 *
 *   1. live run read while it streams
 *   2. planned closes during a run (short max_duration)
 *   3. forced network drops (TCP proxy), backoff
 *   4. SSE approval run: wait longer than max_duration, approve, resumed segment
 *   5. a real "gone" cursor (truncated stream)
 *   6. the custom event `sources` (?custom_events=wrap): own name, exactly once, replayed after a drop
 *
 * Check 6 (IIFE build on a plain HTML page, real browser) uses the same servers:
 *   KEEP_SERVERS=1 npm run integration:sse     keeps them up after the checks
 *   node scripts/sse-integration-harness.mjs stored <stream_url>   prints the stored stream
 *
 * Environment:
 *   SSE_APP_PATH     the Laravel app folder (default ../laravel-app next to this repo)
 *   SSE_APP_HOST     host the servers bind to and the client calls (default 127.0.0.1)
 *   SSE_APP_PORT     first of three ports: normal server, short server, proxy (default 8711)
 *   SSE_HARNESS_OUT  folder for the JSON report (default <os tmpdir>/agenticgraph-sse-harness)
 *
 * Needs: an LLM key in the app's .env, Redis on 127.0.0.1:6379.
 */

import { spawn, spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
import net from 'node:net';
import { tmpdir } from 'node:os';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { AgenticStream, SSETransport, parseSseFrame } from '@agenticgraph/stream-client';

const __dirname = dirname(fileURLToPath(import.meta.url));
const APP_DIR = resolve(process.env.SSE_APP_PATH ?? resolve(__dirname, '../../laravel-app'));
const HOST = process.env.SSE_APP_HOST ?? '127.0.0.1';
const SCRATCH = process.env.SSE_HARNESS_OUT ?? resolve(tmpdir(), 'agenticgraph-sse-harness');
const TOPIC = process.env.SSE_DEMO_TOPIC ?? 'how server-sent events reconnect after a dropped connection';

const LONG_PORT = Number(process.env.SSE_APP_PORT ?? 8711);
const SHORT_PORT = LONG_PORT + 1;
const PROXY_PORT = LONG_PORT + 2;
const SHORT_MAX_DURATION = '0.5';
const SHORT_RETRY_MS = '200';

const LONG = `http://${HOST}:${LONG_PORT}`;
const SHORT = `http://${HOST}:${SHORT_PORT}`;

/** Every PHP process: Redis DB 5/6, Redis queue and stream store, no tracing. */
const PHP_ENV = {
  ...process.env,
  REDIS_DB: '5',
  REDIS_CACHE_DB: '6',
  STREAM_STORE_DRIVER: 'redis',
  QUEUE_CONNECTION: 'redis',
  AGENTICGRAPH_QUEUE_CONNECTION: 'redis',
  AGENTICGRAPH_TRACING: 'false',
  BROADCAST_CONNECTION: 'null',
  PHP_CLI_SERVER_WORKERS: '16',
};

const sleep = (ms) => new Promise((done) => setTimeout(done, ms));

async function waitFor(predicate, timeoutMs, label) {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    if (predicate()) {
      return;
    }

    await sleep(50);
  }

  throw new Error(`Timed out after ${timeoutMs} ms waiting for ${label}`);
}

// --- environment -----------------------------------------------------------------------

const children = [];

function flushRedis() {
  const result = spawnSync('php', ['-r', '$r = new Redis(); $r->connect("127.0.0.1", 6379); foreach ([5, 6] as $db) { $r->select($db); $r->flushDb(); } echo "ok";']);

  if (String(result.stdout) !== 'ok') {
    throw new Error(`Redis flush failed: ${result.stderr}`);
  }
}

function startPhp(name, args, cwd, env) {
  const child = spawn('php', args, { cwd, env, stdio: ['ignore', 'ignore', 'pipe'], detached: true });
  const errors = [];
  child.stderr.on('data', (chunk) => errors.push(String(chunk)));
  child.on('exit', (code) => {
    if (code !== null && code !== 0 && !shuttingDown) {
      console.error(`${name} exited with ${code}: ${errors.join('').slice(-2000)}`);
    }
  });
  children.push(child);

  return child;
}

function startServer(port, extraEnv) {
  return startPhp(
    `php -S :${port}`,
    ['-S', `${HOST}:${port}`, '../vendor/laravel/framework/src/Illuminate/Foundation/resources/server.php'],
    resolve(APP_DIR, 'public'),
    { ...PHP_ENV, ...extraEnv },
  );
}

async function waitForServer(base) {
  const deadline = Date.now() + 20_000;

  while (Date.now() < deadline) {
    try {
      if ((await fetch(`${base}/up`)).ok) {
        return;
      }
    } catch {
      // not listening yet
    }

    await sleep(200);
  }

  throw new Error(`${base} did not come up`);
}

let shuttingDown = false;

function shutdown() {
  shuttingDown = true;

  for (const child of children) {
    // own process group: php -S forks its workers, and they must stop with it
    try {
      process.kill(-child.pid, 'SIGTERM');
    } catch {
      child.kill('SIGTERM');
    }
  }
}

/** TCP proxy in front of a server, for forced network drops. */
function createProxy(listenPort, targetPort) {
  const pairs = new Set();
  const proxy = { refuse: 0, accepted: 0, refused: 0 };

  const server = net.createServer((client) => {
    if (proxy.refuse > 0) {
      proxy.refuse -= 1;
      proxy.refused += 1;
      client.destroy();

      return;
    }

    proxy.accepted += 1;
    const upstream = net.connect(targetPort, HOST);
    const pair = { client, upstream };
    const end = () => {
      client.destroy();
      upstream.destroy();
      pairs.delete(pair);
    };

    pairs.add(pair);
    client.pipe(upstream);
    upstream.pipe(client);
    client.on('error', end).on('close', end);
    upstream.on('error', end).on('close', end);
  });

  proxy.listen = () => new Promise((done) => server.listen(listenPort, HOST, done));
  proxy.close = () => server.close();
  proxy.dropAll = () => {
    for (const pair of pairs) {
      pair.client.destroy();
      pair.upstream.destroy();
    }
  };

  return proxy;
}

// --- client side -----------------------------------------------------------------------

/**
 * Node has no EventSource: a fetch-based one with browser semantics for what the library
 * relies on (onopen on a 200, onerror when the request fails or the response ends, typed
 * `event:` listeners, no native retry). Each instance is one connection in `log`.
 */
function nodeEventSourceFactory(log, { dropFirstConnectionAfterEvents = 0 } = {}) {
  return (url) => {
    const record = { url, startedAt: Date.now(), openedAt: null, failedAt: null, end: null, controls: [] };
    const listeners = new Map();
    const controller = new AbortController();
    let closed = false;
    let readyState = 0;
    let framesDelivered = 0;
    log.push(record);

    const source = {
      url,
      onopen: null,
      onerror: null,
      addEventListener(type, listener) {
        listeners.set(type, [...(listeners.get(type) ?? []), listener]);
      },
      removeEventListener(type, listener) {
        listeners.set(type, (listeners.get(type) ?? []).filter((item) => item !== listener));
      },
      close() {
        if (!closed) {
          closed = true;
          readyState = 2;
          record.end ??= 'closed by client';
          controller.abort();
        }
      },
      get readyState() {
        return readyState;
      },
    };

    void (async () => {
      try {
        const response = await fetch(url, { signal: controller.signal, headers: { Accept: 'text/event-stream' } });

        if (!response.ok || !response.body) {
          throw new Error(`HTTP ${response.status}`);
        }

        readyState = 1;
        record.openedAt = Date.now();
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
          let boundary = buffer.indexOf('\n\n');

          while (boundary !== -1 && !closed) {
            const frame = parseSseFrame(buffer.slice(0, boundary));
            buffer = buffer.slice(boundary + 2);
            boundary = buffer.indexOf('\n\n');

            // Deterministic forced drop: the first connection fails after N events, whatever the chunking.
            if (dropFirstConnectionAfterEvents > 0 && log.length === 1 && framesDelivered >= dropFirstConnectionAfterEvents) {
              throw new Error('forced drop');
            }

            if (frame?.id) {
              framesDelivered += 1;
            }

            if (frame?.eventType) {
              if (frame.data && typeof frame.data === 'object' && 'control' in frame.data) {
                record.controls.push(frame.data.control);
              }

              for (const listener of listeners.get(frame.eventType) ?? []) {
                listener({ type: frame.eventType, data: JSON.stringify(frame.data), lastEventId: frame.id ?? '' });
              }
            }
          }
        }
      } catch (error) {
        if (closed) {
          return;
        }

        readyState = 0;
        record.failedAt = Date.now();
        record.end = error instanceof Error ? (error.cause?.code ?? error.message) : String(error);
        source.onerror?.(new Event('error'));
      }
    })();

    return source;
  };
}

/** Follow one stream URL with the library. Resolves once the stream is complete/error or the timeout. */
async function follow(url, { until = ['complete', 'error'], timeoutMs = 90_000, options = {}, onEvent, dropFirstConnectionAfterEvents } = {}) {
  const connections = [];
  const events = [];
  const states = [];
  const statuses = [];
  const transport = new SSETransport({ url, eventSourceFactory: nodeEventSourceFactory(connections, { dropFirstConnectionAfterEvents }), ...options });
  transport.onConnectionStateChange((state) => states.push({ state, at: Date.now() }));

  const stream = await AgenticStream.connect({ transport });
  stream.onStatusChange((status) => statuses.push(status));
  stream.on((event) => {
    events.push({ ...event, receivedAt: Date.now(), connection: connections.length - 1 });
    onEvent?.(event, stream);
  });

  const run = { stream, transport, events, connections, states, statuses };
  run.done = waitFor(() => until.includes(stream.status), timeoutMs, `status ${until.join('/')}`).then(() => run);

  return run;
}

/** The stored stream, read raw from the normal server until a completed or failed terminal. */
async function readStored(streamUrl) {
  const url = new URL(streamUrl);
  const response = await fetch(`${LONG}${url.pathname}`, { headers: { Accept: 'text/event-stream' } });
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  const events = [];
  let buffer = '';

  for (;;) {
    const { done, value } = await reader.read();

    if (done) {
      return events;
    }

    buffer += decoder.decode(value, { stream: true });
    let boundary = buffer.indexOf('\n\n');

    while (boundary !== -1) {
      const frame = parseSseFrame(buffer.slice(0, boundary));
      buffer = buffer.slice(boundary + 2);
      boundary = buffer.indexOf('\n\n');

      if (frame?.id) {
        events.push({ eventType: frame.eventType, ...frame.data });

        if (frame.eventType === 'execution_terminal' && frame.data.payload.status !== 'interrupted') {
          await reader.cancel();

          return events;
        }
      }
    }
  }
}

const textOf = (events) => events.filter((e) => e.eventType === 'text_delta').map((e) => e.payload.delta).join('');
const seqsOf = (events) => events.filter((e) => typeof e.seq === 'number').map((e) => e.seq);

async function post(base, path, body) {
  const response = await fetch(`${base}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    throw new Error(`POST ${path} ${response.status}: ${await response.text()}`);
  }

  return response.json();
}

/** Text and seq-by-seq comparison of a client run with the stored stream. */
async function compareWithStored(run, streamUrl) {
  const stored = await readStored(streamUrl);
  const clientSeqs = seqsOf(run.events);
  const storedSeqs = seqsOf(stored);

  return {
    storedEvents: stored.length,
    clientEvents: run.events.length,
    textEqual: textOf(run.events) === textOf(stored),
    seqsEqual: JSON.stringify(clientSeqs) === JSON.stringify(storedSeqs),
    duplicates: clientSeqs.length - new Set(clientSeqs).size,
    missing: storedSeqs.filter((seq) => !clientSeqs.includes(seq)).length,
    textLength: textOf(stored).length,
  };
}

const connectionSummary = (run) => ({
  connections: run.connections.length,
  reconnects: run.connections.length - 1,
  plannedCloses: run.connections.filter((c) => c.controls.includes('reconnect')).length,
  replayUnavailable: run.connections.filter((c) => c.controls.includes('replay_unavailable')).length,
  failedState: run.states.some((s) => s.state === 'failed'),
  ends: run.connections.map((c) => c.end),
});

// --- checks ----------------------------------------------------------------------------

const results = {};

async function check(name, body) {
  const started = Date.now();

  try {
    const detail = await body();
    results[name] = { pass: detail.pass, ms: Date.now() - started, ...detail };
  } catch (error) {
    results[name] = { pass: false, ms: Date.now() - started, error: String(error?.stack ?? error) };
  }

  console.log(`${results[name].pass ? 'PASS' : 'FAIL'}  ${name}`);
}

async function check1() {
  const start = await post(LONG, '/demo/linear-sse/start', { topic: TOPIC });
  const connectedAt = Date.now();
  const run = await (await follow(start.stream_url)).done;
  const deltas = run.events.filter((e) => e.eventType === 'text_delta');
  const comparison = await compareWithStored(run, start.stream_url);
  const summary = connectionSummary(run);

  return {
    pass: run.stream.status === 'complete' && comparison.textEqual && comparison.seqsEqual && summary.connections === 1,
    streamUrl: start.stream_url,
    status: run.stream.status,
    firstDeltaAfterConnectMs: deltas[0].receivedAt - connectedAt,
    deltaSpreadMs: deltas.at(-1).receivedAt - deltas[0].receivedAt,
    textDeltas: deltas.length,
    ...comparison,
    ...summary,
  };
}

async function check2() {
  const start = await post(SHORT, '/demo/linear-sse/start', { topic: TOPIC });
  const run = await (await follow(start.stream_url)).done;
  const comparison = await compareWithStored(run, start.stream_url);
  const summary = connectionSummary(run);
  const textConnections = [...new Set(run.events.filter((e) => e.eventType === 'text_delta').map((e) => e.connection))];
  const startedOn = run.events.find((e) => e.eventType === 'graph_start').connection;
  const endedOn = run.events.find((e) => e.eventType === 'execution_terminal').connection;
  const closesDuringRun = run.connections.slice(startedOn, endedOn).filter((c) => c.controls.includes('reconnect')).length;

  return {
    pass: run.stream.status === 'complete' && comparison.textEqual && comparison.seqsEqual
      && closesDuringRun >= 1 && !summary.failedState,
    plannedClosesDuringRun: closesDuringRun,
    graphStartOnConnection: startedOn,
    terminalOnConnection: endedOn,
    maxDurationSeconds: SHORT_MAX_DURATION,
    status: run.stream.status,
    connectionsThatCarriedText: textConnections.length,
    textDeltaTimeline: run.events.filter((e) => e.eventType === 'text_delta').map((e) => [e.seq, e.connection, e.receivedAt - run.connections[0].startedAt]),
    connectionWindows: run.connections.map((c) => [c.startedAt - run.connections[0].startedAt, c.controls.join(',')]),
    ...comparison,
    ...summary,
  };
}

async function check3() {
  const proxy = createProxy(PROXY_PORT, LONG_PORT);
  await proxy.listen();

  try {
    const start = await post(LONG, '/demo/linear-sse/start', { topic: TOPIC });
    const url = start.stream_url.replace(`:${LONG_PORT}/`, `:${PROXY_PORT}/`);
    let dropped = false;
    const run = await follow(url, {
      onEvent: (event) => {
        if (!dropped && event.eventType === 'text_delta') {
          dropped = true;
          proxy.refuse = 2;
          proxy.dropAll();
        }
      },
    });
    await run.done;

    const comparison = await compareWithStored(run, start.stream_url);
    const summary = connectionSummary(run);
    const attemptDelays = run.connections.slice(1).map((c, i) => c.startedAt - run.connections[i].failedAt);
    // base 500: attempt n waits within [250, 500] * 2^(n-1)
    const withinBackoff = attemptDelays.slice(0, 3).every((delay, i) => delay >= 250 * 2 ** i - 20 && delay <= 500 * 2 ** i + 100);

    return {
      pass: run.stream.status === 'complete' && comparison.textEqual && comparison.seqsEqual
        && summary.reconnects === 3 && proxy.refused === 2 && withinBackoff && !summary.failedState,
      status: run.stream.status,
      refusedByProxy: proxy.refused,
      attemptDelaysMs: attemptDelays,
      backoffWindowsMs: ['250-500', '500-1000', '1000-2000'],
      withinBackoff,
      ...comparison,
      ...summary,
    };
  } finally {
    proxy.close();
  }
}

async function check4() {
  const start = await post(SHORT, '/demo/linear-sse/start', { topic: TOPIC, approval: true });
  const run = await follow(start.stream_url, { timeoutMs: 120_000 });
  await waitFor(() => run.stream.status === 'interrupted', 60_000, 'interrupted');

  const waitMs = 3_000;
  await sleep(waitMs);
  const duringWait = {
    status: run.stream.status,
    connectionsSoFar: run.connections.length,
    errorSoFar: run.stream.error,
  };

  await post(SHORT, `/demo/linear-sse/${start.execution_id}/resume`, { approved: true });
  await run.done;

  const comparison = await compareWithStored(run, start.stream_url);
  const summary = connectionSummary(run);
  const types = run.events.map((e) => e.eventType);
  const terminals = run.events.filter((e) => e.eventType === 'execution_terminal').map((e) => e.payload.status);

  return {
    pass: run.stream.status === 'complete' && duringWait.status === 'interrupted' && duringWait.errorSoFar === null
      && summary.replayUnavailable === 0 && !summary.failedState && types.includes('stream_resume')
      && JSON.stringify(terminals) === '["interrupted","completed"]' && comparison.textEqual && comparison.seqsEqual,
    maxDurationSeconds: SHORT_MAX_DURATION,
    waitedWhileInterruptedMs: waitMs,
    duringWait,
    statuses: run.statuses,
    terminals,
    status: run.stream.status,
    ...comparison,
    ...summary,
  };
}

async function check5(streamUrl) {
  const streamId = new URL(streamUrl).pathname.split('/').pop();
  const truncate = spawnSync('php', ['artisan', 'tinker', '--execute',
    `$s = app(\\Shahzad\\AgenticGraph\\Streaming\\Contracts\\StreamStoreInterface::class); $s->truncate("${streamId}", 6); echo $s->firstSeq("${streamId}");`],
  { cwd: APP_DIR, env: PHP_ENV, encoding: 'utf8' });
  const firstSeq = Number(String(truncate.stdout).trim().split('\n').pop());

  const run = await (await follow(`${streamUrl}?cursor=2`, { timeoutMs: 15_000 })).done;
  await sleep(2_000);
  const summary = connectionSummary(run);

  return {
    pass: firstSeq === 6 && run.stream.status === 'error' && run.stream.error?.kind === 'replay_unavailable'
      && run.stream.error.cursor === 2 && summary.connections === 1 && summary.replayUnavailable === 1
      && run.transport.getConnectionState() === 'disconnected' && run.events.length === 0,
    firstSeqAfterTruncate: firstSeq,
    cursor: 2,
    status: run.stream.status,
    error: run.stream.error,
    connectionState: run.transport.getConnectionState(),
    eventsDispatched: run.events.length,
    ...summary,
  };
}

/** The demo's PrepareNode emits `sources` once per run (core 0.2.1, client 0.1.1). */
async function check6() {
  const start = await post(LONG, '/demo/linear-sse/start', { topic: TOPIC });
  // The first connection fails after one event (graph_start), before `sources` (seq 3): it must come from the replay.
  const run = await (await follow(start.stream_url, { dropFirstConnectionAfterEvents: 1 })).done;

  const stored = await readStored(start.stream_url);
  const sources = run.events.filter((e) => e.eventType === 'sources');
  const storedSources = stored.filter((e) => e.eventType === 'sources');
  const comparison = await compareWithStored(run, start.stream_url);
  const summary = connectionSummary(run);
  const wrapped = run.connections.every((c) => new URL(c.url).searchParams.get('custom_events') === 'wrap');

  return {
    pass: run.stream.status === 'complete' && sources.length === 1 && storedSources.length === 1
      && sources[0].seq === storedSources[0].seq && Array.isArray(sources[0].payload?.items) && sources[0].payload.items.length > 0
      && sources[0].connection === 1 && summary.reconnects === 1 && wrapped
      && comparison.seqsEqual && comparison.duplicates === 0 && comparison.missing === 0 && !summary.failedState,
    status: run.stream.status,
    sourcesReceived: sources.length,
    sourcesStored: storedSources.length,
    sourcesSeq: sources[0]?.seq,
    sourcesOnConnection: sources[0]?.connection,
    items: sources[0]?.payload?.items,
    everyConnectionAskedForWrap: wrapped,
    ...comparison,
    ...summary,
  };
}

// --- main ------------------------------------------------------------------------------

async function main() {
  if (!existsSync(resolve(APP_DIR, 'artisan'))) {
    throw new Error(`No Laravel app at ${APP_DIR}: set SSE_APP_PATH to the app folder`);
  }

  if (process.argv[2] === 'stored') {
    const stored = await readStored(process.argv[3]);
    console.log(JSON.stringify({ text: textOf(stored), seqs: seqsOf(stored), types: stored.map((e) => e.eventType) }));

    return;
  }

  mkdirSync(SCRATCH, { recursive: true });
  flushRedis();
  startServer(LONG_PORT, {});
  startServer(SHORT_PORT, {
    AGENTICGRAPH_STREAM_SSE_MAX_DURATION: SHORT_MAX_DURATION,
    AGENTICGRAPH_STREAM_SSE_RETRY_MS: SHORT_RETRY_MS,
  });
  startPhp('queue worker', ['artisan', 'queue:work', 'redis', '--queue=agenticgraph,default', '--tries=1', '--timeout=0'], APP_DIR, PHP_ENV);
  await Promise.all([waitForServer(LONG), waitForServer(SHORT)]);
  console.log(`servers up: ${LONG} (max_duration default), ${SHORT} (max_duration ${SHORT_MAX_DURATION}s, retry ${SHORT_RETRY_MS} ms)`);

  await check('1 live run read while it streams', check1);
  await check('2 planned closes during a run', check2);
  await check('3 forced network drop with backoff', check3);
  await check('4 SSE approval run across planned closes', check4);
  await check('5 gone cursor (truncated stream)', () => check5(results['1 live run read while it streams'].streamUrl));

  await check('6 custom event across a drop and reconnect', check6);

  const report = { date: new Date().toISOString(), long: LONG, short: SHORT, results };
  writeFileSync(`${SCRATCH}/sse-harness-report.json`, JSON.stringify(report, null, 2));
  console.log(JSON.stringify(results, null, 2));

  const failed = Object.values(results).filter((r) => !r.pass).length;
  console.log(`\nVERDICT: ${failed === 0 ? 'ALL CHECKS PASS' : `${failed} CHECK(S) FAILED`} (report: ${SCRATCH}/sse-harness-report.json)`);
  process.exitCode = failed === 0 ? 0 : 1;

  if (process.env.KEEP_SERVERS === '1') {
    console.log(`KEEP_SERVERS=1: plain HTML page at ${LONG}/demo/linear-sse and ${SHORT}/demo/linear-sse. Ctrl+C to stop.`);
    await new Promise(() => {});
  }
}

process.on('SIGINT', () => {
  shutdown();
  process.exit(130);
});

main()
  .catch((error) => {
    console.error('\nHarness failed:', error);
    process.exitCode = 1;
  })
  .finally(() => {
    if (process.env.KEEP_SERVERS !== '1') {
      shutdown();
      // The forked php -S workers keep the stderr pipes open, so node would wait for them forever.
      process.exit();
    }
  });
