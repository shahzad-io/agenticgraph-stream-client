#!/usr/bin/env node
/**
 * Phase 3a — SSE integration harness.
 *
 * Validates @agenticgraph/stream-client SSETransport against REAL laravel-app SSE output.
 * Does NOT modify SDK core — reports mismatches as findings.
 *
 * Usage:
 *   SSE_APP_URL=http://laravel-app.test npm run integration:sse
 *
 * Prerequisites:
 *   - laravel-app running (Herd)
 *   - OPENAI_API_KEY configured
 *   - Redis + stream store available (default redis driver)
 *   - Database queue + migrations
 */

import { spawnSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  AgenticStream,
  SSETransport,
  appendTextDelta,
  FROZEN_WIRE_BODY_KEYS,
  isStreamWireBody,
  parseSseFrame,
} from '@agenticgraph/stream-client';

const __dirname = dirname(fileURLToPath(import.meta.url));
const SSE_APP_DIR = resolve(__dirname, '../../laravel-app');
const BASE_URL = (process.env.SSE_APP_URL ?? 'http://laravel-app.test').replace(/\/$/, '');
const TOPIC = process.env.SSE_DEMO_TOPIC ?? 'What is server-sent events in one sentence?';

/** @type {Record<string, 'CLEAN' | { status: 'MISMATCH', detail: string }>} */
const results = {};

function pass(key, detail = 'CLEAN') {
  results[key] = detail === 'CLEAN' ? 'CLEAN' : { status: 'MISMATCH', detail };
}

function fail(key, detail) {
  results[key] = { status: 'MISMATCH', detail };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function startDemoStream() {
  if (!existsSync(SSE_APP_DIR)) {
    throw new Error(`laravel-app not found at ${SSE_APP_DIR}`);
  }

  const result = spawnSync(
    'php',
    ['artisan', 'linear-sse:start', '--json', '--process-queue', `--topic=${TOPIC}`],
    {
      cwd: SSE_APP_DIR,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
      env: {
        ...process.env,
        QUEUE_CONNECTION: 'sync',
        AGENTICGRAPH_QUEUE_CONNECTION: 'sync',
      },
    },
  );

  if (result.status !== 0) {
    throw new Error(
      `linear-sse:start failed (exit ${result.status}):\n${result.stderr ?? result.stdout}`,
    );
  }

  const jsonLine = result.stdout
    .trim()
    .split('\n')
    .find((line) => line.startsWith('{'));

  if (!jsonLine) {
    throw new Error(`No JSON payload from artisan:\n${result.stdout}`);
  }

  return /** @type {{ execution_id: string, stream_id: string, stream_url: string }} */ (
    JSON.parse(jsonLine)
  );
}

/**
 * Node fetch-based EventSourceLike — the `eventsource` npm package does not dispatch
 * typed `event:` listeners against this server; fetch streaming + parseSseFrame does.
 * Validates SSETransport's listener wiring without modifying SDK core.
 * @param {string} url
 */
function createFetchEventSource(url) {
  /** @type {Map<string, Array<(event: MessageEvent) => void>>} */
  const listeners = new Map();
  let closed = false;
  let readyState = 0;
  const controller = new AbortController();

  void (async () => {
    try {
      const response = await fetch(url, {
        signal: controller.signal,
        headers: { Accept: 'text/event-stream' },
      });

      if (!response.ok || !response.body) {
        throw new Error(`SSE fetch failed (${response.status})`);
      }

      readyState = 1;
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';

      while (!closed) {
        const { done, value } = await reader.read();

        if (done) {
          break;
        }

        buffer += decoder.decode(value, { stream: true });

        let separatorIndex = buffer.indexOf('\n\n');

        while (separatorIndex !== -1) {
          const block = buffer.slice(0, separatorIndex);
          buffer = buffer.slice(separatorIndex + 2);
          const frame = parseSseFrame(block);

          if (frame?.eventType) {
            const handlers = listeners.get(frame.eventType) ?? [];
            const message = {
              data: JSON.stringify(frame.data),
              lastEventId: frame.id ?? '',
              type: frame.eventType,
            };

            for (const handler of handlers) {
              handler(/** @type {MessageEvent} */ (message));
            }

            if (frame.eventType === 'execution_terminal') {
              closed = true;
              readyState = 2;
              await reader.cancel();
              return;
            }
          }

          separatorIndex = buffer.indexOf('\n\n');
        }
      }
    } catch {
      // aborted / connection closed
    } finally {
      readyState = 2;
    }
  })();

  return {
    url,
    onopen: null,
    onerror: null,
    addEventListener(type, listener) {
      if (!listeners.has(type)) {
        listeners.set(type, []);
      }

      listeners.get(type).push(listener);
    },
    removeEventListener(type, listener) {
      const handlers = listeners.get(type) ?? [];
      const index = handlers.indexOf(listener);

      if (index >= 0) {
        handlers.splice(index, 1);
      }
    },
    close() {
      closed = true;
      readyState = 2;
      controller.abort();
    },
    get readyState() {
      return readyState;
    },
  };
}

/**
 * @param {string} url
 * @returns {Promise<{ events: import('@agenticgraph/stream-client').StreamEvent[], rawFrames: Array<{ id: string | null, eventType: string | null, data: unknown }>, status: string, text: string, transport: SSETransport }>}
 */
async function consumeWithSdk(url) {
  /** @type {import('@agenticgraph/stream-client').StreamEvent[]} */
  const events = [];

  const transport = new SSETransport({
    url,
    eventSourceFactory: (streamUrl) => createFetchEventSource(streamUrl),
  });

  const stream = await AgenticStream.connect({ transport });

  stream.on((event) => {
    events.push(event);
  });

  const deadline = Date.now() + 30_000;

  while (Date.now() < deadline) {
    const status = stream.status;

    if (status === 'complete' || status === 'interrupted' || status === 'error') {
      await stream.disconnect();

      let text = '';

      for (const event of events) {
        text = appendTextDelta(text, event);
      }

      return { events, rawFrames: [], status, text, transport };
    }

    await sleep(250);
  }

  await stream.disconnect();
  throw new Error('Timed out waiting for terminal status from SSETransport');
}

/**
 * Raw SSE fetch for framing validation (independent of EventSource typed listeners).
 * @param {string} url
 */
async function fetchRawSseFrames(url) {
  const response = await fetch(url, {
    headers: { Accept: 'text/event-stream' },
  });

  if (!response.ok || !response.body) {
    throw new Error(`SSE GET failed (${response.status})`);
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  /** @type {Array<{ id: string | null, eventType: string | null, data: unknown }>} */
  const frames = [];

  while (true) {
    const { done, value } = await reader.read();

    if (done) {
      break;
    }

    buffer += decoder.decode(value, { stream: true });

    let separatorIndex = buffer.indexOf('\n\n');

    while (separatorIndex !== -1) {
      const block = buffer.slice(0, separatorIndex);
      buffer = buffer.slice(separatorIndex + 2);
      const frame = parseSseFrame(block);

      if (frame !== null) {
        frames.push(frame);

        if (frame.eventType === 'execution_terminal') {
          await reader.cancel();
          return frames;
        }
      }

      separatorIndex = buffer.indexOf('\n\n');
    }
  }

  return frames;
}

function validateFraming(rawFrames) {
  const typed = rawFrames.filter((f) => f.eventType && f.eventType !== 'stream_heartbeat');

  if (typed.length === 0) {
    fail('framing', 'No typed SSE frames received from raw fetch');
    return;
  }

  const bad = [];

  for (const frame of typed) {
    if (!frame.eventType) {
      bad.push('missing event: line');
      continue;
    }

    if (frame.data === null || frame.data === undefined) {
      bad.push(`frame ${frame.eventType}: missing data JSON`);
      continue;
    }

    if (frame.eventType !== 'stream_heartbeat' && frame.eventType !== 'stream_replay_unavailable') {
      const body = frame.data;

      if (typeof body === 'object' && body !== null && 'type' in body) {
        bad.push(`frame ${frame.eventType}: data contains forbidden "type" field (type-external trap)`);
      }
    }
  }

  if (bad.length > 0) {
    fail('framing', bad.join('; '));
  } else {
    pass('framing');
  }
}

/**
 * @param {import('@agenticgraph/stream-client').StreamEvent[]} events
 */
function validateWireTypes(events) {
  const wireIssues = [];
  const textDeltaIssues = [];

  for (const event of events) {
    if (event.eventType === 'stream_close') {
      continue;
    }

    if (!isStreamWireBody(event)) {
      wireIssues.push(`event seq unknown type ${event.eventType}: missing frozen wire body keys`);
      continue;
    }

    for (const key of FROZEN_WIRE_BODY_KEYS) {
      if (!(key in event)) {
        wireIssues.push(`${event.eventType}@${event.seq}: missing ${key}`);
      }
    }

    if (event.eventType === 'text_delta') {
      const payload = event.payload;

      if (payload === null || typeof payload !== 'object' || !('delta' in payload)) {
        textDeltaIssues.push(`text_delta@${event.seq}: payload missing delta`);
      } else if (typeof payload.delta !== 'string') {
        textDeltaIssues.push(`text_delta@${event.seq}: delta is ${typeof payload.delta}, expected string`);
      }
    }
  }

  if (wireIssues.length > 0) {
    fail('types', wireIssues.slice(0, 5).join('; '));
  } else if (textDeltaIssues.length > 0) {
    fail('types', textDeltaIssues.join('; '));
  } else {
    pass('types');
  }
}

/**
 * @param {import('@agenticgraph/stream-client').StreamEvent[]} events
 * @param {string} accumulatedText
 */
function validateTextAccumulation(events, accumulatedText) {
  const deltas = events
    .filter((e) => e.eventType === 'text_delta')
    .map((e) => /** @type {{ delta: string }} */ (e.payload).delta);

  if (deltas.length === 0) {
    fail('text_accumulation', 'No text_delta events received from real Prism stream');
    return;
  }

  const manual = deltas.join('');

  if (manual !== accumulatedText) {
    fail(
      'text_accumulation',
      `appendTextDelta mismatch: manual="${manual.slice(0, 80)}..." accumulated="${accumulatedText.slice(0, 80)}..."`,
    );
    return;
  }

  if (accumulatedText.trim().length === 0) {
    fail('text_accumulation', 'Accumulated text is empty after real stream');
    return;
  }

  pass('text_accumulation');
}

function validateStatus(terminalStatus, events) {
  const terminals = events.filter((e) => e.eventType === 'execution_terminal');

  if (terminals.length === 0) {
    fail('status', 'No execution_terminal in event stream');
    return;
  }

  const terminal = terminals[terminals.length - 1];
  const payload = /** @type {{ status: string }} */ (terminal.payload);

  if (terminalStatus !== 'complete') {
    fail('status', `SDK status=${terminalStatus}, expected complete`);
    return;
  }

  if (payload.status !== 'completed') {
    fail('status', `execution_terminal.payload.status=${payload.status}, expected completed`);
    return;
  }

  pass('status');
}

/**
 * @param {import('@agenticgraph/stream-client').StreamEvent[]} events
 */
function validateLifecycle(events) {
  const types = events.map((e) => e.eventType);
  const required = ['graph_start', 'node_start', 'node_complete', 'graph_complete', 'execution_terminal'];
  const missing = required.filter((t) => !types.includes(t));

  const prepareEvents = events.filter((e) => 'node_name' in e && e.node_name === 'prepare');
  const respondEvents = events.filter((e) => 'node_name' in e && e.node_name === 'respond');

  if (missing.length > 0) {
    fail('lifecycle', `Missing lifecycle types: ${missing.join(', ')}`);
    return;
  }

  if (prepareEvents.length === 0 || respondEvents.length === 0) {
    fail('lifecycle', 'Expected events for prepare and respond nodes');
    return;
  }

  const respondHasPrism = respondEvents.some((e) =>
    ['text_start', 'text_delta', 'text_complete'].includes(e.eventType),
  );

  if (!respondHasPrism) {
    fail('lifecycle', 'respond node missing Prism content events (text_start/delta/complete)');
    return;
  }

  pass('lifecycle');
}

/**
 * @param {import('@agenticgraph/stream-client').StreamEvent[]} events
 */
function validateNodeSeq(events) {
  const issues = [];

  for (const nodeName of ['prepare', 'respond']) {
    const nodeEvents = events.filter(
      (e) => 'node_name' in e && e.node_name === nodeName && e.eventType !== 'execution_terminal',
    );

    const seqs = nodeEvents
      .map((e) => ('node_seq' in e ? e.node_seq : null))
      .filter((v) => v !== null);

    if (seqs.length === 0) {
      issues.push(`${nodeName}: no node_seq values`);
      continue;
    }

    const expected = Array.from({ length: seqs.length }, (_, i) => i + 1);

    if (JSON.stringify(seqs) !== JSON.stringify(expected)) {
      issues.push(`${nodeName}: node_seq ${JSON.stringify(seqs)} expected gapless 1..${seqs.length}`);
    }
  }

  const globalSeqs = events
    .filter((e) => 'seq' in e && typeof e.seq === 'number')
    .map((e) => e.seq);

  for (let i = 1; i < globalSeqs.length; i += 1) {
    if (globalSeqs[i] <= globalSeqs[i - 1]) {
      issues.push(`global seq not monotonic at index ${i}: ${globalSeqs[i - 1]} -> ${globalSeqs[i]}`);
      break;
    }
  }

  if (issues.length > 0) {
    fail('node_seq', issues.join('; '));
  } else {
    pass('node_seq');
  }
}

function printReport(startPayload, sdkResult, rawFrameCount) {
  const mismatches = Object.entries(results).filter(([, v]) => v !== 'CLEAN');
  const verdict = mismatches.length === 0 ? 'CHAIN VALIDATED' : 'MISMATCH FOUND';

  console.log('\n=== Phase 3a SSE Dogfooding Report ===\n');
  console.log(`Base URL: ${BASE_URL}`);
  console.log(`Stream URL: ${startPayload.stream_url}`);
  console.log(`Execution ID: ${startPayload.execution_id}`);
  console.log(`Events received (SDK): ${sdkResult.events.length}`);
  console.log(`Raw SSE frames parsed: ${rawFrameCount}`);
  console.log(`Accumulated text length: ${sdkResult.text.length}`);
  console.log(`SDK terminal status: ${sdkResult.status}`);
  console.log('');

  for (const [key, value] of Object.entries(results)) {
    if (value === 'CLEAN') {
      console.log(`  ${key}: CLEAN`);
    } else {
      console.log(`  ${key}: MISMATCH — ${value.detail}`);
    }
  }

  console.log(`\nVERDICT: ${verdict}\n`);

  if (mismatches.length > 0) {
    process.exitCode = 1;
  }
}

async function main() {
  console.log('Starting linear-sse-demo via artisan (dispatch + queue)...');
  const startPayload = await startDemoStream();
  console.log(`Stream URL: ${startPayload.stream_url}`);

  console.log('Fetching raw SSE frames...');
  const rawFrames = await fetchRawSseFrames(startPayload.stream_url);
  validateFraming(rawFrames);

  console.log('Consuming via SSETransport + AgenticStream...');
  const sdkResult = await consumeWithSdk(startPayload.stream_url);

  validateWireTypes(sdkResult.events);
  validateTextAccumulation(sdkResult.events, sdkResult.text);
  validateStatus(sdkResult.status, sdkResult.events);
  validateLifecycle(sdkResult.events);
  validateNodeSeq(sdkResult.events);

  printReport(startPayload, sdkResult, rawFrames.length);
}

main().catch((error) => {
  console.error('\nHarness failed:', error);
  process.exitCode = 1;
});
