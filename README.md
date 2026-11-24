# AgenticGraph Stream Client

TypeScript SDK for consuming [AgenticGraph](https://github.com/shahzad-io/agenticgraph) streaming wire events in browser and Node.js applications.

**Pre-1.0:** minor versions may contain breaking changes. Read [CHANGELOG.md](CHANGELOG.md) before you upgrade.

This monorepo ships three packages:

| Package | Description |
| --- | --- |
| `@agenticgraph/stream-client` | Framework-agnostic core client, transports, event types, and selectors |
| `@agenticgraph/stream-react` | React hooks (`useAgenticStream`, `useStreamSelector`) |
| `@agenticgraph/stream-vue` | Vue composables (`useAgenticStream`, `useStreamSelector`) |

## Features

- **Multiple transports**: SSE (automatic reconnect, gap-free from the last event), broadcast over a private Laravel Echo channel with the same contract (core 0.3), and a mock transport for tests
- **Browser build**: one `<script>` tag, global `AgenticGraphStream`, no build step
- **Typed wire events**: Framework lifecycle events (graph/node start/complete, interrupts, errors) and Prism content events (text deltas, tool calls, thinking)
- **Selectors**: Derive active node, accumulated text, tool calls, and thinking from the event stream
- **Framework adapters**: React and Vue bindings with shared core semantics

## Requirements

- Node.js 18+
- `laravel-echo` (optional peer dependency, required only for broadcast)

## Compatibility

The three packages are released together:

| Package | Version | Works with |
|---|---|---|
| `agenticgraph/agenticgraph` (core) | 0.2.x | PHP 8.3+, Laravel 12.61.1+ or 13.12+, Prism 0.102 |
| `agenticgraph/vector` | 0.1.x | PHP 8.3+, Laravel 12.61.1+ or 13.12+, Prism 0.102, PostgreSQL with pgvector (0.8+ for iterative scan). Core 0.2.x is optional (tracing) |
| `@agenticgraph/stream-client`, `@agenticgraph/stream-react`, `@agenticgraph/stream-vue` | 0.1.x | Core 0.2.x on the server (stream wire contract v1.1; core 0.2.1 and client 0.1.1 together for custom events and error details), Node.js 18+, React 18+ or Vue 3.4+ for the adapters |

## Installation

```bash
npm install @agenticgraph/stream-client
npm install @agenticgraph/stream-react   # React apps
npm install @agenticgraph/stream-vue       # Vue apps
```

## Quick start

### Core client (SSE)

The SSE route is `GET /agentic-graph/streams/{streamId}` (the package route, prefix `agenticgraph.streaming.routes.prefix`). Your start endpoint returns the stream id (`$handle->streamId()`) or the full URL (`$handle->stream()->streamUrl`). Below, `startResponse` is that endpoint's JSON.

```ts
import { AgenticStream, SSETransport } from '@agenticgraph/stream-client';

const streamId: string = startResponse.stream_id; // from your start endpoint: $handle->streamId()

const transport = new SSETransport({
  url: `https://your-app.test/agentic-graph/streams/${streamId}`,
});

const stream = await AgenticStream.connect({ transport });

stream.on('text_delta', (event) => {
  console.log(event.payload.delta);
});

stream.onStatusChange((status) => {
  console.log('status:', status);
});
```

### React

```tsx
import { SSETransport } from '@agenticgraph/stream-client';
import { useAgenticStream } from '@agenticgraph/stream-react';

function Chat({ streamId }: { streamId: string }) {
  const { text, status, error, reconnect } = useAgenticStream({
    transport: new SSETransport({ url: `/agentic-graph/streams/${streamId}` }),
  });

  return (
    <div>
      <p>{text}</p>
      <p>Status: {status}</p>
      {error && <p>Error: {error.kind}</p>}
      {error?.canReconnect && <button onClick={() => void reconnect()}>Reconnect</button>}
    </div>
  );
}
```

### Vue

```vue
<script setup lang="ts">
import { SSETransport } from '@agenticgraph/stream-client';
import { useAgenticStream } from '@agenticgraph/stream-vue';

const props = defineProps<{ streamId: string }>();

const { text, status, error, reconnect } = useAgenticStream({
  transport: new SSETransport({ url: `/agentic-graph/streams/${props.streamId}` }),
});
</script>

<template>
  <p>{{ text }}</p>
  <p>Status: {{ status }}</p>
  <p v-if="error">Error: {{ error.kind }}</p>
  <button v-if="error?.canReconnect" @click="reconnect()">Reconnect</button>
</template>
```

### Plain HTML (script tag)

`dist/index.global.js` is a minified browser build (about 5 KB gzipped, no dependencies) that sets `window.AgenticGraphStream`. Copy it into your `public/` folder:

```bash
cp node_modules/@agenticgraph/stream-client/dist/index.global.js public/js/agenticgraph-stream.js
```

```html
<pre id="output"></pre>
<script src="/js/agenticgraph-stream.js"></script>
<script>
  const { AgenticStream, SSETransport } = AgenticGraphStream;

  // streamUrl comes from your start endpoint: $handle->stream()->streamUrl
  async function watch(streamUrl) {
    const stream = await AgenticStream.connect({ transport: new SSETransport({ url: streamUrl }) });

    stream.on('text_delta', (event) => {
      document.getElementById('output').textContent += event.payload.delta;
    });
  }
</script>
```

From a CDN, pin the version: `https://unpkg.com/@agenticgraph/stream-client@0.1.1/dist/index.global.js` or `https://cdn.jsdelivr.net/npm/@agenticgraph/stream-client@0.1.1/dist/index.global.js` serves the same file.

## Custom events (SSE)

The transport sends `?custom_events=wrap`, so events your nodes define (`streamEvent('sources', ...)`) arrive as `custom_event` frames and are delivered under your own type: `stream.on('sources', ...)`, selectors, and `lastEvent` in the hooks. Needs core 0.2.1; core 0.2.0 ignores the parameter and these events are not delivered over SSE.

## Reconnect (SSE)

`SSETransport` reconnects by itself. On an error or the end of a response it closes the `EventSource` (no native browser retry) and reopens with `?cursor=<last seq>`, so nothing is missed and nothing is shown twice (events at or below the last seen `seq` are dropped).

- **Planned close:** the server ends each response after `streaming.sse.max_duration_seconds` with a `stream_reconnect` control event. The client reopens after its `after_ms`. Not a failure.
- **Drops:** exponential backoff with jitter. After `maxAttempts` failures in a row the connection state is `failed` and `stream.status` is `'error'` (`error.kind === 'connection'`); `error.canReconnect` is `true` and `await stream.reconnect()` (or `reconnect()` from the hooks) resumes from the last event; on success `status` goes back to `'streaming'` and `error` clears. `canReconnect` is `false` for an ended run, a server error, an expired cursor, a 403 (`unauthorized`) and a failed first connect.
- **Approval waits:** an `interrupted` terminal keeps the connection; the resumed segment arrives on it, and `stream.status` goes from `'interrupted'` back to `'streaming'`.
- **Stops** on a `completed` or `failed` terminal, on `stream_replay_unavailable` (the cursor is older than anything the server still stores), and when `resolveUrl` returns `null`.

```ts
const transport = new SSETransport({
  url: streamUrl,
  maxAttempts: 5,     // default: failed attempts in a row before 'failed'
  baseDelayMs: 500,   // default: first retry delay, doubles each failure
  maxDelayMs: 10000,  // default: cap for one delay
  // Optional: called before each reconnect, for signed or expiring URLs.
  // The client adds ?cursor=<lastSeq> to the URL you return.
  resolveUrl: async (lastSeq) => streamUrl,
});

transport.onConnectionStateChange((state, stopReason) => {
  // 'connecting' | 'connected' | 'reconnecting' | 'failed' | 'disconnected'
  // stopReason on 'disconnected' and 'failed':
  // 'terminal' | 'replay_unavailable' | 'resolver' | 'manual' | 'failed' | 'unauthorized'
  console.log('connection:', state, stopReason);
});
```

### resolveUrl

`resolveUrl(lastSeq)` runs before each reconnect (not before the first `connect()`) and returns one of:

- a URL string: the transport opens it with `?cursor=<lastSeq>` added.
- `null`: the app says the stream is over. The transport stops reconnecting, closes, and moves to `disconnected` with stop reason `'resolver'`. It is not a failure and does not set `stream.error`.
- a throw or rejection: counts as one failed attempt, like a network error (backoff, then `failed` after `maxAttempts`).

The stop reason is on `transport.getStopReason()` and is the second argument of `onConnectionStateChange` for `disconnected` and `failed`: `'terminal'`, `'replay_unavailable'`, `'resolver'`, `'manual'` (`disconnect()`), `'failed'`, or `'unauthorized'` (a 403 that `refreshToken` did not fix). It is `null` while the transport runs. The first stop reason holds until the next `connect()` or `reconnect()`: `disconnect()` on a transport that already stopped keeps that reason (and state) and sends no event.

A signed URL that returns `null` when the server answers 409:

```ts
const transport = new SSETransport({
  url: signedStreamUrl,
  resolveUrl: async (lastSeq) => {
    const response = await fetch(`/chats/${chatId}/stream-url`);

    if (response.status === 409) {
      return null; // the run is over: stop reconnecting, no error
    }

    if (!response.ok) {
      throw new Error(`stream-url ${response.status}`); // counts as a failed attempt
    }

    return (await response.json()).url;
  },
});
```

A Laravel signed stream URL must ignore the `cursor` parameter (`$request->hasValidSignatureWhileIgnoring(['cursor'])`), or use `resolveUrl` to fetch a fresh one.

### Stream token (core 0.3)

Core 0.3 adds a stream token to `stream_url` (`token`). Pass the URL as the server gives it. The token expires: give the transport `refreshToken`, a function that returns a fresh `$handle->streamToken()` from your app, or `null`. On 403 `stream_token_expired` the transport calls it once and retries. `null`, another 403 code or a second 403 stops it with `'unauthorized'`. `EventSource` cannot read a status, so after every error `SSETransport` checks the URL once with `fetch`, with or without `refreshToken`. Without `refreshToken`, any 403 stops it with `'unauthorized'`.

## Broadcast (core 0.3)

```ts
import { AgenticStream, createStreamTransport, streamChannelAuthorizer } from '@agenticgraph/stream-client';
import Echo from 'laravel-echo';

window.Echo = new Echo({
  broadcaster: 'reverb',
  // ...your Reverb or Pusher settings
  channelAuthorization: { customHandler: streamChannelAuthorizer({ fallback: appChannelHandler }) },
});

// descriptor = $handle->stream()->toArray() from your start endpoint
const transport = createStreamTransport(descriptor, {
  echo: window.Echo, // needed for a broadcast descriptor
  refreshToken: async () => (await (await fetch(`/runs/${runId}/stream-token`)).json()).token,
});
const stream = await AgenticStream.connect({ transport });
```

- `createStreamTransport()` returns `SSETransport` or `BroadcastTransport` from `descriptor.transport`. Your handlers, hooks and selectors do not change.
- The channel is private. `streamChannelAuthorizer()` answers Echo's channel auth for stream channels with the token and `auth_url` of the transport. Echo has one handler per instance, so pass your own as `fallback` for your other private channels.
- `BroadcastTransport` waits for the subscription to be confirmed, replays from the store (following `more`), merges by seq, unpacks `batch` frames and drops duplicates. A hole in seq starts one replay after 250 ms. A `completed` or `failed` terminal stops it; `stream_close` without the terminal replays at once; no frame for `idleMs` (5000) while streaming replays, then after 10 s, 20 s and every 30 s. `interrupted` keeps it connected.
- Stop reasons as on SSE: `'terminal'`, `'replay_unavailable'` (replay `gone`), `'resolver'` (`resolveReplayUrl` returned `null`), `'manual'`, `'failed'` (more than `maxAttempts` failed replays in a row, network error or 5xx; default 5), `'unauthorized'` (403 or channel auth refused).
- Queue the run on the server: on the `sync` queue the web request runs the whole broadcast run.
- `EchoTransport` is deprecated. It works only with `AGENTICGRAPH_STREAM_BROADCAST_PRIVATE=false` and `AGENTICGRAPH_STREAM_BROADCAST_BATCH_MS=0`.

## Final state

The terminal event (`execution_terminal`, on SSE and on broadcast; `stream_close` only on the deprecated `EchoTransport`) carries `final_state`, typed `Record<string, unknown> | null`. It is `null` unless the server sets `AGENTICGRAPH_STREAM_EXPOSE_FINAL_STATE=true`, and then holds only the keys in `AGENTICGRAPH_STREAM_FINAL_STATE_KEYS` when that is set (wire contract v1.1). Handle `null`, or fetch what you need from your own status endpoint.

## Development

```bash
npm install
npm run build
npm test
npm run typecheck
```

Run the SSE integration harness against a live Laravel app (default `../workflow-agents`, next to this repo; `SSE_APP_PATH` overrides it; the other settings are in the script header):

```bash
npm run integration:sse
```

Run the parity harness: one scenario set over `SSETransport` and `BroadcastTransport`, results compared. It uses runs recorded from core 0.3, a Node fake server and a fake Echo, so it needs no PHP and no Reverb:

```bash
npm run integration:parity
```

## License

MIT. Each package directory carries the README and LICENSE that npm publishes with it.
