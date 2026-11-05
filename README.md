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

- **Multiple transports**: SSE (automatic reconnect, gap-free from the last event), Laravel Echo/WebSockets, and a mock transport for tests
- **Browser build**: one `<script>` tag, global `AgenticGraphStream`, no build step
- **Typed wire events**: Framework lifecycle events (graph/node start/complete, interrupts, errors) and Prism content events (text deltas, tool calls, thinking)
- **Selectors**: Derive active node, accumulated text, tool calls, and thinking from the event stream
- **Framework adapters**: React and Vue bindings with shared core semantics

## Requirements

- Node.js 18+
- `laravel-echo` (optional peer dependency, required only for Echo transport)

## Compatibility

The three packages are released together:

| Package | Version | Works with |
|---|---|---|
| `agenticgraph/agenticgraph` (core) | 0.2.x | PHP 8.3+, Laravel 12.61.1+ or 13.12+, Prism 0.102 |
| `agenticgraph/vector` | 0.1.x | PHP 8.3+, Laravel 12.61.1+ or 13.12+, Prism 0.102, PostgreSQL with pgvector (0.8+ for iterative scan). Core 0.2.x is optional (tracing) |
| `@agenticgraph/stream-client`, `@agenticgraph/stream-react`, `@agenticgraph/stream-vue` | 0.1.x | Core 0.2.x on the server (stream wire contract v1.1), Node.js 18+, React 18+ or Vue 3.4+ for the adapters |

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
  const { text, status, error } = useAgenticStream({
    transport: new SSETransport({ url: `/agentic-graph/streams/${streamId}` }),
  });

  return (
    <div>
      <p>{text}</p>
      <p>Status: {status}</p>
      {error && <p>Error: {error.kind}</p>}
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

const { text, status, error } = useAgenticStream({
  transport: new SSETransport({ url: `/agentic-graph/streams/${props.streamId}` }),
});
</script>

<template>
  <p>{{ text }}</p>
  <p>Status: {{ status }}</p>
  <p v-if="error">Error: {{ error.kind }}</p>
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

Once the package is published, `https://unpkg.com/@agenticgraph/stream-client` serves the same file.

## Reconnect (SSE)

`SSETransport` reconnects by itself. On an error or the end of a response it closes the `EventSource` (no native browser retry) and reopens with `?cursor=<last seq>`, so nothing is missed and nothing is shown twice (events at or below the last seen `seq` are dropped).

- **Planned close:** the server ends each response after `streaming.sse.max_duration_seconds` with a `stream_reconnect` control event. The client reopens after its `after_ms`. Not a failure.
- **Drops:** exponential backoff with jitter. After `maxAttempts` failures in a row the connection state is `failed` and `stream.status` is `'error'` (`error.kind === 'connection'`); `await stream.reconnect()` tries again.
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
  // 'terminal' | 'replay_unavailable' | 'resolver' | 'manual' | 'failed'
  console.log('connection:', state, stopReason);
});
```

### resolveUrl

`resolveUrl(lastSeq)` runs before each reconnect (not before the first `connect()`) and returns one of:

- a URL string: the transport opens it with `?cursor=<lastSeq>` added.
- `null`: the app says the stream is over. The transport stops reconnecting, closes, and moves to `disconnected` with stop reason `'resolver'`. It is not a failure and does not set `stream.error`.
- a throw or rejection: counts as one failed attempt, like a network error (backoff, then `failed` after `maxAttempts`).

The stop reason is on `transport.getStopReason()` and is the second argument of `onConnectionStateChange` for `disconnected` and `failed`: `'terminal'`, `'replay_unavailable'`, `'resolver'`, `'manual'` (`disconnect()`), or `'failed'`. It is `null` while the transport runs. The first stop reason holds until the next `connect()` or `reconnect()`: `disconnect()` on a transport that already stopped keeps that reason (and state) and sends no event.

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

## Final state

The terminal event (`execution_terminal` on SSE, `stream_close` on a broadcast) carries `final_state`, typed `Record<string, unknown> | null`. It is `null` unless the server sets `AGENTICGRAPH_STREAM_EXPOSE_FINAL_STATE=true`, and then holds only the keys in `AGENTICGRAPH_STREAM_FINAL_STATE_KEYS` when that is set (wire contract v1.1). Handle `null`, or fetch what you need from your own status endpoint.

## Development

```bash
npm install
npm run build
npm test
npm run typecheck
```

Run the SSE integration harness against a live Laravel app (set `SSE_APP_PATH`; the other settings are in the script header):

```bash
npm run integration:sse
```

## License

MIT. Each package directory carries the README and LICENSE that npm publishes with it.
