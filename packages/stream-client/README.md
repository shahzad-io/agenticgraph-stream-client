# @agenticgraph/stream-client

Framework-agnostic TypeScript client for [AgenticGraph](https://github.com/shahzad-io/agenticgraph) streaming wire events: SSE with library-owned reconnect (gap-free from the last event), broadcast over a private Laravel Echo channel with the same contract (core 0.3), a mock transport for tests, typed events, selectors, and a browser build (`dist/index.global.js`, global `AgenticGraphStream`).

**Pre-1.0:** minor versions may contain breaking changes. Read the [CHANGELOG](https://github.com/shahzad-io/agenticgraph-stream-client/blob/main/CHANGELOG.md) before you upgrade.

```bash
npm install @agenticgraph/stream-client
```

```ts
import { AgenticStream, SSETransport } from '@agenticgraph/stream-client';

// streamId comes from your start endpoint: $handle->streamId()
const stream = await AgenticStream.connect({
  transport: new SSETransport({ url: `/agentic-graph/streams/${streamId}` }),
});

stream.on('text_delta', (event) => console.log(event.payload.delta));
```

With core 0.3, build the transport from the descriptor the server sends (`$handle->stream()->toArray()`). It is `SSETransport` or `BroadcastTransport`, and your handlers do not change:

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

`BroadcastTransport` reads the store through the replay route after the subscription is confirmed, merges it with the live frames by seq, unpacks `batch` frames, and fills a drop or a late join from the store. Each event arrives once, in order, with the correct end status. It needs core 0.3.0. `refreshToken` gets a new stream token (`$handle->streamToken()`) when the old one expires; it works on `SSETransport` too. `EchoTransport` is deprecated.

`laravel-echo` is an optional peer dependency, needed only for broadcast. An agent skill for this package (connect, reconnect, stop reasons, resolveUrl, signed URLs, the script-tag build) ships with it: [skills/agenticgraph-stream-client/SKILL.md](skills/agenticgraph-stream-client/SKILL.md). React and Vue bindings: [@agenticgraph/stream-react](https://www.npmjs.com/package/@agenticgraph/stream-react), [@agenticgraph/stream-vue](https://www.npmjs.com/package/@agenticgraph/stream-vue).

Guide: [agenticgraph.dev/docs/stream-getting-started](https://agenticgraph.dev/docs/stream-getting-started). Reconnect, `resolveUrl`, the script-tag build and final state: the [repository README](https://github.com/shahzad-io/agenticgraph-stream-client#readme).

## Compatibility

The three packages are released together:

| Package | Version | Works with |
|---|---|---|
| `agenticgraph/agenticgraph` (core) | 0.2.x | PHP 8.3+, Laravel 12.61.1+ or 13.12+, Prism 0.102 |
| `agenticgraph/vector` | 0.1.x | PHP 8.3+, Laravel 12.61.1+ or 13.12+, Prism 0.102, PostgreSQL with pgvector (0.8+ for iterative scan). Core 0.2.x is optional (tracing) |
| `@agenticgraph/stream-client`, `@agenticgraph/stream-react`, `@agenticgraph/stream-vue` | 0.1.x | Core 0.2.x on the server (stream wire contract v1.1; core 0.2.1 and client 0.1.1 together for custom events and error details), Node.js 18+, React 18+ or Vue 3.4+ for the adapters |

## License

MIT
