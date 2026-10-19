# @agenticgraph/stream-client

Framework-agnostic TypeScript client for [AgenticGraph](https://github.com/shahzad-io/agenticgraph) streaming wire events: SSE with library-owned reconnect (gap-free from the last event), Laravel Echo, a mock transport for tests, typed events, selectors, and a browser build (`dist/index.global.js`, global `AgenticGraphStream`).

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

`laravel-echo` is an optional peer dependency, needed only for the Echo transport. An agent skill for this package (connect, reconnect, stop reasons, resolveUrl, signed URLs, the script-tag build) ships with it: [skills/agenticgraph-stream-client/SKILL.md](skills/agenticgraph-stream-client/SKILL.md). React and Vue bindings: [@agenticgraph/stream-react](https://www.npmjs.com/package/@agenticgraph/stream-react), [@agenticgraph/stream-vue](https://www.npmjs.com/package/@agenticgraph/stream-vue).

Guide: [agenticgraph.dev/docs/stream-getting-started](https://agenticgraph.dev/docs/stream-getting-started). Reconnect, `resolveUrl`, the script-tag build and final state: the [repository README](https://github.com/shahzad-io/agenticgraph-stream-client#readme).

## Compatibility

The three packages are released together:

| Package | Version | Works with |
|---|---|---|
| `shahzad/agenticgraph` (core) | 0.2.x | PHP 8.2+, Laravel 11, 12 or 13, Prism 0.100 |
| `shahzad/agenticgraph-vector` | 0.1.x | PHP 8.2+, Laravel 11, 12 or 13, Prism 0.100, PostgreSQL with pgvector (0.8+ for iterative scan). Core 0.2.x is optional (tracing) |
| `@agenticgraph/stream-client`, `@agenticgraph/stream-react`, `@agenticgraph/stream-vue` | 0.1.x | Core 0.2.x on the server (stream wire contract v1.1), Node.js 18+, React 18+ or Vue 3.4+ for the adapters |

## License

MIT
