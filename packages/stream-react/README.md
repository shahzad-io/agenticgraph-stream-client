# @agenticgraph/stream-react

React hooks for [AgenticGraph](https://github.com/shahzad-io/agenticgraph) streaming: `useAgenticStream` and `useStreamSelector`, on top of [@agenticgraph/stream-client](https://www.npmjs.com/package/@agenticgraph/stream-client).

**Pre-1.0:** minor versions may contain breaking changes. Read the [CHANGELOG](https://github.com/shahzad-io/agenticgraph-stream-client/blob/main/CHANGELOG.md) before you upgrade.

```bash
npm install @agenticgraph/stream-client @agenticgraph/stream-react
```

```tsx
import { SSETransport } from '@agenticgraph/stream-client';
import { useAgenticStream } from '@agenticgraph/stream-react';

function Chat({ streamId }: { streamId: string }) {
  const { text, status, error } = useAgenticStream({
    transport: new SSETransport({ url: `/agentic-graph/streams/${streamId}` }),
  });

  return <p>{error ? `Error: ${error.kind}` : text} ({status})</p>;
}
```

`text` holds every `text_delta` of the run (every step); `finalText` the current or last step, so after a tool run the final answer.

Requires React 18+. Guide: [agenticgraph.dev/docs/stream-getting-started](https://agenticgraph.dev/docs/stream-getting-started); more in the [repository README](https://github.com/shahzad-io/agenticgraph-stream-client#readme).

## Compatibility

The three packages are released together:

| Package | Version | Works with |
|---|---|---|
| `agenticgraph/agenticgraph` (core) | 0.2.x | PHP 8.3+, Laravel 12.61.1+ or 13.12+, Prism 0.102 |
| `agenticgraph/vector` | 0.1.x | PHP 8.3+, Laravel 12.61.1+ or 13.12+, Prism 0.102, PostgreSQL with pgvector (0.8+ for iterative scan). Core 0.2.x is optional (tracing) |
| `@agenticgraph/stream-client`, `@agenticgraph/stream-react`, `@agenticgraph/stream-vue` | 0.1.x | Core 0.2.x on the server (stream wire contract v1.1; 0.2.1 for custom events and error details), Node.js 18+, React 18+ or Vue 3.4+ for the adapters |

## License

MIT

`reconnect()` resumes from the last event after "Reconnect attempts exhausted" (`error.kind === 'connection'` and `error.canReconnect`). The text is kept.
