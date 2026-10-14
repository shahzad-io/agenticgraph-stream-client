# AgenticGraph Stream Client

TypeScript SDK for consuming [AgenticGraph](https://github.com/shahzad-io/agenticgraph) streaming wire events in browser and Node.js applications.

This monorepo ships three packages:

| Package | Description |
| --- | --- |
| `@agenticgraph/stream-client` | Framework-agnostic core client, transports, event types, and selectors |
| `@agenticgraph/stream-react` | React hooks (`useAgenticStream`, `useStreamSelector`) |
| `@agenticgraph/stream-vue` | Vue composables (`useAgenticStream`, `useStreamSelector`) |

## Features

- **Multiple transports**: SSE (with replay cursor support), Laravel Echo/WebSockets, and a mock transport for tests
- **Typed wire events**: Framework lifecycle events (graph/node start/complete, interrupts, errors) and Prism content events (text deltas, tool calls, thinking)
- **Selectors**: Derive active node, accumulated text, tool calls, and thinking from the event stream
- **Framework adapters**: React and Vue bindings with shared core semantics

## Requirements

- Node.js 18+
- `laravel-echo` (optional peer dependency, required only for Echo transport)

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

## Final state

The terminal event (`execution_terminal` on SSE, `stream_close` on a broadcast) carries `final_state`, typed `Record<string, unknown> | null`. It is `null` unless the server sets `AGENTICGRAPH_STREAM_EXPOSE_FINAL_STATE=true`, and then holds only the keys in `AGENTICGRAPH_STREAM_FINAL_STATE_KEYS` when that is set (wire contract v1.1). Handle `null`, or fetch what you need from your own status endpoint.

## Development

```bash
npm install
npm run build
npm test
npm run typecheck
```

Run the SSE integration harness against a live backend:

```bash
npm run integration:sse
```

## License

MIT
