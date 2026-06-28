# AgenticGraph Stream Client

TypeScript SDK for consuming [AgenticGraph](https://github.com/shahzad-io/agenticgraph) streaming wire events in browser and Node.js applications.

This monorepo ships three packages:

| Package | Description |
| --- | --- |
| `@agenticgraph/stream-client` | Framework-agnostic core client, transports, event types, and selectors |
| `@agenticgraph/stream-react` | React hooks (`useAgenticStream`, `useStreamSelector`) |
| `@agenticgraph/stream-vue` | Vue composables (`useAgenticStream`, `useStreamSelector`) |

## Features

- **Multiple transports** — SSE (with replay cursor support), Laravel Echo/WebSockets, and a mock transport for tests
- **Typed wire events** — Framework lifecycle events (graph/node start/complete, interrupts, errors) and Prism content events (text deltas, tool calls, thinking)
- **Selectors** — Derive active node, accumulated text, tool calls, and thinking from the event stream
- **Framework adapters** — React and Vue bindings with shared core semantics

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

```ts
import { AgenticStream, SSETransport } from '@agenticgraph/stream-client';

const transport = new SSETransport({
  url: 'https://your-app.test/api/graphs/run/stream',
});

const stream = await AgenticStream.connect({ transport });

stream.on('text.delta', (event) => {
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

function Chat() {
  const { text, status, error, resume } = useAgenticStream({
    transport: new SSETransport({ url: '/api/graphs/run/stream' }),
  });

  return (
    <div>
      <p>{text}</p>
      <p>Status: {status}</p>
      {error && <p>Error: {error.message}</p>}
    </div>
  );
}
```

### Vue

```vue
<script setup lang="ts">
import { SSETransport } from '@agenticgraph/stream-client';
import { useAgenticStream } from '@agenticgraph/stream-vue';

const { text, status, error } = useAgenticStream({
  transport: new SSETransport({ url: '/api/graphs/run/stream' }),
});
</script>

<template>
  <p>{{ text }}</p>
  <p>Status: {{ status }}</p>
  <p v-if="error">Error: {{ error.message }}</p>
</template>
```

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
