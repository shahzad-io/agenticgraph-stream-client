# @agenticgraph/stream-vue

Vue composables for [AgenticGraph](https://github.com/shahzad-io/agenticgraph) streaming: `useAgenticStream` and `useStreamSelector`, on top of [@agenticgraph/stream-client](https://www.npmjs.com/package/@agenticgraph/stream-client).

```bash
npm install @agenticgraph/stream-client @agenticgraph/stream-vue
```

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
  <p>{{ error ? `Error: ${error.kind}` : text }} ({{ status }})</p>
</template>
```

Requires Vue 3.4+. Guide: [agenticgraph.dev/docs/stream-getting-started](https://agenticgraph.dev/docs/stream-getting-started); more in the [repository README](https://github.com/shahzad-io/agenticgraph-stream-client#readme).

## Compatibility

The three packages are released together:

| Package | Version | Works with |
|---|---|---|
| `shahzad/agenticgraph` (core) | 0.2.x | PHP 8.2+, Laravel 11, 12 or 13, Prism 0.100 |
| `shahzad/agenticgraph-vector` | 0.1.x | PHP 8.2+, Laravel 11, 12 or 13, Prism 0.100, PostgreSQL with pgvector (0.8+ for iterative scan). Core 0.2.x is optional (tracing) |
| `@agenticgraph/stream-client`, `@agenticgraph/stream-react`, `@agenticgraph/stream-vue` | 0.1.x | Core 0.2.x on the server (stream wire contract v1.1), Node.js 18+, React 18+ or Vue 3.4+ for the adapters |

## License

MIT
