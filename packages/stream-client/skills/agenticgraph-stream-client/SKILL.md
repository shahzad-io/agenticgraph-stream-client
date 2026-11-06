---
name: agenticgraph-stream-client
description: "Consume AgenticGraph stream events in JavaScript with @agenticgraph/stream-client: connect an SSE or Echo transport, read status, interrupts and errors, reconnect, resolveUrl and stop reasons, signed URLs, selectors, the mock transport for tests, and the script-tag build."
---

# AgenticGraph stream client (npm)

Package: `@agenticgraph/stream-client` 0.1.x (0.1.1 or later). It needs AgenticGraph core 0.2.x on the server (wire contract v1.1); core 0.2.1 or later for custom events and error details. Server side (routes, `stream()`, `broadcast()`, resume): the `agenticgraph-streaming` skill in the Laravel package.

Only `src/index.ts` exports are public. Every method and name below is checked against it by `src/skill.test.ts`.

## Rules

- `AgenticStream.connect({ transport })` is the only way to build a stream. It resolves when the transport reports `connected`, and rejects if the transport reports `failed` first.
- One `connect()` per transport. With the React or Vue hook, the hook owns the connection: do not also call `connect()` on the same transport.
- Use `SSETransport` when events must survive a drop. Use `EchoTransport` only when losing events during a drop is acceptable (it has no replay).
- `SSETransport` reconnects by itself. Do not write a retry loop around it.
- Read `transport.getStopReason()` to learn why a stream stopped. Do not guess from `status`.

## Connect and read events

```ts
import { AgenticStream, SSETransport } from '@agenticgraph/stream-client';

// streamId comes from your start endpoint: $handle->streamId() or $handle->stream()->streamUrl
const transport = new SSETransport({ url: payload.stream_url });
const stream = await AgenticStream.connect({ transport });

const off = stream.on('text_delta', (event) => output.append(event.payload.delta));
stream.onStatusChange((status, snapshot) => render(status, snapshot.error));
```

- `on(type, handler)` returns an unsubscribe function. `on(handler)` receives every event. `subscribe` is an alias of `on`. `off(type, handler)` removes one.
- `events()` is an async iterable from the moment you start iterating. It has no replay.
- `status`: `idle`, `streaming`, `complete`, `interrupted`, `error`.
- `interrupt`: `{ checkpointId, reason, node, payload }`. `payload` is the full wire interrupt payload; the app's own review data is at `payload.interrupt_payload`.
- `error`: `kind` is `connection`, `stream_error`, `terminal_error` or `replay_unavailable`. `canReconnect` is true only for `connection`. A `terminal_error` carries `meta.message` and `error_type` when the server exposes error details.
- Custom events (client 0.1.1, core 0.2.1): the transport sends `?custom_events=wrap` on the first request, every reconnect and every `resolveUrl` URL, and delivers `custom_event` frames under the author's type to `on()`, the selectors and the hooks' `lastEvent`. Against core 0.2.0 the parameter is ignored and they do not arrive.
- Error details: by default the server sends a generic `error_type` (`ExecutionFailed`, `StreamError`) and message. For a model provider failure `error_type` is a `ProviderErrorCategory`: `rate_limited`, `provider_unavailable` or `provider_error`. The real exception class and message arrive only when the server sets `AGENTICGRAPH_STREAM_EXPOSE_ERROR_DETAILS`. Types: `ProviderErrorCategory`, `ErrorType`, `GraphFailedReason`, `ExecutionTerminalMeta`.
- `execution_terminal` handlers run while `status` and `error` still hold their previous values; the status changes right after.
- `disconnect()` closes the stream and the transport. `reconnect()` is a manual reconnect: it does nothing when the transport is already connected. `useAgenticStream` (React and Vue) returns `reconnect()` too. A successful reconnect moves `status` from `error` to `streaming`, clears `error` and keeps the text. `error.canReconnect` is `false` when the first connect failed.

## Reconnect (SSETransport)

- On an error, or when the response ends, the transport closes the `EventSource` (the browser's own retry is off) and reopens it with `?cursor=<lastSeq>`. It resumes after the last event it delivered, so the gap is filled.
- Backoff: `baseDelayMs` (500) doubled per consecutive failure, with jitter, capped at `maxDelayMs` (10000).
- When consecutive failures go past `maxAttempts` (5), the transport gives up and reports `failed`: that is five retries that also failed. The stream then has `status === 'error'` with `error.kind === 'connection'` and the message `Reconnect attempts exhausted`. `error.canReconnect` is `true`: call `stream.reconnect()` (hooks: `reconnect()`); it resumes from the last event and `status` returns to `streaming`. Or make a new transport.
- A transient drop is not an error state. While retries run, `status` stays where it was.
- The server can ask for a reconnect (control `reconnect` with `after_ms`). The transport honours it.

## Stop reasons

`transport.getStopReason()` is on `SSETransport`. It is `null` while the transport runs.

| Reason | Meaning | Stream status |
|---|---|---|
| `terminal` | `execution_terminal` with `completed` or `failed` arrived | `complete`, or `error` with kind `terminal_error` for `failed` |
| `replay_unavailable` | The cursor is older than the server keeps | `error`, `error.kind` is `replay_unavailable`, with `cursor` and `final_state` |
| `resolver` | `resolveUrl` returned `null` | Unchanged. Not an error. |
| `manual` | `disconnect()` was called | Unchanged |
| `failed` | `maxAttempts` failures in a row | `error`, `connection` |

`final_state` on `replay_unavailable` arrives only when the stream URL carries `execution_id`. Add it to the URL you pass to the transport.

## resolveUrl, null and signed URLs

```ts
const transport = new SSETransport({
  url: payload.stream_url,
  resolveUrl: async (lastSeq) => {
    const response = await fetch(`/drafts/${payload.execution_id}/stream-url`);
    if (response.status === 404) return null; // the run is gone: stop, do not retry
    return (await response.json()).url;
  },
});
```

- `resolveUrl(lastSeq)` runs before each **reconnect**, not before the first connect. `lastSeq` is `null` when no event has arrived yet.
- Return a string: the transport appends `?cursor=` to it.
- Return `null`: the transport stops with reason `resolver`. The app ended the stream, so this is not a failure and it does not retry.
- Throw or reject: counts as one failed attempt, with backoff.
- Signed or expiring URLs: mint a fresh one in `resolveUrl`, from your own endpoint. The client adds `cursor` to whatever you return. If your server signs the whole query string, the signature fails once `cursor` is added. The server must validate the signature while ignoring `cursor`. The Laravel side is in the `agenticgraph-streaming` skill.
- `EventSource` cannot send custom headers. Authenticate with same-origin cookies or with the signed URL.

## Echo transport (broadcast)

```ts
import { AgenticStream, EchoTransport } from '@agenticgraph/stream-client';

const stream = await AgenticStream.connect({
  transport: new EchoTransport({ echo: window.Echo, channel: payload.channel }),
});
```

- `echo` is your configured Laravel Echo instance. The package never creates Echo. `laravel-echo` is an optional peer.
- No cursor, no `resolveUrl`, no replay. Events sent before the subscription are lost. Subscribe before the kickoff request when you can.
- `eventTypes` lists the event names to listen to when `listenToAll` is not available. The defaults cover the framework and Prism events.

## Script-tag build (IIFE)

`dist/index.global.js` sets the global `window.AgenticGraphStream`. The package exports it as `./iife`, and `unpkg` and `jsdelivr` point to it.

```html
<script src="/vendor/agenticgraph/index.global.js"></script>
<script>
  const { AgenticStream, SSETransport } = window.AgenticGraphStream;
  AgenticStream.connect({ transport: new SSETransport({ url: streamUrl }) });
</script>
```

The file is an IIFE with `sideEffects` set for it. Import the ESM or CJS build in bundled apps.

## Selectors and helpers

- `nodeTextSelector(node)`, `activeNodeSelector`, `thinkingSelector`, `toolCallSelector`: derived views of the events. The React and Vue hooks take them through `useStreamSelector`.
- `appendTextDelta(current, event)`: appends a `text_delta` to a string. Other events return the string unchanged. The hooks' `text` is built with it, so it holds every step of a tool run (the narration).
- `appendFinalTextDelta(current, event)`: the same, but `step_start` resets it to `''`. The hooks' `finalText` is built with it: the current or last step, so after a tool run the final answer, like the server's `run()->text`. Decide in the app which of `text` and `finalText` to persist.
- One model request is `stream_start`, then `step_start` ... `step_finish` per step, then `stream_end`. `step_finish.payload` has that step's `finish_reason` and `usage`; `stream_end.payload` the request's and the cumulative `usage`. `stream_end` is not a terminal: the run goes on.
- `executeResume(client, decision, onResume)`: calls your resume handler with the decision and the current `interrupt`. It throws when no handler is given.

## Tests

```ts
import { AgenticStream, MockTransport } from '@agenticgraph/stream-client';

const transport = new MockTransport();
const stream = await AgenticStream.connect({ transport });

transport.emitSequence(events); // StreamEvent objects
transport.simulateDrop();       // exercises the connection-drop path
```

- `emit(event)`, `emitSequence(events)`, `simulateDrop()`, `simulateFailure()` (the transport gave up: the `error` state and `reconnect()`, no server needed), `setConnectDelay(ms)`.
- `MockTransport` does not reconnect. Test reconnect behaviour with `SSETransport` and an injected `eventSourceFactory`.

## Mistakes

1. Two `connect()` calls on one transport, or `connect()` next to the React or Vue hook.
2. A retry loop around `SSETransport`. It already reconnects, and `reconnect()` is only for after `failed`.
3. Treating a transient drop as an error, or waiting for `status` to show one.
4. Expecting `EchoTransport` to replay what was sent during a drop.
5. A signed URL whose signature covers `cursor`.
6. Returning `null` from `resolveUrl` for a network error. Throw instead: `null` stops the stream for good.
