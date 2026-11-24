---
name: agenticgraph-stream-client
description: "Consume AgenticGraph stream events in JavaScript with @agenticgraph/stream-client: build a transport from the stream descriptor (SSE or broadcast over a private Echo channel), read status, interrupts and errors, reconnect, stream tokens and refreshToken, resolveUrl and stop reasons, selectors, the mock transport for tests, and the script-tag build."
---

# AgenticGraph stream client (npm)

Package: `@agenticgraph/stream-client`. `SSETransport` works with AgenticGraph core 0.2.x and 0.3.x (wire contract v1.1; core 0.2.1 or later for custom events and error details). `BroadcastTransport`, `createStreamTransport()` and `streamChannelAuthorizer()` need core 0.3.0. Server side (routes, stream token, `stream()`, `broadcast()`, resume): the `agenticgraph-streaming` skill in the Laravel package.

Only `src/index.ts` exports are public. Every method and name below is checked against it by `src/skill.test.ts`.

## Rules

- `AgenticStream.connect({ transport })` is the only way to build a stream. It resolves when the transport reports `connected`, and rejects if the transport reports `failed`, or stops (for example `unauthorized`), first.
- Build the transport from the descriptor the server sends (`$handle->stream()->toArray()`) with `createStreamTransport(descriptor, { echo })`. The handlers do not change between SSE and broadcast.
- One `connect()` per transport. With the React or Vue hook, the hook owns the connection: do not also call `connect()` on the same transport.
- `SSETransport` and `BroadcastTransport` both give every stored event once, in seq order, after a drop or a late join. `EchoTransport` is deprecated: it loses events, drops `batch` frames and works only with `private = false` and `batch_ms = 0` on the server.
- `SSETransport` and `BroadcastTransport` recover by themselves. Do not write a retry loop around them.
- Read `transport.getStopReason()` to learn why a stream stopped. Do not guess from `status`.
- In a Laravel app with Vite (the default), import the package from `resources/js/app.js` and load it with `@vite()`. Use the script-tag build only when there is no bundler. Never copy `dist/index.global.js` into `public/`.

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

`transport.getStopReason()` is on `SSETransport` and `BroadcastTransport`. It is `null` while the transport runs.

| Reason | Meaning | Stream status |
|---|---|---|
| `terminal` | `execution_terminal` with `completed` or `failed` arrived | `complete`, or `error` with kind `terminal_error` for `failed` |
| `replay_unavailable` | The cursor is older than the server keeps (SSE control, or replay `gone` on broadcast) | `error`, `error.kind` is `replay_unavailable`, with `cursor` and `final_state` |
| `resolver` | `resolveUrl` or `resolveReplayUrl` returned `null` | Unchanged. Not an error. |
| `manual` | `disconnect()` was called | Unchanged |
| `failed` | `maxAttempts` failures in a row (SSE) | `error`, `connection` |
| `unauthorized` | 403 from a stream route or from channel auth: invalid token, `refreshToken` gave no new token, or the app authorizer said no | `error`, `connection` with `canReconnect` false; `connect()` rejects if it happens before `connected` |

An exhaustive `switch` over `StreamTransportStopReason` must handle `unauthorized`.

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

## Stream token and refreshToken (core 0.3)

- Core 0.3 puts a stream token in `stream_url` and `replay_url` (query `token`) and in the descriptor (`token`). Pass the URLs as the server gives them. Do not build them yourself.
- The token expires (`streaming.token.ttl`, 3600 s by default). Give the transport `refreshToken`: an async function that asks your app for `$handle->streamToken()` and returns it, or `null`.
- On 403 `stream_token_expired` the transport calls `refreshToken` once and retries with the new token. `null`, any other 403 code, or a second 403 stops it with `unauthorized`.
- `SSETransport` cannot read a status from `EventSource`. With `refreshToken` set, it checks a failed connection once with `fetch` to read the 403 code. Without `refreshToken`, a 403 counts as a normal failure and ends in `failed`.

## Broadcast (BroadcastTransport, core 0.3)

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
  echo: window.Echo,
  refreshToken: async () => (await (await fetch(`/runs/${runId}/stream-token`)).json()).token,
});
const stream = await AgenticStream.connect({ transport });
```

- `createStreamTransport()` returns `SSETransport` for `transport: 'sse'` and `BroadcastTransport` for `transport: 'broadcast'`. A broadcast descriptor without `echo` throws.
- The channel is private (`private-agenticgraph.stream.{id}`). `streamChannelAuthorizer()` answers Echo's auth for stream channels with the token and `auth_url` of the transport that subscribed. Echo has one auth handler per instance: pass your own handler as `fallback` for your other private channels.
- `BroadcastTransport` waits for the subscription to be confirmed, then reads the store through the replay route, and merges it with the live frames by seq. It unpacks `batch` frames, drops events it has seen, and holds frames after a hole until a replay fills it (250 ms debounce). The store decides: seq can have holes that exist nowhere (an event filter).
- End: a `completed` or `failed` `execution_terminal` stops it (`terminal`). `stream_close` without the terminal starts a replay at once. With no frame for `idleMs` (5000) while streaming it replays, then after 10 s, 20 s and every 30 s; a frame resets this. `interrupted` keeps it connected.
- `stream_close` is not passed to handlers: on SSE it does not exist, so the handlers see the same events on both transports.
- Options: `echo`, `channel`, `replayUrl`, `authUrl`, `token`, `refreshToken`, `resolveReplayUrl`, `idleMs`. `resolveReplayUrl(cursor)` runs before each replay; `null` stops with `resolver`.
- Queue the run on the server (not `sync`), or the web request runs the whole broadcast run.

## Echo transport (deprecated)

`EchoTransport` is deprecated and will be removed in a later minor version. It has no cursor, no replay, no order and no dedupe, and core 0.3 sends to a private channel in `batch` frames, which it does not read. It works only when the server sets `AGENTICGRAPH_STREAM_BROADCAST_PRIVATE=false` and `AGENTICGRAPH_STREAM_BROADCAST_BATCH_MS=0`. Move to `createStreamTransport()`.

## Script-tag build (IIFE, only when there is no bundler)

With Vite, import the package from `resources/js/app.js` and load it with `@vite()`. With no bundler, load the pinned CDN build: `dist/index.global.js` sets the global `window.AgenticGraphStream`. The package exports it as `./iife`, and `unpkg` and `jsdelivr` point to it.

```html
<script src="https://cdn.jsdelivr.net/npm/@agenticgraph/stream-client@0.1.2/dist/index.global.js"></script>
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
4. Using `EchoTransport` with core 0.3, or expecting it to replay what was sent during a drop.
5. A signed URL whose signature covers `cursor`.
6. Returning `null` from `resolveUrl` for a network error. Throw instead: `null` stops the stream for good.
7. Building the stream URL by hand with core 0.3: it has no token, so the route answers 403.
8. A second Echo `customHandler` for stream channels. Use `streamChannelAuthorizer({ fallback })` as the only handler.
