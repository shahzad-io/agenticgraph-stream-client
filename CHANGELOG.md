# Changelog

## 0.2.0 - Unreleased

Broadcast keeps the SSE contract: the same events, in the same order, with no loss, no duplicates and the correct end status, after a drop, a reconnect and a late join. Needs core 0.3.0 for `BroadcastTransport`.

### Added

- **`BroadcastTransport`** for core 0.3: subscribes to the private stream channel, waits for the confirmed subscription, replays the store from the start, and merges the live frames with the replay by `seq`, dropping duplicates. It unpacks `batch` frames. A hole in `seq` starts one replay after the confirmed cursor (debounced 250 ms). A terminal with a hole before it replays first, then finishes. `stream_close` with no terminal replays at once. No frame for `idleMs` (5000) while connected starts a replay, with backoff 5 s, 10 s, 20 s, then every 30 s, reset on any frame. Options: `echo`, `channel`, `replayUrl`, `authUrl`, `token`, `refreshToken`, `resolveReplayUrl`, `idleMs`, `maxAttempts`. Stop reasons as on `SSETransport`: `terminal`, `replay_unavailable` (replay `gone`), `resolver`, `manual`, `failed`, `unauthorized`. `stream_close` is never passed to the handlers, so they see the same events as on SSE.
- **`createStreamTransport(descriptor, { echo, refreshToken, maxAttempts })`** builds `SSETransport` or `BroadcastTransport` from `$handle->stream()->toArray()`. It passes `refreshToken` and `maxAttempts` to either transport. A broadcast descriptor with no `echo` throws, and so does a descriptor it cannot use.
- **`streamChannelAuthorizer({ fallback })`** for Echo `channelAuthorization.customHandler`. It authorizes stream channels on the core's channel auth route (`auth_url`) with the token of the transport that subscribed, and passes any other channel to `fallback`.
- **`refreshToken` on both transports.** On 403 `stream_token_expired` the transport calls it once and retries with the new token. Null, another 403 code or a second 403 stops it with `unauthorized`.
- **Stop reason `'unauthorized'`** in `StreamTransportStopReason`: a 403 from a stream route that was not refreshed, or a failed channel auth. `AgenticStream` then sets `error.kind` `connection` with `canReconnect` false.
- **`authUrl`** option on `BroadcastTransport`: the descriptor `auth_url`, used by `streamChannelAuthorizer()`.
- **New exported types:** `StreamDescriptor`, `CreateStreamTransportOptions`, `ChannelAuthorizationHandler`, `BroadcastTransportOptions`, `BroadcastEchoInstance`, `BroadcastEchoChannel` and `ReplayPage`. `SSETransportOptions` has `refreshToken`.
- **`getLastSeq()`** on `BroadcastTransport`: the confirmed cursor, as on `SSETransport`.
- **Parity harness** (`npm run integration:parity`): one scenario set over SSE and over broadcast (a drop, a tool with text reset, a held answer, interrupted and resumed, failed, a late join, an event filter), plain, with chaos (duplicated and swapped frames) and batched, compared after normalization. It runs in CI with fakes, from recorded core runs.

### Behavior changes

- **`SSETransport` probes once with `fetch` after each error.** `EventSource` cannot read a status, so the transport checks the URL once, with or without `refreshToken`. Any 403 that is not refreshed stops it with `unauthorized`, not with `failed` after `maxAttempts`. Any other answer or a network error: the normal backoff.
- **The `SSETransport` state is `reconnecting` at once on an error**, before the probe answers.
- **`AgenticStream.connect()` resolves on a manual stop.** When the transport is stopped by hand (`transport.disconnect()`, stop reason `manual`) before it connects, `connect()` resolves and the stream is already stopped. Any other stop before `connected` (`unauthorized`, `resolver`, `replay_unavailable`, `terminal`) rejects, as does state `failed`. The React and Vue hooks stop the transport by hand when the component unmounts during connect, so an unmount gives no rejection and no transport that goes on trying.
- **`AgenticStream.disconnect()` stops the transport only for the stream that connected last on it.** A React StrictMode remount, or a remount with the same transport during connect, ends with one connected stream: the late cleanup of the first stream does not stop the transport of the second.
- **Broadcast replay failures use the SSE backoff and `maxAttempts`.** A failed replay (network error or 5xx) is retried after a random time between half and all of `min(10 s, 500 ms * 2^(n-1))`. More than `maxAttempts` (5) failed replays in a row stop the transport with `failed`. A replay that answers resets the count.
- **While interrupted, the idle replay runs every 6 x `idleMs`** (30 s by default), so a resume that the socket did not deliver is found in the store. `stream_resume` brings back the normal schedule.

### Deprecated

- **`EchoTransport`.** It works only with core `streaming.broadcast.private = false` and `batch_ms = 0`, and has no replay, order or dedupe. Use `createStreamTransport()` or `BroadcastTransport`. Removal in a later minor version.

### Upgrade

1. Send `$handle->stream()->toArray()` from your start endpoint and build the transport with `createStreamTransport(descriptor, { echo })` in place of `EchoTransport`.
2. Add `streamChannelAuthorizer()` to your Echo `channelAuthorization` as `customHandler`, and pass your own handler as `fallback` if you have other private channels.
3. Long sessions: give the transport a `refreshToken` callback that asks your app for `$handle->streamToken()`.
4. TypeScript: an exhaustive `switch` on `StreamTransportStopReason` must handle `'unauthorized'`.

**Requires:** core `agenticgraph/agenticgraph` 0.3.0 for `BroadcastTransport`. `SSETransport` also works with core 0.2.x. The React and Vue adapters depend on `@agenticgraph/stream-client` `^0.2.0`.

## 0.1.2 - 2026-11-08

### Changed

- Skills and docs guidance only, no code change. The `agenticgraph-stream-client` skill says to import the package from `resources/js/app.js` with Vite and load it with `@vite()`, and to use the script-tag build only without a bundler. The React and Vue adapters now depend on `@agenticgraph/stream-client` `^0.1.2`.

## 0.1.1 - 2026-11-07

Fixes from the first public install.

### Fixed

- **Custom events reach the app.** The SSE transport sends `?custom_events=wrap` on the first request, every reconnect and every `resolveUrl` URL, and turns `custom_event` frames back into the author's event type. `on('sources', ...)`, the selectors and the hooks' `lastEvent` now see them. Needs core 0.2.1; against core 0.2.0 the parameter is ignored and custom events are still not delivered over SSE.
- **The terminal event comes before the status change.** `execution_terminal` handlers run while `status` and `error` still hold their previous values; the status changes right after, even if a handler throws.
- **`reconnect()` in the hooks.** `useAgenticStream` (React and Vue) returns `reconnect()`, which resumes from the last event after "Reconnect attempts exhausted". `error.canReconnect` says when that is possible (a lost connection only). A successful reconnect moves `status` from `error` back to `streaming` and clears `error`; before, it stayed in `error`. The text is kept.
- **Vue types.** `text`, `finalText`, `status`, `error` and `interrupt` are `Ref<string>`, `Ref<StreamStatus>` and so on, no longer `Ref<... | undefined>`.
- **Error types.** New `ProviderErrorCategory` (`rate_limited`, `provider_unavailable`, `provider_error`), `ErrorType`, `GraphFailedReason` and `ExecutionTerminalMeta`. `error_type` and `message` are generic by default and carry the exception class and message when the server sets `AGENTICGRAPH_STREAM_EXPOSE_ERROR_DETAILS`. `error.kind === 'terminal_error'` now uses `meta.message` and adds `error_type` from `meta.error_type` when present.

### Added

- `MockTransport.simulateFailure()` simulates a transport that gave up reconnecting (connection state `failed`), so tests can cover the `error` state and `reconnect()` without a server. `simulateDrop()` is unchanged.

**Compatible core:** `agenticgraph/agenticgraph` 0.2.1 or later for custom events and error details; 0.2.0 works for everything else.

**Later:** retry the first connect from the hooks (`canReconnect` is `false` when the first connect fails).

## 0.1.0 - 2026-11-06

Initial release of `@agenticgraph/stream-client`, `@agenticgraph/stream-react` and `@agenticgraph/stream-vue`: show AgenticGraph runs live in the browser or Node.js.

- **Typed events** for AgenticGraph's stream wire contract v1.1: graph and node lifecycle, interrupts, errors, text deltas, tool calls, thinking, model steps and the final `execution_terminal`.
- **`AgenticStream`:** subscribe with `.on()` and follow `status`, `error` and the pending `interrupt`; `executeResume()` sends the decision through your own resume endpoint.
- **SSE transport** that reconnects by itself: resumes from the last event (nothing missed, nothing shown twice), backs off with jitter, follows the server's planned reconnects, and keeps the connection through an approval wait. `resolveUrl` supports signed or expiring stream URLs.
- **Laravel Echo transport** for broadcast streams, and **`MockTransport`** for tests.
- **Selectors** for the active node, accumulated text, the final answer after a tool run (`finalText`), tool calls and thinking.
- **React and Vue adapters:** `useAgenticStream` (text, final text, status, error, interrupt, resume) and `useStreamSelector`.
- **Browser build:** one `<script>` tag, global `AgenticGraphStream`, about 5 KB gzipped, no dependencies.
- `final_state` is typed `Record<string, unknown> | null`: it is `null` unless the server exposes it.

**Requirements:** Node.js 18+. React 18+ for `stream-react`, Vue 3.4+ for `stream-vue`, `laravel-echo` 1.16+ only for the Echo transport.

**Compatible core:** `agenticgraph/agenticgraph` 0.2.x on the server (stream wire contract v1.1).
