# Changelog

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
