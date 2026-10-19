# Changelog

## 0.1.0 (unreleased)

### BREAKING (B13b B: multi-step streaming)
Needs the agenticgraph core from the same release (wire contract v1.1 B13b amendment).

- New Prism content types `stream_start` (`StreamStartPayload`: `model`, `provider`, `metadata`) and `stream_end` (`StreamEndPayload`: `finish_reason`, cumulative `usage`, `citations`, `additional_content`), with `StreamStartEvent` and `StreamEndEvent`. `SSETransport` and `EchoTransport` listen to them by default.
- `StepStartPayload` is `{ id, timestamp }` only. `StepFinishPayload` has required `finish_reason` (`string | null`) and `usage` (that step's); `citations` and `additional_content` moved to `StreamEndPayload`. Code that read `usage` from the last `step_finish` reads one step's: use `stream_end`.
- A tool run now streams one `step_start` and `step_finish` pair per step, each step with its own `message_id`. The hooks' `text` keeps every step's deltas, in order (unchanged rule); `stream_end` does not change `status`.
- New: `finalText` on the React and Vue hooks, the text of the current or last step (`step_start` resets it), so after a tool run the final answer, like the server's `LlmResponse::text`. New helper `appendFinalTextDelta`.

### Release metadata (B11 session C)

- Each package: `publishConfig.access: public`, a `prepublishOnly` build, its own README and LICENSE (published with `dist`), `repository` (with its directory), `homepage`, `bugs`, `keywords` and `sideEffects` (`false`; the client keeps `dist/index.global.js`, which sets a global). React and Vue depend on `@agenticgraph/stream-client` `^0.1.0` (was `*`). The stray root dependency on the client is removed.
- Dev: vitest 5, vue 3.5.44 (advisory), lock refreshed. `npm audit` leaves one low esbuild advisory (Windows dev server only) that needs tsup 9, a major.

### BREAKING (B2: SSE reconnect)
Needs the agenticgraph core from the same release (wire contract v1.1 B2 amendment: SSE control events).

- `SSETransport` reconnects by itself. On an error or the end of a response it closes the `EventSource` at once (no native browser retry) and reopens with `?cursor=<lastSeq>`, with exponential backoff and jitter. New options: `maxAttempts` (default 5, consecutive failures only; a successful open or a planned server close resets it), `baseDelayMs` (500), `maxDelayMs` (10000), and `resolveUrl(lastSeq)` (async, called before each reconnect for signed or expiring URLs).
- `useCursorQuery` is removed. The cursor is always sent as `?cursor=` (a new `EventSource` cannot set `Last-Event-ID`).
- New connection state `failed` on `StreamTransportConnectionState`: the transport gave up after `maxAttempts`. `AgenticStream` sets `status: 'error'` with `error.kind === 'connection'`, and `AgenticStream.connect()` rejects if it happens before the first open. Exhaustive `switch` statements over the state need a new case.
- `connected` is reported on the real `open` event, not when the `EventSource` is created. Test doubles must fire `onopen`.
- The transport stops on an `execution_terminal` with `completed` or `failed` and on control `replay_unavailable`. An `interrupted` terminal keeps the connection open for the resumed segment.
- Duplicates: stored events with `seq <= lastSeq` are dropped. Control events (heartbeat, reconnect, replay_unavailable) are recognized by their `control` key; they never move `lastSeq` and are not dispatched to `.on()`.
- `stream_replay_unavailable` is no longer a 9-field wire event: it is `StreamReplayUnavailableEvent` `{ eventType, payload: { cursor, final_state? } }`, part of the `StreamEvent` union. It sets `error` only if the stream is not already `complete`.
- `stream_resume` moves the status from `interrupted` back to `streaming` and clears `interrupt`.
- A drop no longer overwrites an existing error (for example a failed terminal followed by the close).
- `_handleSseMessage(eventType, data)` (internal test hook) no longer takes an id.
- `resolveUrl` may return `null`: the transport stops reconnecting, closes, and moves to `disconnected`. Not a failure, and `AgenticStream` does not set an error. A throw still counts as a failed attempt.
- New `SSETransport.getStopReason()` and `StreamTransportStopReason` (`'terminal' | 'replay_unavailable' | 'resolver' | 'manual' | 'failed'`, `null` while running). `onConnectionStateChange` callbacks receive it as a second argument. `StreamTransport` gains optional `getStopReason?()`; custom transports need no change.

### Added
- Browser build `dist/index.global.js`: IIFE, global `AgenticGraphStream`, minified, no runtime dependencies (16.6 KB, 4.8 KB gzipped). `unpkg`, `jsdelivr` and the `./iife` export point to it.

### BREAKING
- `ExecutionTerminalPayload.final_state` is `Record<string, unknown> | null`. The server sends `null` unless it sets `AGENTICGRAPH_STREAM_EXPOSE_FINAL_STATE=true` (wire contract v1.1). The `replay_unavailable` error and test fixtures use the same type.
