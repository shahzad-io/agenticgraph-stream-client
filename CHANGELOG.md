# Changelog

## Unreleased

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
