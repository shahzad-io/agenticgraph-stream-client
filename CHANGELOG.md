# Changelog

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
