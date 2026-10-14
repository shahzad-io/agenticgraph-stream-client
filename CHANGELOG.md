# Changelog

## Unreleased

### BREAKING
- `ExecutionTerminalPayload.final_state` is `Record<string, unknown> | null`. The server sends `null` unless it sets `AGENTICGRAPH_STREAM_EXPOSE_FINAL_STATE=true` (wire contract v1.1). The `replay_unavailable` error and test fixtures use the same type.
