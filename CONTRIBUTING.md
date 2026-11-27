# Contributing

Thank you for helping. This guide tells you how to send a change.

## Before you start

- For a bug, open an issue with the bug template first, unless the fix is very small.
- For a new feature, open an issue with the feature template. Agree on the design before you write the code.
- For a security problem, do not open an issue. Read [SECURITY.md](SECURITY.md).

## Gates

Run these commands before you open a pull request. All of them must pass.

```bash
npm ci
npm run build
npm run typecheck
npm test
```

For development, use Node.js 22.18 or later in 22.x, or 24.11 or later in 24.x (the build tool needs this; CI runs 22 and 24). The packages support Node.js 18+.

## Live SSE harness

`npm run integration:sse` runs the client against a real Laravel app that uses AgenticGraph. It needs an LLM key in the app's `.env` and Redis on 127.0.0.1:6379. It flushes Redis databases 5 and 6 before it starts. These environment variables are optional:

| Variable | Use | Default |
|---|---|---|
| `SSE_APP_PATH` | The Laravel app folder | `../workflow-agents`, next to this repository |
| `SSE_APP_HOST` | The host that the servers bind to and that the client calls | `127.0.0.1` |
| `SSE_APP_PORT` | The first of three ports: normal server, short server, proxy | `8711` |
| `SSE_DEMO_TOPIC` | The topic that the demo graph gets for its runs | `how server-sent events reconnect after a dropped connection` |
| `SSE_HARNESS_OUT` | The folder for the JSON report | `agenticgraph-sse-harness` in the OS temporary folder |
| `KEEP_SERVERS` | Set to `1` to keep the servers up after the checks, for example for the script-tag page in a browser. Press Ctrl+C to stop. | not set |

## Rules for code and text

- Add a test that fails without your change and passes with it.
- Do not use em-dashes (the character U+2014) in code, comments, tests, docs or commit messages. Use a colon, a comma, parentheses or a new sentence.
- Write docs, comments and messages in Simplified Technical English: short sentences, one instruction in each sentence, active voice, and the same word for the same thing.
- If you change public behavior, add a line to `CHANGELOG.md` under the unreleased version.
- Keep a pull request to one change.

## Pull requests

1. Fork the repository and make a branch from `main`.
2. Make your change and run the gates.
3. Open a pull request. Tell what changed and why, and link the issue.
