# agui-reel

`agui-reel` validates and visualizes [AG-UI 1.0](https://github.com/ag-ui-protocol/ag-ui/tree/main/docs/spec/1.0) event streams before they reach a frontend.

It accepts JSON Lines or Server-Sent Events, checks run, message, tool-call, step, and nested-subagent lifecycles, then emits a human or JSON report. Valid streams can be rendered as a standalone SVG replay timeline.

## Why

An event can be valid JSON and still be impossible to replay: a tool result can arrive before its call closes, a message can switch subagent lineage, a nested worker can outlive its run, or a retry can emit events after a terminal event. `agui-reel` catches those sequence failures locally.

The AG-UI specification says arrival order is protocol order and timestamps are informational. The timeline therefore uses event ordinal and never reorders a stream by timestamp.

## Install

Stable Rust 1.85 or newer is required.

```bash
cargo install --path .
```

## Use

Validate JSONL:

```bash
agui-reel check examples/valid.jsonl
```

Emit a machine-readable report suitable for CI:

```bash
agui-reel check trace.jsonl --json > report.json
```

Pipe an SSE capture through auto-detection:

```bash
curl -N http://localhost:8000/agent | agui-reel check -
```

Render a valid trace without overwriting an existing file:

```bash
agui-reel render examples/valid.jsonl --output timeline.svg
```

Exit codes are `0` for a valid stream, `2` for semantic diagnostics, and `1` for malformed input or operational failure.

## Checks

The validator currently enforces:

- exactly one first-position `RUN_STARTED` and one last terminal event;
- matching `threadId` and `runId` on successful completion;
- unique message, tool-call, and subagent invocation IDs;
- start/content/end ordering for streamed messages and tool arguments;
- exactly one result after a tool call closes;
- step pairing within each root or subagent lane;
- active, known lineage for attributable events;
- valid nested-subagent parents and terminal closure;
- resolvable shorthand chunk continuations;
- required string fields for the lifecycle families it interprets.

The tool is a semantic sequence checker, not a replacement for the official JSON Schema. Unknown event types fail closed; `CUSTOM` remains the protocol's extension point.

## Development

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```

CI repeats these checks on stable Rust and runs `cargo check` on Rust 1.85.

## Limitations

- Input is buffered so the report can include an exact SHA-256 digest; it is not intended for unbounded live streams.
- The SVG shows causal lanes and arrival order, not wall-clock duration.
- Structural validation is intentionally focused on lifecycle fields; use the upstream AG-UI schema for exhaustive payload-shape validation.

## License

MIT
