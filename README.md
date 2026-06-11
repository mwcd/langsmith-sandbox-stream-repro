# LangSmith Sandbox Stream Repro

Minimal repro for LangSmith sandbox stdout data loss through the JS SDK
`onStdout` callback path.

## Setup

```sh
pnpm install
LANGSMITH_API_KEY=... pnpm repro
```

This repro uses `langsmith@0.7.6`, the latest published SDK version at the
time this repro was created.

## Options

```sh
REPRO_BYTES=120000000 pnpm repro
REPRO_RECORD_BYTES=1024 pnpm repro
LANGSMITH_SANDBOX_SNAPSHOT_NAME=python pnpm repro
LANGSMITH_SANDBOX_TIMEOUT_SECONDS=300 pnpm repro
KEEP_SANDBOX=1 pnpm repro
```

The script streams JSONL records through `sandbox.run(..., { onStdout })` to
match Console's usage. It checks:

- JSONL sequence numbers are contiguous
- final record count matches the expected count

On failure, it prints the sandbox ID, command ID, chunk count, byte count, and
the first observed sequence gap or final record count mismatch.
