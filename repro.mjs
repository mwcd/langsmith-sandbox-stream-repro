import { randomUUID } from "node:crypto";
import { SandboxClient } from "langsmith/sandbox";

const targetBytes = readPositiveInt("REPRO_BYTES", 120_000_000);
const recordBytes = readPositiveInt("REPRO_RECORD_BYTES", 1024);
const timeoutSeconds = readPositiveInt("LANGSMITH_SANDBOX_TIMEOUT_SECONDS", 300);
const snapshotName = process.env.LANGSMITH_SANDBOX_SNAPSHOT_NAME;
const keepSandbox = process.env.KEEP_SANDBOX === "1";

const recordCount = Math.ceil(targetBytes / recordBytes);
const commandId = `stream-repro-${randomUUID()}`;

const command = String.raw`
set -euo pipefail
PYTHON_BIN="$(command -v python3 || command -v python)"
"$PYTHON_BIN" - <<'PY'
import json
import os
import sys

record_count = int(os.environ["REPRO_RECORD_COUNT"])
record_bytes = int(os.environ["REPRO_RECORD_BYTES"])

for i in range(record_count):
    prefix = json.dumps({"seq": i, "payload": ""}, separators=(",", ":"))
    # Replace the empty payload while keeping every line close to record_bytes.
    payload_len = max(record_bytes - len(prefix) - 1, 0)
    line = json.dumps(
        {"seq": i, "payload": "x" * payload_len},
        separators=(",", ":"),
    )
    sys.stdout.write(line + "\n")
    if i % 100 == 0:
        sys.stdout.flush()

sys.stdout.flush()
PY
`;

const client = new SandboxClient();
let sandbox;

try {
  console.log("creating sandbox", {
    snapshotName: snapshotName ?? null,
    targetBytes,
    recordBytes,
    recordCount,
    commandId,
  });

  sandbox = await client.createSandbox(
    snapshotName ? { snapshotName, timeout: 60 } : { timeout: 60 },
  );

  console.log("sandbox ready", { sandboxId: sandbox.id });

  const handle = await sandbox.run(command, {
    wait: false,
    commandId,
    timeout: timeoutSeconds,
    idleTimeout: -1,
    killOnDisconnect: false,
    ttlSeconds: 600,
    env: {
      REPRO_RECORD_COUNT: String(recordCount),
      REPRO_RECORD_BYTES: String(recordBytes),
    },
  });

  const stats = await consumeStdout(handle, recordCount);
  const result = await handle.result;

  if (result.exitCode !== 0) {
    throw new Error(
      `sandbox command exited ${result.exitCode}: ${result.stderr.slice(0, 500)}`,
    );
  }

  if (stats.records !== recordCount) {
    throw new Error(
      `record count mismatch: expected ${recordCount}, received ${stats.records}`,
    );
  }

  console.log("stream ok", {
    sandboxId: sandbox.id,
    commandId,
    stdoutChunks: stats.stdoutChunks,
    stdoutBytes: stats.stdoutBytes,
    records: stats.records,
  });
} catch (error) {
  console.error("stream failed", {
    sandboxId: sandbox?.id,
    commandId,
    errorName: error?.name,
    errorMessage: error?.message,
    stack: error?.stack,
  });
  process.exitCode = 1;
} finally {
  if (sandbox && !keepSandbox) {
    await sandbox.delete().catch((error) => {
      console.error("failed to delete sandbox", {
        sandboxId: sandbox.id,
        errorName: error?.name,
        errorMessage: error?.message,
      });
    });
  }
}

async function consumeStdout(handle, expectedRecords) {
  let expectedOffset = 0;
  let stdoutBytes = 0;
  let stdoutChunks = 0;
  let expectedSeq = 0;
  let pending = "";

  for await (const chunk of handle) {
    if (chunk.stream !== "stdout") {
      continue;
    }

    stdoutChunks += 1;
    const bytes = Buffer.byteLength(chunk.data);

    if (chunk.offset !== expectedOffset) {
      throw new Error(
        [
          "stdout offset gap",
          `expectedOffset=${expectedOffset}`,
          `receivedOffset=${chunk.offset}`,
          `missingBytes=${chunk.offset - expectedOffset}`,
          `chunkBytes=${bytes}`,
          `stdoutChunks=${stdoutChunks}`,
          `stdoutBytes=${stdoutBytes}`,
          `records=${expectedSeq}`,
        ].join(" "),
      );
    }

    expectedOffset += bytes;
    stdoutBytes += bytes;
    pending += chunk.data;

    let newlineIndex;
    while ((newlineIndex = pending.indexOf("\n")) !== -1) {
      const line = pending.slice(0, newlineIndex);
      pending = pending.slice(newlineIndex + 1);
      if (line.length === 0) {
        continue;
      }

      let record;
      try {
        record = JSON.parse(line);
      } catch (error) {
        throw new Error(
          `malformed JSONL at seq=${expectedSeq}: ${error.message}; line prefix=${line.slice(0, 120)}`,
        );
      }

      if (record.seq !== expectedSeq) {
        throw new Error(
          `sequence gap: expected seq=${expectedSeq}, received seq=${record.seq}`,
        );
      }

      expectedSeq += 1;

      if (expectedSeq % 10_000 === 0 || expectedSeq === expectedRecords) {
        console.log("stream progress", {
          records: expectedSeq,
          stdoutChunks,
          stdoutBytes,
        });
      }
    }
  }

  if (pending.length > 0) {
    throw new Error(
      `stdout ended with partial line: pendingBytes=${Buffer.byteLength(pending)}`,
    );
  }

  return {
    stdoutChunks,
    stdoutBytes,
    records: expectedSeq,
  };
}

function readPositiveInt(name, fallback) {
  const raw = process.env[name];
  if (raw == null || raw === "") {
    return fallback;
  }

  const value = Number(raw);
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer, got ${raw}`);
  }
  return value;
}
