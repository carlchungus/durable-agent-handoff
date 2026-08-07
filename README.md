# handoff Supervisor v2

`handoff` is a durable control plane for long-running agent execution. The
Supervisor owns one append-only journal and one reducer projection. Sessions,
Activities, Attempts, Results, Messages, Controls, and writer Leases remain
separate identities, but no legacy store may mutate alongside the journal.

The state root must be supervisor-private and outside every worker-writable
root. Set `HANDOFF_HOME` or pass `--state` to select it.

## Start and observe

Start requires the exact native runtime Session identity; there is no global
"last session" selector:

```sh
handoff start --runtime codex --session THREAD_ID --prompt 'work' \
  --root /repo --authorized-by human:id --idempotency-key request-01 --json
handoff status EXECUTION_ID --json
handoff list --json
handoff tui --snapshot
handoff activity list --json
```

The arca-cloud promotion seam accepts one strict JSON object on stdin:

```sh
handoff execution start --file - --json <<'JSON'
{
  "native_session": {"runtime": "codex", "id": "THREAD_ID"},
  "prompt": "work",
  "runtime": {"name": "codex", "sandbox": "workspace-write"},
  "root": "/repo",
  "authority": {"requested_by": "human:id", "human_authorized": true, "sandbox": "workspace-write"},
  "finalizer": {"enabled": false},
  "budget": {"max_task_attempts": 3, "max_launches": 12},
  "idempotency_key": "request-01"
}
JSON
```

The response is exactly `{"execution": ..., "receipt": ...}`. Reusing a key
with different canonical input fails closed.

Pause is synchronous and idempotent. It fences active Attempts, releases all
writer Leases, and only then returns:

```sh
handoff execution pause --workflow WORKFLOW_ID --json
```

Replies create a new immutable Activity generation on the exact Session; they
never reopen or modify a completed Result:

```sh
handoff reply --execution EXECUTION_ID --activity ACTIVITY_ID \
  --message 'continue' --idempotency-key reply-01 --json
```

## Runtime and service boundary

Codex, Claude, and Pi drivers construct argv, resume only the supplied exact
Session, decode typed provider milestones, and receive prompts on stdin. A
prompt is never placed in argv, Supervisor responses, captured stdout, or a
service unit. Full trust uses provider-native flags and still launches through
argv; no shell wrapper is used. Pi fails closed for read-only work because it
does not provide a native OS sandbox.

Run or serve queued Activities through the Supervisor projection:

```sh
handoff run WORKFLOW_ID --trust-mode workspace
handoff serve --trust-mode full --environment-json /private/env.json
```

`--environment-json` must be a regular mode-0600 JSON object. Values are read
only at service startup and passed to drivers without being persisted. Service
units contain only argv, the private state root, the environment-file path,
and trust mode.

## Migration

`handoff execution import-v1` is the only v1 compatibility path. It hashes and
replays legacy event ledgers deterministically, preserves source bytes, and
records one `legacy.imported` transaction. After import, all execution reads
and writes use the Supervisor journal.

Required checks before handoff:

```sh
gofmt -w cmd internal supervisor
go test ./...
go test -race ./...
go vet ./...
git diff --check
```
