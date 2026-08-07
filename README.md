# handoff Supervisor v2

`handoff` is a durable control plane for long-running agent execution. The
Supervisor owns one append-only journal and one reducer projection. Sessions,
Activities, Attempts, Results, Messages, Controls, and writer Leases remain
separate identities, but no legacy store may mutate alongside the journal.

The state root must be supervisor-private and outside every worker-writable
root. Set `HANDOFF_HOME` or pass `--state` to select it.

## Start and observe

Ordinary `start` and `create` may launch a new native Session; passing
`--session` resumes only that exact identity. There is no global "last session"
selector:

```sh
printf '%s' 'work' | handoff start --runtime codex --prompt-file - \
  --root /repo --authorized-by human:id --idempotency-key request-01 --json
handoff status EXECUTION_ID --json
handoff list --json
handoff tui --snapshot
handoff activity list --json
handoff preference set planner --candidate claude:sonnet:high --candidate codex:gpt-5:xhigh
```

The arca-cloud promotion seam accepts one strict JSON object on stdin:

```sh
handoff execution start --file - --json <<'JSON'
{
  "idempotency_key": "request-01",
  "goal": "work",
  "prompt": "work",
  "remote_root": "/repo",
  "runtime": "codex",
  "resume_id": "THREAD_ID",
  "sandbox": "workspace-write",
  "role": "human:id"
}
JSON
```

The promotion response is exactly `{"workflow_id":"...","node_id":"..."}`.
Reusing a key with different canonical input fails closed.

Pause is synchronous and idempotent. It records exact stop controls, waits for
the executor to apply them and record terminal exits, releases writer Leases
only after those exits, and returns a completed pure projection or a bounded
timeout:

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
handoff service install --enable --trust-mode workspace --environment-json /private/env.json
handoff github merge --execution EXECUTION_ID --repo OWNER/REPO --pr 7 \
  --gate verify --idempotency-key publication-01 --approved --json
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
gofmt all Go files before running the checks below.
go test ./...
go test -race ./...
go vet ./...
git diff --check
```
