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
printf '%s' 'work' | handoff start --runtime codex --file - \
  --root /repo --authorized-by human:id --idempotency-key request-01 --json
# An autonomous merge-capable execution must configure its gates at start.
printf '%s' 'ship it' | handoff start --runtime codex --file - \
  --root /repo --authorized-by human:id --idempotency-key request-02 \
  --finalizer-enabled --required-check verify --require-human \
  --require-verifier --verifier verifier:ci --json
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
  "role": "human:id",
  "finalizer_enabled": true,
  "finalizer_required_checks": ["verify"],
  "finalizer_require_human": true,
  "finalizer_require_verifier": true,
  "finalizer_verifiers": ["verifier:ci"]
}
JSON
```

The promotion response is exactly `{"workflow_id":"...","node_id":"..."}`.
Finalizer fields are flat and immutable once the execution starts; an enabled
finalizer requires nonempty named checks, human approval, and independent
verifier identities. Reusing a key with different canonical input fails closed.

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
  --file - --idempotency-key reply-01 --json <<'TEXT'
continue
TEXT
```

Independent verification is an authority-owned command. The verifier identity
and Result ID are explicit flags, while the evidence summary is stdin-only; a
worker cannot self-attest through its Result payload:

```sh
printf '%s' 'independent verifier pass' | handoff attest \
  --result RESULT_ID --verifier verifier:ci --verdict pass \
  --evidence evidence:verify --file - --idempotency-key attestation-01 --json
```

The configured verifier must be authorized by the immutable Workflow and must
differ from its requester. Unknown Result IDs, unauthorized or duplicate
attestations, and summary values supplied through argv fail closed.

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
and trust mode. On every service start, inherited Attempts are reconciled before
the queue is scheduled: dead or prepared orphans receive durable terminal exit
evidence and release their exact writer Lease. An exact live orphan fails
closed because Supervisor v2 does not guess at runtime adoption.

## Migration

`handoff execution import-v1` is the only v1 compatibility path. It inventories
and normalizes only legacy workflow-history ledgers, preserves source bytes,
and records one `legacy.imported` transaction. Legacy Session, Activity, and
team ledgers are not replayed; exact native Session/Activity recovery is
unsupported and marked unresolved. After import, all execution reads and
writes use the Supervisor journal.

Required checks before handoff:

```sh
gofmt all Go files before running the checks below.
go test ./...
go test -race ./...
go vet ./...
git diff --check
```
