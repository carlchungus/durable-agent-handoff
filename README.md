# handoff

`handoff` keeps long-running agent work alive, observable, and resumable. It
stores every change in one append-only journal so a crash cannot leave several
state files disagreeing with each other.

The state root must be supervisor-private and outside every worker-writable
root. Set `HANDOFF_HOME` or pass `--state` to select it.

## Start and observe

Ordinary `start` and `create` may launch a new native Session; passing
`--session` resumes only that exact identity. There is no global "last session"
selector:

```sh
printf '%s' 'work' | handoff start --runtime codex --file - \
  --root /repo --authorized-by human:id --idempotency-key request-01 --json
# A goal keeps running until it is done or genuinely needs a human.
printf '%s' 'ship it' | handoff goal start --runtime codex --file - \
  --root /repo --goal 'ship the verified change' \
  --authorized-by human:id --idempotency-key request-02 \
  --finalizer-enabled --required-check verify --require-human --json
# A monitoring supervisor can persist a ten-minute cadence without wasting a
# live model turn while it waits.
printf '%s' 'audit every active workstream' | handoff goal start \
  --runtime codex --file - --root /repo --goal 'keep work unblocked' \
  --authorized-by human:id --idempotency-key supervisor-01 \
  --wake-interval 10m --json
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
  "evaluator_model": "deepseek/deepseek-v4-flash-0731",
  "wake_interval_seconds": 600
}
JSON
```

The promotion response is exactly `{"workflow_id":"...","node_id":"..."}`.
Finalizer fields are flat and immutable once the execution starts; an enabled
finalizer requires a nonempty canonical exact set of external GitHub checks.
Human approval is independently optional. Independently hosted CI and GitHub
checks are the verification authority; handoff does not pretend that same-UID
workers can authenticate their own Results. Reusing a key with different
canonical input fails closed.

A goal has a simple loop: run one worker turn, ask a small tool-less model to
choose `accept`, `continue`, or `escalate`, then act on that choice. The worker
turn stays on the exact process attempt that produced it; the decision is
stored on the normal result. `continue` creates the next turn on the same
native session in the same journal write. `escalate` must include one concrete
blocker and question. A model failure leaves the turn pending and retries it.
Goals are unbounded by default. `--max-turns N` is an explicit opt-in safety
cap, not a required budget and not an unattended-operation default.
`--wake-interval 10m` durably schedules each automatic continuation for a
future time. `status` and `list` expose `next_wake_at`/`next_wake`; queued human
replies remain immediately runnable. The service checks persisted deadlines,
so no sleeping shell or occupied model turn is involved.

DeepSeek's forced decision tool is the default because strict
`response_format` was unreliable against the checked-in real transcripts. The
model is told exactly what the configured follow-up step can do: the current
GitHub finalizer can merge an explicitly supplied PR, but cannot push a branch,
create or discover a PR, or start itself.

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

The immutable worker Result is not a publication authority. The finalizer
rechecks the exact configured external GitHub checks on the prepared PR head;
human approval, when configured, is a separate gate. Same-UID handoff workers
cannot self-authenticate through their Result payload.

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

`service install --enable` also installs a ten-minute OS watchdog. The normal
service loop is the only scheduler; the watchdog merely starts it when it is
inactive after a crash, reboot, or service-manager failure. It never restarts a
healthy service or interrupts a live worker. systemd timers are persistent
across missed wakeups, and launchd uses the equivalent `StartInterval` fallback.

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
