# Supervisor v2 protocol

## Go API

Import:

```go
import "github.com/carlchungus/durable-agent-handoff/supervisor"
```

Open one Store for the private state root and call `StartExecution`:

```go
store, err := supervisor.Open(stateRoot, supervisor.Options{})
execution, receipt, err := store.StartExecution(ctx, supervisor.StartExecutionInput{
    NativeSession: supervisor.NativeSessionIdentity{
        Runtime: "codex",
        ID:      exactThreadID,
    },
    Prompt:  prompt,
    Runtime: supervisor.RuntimeSpec{
        Name:    "codex",
        Sandbox: supervisor.SandboxWorkspaceWrite,
    },
    Root: root,
    Authority: supervisor.AuthoritySpec{
        RequestedBy:     humanIdentity,
        HumanAuthorized: true,
        Sandbox:         supervisor.SandboxWorkspaceWrite,
    },
    Finalizer: supervisor.FinalizerSpec{
        Enabled:         true,
        RequireHuman:    true,
        RequiredChecks:  []string{"test"},
    },
    Budget:         supervisor.DefaultBudget(),
    IdempotencyKey: requestID,
})
```

`StartExecution` accepts an optional native Session ID for ordinary new-session
launches. A missing ID creates an unbound Session; the first
typed `session_bound` milestone binds it immutably. Continuations and the
arca-cloud continuations require an exact bound identity. It never selects a global last
Session. The same idempotency key and canonical request returns the same
Execution and `receipt.Existing=true`; divergent input returns
`ErrIdempotencyConflict`.

All changes use typed Store methods and are written as one command:

| Method | Atomic effect |
| --- | --- |
| `StartExecution` | Execution + Workflow + root Node + exact Session + first Activity |
| `AddNode` | desired Work and dependencies |
| `QueueActivity` | derived immutable dependency Result bindings + Activity |
| `ContinueSession` | human Message + exact-Session continuation Activity generation |
| `PrepareAttempt` | immutable Attempt + canonical-worktree writer Lease + inbox dispatch |
| `RecordMilestone` | typed milestone plus any Result, inbox settlement, or terminal-exit Lease release |
| `DecideTurn` | decision plus Result and, for `continue`, the next turn on the same Session |
| `RequestControl` | accepted/rejected exact Activity-generation and Attempt fence |
| `PauseWorkflow` | records exact controls and enters requested/draining; executor-applied terminal exits release Leases |
| `SettlePause` | idempotently marks a draining pause completed after every fenced Attempt has exited |
| `ApplyControl` | records that an executor applied one exact Activity-generation/Attempt control |
| `ReconcileStartup` | validates inherited exact process identities; terminalizes dead/prepared orphans and releases their exact Leases, while leaving exact live Attempts for service adoption |
| `ImportV1` | one deterministic one-way legacy import transaction |

Every mutating call requires an idempotency key of 8–256 safe characters.

Role/model preferences are journal commands. A fallback is not a mutable
runtime field on the original Session: when the selected provider differs, the
executor records a child Session and child Activity with the parent identity,
then binds the child's exact native Session ID. The original Session remains
immutable and exact continuations target whichever bound Session owns the
predecessor Activity. Once the child exists, the parent remains visible only as
lineage and is excluded from Queue and fenced from Attempt preparation.

Publication is an authority-owned durable effect. `PrepareFinalization` records
the exact PR, head SHA, canonical external check set, approval, and idempotency
key. Only after that append may the finalizer invoke argv-only `gh`, and it
rechecks those independently hosted checks on the unchanged head;
`SettleFinalization` records merged or blocked outcome. A retry first reads the
prepared record, rejects divergent reuse, fences a changed head, and never
guesses whether a post-merge crash succeeded.

## Runtime milestone protocol

Drivers may emit only:

| Milestone | Required payload | Meaning |
| --- | --- | --- |
| `process_spawned` | exact PID and process start token | OS launch exists; still starting |
| `session_bound` | exact native runtime + Session ID | resumable identity persisted; still starting |
| `turn_started` | none | provider accepted useful turn; consumes task-attempt budget |
| `effect_started` | typed effect summary | tool/command/file effect began |
| `meaningful_progress` | semantic summary | explicit progress; output bytes do not qualify |
| `result` | status, summary | creates an immutable Result; external GitHub checks are the verification authority |
| `provider_unavailable` | classified reason | routing evidence; not inferred from arbitrary text |
| `adapter_start_failed` | reason | terminal pre-turn startup failure |
| `exit` | exit code and optional error | terminal OS process fact |

For Codex, `item.completed` agent messages are non-terminal even when they
match the worker result schema. The latest structured candidate becomes the
worker Result only after the documented `turn.completed` event.

`process_spawned` and `session_bound` do not imply `turn_started`. Duplicate
turns/results, milestones after a Result other than `exit`, non-monotonic event
times, and stale Lease/Attempt fences are rejected.

Worker status is `completed`, `continue`, `needs_human`, or `blocked`. For a
one-shot run, `needs_human` requires a blocker and concrete question. For a
goal, the service reads the worker result from its exact Attempt and asks a
fresh tool-less model for `accept`, `continue`, or `escalate`. `DecideTurn`
stores that decision on the normal Result. A reply to any Result creates a new
Activity; it never changes the Result or predecessor.
If a reply is already queued while another turn finishes, `DecideTurn` reuses
that continuation instead of creating an evaluator sibling. Explicit guidance
therefore cannot be starved by repeated automatic continuations.

Worker Result payloads do not carry publication authority. Independently hosted
CI and GitHub checks provide verification; handoff does not pretend that
same-UID workers can authenticate their own Results.

Goals are unbounded when `max_turns` is zero or omitted. A positive value is an
explicit safety cap. Unattended workers treat missing optional verification or
external CI as a confidence limit: when publication is authorized, they prefer
an honest draft PR and continue other independent work. `needs_human` is for an
indispensable workflow-wide authority or information gap, not a request for
optional evidence or for someone to watch CI.

The goal turn-decision request carries the workflow's durable publication
outlet state (`publication`: `disabled`, `awaiting_result`, `awaiting_human`,
or `eligible`) computed by `ProjectPublication`. The evaluator is instructed
that producing work which cannot reach a consumer is not progress: when the
outlet is disabled or blocked and the worker reports accumulated unpublished
candidates or deferred publication, it escalates instead of continuing to
produce more un-consumable work. This is the backpressure guard against an
open-ended count goal grinding ahead with no consumer feedback.

`wake_interval_seconds` is an optional durable cadence for automatic goal
continuations. `DecideTurn` records `not_before` on the continuation in the
same transaction. Before that instant the projection reports `scheduled` and
`next_wake_at`, excludes it from the runnable queue, and `PrepareAttempt`
independently rejects an early launch. Human replies do not inherit the delay.

## Projection protocol

`Store.Projection`, `Store.View`, and `Store.Events` are pure reads. They never
reconcile, adopt, deliver inbox messages, change health, or append an event.

The typed `ActivityView` is intentionally minimal: clients read lifecycle from
`Status`, identity/generation, immutable dependency bindings, and result
identity. Attempt and Control projections live at the execution level;
legacy aliases and compatibility envelopes are not part of v2.

`ReconcileStartup` runs once after a restart and before
`ServeV2` schedules work, under the journal transaction. Prepared-but-never-
spawned Attempts are inherited orphans. Dead Attempts receive existing typed
failure/exit facts plus exact Lease release so their immutable Activity can
retry. An exact live PID/start-token match remains nonterminal and is returned
to the service for observation; the service adopts that exact process instead
of launching a duplicate.

`Store.View(executionID, asOf)` returns one `ExecutionView` containing the Node,
Activity, Attempt, queue, publication, and overhead projections.
Human, JSON, JSONL, and TUI consumers must render this same structure.

Process health is:

- `starting` before `turn_started`, even after a PID or Session is observed;
- `running` after `turn_started` until a terminal milestone; and
- `exited` after `adapter_start_failed` or `exit`.

Attempt JSON exposes `health`, `terminal_reason`, and any immutable
`result_status`; it has no duplicate compatibility `state` field. An exit
milestone with no error cannot overwrite an earlier provider or adapter
failure reason.

Task-attempt ordinals count only Attempts containing `turn_started`. All OS
launch Attempts remain listed.

JSONL journal followers retain the global `sequence` cursor and resume with
events strictly greater than that cursor. Object key order has no meaning.

## Writer and control fences

Workspace-writing launch requires an active Lease whose canonical worktree,
Activity ID, Activity generation, and Attempt ID exactly match the command.
Only one unreleased Lease may name a canonical worktree. Symlink aliases resolve
to the same key.

A Control names exact Activity generation and Attempt ID. A stale request is
journaled as rejected evidence and cannot signal a process. PID-only control is
never valid.

## Crash and retry contract

One command writes one journal record.

- Error at `after_validation`: no state committed.
- Error at `after_append`: the full command committed; replay recovers it.
- Error at `after_snapshot`: the full command committed; retry returns it.

Callers must retry ambiguous responses with the identical idempotency key and
input. They must not synthesize a new Activity, Attempt, or Session identity.

## Starting a resumed session and running the service

`handoff session start` is the simple background-session surface. It creates a
Session-mode execution without a goal, evaluator, or mandatory completion
contract. `--check-interval` defaults to 20 minutes; the quiet check is
read/reconcile-only and never sends a prompt or signal to a live Attempt.
`handoff status SESSION` accepts the exact handoff Session ID, and
`handoff tail SESSION --lines N [--follow]` reads the latest Attempt's private
stdout (or `--stderr`) without changing lifecycle state. Named Codex, Claude,
and Pi adapters support their exact native identities; unknown harness names
use stdin/argv without inferred native resume semantics.

Ordinary `start`/`create` prompts and `reply` bodies are stdin-only: callers
must pass `--file -`. Arbitrary prompt paths, `--prompt`, `--prompt-file`, and
reply `--message` argv content are rejected.

`handoff execution start --file - --json` accepts exactly one flat JSON object
with `idempotency_key`, `goal`, `prompt`, `remote_root`, `runtime`, `resume_id`,
`sandbox`, and `role`, plus optional `model`, `effort`, and flat
`finalizer_enabled`, `finalizer_required_checks`, and
`finalizer_require_human` fields. Unknown fields and a second JSON value are
rejected. It also accepts `one_shot`, `evaluator_model`, and `max_turns`.
Machine starts run as goals unless `one_shot` is true. These settings are
persisted in `StartExecutionInput`; an enabled finalizer requires a nonempty canonical exact
set of external GitHub checks, while human approval is independently optional.
Its only JSON response shape is:

```json
{"workflow_id":"...","node_id":"..."}
```

`handoff execution pause --workflow ID --json` uses a deterministic default
idempotency key when one is not supplied. The command commits pause controls,
then waits on pure projection reads until the executor records exact terminal
exit evidence and the later idempotent settle command marks the pause complete.
An Attempt holding an old fence cannot append a later non-terminal milestone.

`serve` accepts `--environment-json FILE` only when `FILE` is a private regular
file containing one JSON object, and `--trust-mode workspace|full`. POSIX hosts
require mode `0600`; Windows requires an owner/System/Administrators-only DACL.
The file is read into transient driver environment values. Prompts and secret
values are never serialized into a service unit. Drivers apply trust flags
themselves and execute provider argv directly, without shell wrappers.
The default embedded service waits for all active executor goroutines to record
their terminal milestones and release their exact Leases before returning. The
CLI service opts into detached POSIX session workers so a supervisor restart can
adopt them.
Failed runtime launches are retried only after the service retry delay; the
same queued Activity remains durable during that delay.

The installed OS service has a ten-minute liveness watchdog. It issues a start,
not a restart, so a healthy supervisor and its live Attempts are untouched. If
the service is inactive after a crash, reboot, or missed wake, systemd or
launchd starts the same journal-backed scheduler and normal reconciliation
resumes exact Sessions and Attempts. Service shutdown detaches active Attempts;
a later POSIX service instance adopts exact live processes instead of
interrupting them. The runner writes the child exit code before POSIX
containment teardown; missing or unknown exit evidence becomes blocked, never
successful completion. Windows Job Object containment ends the live tree when
the service exits, so restart adoption is not promised there.

## One-way migration

`ImportV1` reads only legacy workflow-history event ledgers, calculates the
source digest before mutation, and leaves source bytes unchanged. Legacy
Session, Activity, and team ledgers are not replayed; exact native
Session/Activity recovery is unsupported and remains explicitly unresolved.
V1 snapshots, output scraping, guessed Activity IDs, and timestamp-based
cross-ledger ordering are not import authorities. A second import of the same
source under a new key is rejected.
