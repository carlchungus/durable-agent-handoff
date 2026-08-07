# Supervisor v2 protocol

## Public Go interface

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
        RequireVerifier: true,
        RequiredChecks:  []string{"test"},
    },
    Budget:         supervisor.DefaultBudget(),
    IdempotencyKey: requestID,
})
```

`StartExecution` accepts an optional native Session ID for ordinary new-session
launches. A missing ID creates a Supervisor-owned unbound Session; the first
typed `session_bound` milestone binds it immutably. Continuations and the
promotion seam require an exact bound identity. It never selects a global last
Session. The same idempotency key and canonical request returns the same
Execution and `receipt.Existing=true`; divergent input returns
`ErrIdempotencyConflict`.

All mutations use typed Store methods backed by the one transactional Command
interface:

| Method | Atomic effect |
| --- | --- |
| `StartExecution` | Execution + Workflow + root Node + exact Session + first Activity |
| `AddNode` | desired Work and dependencies |
| `QueueActivity` | derived immutable dependency Result bindings + Activity |
| `ContinueSession` | human Message + exact-Session continuation Activity generation |
| `PrepareAttempt` | immutable Attempt + canonical-worktree writer Lease + inbox dispatch |
| `RecordMilestone` | typed milestone plus any Result, inbox settlement, or terminal-exit Lease release |
| `RequestControl` | accepted/rejected exact Activity-generation and Attempt fence |
| `PauseWorkflow` | records exact controls and enters requested/draining; executor-applied terminal exits release Leases |
| `SettlePause` | idempotently marks a draining pause completed after every fenced Attempt has exited |
| `ApplyControl` | records that an executor applied one exact Activity-generation/Attempt control |
| `ImportV1` | one deterministic one-way legacy import transaction |

Every mutating call requires an idempotency key of 8–256 safe characters.

## Runtime milestone protocol

Drivers may emit only:

| Milestone | Required payload | Meaning |
| --- | --- | --- |
| `process_spawned` | exact PID and process start token | OS launch exists; still starting |
| `session_bound` | exact native runtime + Session ID | resumable identity persisted; still starting |
| `turn_started` | none | provider accepted useful turn; consumes task-attempt budget |
| `effect_started` | typed effect summary | tool/command/file effect began |
| `meaningful_progress` | semantic summary | explicit progress; output bytes do not qualify |
| `result` | status, summary, attestations | creates immutable Result |
| `provider_unavailable` | classified reason | routing evidence; not inferred from arbitrary text |
| `adapter_start_failed` | reason | terminal pre-turn startup failure |
| `exit` | exit code and optional error | terminal OS process fact |

`process_spawned` and `session_bound` do not imply `turn_started`. Duplicate
turns/results, milestones after a Result other than `exit`, non-monotonic event
times, and stale Lease/Attempt fences are rejected.

Result status is `completed`, `needs_human`, or `blocked`. A reply to any Result
creates a continuation Activity; it never changes the Result or predecessor.

Attestation source verdicts use the exact allowlist:

- `pass`, `repair`, `blocked` remain canonical;
- `fail_blocking` normalizes to `blocked` while retaining `raw_verdict`;
- `pass_with_limit` and `pass_with_runtime_limit` normalize to `repair`; and
- unknown verdicts fail closed.

## Projection protocol

`Store.Projection`, `Store.View`, and `Store.Events` are pure reads. They never
reconcile, adopt, deliver inbox messages, change health, or append an event.

`Store.View(executionID, asOf)` returns one `ExecutionView` containing the Node,
Activity, Attempt, queue, verification, publication, and overhead projections.
Human, JSON, JSONL, and TUI consumers must render this same structure.

Process health is:

- `starting` before `turn_started`, even after a PID or Session is observed;
- `running` after `turn_started` until a terminal milestone; and
- `exited` after `adapter_start_failed` or `exit`.

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

One command equals one aggregate journal append.

- Error at `after_validation`: no state committed.
- Error at `after_append`: the full command committed; replay recovers it.
- Error at `after_snapshot`: the full command committed; retry returns it.

Callers must retry ambiguous responses with the identical idempotency key and
input. They must not synthesize a new Activity, Attempt, or Session identity.

## CLI promotion and service boundary

`handoff execution start --file - --json` accepts exactly one flat JSON object
with `idempotency_key`, `goal`, `prompt`, `remote_root`, `runtime`, `resume_id`,
`sandbox`, and `role`, plus optional `model` and `effort`. Unknown fields and a
second JSON value are rejected. Its only JSON response shape is:

```json
{"workflow_id":"...","node_id":"..."}
```

`handoff execution pause --workflow ID --json` uses a deterministic default
idempotency key when one is not supplied. The command commits pause controls,
then waits on pure projection reads until the executor records exact terminal
exit evidence and the later idempotent settle command marks the pause complete.
An Attempt holding an old fence cannot append a later non-terminal milestone.

`serve` accepts `--environment-json FILE` only when `FILE` is a regular
mode-0600 file containing one JSON object, and `--trust-mode workspace|full`.
The file is read into transient driver environment values. Prompts and secret
values are never serialized into a service unit. Drivers apply trust flags
themselves and execute provider argv directly, without shell wrappers.

## One-way migration

`ImportV1` reads legacy event ledgers only, calculates the source digest before
mutation, and leaves source bytes unchanged. V1 snapshots, output scraping,
guessed Activity IDs, and timestamp-based cross-ledger ordering are not import
authorities. A second import of the same source under a new key is rejected.
