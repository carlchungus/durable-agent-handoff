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
| `RecordAttestation` | exact immutable Result + authorized independent verifier evidence |
| `RequestControl` | accepted/rejected exact Activity-generation and Attempt fence |
| `PauseWorkflow` | records exact controls and enters requested/draining; executor-applied terminal exits release Leases |
| `SettlePause` | idempotently marks a draining pause completed after every fenced Attempt has exited |
| `ApplyControl` | records that an executor applied one exact Activity-generation/Attempt control |
| `ReconcileStartup` | validates inherited exact process identities; terminalizes dead/prepared orphans and releases their exact Leases, or fails closed on an exact live orphan |
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
the exact PR, head SHA, named gates, approval, and idempotency key. Only after
that append may the finalizer invoke argv-only `gh`; `SettleFinalization`
records merged or blocked outcome. A retry first reads the prepared record,
rejects divergent reuse, fences a changed head, and never guesses whether a
post-merge crash succeeded.

## Runtime milestone protocol

Drivers may emit only:

| Milestone | Required payload | Meaning |
| --- | --- | --- |
| `process_spawned` | exact PID and process start token | OS launch exists; still starting |
| `session_bound` | exact native runtime + Session ID | resumable identity persisted; still starting |
| `turn_started` | none | provider accepted useful turn; consumes task-attempt budget |
| `effect_started` | typed effect summary | tool/command/file effect began |
| `meaningful_progress` | semantic summary | explicit progress; output bytes do not qualify |
| `result` | status, summary | creates immutable Result; verification is a separate authority command |
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

`RecordAttestation` accepts only an exact existing Result and a verifier named
by the Workflow's immutable finalizer configuration. The verifier must differ
from the Workflow requester, and each verifier may attest a given Result only
once. Worker Result payloads do not carry attestation authority.

## Projection protocol

`Store.Projection`, `Store.View`, and `Store.Events` are pure reads. They never
reconcile, adopt, deliver inbox messages, change health, or append an event.

The typed `ActivityView` is intentionally minimal: clients read lifecycle from
`Status`, identity/generation, immutable dependency bindings, result identity,
and verification. Attempt and Control projections live at the execution level;
legacy aliases and compatibility envelopes are not part of v2.

`ReconcileStartup` is the explicit restart boundary. It runs once before
`ServeV2` schedules work, under the journal transaction. Prepared-but-never-
spawned Attempts are inherited orphans. Dead Attempts receive existing typed
failure/exit facts plus exact Lease release so their immutable Activity can
retry. An exact live PID/start-token match returns `ErrLiveOrphan` and the
service refuses to schedule until an explicit adoption protocol is available.

`Store.View(executionID, asOf)` returns one `ExecutionView` containing the Node,
Activity, Attempt, queue, verification, publication, and overhead projections.
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

One command equals one aggregate journal append.

- Error at `after_validation`: no state committed.
- Error at `after_append`: the full command committed; replay recovers it.
- Error at `after_snapshot`: the full command committed; retry returns it.

Callers must retry ambiguous responses with the identical idempotency key and
input. They must not synthesize a new Activity, Attempt, or Session identity.

## CLI promotion and service boundary

Ordinary `start`/`create` prompts and `reply` bodies are stdin-only: callers
must pass `--file -`. Arbitrary prompt paths, `--prompt`, `--prompt-file`, and
reply `--message` argv content are rejected.

`handoff execution start --file - --json` accepts exactly one flat JSON object
with `idempotency_key`, `goal`, `prompt`, `remote_root`, `runtime`, `resume_id`,
`sandbox`, and `role`, plus optional `model`, `effort`, and flat
`finalizer_enabled`, `finalizer_required_checks`, `finalizer_require_human`,
`finalizer_require_verifier`, and `finalizer_verifiers` fields. Unknown fields
and a second JSON value are rejected. Finalizer configuration is persisted in
the immutable `StartExecutionInput`; an enabled finalizer requires nonempty
named checks, human approval, and independent verifier identities. Its only
JSON response shape is:

```json
{"workflow_id":"...","node_id":"..."}
```

`handoff execution pause --workflow ID --json` uses a deterministic default
idempotency key when one is not supplied. The command commits pause controls,
then waits on pure projection reads until the executor records exact terminal
exit evidence and the later idempotent settle command marks the pause complete.
An Attempt holding an old fence cannot append a later non-terminal milestone.

`handoff attest --result RESULT_ID --verifier ID --verdict pass|repair|blocked
--file - --idempotency-key KEY [--evidence ID ...]` reads the evidence summary
from stdin and records one strict verifier command. `--summary` and other
argv-bearing summary forms are rejected. The command is idempotent with the
same canonical input; a stale Result, unauthorized verifier, or duplicate
verifier/Result pair is rejected without journal mutation.

`serve` accepts `--environment-json FILE` only when `FILE` is a regular
mode-0600 file containing one JSON object, and `--trust-mode workspace|full`.
The file is read into transient driver environment values. Prompts and secret
values are never serialized into a service unit. Drivers apply trust flags
themselves and execute provider argv directly, without shell wrappers.
Service cancellation waits for all active executor goroutines to record their
terminal milestones and release their exact Leases before returning.

## One-way migration

`ImportV1` reads only legacy workflow-history event ledgers, calculates the
source digest before mutation, and leaves source bytes unchanged. Legacy
Session, Activity, and team ledgers are not replayed; exact native
Session/Activity recovery is unsupported and remains explicitly unresolved.
V1 snapshots, output scraping, guessed Activity IDs, and timestamp-based
cross-ledger ordering are not import authorities. A second import of the same
source under a new key is rejected.
