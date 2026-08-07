# Architecture

## Supervisor v2

`handoff` is built around one Go Supervisor, not coordination between several
durable stores.

```text
human / scheduler / runtime Driver / finalizer
                     │
                     ▼
              typed Command
                     │
          decide against cloned State
                     │
                     ▼
      one append-only Supervisor journal
                     │
                     ▼
                 pure reducer
                     │
       ┌─────────────┼──────────────┐
       ▼             ▼              ▼
   scheduler     JSON / TUI      policy gates
```

The public Go seam is `github.com/carlchungus/durable-agent-handoff/supervisor`.
Its `Store.StartExecution` method accepts an optional native Session identity,
prompt, RuntimeSpec, root, authority and finalizer configuration, budget, and
idempotency key. Ordinary starts create an unbound Session; the arca-cloud
promotion seam supplies the exact resume identity and requires it.

## One journal, distinct identities

ADR 0001 remains binding: conversation, logical work, process execution, and
observation are different resources.

- **Workflow** owns desired Nodes, dependency declarations, root, budgets, and
  authority/finalizer configuration.
- **Session** owns the exact opaque native runtime identity once bound, lineage,
  and root. Ordinary new-session starts persist an unbound identity first; it
  never owns a PID, output pipe, or process state.
- **Activity** owns one immutable logical result generation, prompt, Session,
  and exact dependency Result bindings.
- **Attempt** owns one immutable OS launch plus its ordered typed milestones,
  process identity, command digest, outputs, and Lease identity.
- **Claim** is an autonomous worker's untrusted terminal assertion and names
  the exact Activity generation and Attempt that produced it.
- **Evaluation** is one fresh tool-less semantic decision over a Claim.
- **Result** is immutable and names the exact Activity generation and Attempt
  that produced it; autonomous Claims create Results only after Evaluation.
- **Message**, **Control**, and canonical-worktree **Lease** retain independent
  identities and fences.

These resources are maps in one rebuildable projection. They are not separate
mutation authorities. Every command appends one journal entry containing all
domain events required for the transition.

The journal lives in the hardened `secureledger` primitive at:

```text
$HANDOFF_HOME/supervisor-v2/canonical/
  events.jsonl
  state.json       # disposable projection snapshot
  .write.lock
```

`events.jsonl` owns global commit order. `state.json` is never trusted over the
journal. A missing, stale, or invalid snapshot is rebuilt by replay.

## Transaction and crash model

For every accepted command the Store:

1. locks the canonical journal record;
2. replays current State;
3. returns an earlier receipt if idempotency key and input digest match;
4. decides a complete domain-event set;
5. applies that set to a clone and validates all invariants;
6. appends and fsyncs one journal entry; and
7. atomically replaces the projection snapshot.

Failure before append commits nothing. Failure after append commits the whole
command, even if snapshot replacement or response delivery fails. Retrying the
same key returns the same resource and sequence. Reusing a key for divergent
input fails without mutation.

Rejected commands never partially mutate live State. Crash-injection tests cover
every transaction boundary, replay without a snapshot, and concurrent writers.

## Desired work and immutable dependency binding

A Node contains desired Work and dependency Node identities. It has no
`ready`, `running`, `attempt`, process, or Session field.

Eligibility is a query:

- each dependency must have an immutable completed Result;
- queuing an Activity captures the exact Result ID for each dependency; and
- later results or continuations cannot change an existing binding.

Completed Results never reopen. A human reply is one atomic command that queues
an inbox Message and creates the next Activity generation on the exact Session.
The predecessor Activity and Result remain immutable. A successor that already
bound the predecessor Result keeps that binding.

An autonomous `continue` decision uses the same transaction boundary: it records
the Evaluation and predecessor Result, queues the evaluator reason as an inbox
Message, and creates the next Activity generation on the exact bound Session.
An untyped worker request for a human remains a Claim and cannot stop a goal by
itself. A real escalation requires a typed workflow-wide blocker and concrete
question. Exhausting the durable turn budget becomes a visible `budget`
escalation instead of a hidden retry error.

## Attempts, budgets, and health

Preparing an Attempt records its immutable launch identity and exact output
identities before an OS process is allowed to run. Every launch is retained,
including adapter startup failures and provider-unavailable exits. Typed
fallback candidates remain part of Work and are selected from journaled
provider-unavailable evidence without widening sandbox authority.

Service startup runs one authority-owned recovery command before scheduling. It
validates each inherited nonterminal Attempt against its exact PID and process
start token. A dead or prepared orphan is durably recorded with typed terminal
milestones and its exact Lease is released; an exact live orphan fails closed
until a safe adoption protocol exists. Projections never perform this work and
polling never appends recovery state.

The task-attempt budget counts Attempts containing `turn_started`. Pre-turn
failures consume only the independent launch budget; there are no refunds.

Health is derived from typed milestones:

| Facts observed | Derived process health |
| --- | --- |
| Attempt prepared, process spawned, or Session bound | `starting` |
| `turn_started` without terminal milestone | `running` |
| `adapter_start_failed` or `exit` | `exited` |

Therefore Codex `thread.started` without `turn.started` can never be reported as
healthy/running. Output byte growth is a transport fact, not progress.
Meaningful progress exists only after a Driver emits `meaningful_progress`.

## Runtime Drivers

Codex, Claude, and Pi implement a deep Driver contract. Each Driver owns:

- non-shell argv construction;
- explicit worktree, sandbox, model, and effort selection;
- exact native Session resume;
- provider-specific stream decoding; and
- typed adapter-start and process-exit milestones.

The normalized milestone vocabulary is:

```text
process_spawned
session_bound
turn_started
effect_started
meaningful_progress
result
provider_unavailable
adapter_start_failed
exit
```

Decoders read only documented provider fields. They do not recursively search
arbitrary JSON for `session_id`, `thread_id`, a result, or a limit string.
Runtime Drivers never receive GitHub merge authority.

Terminal evaluation is not a runtime Driver. It is a stateless OpenRouter HTTP
adapter with no tools, workspace, native Session, or merge authority. The
service invokes it only for persisted autonomous Claims. A forced decision tool
is used by default; strict structured output is retained only for live provider
compatibility probes. Evaluator failure leaves the Claim pending and cannot
manufacture completion or human escalation.

Codex and Claude enforce `read-only` through native restrictions. Pi fails
closed for read-only work until an external OS sandbox is configured.

Service trust is explicit: `workspace` selects the provider's workspace
restrictions and `full` selects its native full-trust flag. This policy is
translated inside each Driver and is never implemented with a shell command or
by placing prompt/environment data in a unit file.

## Canonical-worktree writer Lease

Workspace-writing Attempt creation and Lease acquisition share one journal
transaction. The Lease key is the filesystem-canonical worktree path and its
fence contains:

```text
Activity ID + Activity generation + Attempt ID
```

At most one unreleased writer Lease may exist for a canonical path across all
workflows, schedulers, and symlink aliases. A stale generation, Attempt, or
Lease cannot record milestones or apply a Control. Pause controls enter a
draining phase; only an executor-applied terminal `exit` releases a Lease,
after which a separate idempotent settle command marks the pause complete. A
queued continuation waits instead of overlapping an already-running successor.

## Pure projections

The reducer is the only lifecycle interpretation. `status`, list, queue, TUI,
health, meaningful progress, publication eligibility,
and orchestration overhead all use the same State.

Queries are read-only. Time-sensitive views accept an explicit `asOf` value;
polling never appends an observation or invokes reconciliation. Orchestration
overhead is derived from persisted milestone timestamps:

- Attempt preparation to `process_spawned`;
- `process_spawned` to `turn_started`; and
- `turn_started` to first `meaningful_progress`.

Publication is an authority-owned durable effect. The projection derives
whether a finalizer is disabled, awaiting a Result, awaiting configured human
authorization, or eligible for its external-check gate. `PrepareFinalization`
records the exact PR/head/named-check decision before an argv-only GitHub merge;
the finalizer then rechecks the independently hosted GitHub checks on that
unchanged head. `SettleFinalization` records merged or blocked outcome so
retries after a crash remain idempotent and changed heads fail closed.

## Policy and trust boundaries

- The Supervisor root is private and outside worker-writable sandboxes.
- Paths are canonicalized before authority checks or Lease keys are created.
- An authority envelope may narrow but never widen a runtime sandbox.
- Enabled finalization requires a nonempty canonical exact set of external
  GitHub checks; human approval is independently optional.
- Independently hosted CI and GitHub checks are the verification authority.
  Handoff does not pretend that same-UID workers can authenticate their own
  Results.
- Privileged Git/GitHub execution uses argv, never a shell.
- Proposals and imported State are validated against a clone before journal
  mutation.

## Migration

V1 is imported once; it is not dual-run. `Store.ImportV1` reads and hashes only
legacy workflow-history ledgers, replays each workflow by its own sequence,
normalizes completed and reopened histories, and appends one
`legacy.imported` transaction. Legacy Session, Activity, and team ledgers are
not replayed. Legacy bytes remain unchanged, and exact native Session/Activity
recovery is explicitly unsupported and unresolved instead of scraped or
guessed. See ADR 0003.

## Extension boundaries

Coordination contracts such as dynamic JavaScript workflows, teams, goals, and
schedules remain distinct state machines, but their durable changes must be
Supervisor Commands in the same global journal. They may not introduce another
authoritative lifecycle store.

Dynamic JavaScript/QuickJS/WASM workflow execution is not implemented in v2;
there is no such package or dependency in this repository. If it is added in a
future release, its isolation and deterministic ordered-prefix replay must use
Supervisor commands so a script journal cannot race execution State.
