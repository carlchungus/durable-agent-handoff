# ADR 0002: One Supervisor journal and transactional command boundary

- Status: accepted
- Date: 2026-08-07

## Context

The v1 control plane persisted Workflow, Session, Activity, Attempt, inbox,
provider health, and process lifecycle through independently locked ledgers and
files. The engine then coordinated ordered writes across those stores and used
reconciliation to repair crash windows. Three failures followed from the model:

- a spawned process or Codex `thread.started` event was reported as running even
  when no provider turn began;
- adapter startup deaths consumed the Workflow node's task retry count; and
- inbox delivery reopened a completed predecessor after a successor had already
  become ready, permitting two writers in one worktree.

The project has one current user and accepts a breaking v2. Compatibility is
therefore less valuable than removing the competing mutation authorities.

## Decision

`internal/supervisor` is the sole durable state mutation boundary. The public
promotion seam is the top-level `supervisor` package.

The Supervisor stores one append-only journal record with a global sequence.
Every accepted command:

1. replays the journal to one projection;
2. decides all domain events for the command;
3. applies them to a cloned projection and validates the complete result;
4. appends one aggregate journal entry and fsyncs it; and
5. replaces a disposable snapshot.

A snapshot failure cannot hide an appended command because reads replay the
journal. A retry after an ambiguous response uses the command idempotency key.
The same key and canonical input digest returns the original resource; the same
key with divergent input fails without mutation.

Workflow, Session, Activity, Attempt, Result, inbox Message, Control, and Lease
remain distinct identities as required by ADR 0001. They are indexes reduced
from one commit order, not independently writable stores.

### Domain rules

- A Node contains desired Work and dependency Node identities only. It has no
  operational state, attempt counter, process data, or native Session ID.
- An Activity is one immutable logical result generation. Its dependency
  bindings capture exact immutable Result IDs when the Activity is queued.
- A completed Result is never changed or reopened. A human reply atomically
  queues a Message and creates a new Activity generation on the exact Session.
- Preparing a workspace-writing Attempt atomically acquires the one active
  Lease for the canonical worktree. The Lease fence names exact Activity
  generation and Attempt ID. Stale events fail closed.
- Every OS launch is an immutable Attempt. Task-attempt usage is the number of
  Attempts containing `turn_started`; pre-turn failures remain visible but use
  only the independent launch budget.
- A process is `starting` after `process_spawned` or `session_bound`. It becomes
  running only after `turn_started`.

### Runtime boundary

`internal/driver` provides deep Codex, Claude, and Pi Drivers. Each owns argv
construction, exact-session resume, and provider-specific decoding. The only
milestones are `process_spawned`, `session_bound`, `turn_started`,
`effect_started`, `meaningful_progress`, `result`, `provider_unavailable`,
`adapter_start_failed`, and `exit`. Decoders use documented provider fields;
they do not recursively search arbitrary JSON for a session or result.

Runtime Drivers never receive GitHub merge authority. Publication remains an
authority-owned effect gated by the Supervisor projection.

### Query boundary

Status, list, queue, health, meaningful progress, external-check publication,
and orchestration overhead are pure functions of the same projection. Queries
accept an explicit `asOf` time where needed and append no observations merely
because a client polled.

## Consequences

- Cross-store partial-write reconciliation, retry refunds, `reopen_agent`, and
  operational `set_state` mutations are not v2 concepts.
- Output bytes are transport cursors, never health or meaningful progress.
- Exact writer exclusion works across workflows and symlink aliases because the
  Lease key is a canonical filesystem path.
- Commands are somewhat larger aggregate events, but crash behavior is local:
  before append nothing committed; after append the whole command committed.
- The v1 engine and stores are import sources only while migration is staged;
  new execution must not dual-write them.

## Rejected alternatives

- Extending reconciliation between the Workflow, Session, and Activity stores.
- Keeping Node readiness/running flags authoritative alongside a Supervisor.
- Treating PID existence, `thread.started`, or output growth as worker health.
- Refunding task attempts after guessing a provider failure from raw output.
- Reopening a completed Node to model a new human turn.
