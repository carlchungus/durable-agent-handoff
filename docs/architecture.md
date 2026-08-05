# Architecture

## Core idea

The harness is an event-driven control plane, not a fixed pipeline. A workflow is a mutable directed acyclic graph. Nodes describe capabilities (`agent`, `command`, `human`, `finalize`, or an extension kind), not lifecycle phases.

```text
transcript / prompt / event
           │
           ▼
   durable event ledger ───────► live TUI / JSONL followers
           │
           ▼
       pure reducer
           │
           ▼
  materialized workflow graph
           │
     ready-node scheduler
           │
     ┌─────┼──────────────┐
     ▼     ▼              ▼
   Codex  Claude       Pi / custom
     │     │              │
     └─────┴── proposal ──┘
                 │
                 ▼
           policy kernel
            │          │
         accept      reject + evidence
```

Workers decide how to pursue the goal. They may add nodes, dependencies, evidence, or attestations; supersede obsolete work; retry; or request human input. They cannot alter the root, budgets, pause state, or finalization authority.

Verifier language is normalized only at the runtime protocol boundary. The ledger stores a three-state canonical verdict for policy and, when normalization was required, the exact source verdict for auditability. Qualified passes remain non-passing, blocking failures remain blocked, and unrecognized semantics fail closed.

## Why there are no mandatory phases

Discovery, planning, implementation, and evaluation are useful roles, but they are not globally correct states. A tiny fix may inspect, edit, and verify in one agent turn. An uncertain incident may create several read-only discovery nodes before any plan exists. An evaluator may find a different root cause and supersede the current implementation.

The graph records what exists and why. Policy records what is allowed. Attestations record why a result should be trusted. This separates workflow intelligence from authority.

## Event and recovery model

Every accepted proposal appends one event and then atomically replaces `state.json`. The ledger uses monotonically increasing sequence numbers and `fsync`. A cross-process lease serializes writers. If the snapshot is absent or invalid, the reducer rebuilds it from `events.jsonl`.

Runtime output is stored per workflow, node, and attempt:

```text
$HANDOFF_HOME/workflows/WF_ID/
  events.jsonl
  state.json
  runs/NODE_ID/ATTEMPT/
    last-message.json
    result.schema.json
```

Background sessions have their own event-sourced identity and inbox kernel:

```text
$HANDOFF_HOME/sessions/AGENT_ID/
  events.jsonl
  state.json
```

This is deliberately separate from workflow nodes: a node describes graph work, while an agent session describes a durable conversation and its queued replies. Logical state and process liveness are independent. Message dispatch uses a monotonic fence independent of refundable node retry counts. Every accepted or rejected agent exit records a typed attempt outcome atomically with its workflow transition; reconciliation delivers or requeues only the inbox batch named by that outcome. Exact opaque runtime session IDs are retained across process exit and reply-triggered restart.

Independently controllable work uses a separate Activity ledger:

```text
$HANDOFF_HOME/activities/ACTIVITY_ID/
  events.jsonl
  state.json
  ATTEMPT_ID_stdout.log
  ATTEMPT_ID_stderr.log
```

A Session owns conversational identity; an Activity owns work lifecycle and
durable output; an Attempt records one immutable process execution. PID,
command, output, and stop authority therefore never become Session fields.
Checklist tasks remain separate again: they are planning state, not background
processes.

An attachment is an ephemeral reader over an Activity revision and byte cursor.
Disconnecting it has no lifecycle effect. Stop, signal, adopt, and restart are
durable control intents that include the expected Activity generation and exact
Attempt process identity (attempt ID, PID plus platform start token, supervisor
ID, and supervisor generation). A live process is adopted once per supervisor
incarnation; routine reconciliation by the same owner does not churn fencing
tokens. Stale controllers and PID reuse fail closed.

Every new Attempt starts through a tiny gated runner. The runner establishes a
dedicated process tree, but cannot execute the target until the supervisor has
fsynced the exact PID, birth token, owner, and generation to the Activity
ledger. Supervisor death before that release closes the inherited gate and the
target never starts. After release, the runner waits for the target and writes
its exact completion back through the same fenced Activity transaction before
exiting. Recovery can therefore adopt a live runner or replay its terminal
record without a second process-authority file. `attempt.json` is read only as
legacy compatibility for pre-v0.4 workflows.

The ledger reducer produces the sole Activity projection used by human TUI,
JSON/JSONL, RPC, and policy. A combined snapshot-and-subscribe operation orders
subscriber registration with the initial read so reconnect cannot lose events
between snapshot and follow. The prior-art evidence and license boundary are in
[`prior-art-codex-pi-omp.md`](prior-art-codex-pi-omp.md); the domain decision is
recorded in
[`ADR 0001`](adr/0001-separate-sessions-and-activities.md).

The ledger root is a supervisor-private trust boundary and must remain outside
worker-writable sandboxes. Descriptor-relative opens, ownership/mode checks,
single-link regular files, pinned identities, locks, fsync, and replay repair
fail closed on accidental or uncoordinated path replacement. They do not claim
to isolate an actively malicious unsandboxed process running as the supervisor's
same OS user; such a process can also inspect or signal the supervisor directly.
Windows fsyncs ledger files but exposes no POSIX directory-fsync contract through
Go's directory handles; process-crash recovery is covered, while metadata
durability across sudden power loss remains filesystem-defined on Windows.

## Scheduling

`handoff serve` scans active workflows and runs ready nodes up to a configurable cross-workflow concurrency bound. Per-workflow writes remain serialized. It can run under launchd or systemd-user and resumes from durable state after restart.

The scheduler does not infer health from a PID. The observable contract is state plus events, evidence, attempt count, and persisted session ID.

## Claude-compatible dynamic workflows

Dynamic workflows are a separate coordination contract layered over the same runtime adapters; they are not translated into a static phase DAG. A sandboxed JavaScript program owns loops, branches, `parallel`, `pipeline`, phases, and intermediate values. Go owns agent execution, caps, permissions, storage, leases, and replay.

The durable journal records every `agent()` start in start order, its prompt/options fingerprint, and its result. On restart the script executes again from the beginning. Cached results are returned only through the completed ordered prefix. At the first unfinished or changed call, that call and the entire suffix execute live, even when later calls completed before interruption. The JavaScript heap therefore does not need to be serialized, and replay cannot manufacture an execution order the original run never observed.

The compatibility profile targets Claude's documented 16 concurrent and 1,000 total agents. Policy may choose lower caps. The workflow program itself receives no filesystem, shell, network, process, import, or package-loader capability; only the agents it launches can request tools through their inherited capability envelope.

The first embedded VM milestone evaluates async JavaScript function bodies in
QuickJS-ng/WASM under wazero. The guest sees only frozen workflow, node,
evidence, and structured-argument data plus a single `propose()` capability.
Go validates the complete proposal against a cloned graph before returning it.
WASI receives no filesystem preopens, environment, sockets, process surface, or
host entropy; source/input/output, memory, stack, deterministic execution fuel,
time, and mutation count are bounded. See
[`javascript-workflow-vm.md`](javascript-workflow-vm.md) for the exact API and
engine tradeoffs.

## Routing and usage limits

Role-specific preference ladders are stored outside individual workflows. Before a node starts, the supervisor selects the first candidate without an active cooldown. A recognized quota/usage-limit or rate-limit failure records provider health, returns the node to `ready`, persists the next runtime choice, and appends evidence. Ordinary runtime, auth, model-name, code, and test failures stay on the normal failure path.

Cooldown state is durable across scheduler restarts. When every candidate is cooling down, resolution returns the earliest wake time and no worker is launched. The model never decides that its provider is exhausted and never edits provider health directly.

## Trust boundaries

1. Transcript discovery is read-only and text-only. It redacts common credentials and classifies obvious high-risk work.
2. Runtime workers can change files only within their configured worktree and can propose graph mutations.
3. The policy kernel validates an entire proposal against a cloned graph before applying any mutation.
   Read-only workers cannot create a write-capable child; runtime routing preserves the parent's sandbox envelope.
4. A finalizer is privileged and must be authorized by a human/supervisor node. Agent proposals cannot create one.
5. Finalization requires an independent passing attestation, local diff budgets, a non-protected branch, exact named CI checks, and an unchanged PR head.

## Runtime adapters

Runtime-specific command construction lives behind one interface. Each adapter must provide noninteractive execution, an explicit working directory, a parseable event stream, a final result object, and exact-session resume when supported.

Claude runs in safe mode with an empty strict MCP configuration. This intentionally does not inherit ambient plugins, hooks, or MCP servers. Pi and OhMyPi need stronger external isolation because they do not provide an OS sandbox.

Codex and Claude support a portable `read-only` profile. Codex receives its native read-only sandbox; Claude receives a read-only tool allowlist with Bash/Edit/Write removed. Pi, OhMyPi, and arbitrary executables fail closed for `read-only` until wrapped by an external OS sandbox.

## Extension path

The node `kind` field is data rather than a Go enum. A future handler registry can add issue trackers, deployment observers, browser evaluators, or remote harnesses without changing graph semantics. Unknown kinds stay visible and non-runnable instead of being guessed.

Likewise, discovery is source-specific. Claude Code is the first source; Codex and Pi transcript importers can implement the same sanitized record contract.

## Team coordination

Teams use a separate append-only ledger rather than encoding peers as workflow nodes. Logical members outlive runtime sessions. Member and process state are orthogonal, task claims use expiring fencing generations, and plan approval and cooperative shutdown are explicit messages and state transitions. This preserves the observable peer-coordination contract while allowing a Claude member to resume as Codex or Pi after a provider limit or crash.
