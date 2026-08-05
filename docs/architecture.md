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
    events.jsonl
    stderr.log
    last-message.json
    result.schema.json
```

## Scheduling

`handoff serve` scans active workflows and runs ready nodes up to a configurable cross-workflow concurrency bound. Per-workflow writes remain serialized. It can run under launchd or systemd-user and resumes from durable state after restart.

The scheduler does not infer health from a PID. The observable contract is state plus events, evidence, attempt count, and persisted session ID.

## Routing and usage limits

Role-specific preference ladders are stored outside individual workflows. Before a node starts, the supervisor selects the first candidate without an active cooldown. A recognized quota/usage-limit or rate-limit failure records provider health, returns the node to `ready`, persists the next runtime choice, and appends evidence. Ordinary runtime, auth, model-name, code, and test failures stay on the normal failure path.

Cooldown state is durable across scheduler restarts. When every candidate is cooling down, resolution returns the earliest wake time and no worker is launched. The model never decides that its provider is exhausted and never edits provider health directly.

## Trust boundaries

1. Transcript discovery is read-only and text-only. It redacts common credentials and classifies obvious high-risk work.
2. Runtime workers can change files only within their configured worktree and can propose graph mutations.
3. The policy kernel validates an entire proposal against a cloned graph before applying any mutation.
4. A finalizer is privileged and must be authorized by a human/supervisor node. Agent proposals cannot create one.
5. Finalization requires an independent passing attestation, local diff budgets, a non-protected branch, exact named CI checks, and an unchanged PR head.

## Runtime adapters

Runtime-specific command construction lives behind one interface. Each adapter must provide noninteractive execution, an explicit working directory, a parseable event stream, a final result object, and exact-session resume when supported.

Claude runs in safe mode with an empty strict MCP configuration. This intentionally does not inherit ambient plugins, hooks, or MCP servers. Pi and OhMyPi need stronger external isolation because they do not provide an OS sandbox.

## Extension path

The node `kind` field is data rather than a Go enum. A future handler registry can add issue trackers, deployment observers, browser evaluators, or remote harnesses without changing graph semantics. Unknown kinds stay visible and non-runnable instead of being guessed.

Likewise, discovery is source-specific. Claude Code is the first source; Codex and Pi transcript importers can implement the same sanitized record contract.
