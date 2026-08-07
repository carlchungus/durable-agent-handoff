# Claude observable conformance suite

This directory turns the compatibility matrix into a differential test contract. A runner executes the same fixture against a pinned Claude Code build and `handoff`, normalizes runtime-specific identifiers, and compares the observations. The suite tests observable behavior only; it does not depend on Claude Code's private source or undocumented file formats.

The reference snapshot is **2026-08-05**. Claude research-preview behavior changes quickly, so every oracle capture records the Claude Code version, platform, provider, terminal mode, and source anchors from [`manifest.json`](manifest.json).

## Candidate command contract

The conformance matrix describes the broader Claude workflow vocabulary; it is
not a promise that every v1 `handoff` command still exists. Supervisor v2's
implemented candidate surface is `start|create`, `execution start|pause|status|
list|import-v1`, `run`, `serve`, `status`, `list`, `reply`, `activity list|read`,
`tui`, `events`, `preference set|list|health`, `service install`, and
`github merge`. All of these use the Supervisor journal and pure projections.
Prompts are stdin/file-only, and activity JSON retains the cloud active-state
shape without prompt bodies.

The v1 `doctor`, `discover`, Claude import, team, preference-file reset,
activity follow/stop, transcript attachment, and byte-cursor output surfaces
are explicitly deferred. They must acquire journal-backed commands and
positive/denial/restart tests before being added to the shipping contract.

## Profiles and match policy

Every case names one or both profiles:

- `claude-current`: the behavior visible in the current documented Claude Code release. A mismatch is a compatibility failure unless the case says otherwise.
- `durable-extension`: the portable behavior `handoff` adds where Claude documents a lifecycle limitation. The Claude observation is retained in `claude_expected`; the stronger `handoff` contract is in `then`.

`match_policy` has four values:

- `exact`: event order, state, and result must match after identifier and timestamp normalization.
- `normalized`: semantic state must match, but provider names, timing, copy, or presentation may differ.
- `extension`: Claude's baseline path must work and `handoff` exposes additional machine-readable state or controls.
- `deliberate-divergence`: the difference is intentional and both oracle expectations are specified. A divergence without a case is a bug.

## Runner protocol

Each case is a sequence of typed operations. The runner owns adapters for the operation vocabulary below and records a JSON observation bundle.

| Prefix | Meaning |
| --- | --- |
| `fixture.*` | Create disposable repos, processes, clocks, provider stubs, terminals, remote clients, or stores |
| `session.*` | Start, resume, fork, rename, clear, export, attach, detach, reply, stop, or delete a session |
| `agent.*` | Spawn, message, resume, stop, inspect, or fork a subagent/background session |
| `team.*` | Create peers/tasks/dependencies, claim, message, submit/review plans, idle, and shut down |
| `workflow.*` | Plan, approve, run, pause, resume, restart an agent, stop, save, and invoke a script |
| `goal.*`, `hook.*`, `task.*` | Exercise autonomous turn loops, lifecycle handlers, and commands/background jobs |
| `worktree.*`, `checkpoint.*`, `context.*`, `memory.*` | Exercise isolation, rewind, compaction, instruction reload, and retained knowledge |
| `schedule.*`, `channel.*` | Exercise session loops, durable schedules/routines, inbound events, replies, and approvals |
| `permission.*`, `model.*`, `usage.*` | Exercise policy precedence, inheritance, model routing, and usage visibility |
| `pr.*`, `remote.*`, `store.*`, `host.*` | Exercise PR linkage/gates, remote steering, storage, and process placement |
| `ui.*`, `config.*`, `package.*` | Exercise terminal/machine views, config resolution, and reusable component packaging |
| `fault.*` | Kill or stall a process, sleep the machine clock, drop the network, fail a provider/store, or corrupt a snapshot |
| `assert.*` | Compare values, state, order, counts, files, events, refusals, exits, and eventual conditions |

An operation can bind its result with `as`. Later operations refer to it with `$name` or `$name.field`. All paths use dotted field lookup; array indexes use brackets. Dynamic identifiers, absolute temp paths, process IDs, timestamps, and provider request IDs are normalized into stable aliases before comparison.

The required observation bundle is:

```json
{
  "case_id": "SES-001",
  "implementation": "claude",
  "version": "2.1.x",
  "platform": "darwin-arm64",
  "profile": "claude-current",
  "started_at": "RFC3339",
  "events": [],
  "snapshots": [],
  "processes": [],
  "files": [],
  "ui_frames": [],
  "result": {},
  "normalization": {}
}
```

The runner must never infer success from a process exit alone. It waits for the case's `assert.eventually` condition, captures all streams, and fails on an unexpected permission prompt, filesystem path, network target, or unmodeled state transition.

## Differential procedure

1. Provision a disposable repository, config directory, terminal, provider stub, and clock exactly as specified by `given`.
2. Run the case on the pinned Claude Code oracle and save the raw bundle unchanged.
3. Normalize only fields allowed by the case's `normalize` list.
4. Run the same case on `handoff` with the runtime adapter named by the fixture.
5. Check `then` independently on both outputs, then compare outputs according to `match_policy`.
6. For every entry in `faults`, rerun from a clean fixture, inject the fault at `at`, execute `recover`, and check the fault-specific `then` assertions.
7. Keep the raw oracle, normalized oracle, candidate output, and diff as CI artifacts.

Oracle refreshes are reviewed changes. A newer Claude version changing a result does not silently bless the new result: update the relevant source anchor, fixture expectation, and compatibility decision together.

## Coverage rules

A surface is conformant only when:

- its positive lifecycle and its denial/failure lifecycle have cases;
- exact identity, lineage, authority, and worktree ownership are asserted where applicable;
- any state advertised after restart is tested after process kill, supervisor kill, and machine-sleep clock advance where relevant;
- both the JSON/event interface and the human terminal view derive from the same state;
- costs, limits, and provider fallback are visible rather than silently substituted;
- destructive cleanup is exercised in a disposable fixture and verifies preservation guards;
- a documented Claude limitation is either reproduced in `claude-current` or recorded as a `durable-extension` divergence.

## Suites

[`manifest.json`](manifest.json) is the source index and suite catalog. Fixtures are grouped by behavior rather than implementation package:

- [`sessions-agent-view.json`](sessions-agent-view.json)
- [`coordination.json`](coordination.json)
- [`workflows-automation.json`](workflows-automation.json)
- [`isolation-context.json`](isolation-context.json)
- [`scheduling-channels-policy.json`](scheduling-channels-policy.json)
- [`models-pr-remote-hosting.json`](models-pr-remote-hosting.json)
- [`ux-config-packaging.json`](ux-config-packaging.json)

Validate syntax and references with the commands in the compatibility document. Implementations may generate native unit/integration tests from these cases, but must preserve the case IDs in test names and reports.
