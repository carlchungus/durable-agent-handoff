# handoff

`handoff` is a durable meta-harness for continuing interrupted coding-agent work. It can recover sanitized context from Claude Code transcripts, hand the work to Codex, Claude, Pi, OhMyPi, or a custom executable, and keep the workflow observable through a live TUI and a stable JSON/JSONL interface.

The workflow is not a fixed `discover -> plan -> lint -> done` pipeline. Workers can reshape a task graph as evidence changes: add a specialist, replace an obsolete task, request a human, retry after infrastructure failure, or ask an independent agent to verify the result. A small deterministic policy kernel—not the model—retains authority over budgets, workspace boundaries, and PR merging.

> Status: early but functional. The ledger, reducer, adapters, service templates, transcript sanitizer, TUI, and GitHub gates are covered by unit, integration, race, and cross-platform CI. Use disposable branches while evaluating it.

## Why this shape

Claude Code's useful orchestration ideas are dynamic: shared task state, background subagents, resumable sessions, event hooks that can send an agent back to work, and an agent-owned maintenance loop. `handoff` implements the portable contracts itself so one workflow can mix runtimes and survive the lifecycle of any one harness.

The design borrows the good parts of [Claude Code hooks](https://code.claude.com/docs/en/hooks), [agent teams](https://code.claude.com/docs/en/agent-teams), [subagent isolation](https://code.claude.com/docs/en/sub-agents), and the dynamic maintenance behavior of [`/loop`](https://code.claude.com/docs/en/scheduled-tasks), while keeping durable state outside Claude. The TUI uses [Bubble Tea v2](https://github.com/charmbracelet/bubbletea).

## Install

With Go 1.25 or newer:

```sh
go install github.com/carlchungus/durable-agent-handoff/cmd/handoff@latest
handoff doctor
```

Release binaries for macOS, Linux, and Windows are attached to tagged GitHub releases. No Node or Python runtime is required. The coding harnesses you choose (`codex`, `claude`, `pi`, or `omp`) must already be installed and authenticated.

State defaults to the OS user-config directory. Set `HANDOFF_HOME` to put it elsewhere.

## Start a workflow

```sh
handoff start \
  --goal "finish the parser fix and verify the regression" \
  --root "$PWD" \
  --runtime codex \
  --model gpt-5.6-luna \
  --effort xhigh

# Run continuously in this terminal.
handoff serve

# Or install a restartable user service.
handoff service install --enable
```

`codex` with Luna/xhigh is the default executor. A cheaper Pi worker is just a runtime choice:

```sh
handoff start --goal "review this narrow fix" --runtime pi \
  --model openrouter/deepseek/deepseek-v4-flash
```

Pi and OhMyPi do not provide an OS sandbox. Use a dedicated worktree and narrowly scoped credentials.

## Recover stopped Claude Code work

Discovery reads only recent local transcripts and emits sanitized text metadata. Tool inputs and outputs are skipped.

```sh
handoff discover claude --since 4h
handoff discover claude --since 4h --json

# Move the work to Codex (default).
handoff import claude --session SESSION_ID --runtime codex

# Or resume the exact Claude session under the external supervisor.
handoff import claude --session SESSION_ID --runtime claude
```

The importer classifies obvious production, auth, billing, migration, credential, and infrastructure work as high risk and refuses it by default. `--allow-risk` is an explicit human override, not a model decision.

## Live TUI

Run `handoff` with no arguments:

```text
 handoff  durable, mutable agent workflows  ◓

 ╭─ WORKFLOWS ───────────────╮ ╭─ ACTIVE ───────────────────────────────────╮
 │ › ● repair parser         │ │ Repair parser and verify the regression   │
 │     active · 3 nodes      │ │                                           │
 │   ● docs follow-up        │ │ GRAPH                                     │
 │     waiting · 2 nodes     │ │  ✓ inspect           completed codex      │
 ╰───────────────────────────╯ │  ◆ implement         running   codex/luna  │
                               │  ○ independent review pending   pi/flash   │
                               │                                           │
                               │ EVENTS                                    │
                               │  14:03:12 proposal.applied · implement     │
                               ╰───────────────────────────────────────────╯
 ↑/↓ select • space run next • p pause/resume • r refresh • q quit
```

The display refreshes live, animates active state, and stays useful at narrow terminal widths. It is not the API: agents should use the stable machine interface.

## Agent-facing interface

```sh
handoff status --json
handoff status WORKFLOW_ID --json
handoff events WORKFLOW_ID --after 20 --follow
handoff run WORKFLOW_ID --once
handoff propose --file proposal.json
```

Events are newline-delimited JSON with monotonically increasing sequence numbers. Proposals are atomic: either every mutation passes policy or none is applied.

```json
{
  "workflow_id": "wf_1234",
  "actor": "lead",
  "rationale": "The failing behavior crosses a package boundary, so add a fresh verifier.",
  "mutations": [
    {
      "op": "add_node",
      "node": {
        "id": "verify-boundary",
        "title": "Independently test the package boundary",
        "kind": "agent",
        "depends_on": ["implement"],
        "runtime": {"name": "pi", "model": "openrouter/deepseek/deepseek-v4-flash"}
      }
    }
  ]
}
```

See [docs/protocol.md](docs/protocol.md) for the mutation and result contracts.

## Safe autonomous PRs

PR finalization is opt-in authority granted when the workflow is created:

```sh
handoff start \
  --goal "fix the parser regression" \
  --root "$PWD" \
  --finalize-repo owner/repo \
  --merge-gate "verify (24)"
```

This seeds an executor, a fresh verifier, and a privileged deterministic finalizer. Workers cannot add merge/finalize nodes. The finalizer requires a passing attestation, enforces changed-file and diff-line budgets, refuses `main`, `master`, or detached HEAD, commits without a shell, creates or reuses the PR, then merges only when every exact named gate is successful on the unchanged head SHA.

For a manually created PR, the same last gate is available directly:

```sh
handoff github merge --repo owner/repo --pr 123 \
  --gate "verify (24)" --method squash
```

## Runtime model

| Runtime | Default model | Resume | Safety posture |
| --- | --- | --- | --- |
| Codex | `gpt-5.6-luna`, xhigh | exact thread ID | workspace-write sandbox, no approval prompts |
| Claude | `sonnet`, high | exact session ID | safe mode, strict empty MCP config, `dontAsk` |
| Pi | DeepSeek V4 Flash | adapter session | dedicated worktree required; no OS sandbox |
| OhMyPi | DeepSeek V4 Flash | adapter session | dedicated worktree required; no OS sandbox |
| `exec` | user supplied | protocol defined | caller owns isolation |

Adapters emit a common result object. The supervisor stores exact session IDs and resumes only that session—never a global “last session.” Missing runtimes are reported by `handoff doctor`; there is no silent fallback to a different provider.

## Durability and safety boundaries

- The append-only event ledger is the source of truth. Materialized snapshots are rebuilt from it if missing or corrupt.
- Cross-process writes use a per-workflow lease; rejected proposals are recorded but cannot partially mutate state.
- A resident scheduler is not proof of health. Node state, events, evidence, attempts, session IDs, and attestations remain observable.
- Agents may mutate workflow shape, but not budgets, roots, pause state, or merge authority.
- Completion can require a separate attestation. Deterministic checks and GitHub state remain authoritative over model claims.
- Transcript imports redact common credential forms and omit tool payloads. They still require live-checkout revalidation.

Read [docs/architecture.md](docs/architecture.md) for the full reducer and trust model. Security reports belong in [SECURITY.md](SECURITY.md).

## Development

```sh
go test -race ./...
go vet ./...
go build ./cmd/handoff
```

Contributions are welcome. New adapters need command-construction tests, a disposable-repository conformance test, exact session-resume evidence, and a documented permission boundary.
