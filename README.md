# handoff

`handoff` is a durable meta-harness for continuing interrupted coding-agent work. It can recover sanitized context from Claude Code transcripts, hand the work to Codex, Claude, Pi, OhMyPi, or a custom executable, and keep the workflow observable through a live TUI and a stable JSON/JSONL interface.

The workflow is not a fixed `discover -> plan -> lint -> done` pipeline. Workers can reshape a task graph as evidence changes: add a specialist, replace an obsolete task, request a human, retry after infrastructure failure, or ask an independent agent to verify the result. A small deterministic policy kernel—not the model—retains authority over budgets, workspace boundaries, and PR merging.

> Status: early but functional. The ledger, reducer, adapters, service templates, transcript sanitizer, TUI, and GitHub gates are covered by unit, integration, race, and cross-platform CI. The complete high-fidelity Claude workflow compatibility surface is an explicit work in progress; see the [compatibility target](docs/claude-workflows-compatibility.md). Use disposable branches while evaluating it.

## Why this shape

Claude Code's useful orchestration ideas are dynamic: shared task state, background subagents, resumable sessions, event hooks that can send an agent back to work, and an agent-owned maintenance loop. `handoff` implements the portable contracts itself so one workflow can mix runtimes and survive the lifecycle of any one harness.

The design borrows the good parts of [Claude Code hooks](https://code.claude.com/docs/en/hooks), [agent teams](https://code.claude.com/docs/en/agent-teams), [subagent isolation](https://code.claude.com/docs/en/sub-agents), and the dynamic maintenance behavior of [`/loop`](https://code.claude.com/docs/en/scheduled-tasks), while keeping durable state outside Claude. It also takes narrowly scoped contracts from Codex, Pi, and Oh My Pi; the pinned source review and the ideas intentionally rejected are documented in [the prior-art study](docs/prior-art-codex-pi-omp.md). The TUI uses [Bubble Tea v2](https://github.com/charmbracelet/bubbletea).

Durable team coordination is also available through a machine-first interface:

```sh
handoff team create --name review-squad --workflow wf_123 --lead lead
handoff team apply team_123 --file command.json
handoff team inbox team_123 --member reviewer --after 0
```

Team members, logical/process state, dependency tasks, generation-fenced claims, plan approval, direct/broadcast mailboxes, idle notices, and cooperative shutdown are durable. Runtime spawning and the combined Agent View UI remain `partial` in the compatibility matrix.

Press `tab` in the animated TUI to switch between workflow and team views. The team view shows peer lifecycle and process state independently, current plan status, task claims and fencing generations, dependency blockers, and recent mailbox traffic.

## Install

With Go 1.25 or newer:

```sh
go install github.com/carlchungus/durable-agent-handoff/cmd/handoff@latest
handoff doctor
```

Release binaries for macOS, Linux, and Windows are attached to tagged GitHub releases. No Node or Python runtime is required. The coding harnesses you choose (`codex`, `claude`, `pi`, or `omp`) must already be installed and authenticated.

For example, install the Apple Silicon archive with the GitHub CLI:

```sh
gh release download v0.4.0 \
  --repo carlchungus/durable-agent-handoff \
  --pattern 'durable-agent-handoff_0.4.0_darwin_arm64.tar.gz'
tar -xzf durable-agent-handoff_0.4.0_darwin_arm64.tar.gz
mkdir -p "$HOME/.local/bin"
install -m 0755 handoff "$HOME/.local/bin/handoff"
"$HOME/.local/bin/handoff" doctor
```

Use the corresponding versioned `durable-agent-handoff_0.4.0_<os>_<arch>` archive for other platforms. GoReleaser uses lowercase OS names (`darwin`, `linux`, `windows`). Verify downloads against the release's `checksums.txt` before installation.

State defaults to the OS user-config directory. Set `HANDOFF_HOME` to put it elsewhere. Keep that directory supervisor-private and outside every worker-writable worktree or sandbox mount; do not point it into the repository being edited.

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

## Role-specific model ladders

Define ordered candidates once, then assign a role to any node. Fallback happens only for recognized usage/quota or rate-limit failures; auth failures, test failures, invalid models, and ordinary runtime errors do not silently switch providers.

```sh
handoff preference set planner \
  --candidate claude:opus:xhigh \
  --candidate codex:gpt-5.6-sol:xhigh \
  --candidate pi:openrouter/moonshotai/kimi-latest:xhigh

handoff start --goal "study the design and propose a plan" --role planner
handoff preference list
handoff preference health
```

Use `--sandbox read-only` for discovery and independent verification. Codex uses its native read-only sandbox and Claude receives a narrowed read-only tool list. Pi, OhMyPi, and arbitrary executables currently reject read-only mode unless an external OS sandbox is added; they never silently widen it.

When a limit is observed, `handoff` records the provider/model cooldown durably, appends routing evidence to the workflow, and selects the next healthy candidate. Usage-limit cooldowns default to one hour; transient rate limits default to five minutes. If every candidate is cooling down, the node remains ready and the scheduler waits rather than treating the work as failed. Reset observed health explicitly with `handoff preference reset [runtime/model]`.

Claude's own headless `--fallback-model` can still be useful within Claude for overload, but the external ladder is what crosses harness and billing boundaries. Model names are deliberately not hardcoded; inspect the live runtime catalog (for example, `pi --list-models kimi`) before choosing aliases.

Fallback changes execution capacity, never authority: sandbox selection takes the narrower of the job and candidate, so a `read-only` job remains read-only even if a configured candidate requests workspace-write.

## Observe and control background work

Activities are the process-lifecycle side of agent Sessions. They retain each runtime attempt, exact process identity, and stdout/stderr even when the scheduler dies. A gated runner prevents the target from executing until that identity is durable, then records its fenced completion before exiting; new agent turns do not maintain a competing process manifest.

```sh
handoff activity list --json
handoff activity read ACTIVITY_ID --json

# Reattach to an exact output at a durable byte cursor.
handoff activity follow ACTIVITY_ID \
  --stream stdout --output OUTPUT_ID --after BYTE_OFFSET --json

# Automation should fence control with values returned by read/list.
handoff activity stop ACTIVITY_ID \
  --if-generation GENERATION --if-attempt ATTEMPT_ID --json
```

`read` returns lifecycle metadata; `follow` reads output. Omitting `--output` follows the newest attempt, which is convenient for humans but not a safe reconnect contract for automation. Disconnecting a follower never stops the worker. A fenced stop is rejected if recovery, fallback, or another controller changed the Activity generation or exact Attempt.

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

The embedded dynamic-workflow kernel can also evaluate top-level-await
JavaScript in a QuickJS-ng/WASM sandbox and return one policy-validated
proposal. Its capability surface, replay fingerprint, limits, and current
boundary are documented in
[`docs/javascript-workflow-vm.md`](docs/javascript-workflow-vm.md). The public
`agent()` / `pipeline()` CLI remains compatibility work; this kernel does not
pretend that surface is complete.

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
- Provider fallback follows user-authored role ladders and is recorded as evidence; it is never an invisible substitution.
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
