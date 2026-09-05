# Whole-system Claude Code workflow compatibility

`handoff` targets the **entire observable Claude Code workflow system**, not only process durability. The target is a clean-room implementation with the same user-visible states, commands, identities, limits, precedence rules, failure modes, and recovery behavior documented by Anthropic, plus documented portable features where Claude stops short.

The source snapshot is **2026-08-05**. Claude Code research-preview behavior is version-sensitive. Every differential capture must record the installed Claude Code version; new oracle output never replaces an expectation without a reviewed source and fixture change.

The current repository is a useful durable kernel, not yet a whole-system clone. Compatibility is complete only when every surface below passes its positive, denial, restart, and human/machine-view cases against a pinned Claude oracle. The 102-case contract lives in [`docs/conformance/`](conformance/README.md).

## Supervisor v2 shipping contract

The released `handoff` binary uses the breaking Supervisor v2 state format. Its
normal start, run, serve, status, list, reply, TUI, and activity paths read and
write one Supervisor journal. The supported command surface is documented in
[`README.md`](../README.md) and the protocol in [`protocol.md`](protocol.md):
ordinary prompts use `--file -` (stdin-only), promotion uses strict
`execution start --file - --json`, pause uses exact fenced Attempts, role/model
ladders use journaled preference commands, and guarded publication uses
`github merge`. `handoff.service` is the stable installed service name.

The old `doctor`, Claude discovery/import, team store, preference-file reset,
activity follow/stop, and transcript/output attachment commands are deferred
v1/product surfaces, not valid v2 compatibility adapters. They must not be
advertised as available until they have Supervisor-journal projections and
tests. `execution import-v1` is the sole deterministic one-way v1 importer; it
normalizes workflow-history ledgers only and does not recover exact legacy
Session or Activity identities.

## What “high fidelity” means

We reproduce observable contracts, including inconvenient limitations. We do not copy proprietary source, depend on undocumented private files, or call a stronger `handoff` behavior “Claude-compatible” when it differs.

Each fixture selects one of two profiles:

- `claude-current` reproduces the documented Claude behavior exactly or after narrowly declared normalization.
- `durable-extension` adds a portable capability. A deliberate divergence includes both `claude_expected` and the stronger `handoff` expectation.

The initial named divergences are durable fenced team claims and restore (`TEAM-003`, `TEAM-007`), cross-runtime preference ladders (`MOD-004`), and unchanged-head gated autonomous merge (`PR-004`). Other extensions add observability or persistence while retaining Claude’s baseline path. An undocumented difference is a bug.

## Complete acceptance matrix

“Runtime status” describes the implementation today, not the completeness of the contract. `partial` means some kernel or CLI behavior exists but the differential cases do not all pass. `planned` means the contract is defined but the runtime surface is materially absent.

| Feature | Claude behavior | Portable addition | Cases | Runtime status |
| --- | --- | --- | ---: | --- |
| Sessions | continuously saved transcript; exact-ID resume; continue; rename; picker scoping; fork with a new identity; clear, compact, export, and retention | explicit lineage, transcript locator, retention and normalized runtime identity | 5 | partial: SES-001 passes; SES-002–005 are gaps |
| Agent View | dispatch independent background sessions; needs-input/working/completed grouping; pin/filter; peek/reply; attach/detach/stop/delete; restart exited work on reply | durable inbox and separate logical/process state exposed in JSON and TUI | 5 | partial: VIEW-001 and VIEW-002 pass; VIEW-003–005 are gaps |
| Supervisor | session execution survives terminal exit, update, sleep and supported reconnect; stale processes are not shown as live | leases, heartbeat, PID start-token validation, fencing, fail-closed orphan recovery, and exact resume; POSIX session adoption is now supported | 4 | partial: SUP-001 passes; broader reconnect/compatibility gaps remain |
| Subagents | fresh or forked context; named definitions; model/effort/tools/skills/MCP/hooks/memory/permissions/worktree/background; exact resume; depth, count and concurrency limits | immutable runtime identity, transcript isolation and capability narrowing | 7 | partial substrate only: SUB-001–007 remain gaps |
| Teams | fixed lead plus peers; shared tasks/dependencies/claims; direct and broadcast mailbox; plan approval; idle/failure/shutdown; no nesting | typed durable mailbox, generation-fenced claims, crash restore by exact identity | 8 | partial |
| Dynamic workflows | real JavaScript with top-level `await`, `agent()` and `pipeline()`; dynamic branches/loops; 16 concurrent and 1,000 total agents; pause/stop/restart; ordered replay; saved scopes and args | sandboxed runtime adapters over one journaled invocation model | 5 | partial |
| Workflow planning | natural-language and human-only keyword triggers; inspectable script; launch approval; project decisions; ultracode may choose several workflows | role-specific planner ladder and inspectable IR without a mandatory phase graph | 3 | planned |
| Goals | one session-scoped condition; independent fresh evaluator after every turn; reason feeds the next turn; resume, status, clear, time/turn bounds | tool-less OpenRouter evaluator, exact-Session continuation, typed escalation and turn bounds; evaluator ladder and spend evidence remain | 2 | partial |
| Hooks | full lifecycle event vocabulary; command, HTTP, prompt and agent handlers; matchers; allow/block/retry/context; sync/async behavior | typed durable outcomes and independently verifiable agent hooks | 4 | planned |
| Tasks/background | `/tasks` unifies jobs and agents; live output, inspect, attach, stop and completion/input notification | durable activity identity, logs and controls across runtime restart | 2 | partial substrate only: TASK-001–002 remain gaps |
| Worktrees | default path/branch/base; resume/fork cwd; ignored-file copy; cleanup guards; create/remove hooks; isolated background edits | ownership locks, provenance and safe cleanup evidence | 6 | partial |
| Checkpoints | prompt checkpoints; newest 100; code/conversation rewind independently; targeted summary; explicit Bash/external/subagent/link limitations | Git-native snapshots with untracked-change provenance | 4 | planned |
| Compaction/memory | auto/manual compaction; survival/reload matrix; raw transcript retention; scoped project/user/local/agent memory | summary provenance, durable pointers and stable prompt-prefix construction | 4 | planned |
| Schedules/routines | session `/loop`, dynamic cadence, rounding/jitter/expiry/no catch-up; desktop new-session tasks and skip history; autonomous fresh-clone cloud routines | portable cron/interval/event schedules with explicit missed-run and delivery policy | 6 | planned |
| Channels | ordered grouped inbound notifications; source/meta rules; no ack; two-way reply tools; plugin/org/session/sender gates; first-valid permission relay | authenticated adapters with durable inbox, dedupe and wake-up evidence | 4 | planned |
| Permissions | deny-before-ask-before-allow across scopes; mode-specific baselines; protected paths; hooks; workspace trust; child narrowing; additional directories do not load config | one policy kernel owns privileged actions for every runtime | 5 | partial |
| Models/usage | aliases, allowlists, per-role model/effort; turn-only three-entry fallback for specific server errors; visible token/cost/plan data | per-job cross-runtime ladders, cooldown health, spend and fallback evidence | 5 | partial |
| PR workflow | linked PR status; `/batch` creates 5–30 worktree PRs; multi-agent verified/deduplicated review; neutral check result | unchanged-head and specifically named check/review/authorization merge gate | 4 | partial |
| Remote Control | local execution/filesystem/permissions remain authoritative; outbound TLS sync; reconnect/queue after sleep or short network loss; documented timeout and command limits | authenticated local API/socket with event cursor and no implicit filesystem export | 2 | planned |
| Storage/hosting | opaque ordered session store; optional methods/subkeys; mirror retry/error semantics; ephemeral, long-running, hybrid and multi-agent placement; tenant isolation | leases, reference backend, resource state, deployable supervisor and conformance reports | 5 | planned |
| Human and agent UX | animated, responsive terminal view; session detail/actions; fullscreen behavior; linear screen-reader mode, numbered menus and attention alerts | same ledger powers TUI, JSON snapshot and reconnectable JSONL stream | 5 | partial |
| Config/packaging | managed > CLI > local > project > user; managed drop-in merge rules; reload/provenance; alternate config roots; nearest definitions; live skills; plugin namespaces/cache/components | source-labelled portable profiles, workflows, skills and plugin bundles | 7 | planned |

## Semantic architecture

The implementation shares infrastructure without flattening Claude’s different coordination models.

| Observable model | Who owns control flow | Required identity/result contract |
| --- | --- | --- |
| Subagent | one parent model | delegated result returns to the parent; independent transcript and exact resume identity |
| Background session | human through Agent View | independent conversation, durable queued reply, attach/detach and logical/process state |
| Agent team | lead and peer models | shared dynamic task/mailbox protocol; peers do not collapse into child return values |
| Dynamic workflow | JavaScript program | script variables own loops, branches and intermediate values; `agent()` calls replay in start order |
| Goal | evaluator-controlled session loop | one condition checked after every completed turn; evaluator reason becomes next-turn context |
| Scheduled work | clock or external trigger | fire policy creates a turn or a fresh session according to the selected scheduling product |
| Channel | authenticated external source | ordered event injection into a running session with visible source and permission provenance |

Shared infrastructure is intentionally smaller than the product surface:

1. **Identity and event ledger:** sessions, attempts, processes, agents, teams, tasks, workflows, schedules, worktrees, PRs and messages have stable typed identities and ordered events.
2. **Supervisor:** leases, heartbeats, start tokens, fencing, reconciliation, inbox wake-up and exact runtime resume.
3. **Runtime adapters:** Claude Code, Codex, Pi/Oh My Pi and future runtimes implement spawn/resume/stream/stop/capability/model/usage contracts without leaking their command lines into workflow semantics.
4. **Coordination engines:** subagent return, background session control, team mailbox, JS workflow replay, goals and schedules remain distinct state machines over the ledger.
5. **Policy and routing:** permissions, launch approvals, model allowlists/ladders, provider cooldowns and budgets are evaluated before adapters act.
6. **Isolation and delivery:** worktrees, checkpoints, memory, channel adapters, remote transport, storage mirrors and hosting leases preserve ownership and provenance.
7. **Views:** the animated TUI, accessible static mode, JSON snapshot and cursor-based JSONL follow derive from the same reducers.

## Recovery and identity contract

Before useful work starts, every attempt durably records workflow/node/parent/team/task identity, runtime/model/effort, exact runtime session ID when emitted, PID plus process start token, lease owner and generation, worktree, permissions, argv digest, output locations and last stream cursor.

At supervisor start:

1. An exact live PID/start-token orphan is detected before scheduling; the v2 service adopts it for observation and output recovery without launching a duplicate.
2. A dead or prepared inherited Attempt is terminalized through one authority-owned journal command, its exact Lease is released, and the immutable Activity becomes retryable.
3. A dead process with an exact resumable runtime session ID may then be retried by the normal scheduler, resuming only that ID; a dead process without a resume ID is retried only when the operation is restart-safe.
4. Ambiguous identity, stale fencing, dirty worktree ownership or non-idempotent work transitions to `needs_input` with evidence.
5. Sleep advances wall time but never fabricates worker idleness or lease expiry. Remote and runtime updates are replayed from durable cursors.

Claude-current cases retain Claude’s documented restore limitations. The durable profile strengthens recovery where it can be done without changing the baseline interaction.

## Dynamic workflows, planning, and self-mutation

This is not a checklist engine with hard-coded discovery, plan, execute and verify gates. The high-fidelity workflow surface runs sandboxed JavaScript with top-level `await`; the script may branch, loop, fan out, launch different agent roles, inspect results, revise its approach, or call several workflows sequentially. Only the orchestration API is available to the script; filesystem and shell work goes through policy-bounded agents.

Planning is itself agent work. A planner can emit or revise a workflow script from current evidence. Independent review/check agents can decide semantic conditions that commands cannot establish. A workflow may self-mutate its future control flow, but it cannot rewrite completed ledger history, elevate permissions, change its oracle profile, erase costs, or weaken merge/recovery gates.

On pause or crash, completed `agent()` results replay only through the ordered prefix before the first unfinished invocation. That invocation and all later invocations run again. Stopped or unrecoverable agents retain pipeline shape with a `null` result, matching the documented workflow contract.

## Model ladders and graceful exhaustion

Claude-current fallback stays exact: at most three de-duplicated models, allowlist-filtered, switching for overload/unavailability/other non-retryable server errors, never for authentication, billing, rate limit, request size or transport errors, and retrying the primary on the next turn.

The portable router adds per-role ordered runtime/model ladders. A user may configure, for example, planner = Claude Opus → Codex GPT-5.6 Sol → Pi Kimi K3. Each route decision records attempted entry, resolved provider/model, failure class, cooldown/usage window, cost and why the next entry was eligible. Authorization, malformed requests, failing tests and tool denial are not usage exhaustion and never silently trigger a weaker model.

## Pull requests and autonomous merge

Creating a branch, commit, push or PR is normal agent work only when the task authorizes it. Direct default-branch mutation and force push are outside the background-worker contract. The portable merge extension additionally requires:

- an explicit repository/task merge policy;
- a recorded PR head SHA;
- only the named required checks, with their raw conclusions;
- the configured human or independent-agent review decision;
- a final unchanged-head comparison immediately before merge.

Any head change invalidates prior gates. Claude Code’s Code Review remains advisory/neutral in the `claude-current` profile; the stricter merge gate is not misrepresented as native Claude behavior.

Runtime drivers cannot merge on GitHub. They may propose work and report evidence; the supervisor checks the unchanged head and required checks before running `gh` directly without a shell.

## Configuration and distribution

Configuration resolution carries source provenance and preserves Claude’s scope order. Permission rules merge with deny precedence rather than scalar override. Project-provided capability grants remain subject to workspace trust. Worktrees share repository-local settings where Claude does, while alternate config directories isolate user data.

Agents, workflows and skills resolve from user and nearest project scopes according to their documented rules. Plugins package namespaced skills, agents, hooks, MCP servers and workflows through a cache without rewriting standalone user configuration. Managed restrictions can prohibit sideloading or standalone customization and must fail closed where the official setting requires it.

## Convergence loop

Implementation proceeds as a dynamic evidence loop, not mandatory product phases:

1. Select the highest-value failing capability slice and its dependency cases.
2. Capture the pinned Claude oracle, including failures, UI frames, process tree and files allowed by the fixture.
3. Have the implementing agent change the smallest shared primitive or surface adapter that explains the diff.
4. Run the candidate cases plus affected restart, denial and view cases.
5. Use deterministic assertions where semantics are mechanical and independent review/check agents where correctness is semantic.
6. Keep, revise or replace the approach based on evidence; record any plan mutation as an event.
7. Never relax an expectation as an implementation shortcut. Oracle changes require source review and an explicit compatibility decision.

Parts can be built in parallel when they do not change the same state. Useful early work includes event and identity normalization, exact session resume, Agent View controls, JS workflow execution and replay, runtime fallbacks, and consistent machine and human views. Many later features depend on these, but the order can change when needed.

## Differential suite and release gate

[`docs/conformance/README.md`](conformance/README.md) defines the runner and observation bundle. [`manifest.json`](conformance/manifest.json) maps every source and required surface. The seven fixture suites define 102 cases.

For a pinned Claude release, a release candidate must:

- produce raw and normalized oracle/candidate bundles for every applicable `claude-current` case;
- pass every exact/normalized comparison and every independent `then` assertion;
- pass all fault injections without duplicate execution or stale ownership;
- emit a reviewed report for every deliberate divergence;
- show the same logical state in TUI, JSON and JSONL views;
- pass race tests, static analysis, storage adapter conformance and disposable Git/PR integration tests;
- record unsupported platform/provider features explicitly instead of silently skipping them.

Syntax and catalog checks:

```sh
jq empty docs/conformance/*.json

set -- \
  docs/conformance/sessions-agent-view.json \
  docs/conformance/coordination.json \
  docs/conformance/workflows-automation.json \
  docs/conformance/isolation-context.json \
  docs/conformance/scheduling-channels-policy.json \
  docs/conformance/models-pr-remote-hosting.json \
  docs/conformance/ux-config-packaging.json

jq -s -e '
  .[0:-1] as $suites
  | .[-1] as $manifest
  | ($suites | map(.cases) | add) as $cases
  | ($cases | length) == 102
  and ($cases | map(.id) | length) == ($cases | map(.id) | unique | length)
  and ($cases | map(.surface) | unique | sort) == ($manifest.required_surfaces | sort)
  and all($cases[].source_refs[]; (split("#")[0]) as $key | $manifest.sources[$key] != null)
' "$@" docs/conformance/manifest.json
```

The fixture schema is [`case.schema.json`](conformance/case.schema.json). A production runner must validate each suite against it, verify every `source_refs` prefix exists in the manifest, and preserve case IDs in generated test names and reports.

## Official source index

Anthropic’s [official documentation index](https://code.claude.com/docs/llms.txt) is the discovery root. The snapshot uses only official primary documentation:

- Coordination: [agents](https://code.claude.com/docs/en/agents), [Agent View](https://code.claude.com/docs/en/agent-view), [sessions](https://code.claude.com/docs/en/sessions), [subagents](https://code.claude.com/docs/en/sub-agents), and [agent teams](https://code.claude.com/docs/en/agent-teams).
- Orchestration: [dynamic workflows](https://code.claude.com/docs/en/workflows), [goals](https://code.claude.com/docs/en/goal), [hook reference](https://code.claude.com/docs/en/hooks), [hook guide](https://code.claude.com/docs/en/hooks-guide), and [commands/tasks](https://code.claude.com/docs/en/commands).
- Isolation and context: [worktrees](https://code.claude.com/docs/en/worktrees), [checkpointing](https://code.claude.com/docs/en/checkpointing), [context windows](https://code.claude.com/docs/en/context-window), and [memory](https://code.claude.com/docs/en/memory).
- Automation and ingress: [session schedules](https://code.claude.com/docs/en/scheduled-tasks), [Desktop scheduled tasks](https://code.claude.com/docs/en/desktop-scheduled-tasks), [cloud routines](https://code.claude.com/docs/en/routines), [channels](https://code.claude.com/docs/en/channels), and the [channel protocol](https://code.claude.com/docs/en/channels-reference).
- Policy and routing: [permissions](https://code.claude.com/docs/en/permissions), [permission modes](https://code.claude.com/docs/en/permission-modes), [model configuration](https://code.claude.com/docs/en/model-config), [costs and usage](https://code.claude.com/docs/en/costs), and [status line data](https://code.claude.com/docs/en/statusline).
- Delivery: [Code Review](https://code.claude.com/docs/en/code-review), [GitHub Actions](https://code.claude.com/docs/en/github-actions), and [Remote Control](https://code.claude.com/docs/en/remote-control).
- SDK durability and deployment: [sessions](https://code.claude.com/docs/en/agent-sdk/sessions), [session storage](https://code.claude.com/docs/en/agent-sdk/session-storage), [hosting](https://code.claude.com/docs/en/agent-sdk/hosting), [streaming output](https://code.claude.com/docs/en/agent-sdk/streaming-output), and [user input](https://code.claude.com/docs/en/agent-sdk/user-input).
- UX and packaging: [settings](https://code.claude.com/docs/en/settings), [`.claude` directory](https://code.claude.com/docs/en/claude-directory), [skills](https://code.claude.com/docs/en/skills), [plugins](https://code.claude.com/docs/en/plugins), [plugin reference](https://code.claude.com/docs/en/plugins-reference), [accessibility](https://code.claude.com/docs/en/accessibility), and [fullscreen rendering](https://code.claude.com/docs/en/fullscreen).
