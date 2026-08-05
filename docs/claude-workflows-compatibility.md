# Claude workflows compatibility target

`handoff` is a clean-room implementation of the observable Claude Code workflow system, with runtime-neutral execution underneath. Compatibility means preserving the user-facing contracts and lifecycle semantics documented by Anthropic; it does not mean copying proprietary source code or private storage formats.

The current implementation is a durable kernel, not yet the complete compatibility surface. This matrix is the acceptance contract for that work. A row is not complete until it has behavior tests, restart tests where relevant, a machine-readable interface, and a human-facing view.

Status: **done**, **partial**, **planned**.

| Surface | Observable Claude behavior to preserve | Portable `handoff` contract | Status |
| --- | --- | --- | --- |
| Sessions | continuously saved conversations; name, list, resume exact ID, continue, branch/fork, import/export, retention | durable session record, exact adapter session ID, named branch lineage, transcript locator/export, configurable retention | partial |
| Background agent view | dispatch full sessions; group by needs-input/working/completed; animated liveness; peek, reply, attach/detach, stop, pin, filter; exited sessions restart on reply | `agents` TUI plus `agents --json`; durable inbox; attach transport; process state separate from task state | partial |
| Supervisor | sessions survive terminal exit, supervisor restart, updates, and sleep; stopped or wedged processes can continue from saved state | service-managed run leases, heartbeats, PID/start-token validation, adoption, exact resume, atomic exit records | partial |
| Subagents | fresh or forked context; named definitions with model, effort, tools, skills, MCP, hooks, memory, permissions, worktree, background mode; exact-ID resume; nested spawning | portable agent profiles and spawn API; parent/child lineage; isolated transcript; immutable runtime identity; capability narrowing | partial |
| Agent teams | lead plus peer sessions; shared task list; dependencies; claiming/assignment; direct messages; idle and shutdown protocol; optional plan approval | durable team, member, task, dependency, fenced claim, mailbox, idle, shutdown, and approval records; runtime-neutral peers | partial |
| Dynamic workflows | JavaScript with top-level await; `agent()` and `pipeline()`; phases, loops and branching in script variables; background run; 16 concurrent/1,000 total agents; pause, stop, restart, cached ordered replay; save as command; structured args | sandboxed workflow SDK with compatible primitives and caps; append-only invocation journal; deterministic replay frontier; reusable project/user workflow resolution | partial |
| Workflow planning | natural language or keyword asks Claude to write a workflow; optional raw-script review and per-project approval; ultracode can choose multiple workflows for one request | planner emits inspectable script/IR; human/auto/bypass launch policy; role-specific planner ladder; multiple sequential runs allowed | planned |
| Goals | one session-scoped condition; fresh small-model evaluator after every turn; reason feeds next turn; persists active condition on resume; clear/status; turn/time bounds | evaluator hook over a session loop with condition, reason, spend, turns, bounds, and independent model ladder | planned |
| Hooks | pre/post tool, permission, subagent, task, stop/failure, teammate idle, worktree, notification, config and session lifecycle events; commands, prompts, and agent verification may allow, block, retry, or inject context | ordered typed lifecycle bus; deterministic command hooks and model/agent hooks; policy-controlled decisions; durable hook outcomes | planned |
| Tasks and background commands | task panel lists running/completed shell jobs and agents; inspect output, attach, stop; notification on completion or input | unified activity records for agents, workflows, hooks, and commands; durable logs and control messages | partial |
| Worktrees | isolated parallel edits; configurable setup/copy hooks; cleanup; PR association | worktree allocator with ownership, setup/remove hooks, branch/PR identity, dirty-state and cleanup policy | partial |
| Checkpoints | checkpoint before each user prompt; restore code and/or conversation; summarize ranges; retain recent checkpoints; explicit limitations for shell, subagent, external and linked-file edits | transcript checkpoints plus Git-native file snapshots; independent code/conversation rewind; provenance and explicit untracked-change warnings | planned |
| Compaction and memory | automatic context compaction; summaries preserve continuity; subagent transcripts survive parent compaction; scoped memories/instructions | runtime transcript pointers, generated summaries with provenance, per-agent memory scopes, stable prompt-prefix construction | planned |
| Scheduling | session `/loop`; durable desktop tasks; cloud routines with isolated scheduled sessions and delivery | interval jobs, cron/routines, jitter, expiration, concurrency/missed-run policy, isolated session creation and result sinks | planned |
| Channels | authenticated external events enter a running session; source metadata and permissions remain visible | typed inbound channel adapters, deduplication, audit metadata, allowlists, durable inbox and wake-up signal | planned |
| Permissions | modes and tool allow/deny rules; launch approval distinct from subagent tool checks; child cannot expand parent authority | capability envelope inherited by default and only narrowed by workers; explicit human grants; policy kernel owns privileged actions | partial |
| Models and usage | per-session/subagent/stage model and effort; token/cost display; plan limits; overload fallback | per-role ordered provider/model ladders, cooldown health, budget accounting, stage overrides, visible routing evidence | partial |
| PR workflow | background session discovers linked PR and status; `/batch` fans worktree agents into PRs; checks/review gate completion | PR records linked to sessions; bounded worktree fan-out; deterministic unchanged-head and exact-check merge gate | partial |
| Remote operation | attach from another client while execution remains local; local permissions and filesystem stay authoritative | authenticated local control socket/API with event cursor, attach/reply/approve/stop; no implicit filesystem export | planned |
| SDK session storage/hosting | pluggable session persistence and hosted long-running agent process patterns | storage interface, local reference backend, conformance suite, lease/fencing protocol, deployable supervisor | planned |
| Human and agent UX | animated terminal UI plus stable JSON/stream-JSON controls; reduced-motion/screen-reader modes; notifications | Bubble Tea TUI, JSON snapshots, JSONL event follow, accessible static mode, terminal/desktop notification hooks | partial |
| Configuration and packaging | user/project/local precedence; reusable agents, skills, hooks, workflows and plugins | explicit layered config with source provenance; project/user workflow and profile directories; plugin/skill packaging | planned |

## Semantic boundaries

Claude exposes four different coordination models and `handoff` must not collapse them into one gated phase graph:

1. **Subagent:** a delegated worker whose result returns to one parent conversation.
2. **Background session:** an independent resumable conversation controlled by a human.
3. **Agent team:** peer sessions coordinated dynamically by a lead through tasks and messages.
4. **Dynamic workflow:** a script, not a lead model, owns branching, loops, fan-out, intermediate values, and replay.

The event ledger, policy kernel, runtime adapters, logs, leases, and model router are shared infrastructure. Their public semantics remain distinct.

## Agent-team state

The team ledger is implemented independently of the workflow graph. A member's logical state (`working`, `idle`, `needs_input`, `stopped`) is separate from whether its current OS process is live. Tasks have dependencies and expiring, generation-fenced claims so an old worker cannot complete work after another member reclaims it. Direct and broadcast messages, idle notifications, submitted/reviewed plans, and cooperative shutdown requests are durable mailbox entries.

The machine interface is `handoff team create|status|apply|inbox`; the animated TUI has a team view for member, task, claim, plan, and mailbox state. Runtime spawning, automatic mailbox injection/wake-up, and full Agent View controls remain compatibility work; the ledger deliberately exists first so replacing a runtime session cannot destroy the logical member, task, or message.

## Recovery contract

Each attempt records, before useful work proceeds:

- workflow, node, parent, team and task identity;
- runtime, model, effort and exact runtime session ID as soon as emitted;
- PID plus process start token, supervisor lease owner and fencing generation;
- worktree, argv digest, capability envelope and output paths;
- heartbeat, last durable stream offset, result and exit status.

On startup the supervisor reconciles every `running` attempt:

1. Matching live process and fencing token: adopt and continue tailing.
2. Dead process with an exact resumable session ID: start a new process that resumes only that ID.
3. Dead process without a session ID: retry only when the node is declared restart-safe.
4. Ambiguous identity or non-idempotent work: move to `needs_input` with the evidence required to decide.

Provider exhaustion is routing, not a failed task attempt. Machine sleep is not idle time. A PID alone is never proof of identity.

## Dynamic workflow replay

The workflow runtime journals every `agent()` invocation in script start order with an input digest and durable result. On resume, completed results are replayed only through the first unfinished invocation; that invocation and every later invocation execute again. This matches Claude's documented ordered replay frontier and prevents a script from observing a completion sequence that could not have occurred in the original run.

The orchestration script has no direct shell or filesystem access. It coordinates agents; those agents operate under capability policy. Runs support pause, resume, per-agent restart, whole-run stop, progress inspection, token accounting, and saved project/user commands.

## Sources

- [Run agents in parallel](https://code.claude.com/docs/en/agents)
- [Agent view](https://code.claude.com/docs/en/agent-view)
- [Agent teams](https://code.claude.com/docs/en/agent-teams)
- [Subagents](https://code.claude.com/docs/en/sub-agents)
- [Dynamic workflows](https://code.claude.com/docs/en/workflows)
- [Goals](https://code.claude.com/docs/en/goal)
- [Hooks](https://code.claude.com/docs/en/hooks)
- [Scheduled tasks](https://code.claude.com/docs/en/scheduled-tasks)
- [Routines](https://code.claude.com/docs/en/routines)
- [Channels](https://code.claude.com/docs/en/channels)
- [Sessions](https://code.claude.com/docs/en/sessions)
- [Checkpointing](https://code.claude.com/docs/en/checkpointing)
- [Remote Control](https://code.claude.com/docs/en/remote-control)
- [Agent SDK session storage](https://code.claude.com/docs/en/agent-sdk/session-storage)
- [Hosting the Agent SDK](https://code.claude.com/docs/en/agent-sdk/hosting)
