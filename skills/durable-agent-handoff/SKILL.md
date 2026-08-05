---
name: durable-agent-handoff
description: Recover interrupted Claude Code, Codex, Pi, or other agent work and operate it through a high-fidelity, runtime-neutral implementation of Claude-style background sessions, subagents, teams, dynamic workflows, goals, hooks, worktrees, model ladders, and supervised PR gates. Use when agent work stopped, hit usage limits, must survive crashes, needs parallel coordination, or should continue autonomously with observable state.
---

# Durable Agent Handoff

Use `handoff` as the durable control plane. Trust live repositories, processes, tests, CI, and PR state over transcript narration.

## Choose the coordination contract

Do not flatten every task into one phase graph.

- Use a **background session** for independent work a human may peek, reply to, attach to, stop, or resume.
- Use a **subagent** for a bounded delegated task that returns to one parent context. Choose fresh or forked context explicitly.
- Use an **agent team** when a lead should dynamically assign peer sessions through shared tasks, dependencies, claims, and messages.
- Use a **dynamic workflow** when a readable script should own fan-out, loops, branching, intermediate results, verification, and replay.
- Use a **goal** when one session should keep taking turns until a fresh evaluator confirms a measurable condition.

Read `../../docs/claude-workflows-compatibility.md` before changing these semantics.

## Recover and launch

1. Run `handoff doctor` and, for Claude recovery, `handoff discover claude --since 8h --json`.
2. Inspect each worktree, branch, dirty path, commit, PR, process, transcript age, and applicable `AGENTS.md`.
3. Skip completed or ambiguous work. Require explicit authority for auth, tenant isolation, secrets, security, billing, compliance, migrations, dependencies, infrastructure, production operations, destructive changes, or broad refactors.
4. Use one worktree per mutable worker. Never overlap writers.
5. Resume exact runtime session IDs; never use a global last-session selector. Revalidate imported claims against the live checkout.
6. Configure role ladders with `handoff preference set ROLE --candidate runtime:model:effort ...`. Provider quota and rate exhaustion may advance the ladder; auth, invalid-model, code, and test failures may not.
7. Run under `handoff serve` or `handoff service install --enable`. Observe through the TUI, JSON status, JSONL events, durable attempt logs, and provider health.

## Durability contract

- Persist runtime session identity as soon as the stream emits it.
- Record PID plus process start token, worktree, command digest, heartbeat, stream offset, exit result, and lease owner for every attempt.
- Give workers direct durable log file descriptors so a supervisor crash does not break their output pipe.
- On restart: adopt a matching live process; otherwise reconcile a completed result, resume the exact session, rerun only an explicitly restart-safe task, or request human input.
- Treat provider exhaustion as routing, not a failed task attempt. Treat machine sleep separately from idle.
- Preserve task lists, dependencies, mailboxes, parent/child lineage, workflow replay state, and active goals across restarts.

## Runtime and authority boundaries

- Prefer the user's role ladder. A useful planner example is `claude:opus:xhigh`, then `codex:gpt-5.6-sol:xhigh`, then `pi:openrouter/moonshotai/kimi-latest:xhigh`.
- Claude uses strict declared MCP configuration. Codex uses an explicit workspace sandbox and exact thread resume. Pi/OhMyPi require external isolation because they do not provide an OS sandbox.
- Children inherit authority and may narrow it; they may not grant themselves more tools, filesystem scope, credentials, or merge rights.
- Hooks and evaluators may block, retry, inject context, or attest, but deterministic policy owns roots, budgets, privileged actions, and merge gates.

## GitHub boundary

Workers may edit and verify. Only explicitly authorized finalization may commit, push, create a PR, or merge. Require independent pass evidence, changed-file and diff-line budgets, a non-protected branch, exact named CI checks, and an unchanged verified head SHA. Never use admin merge or force push.

Stop on unexpected paths, budget exhaustion, ambiguous recovery, failed verification, changed PR heads, or missing named checks.
