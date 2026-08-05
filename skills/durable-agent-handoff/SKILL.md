---
name: durable-agent-handoff
description: Recover interrupted Claude Code, Codex, Pi, or other coding-agent work from local transcripts and live repository state, then run bounded, observable, restartable dynamic workflows with role-specific model ladders and supervised PR gates. Use when coding sessions stopped, hit usage limits, became stale, need migration across runtimes or model tiers, or should continue autonomously without losing auditability.
---

# Durable Agent Handoff

Use the `handoff` CLI as the durable control plane. Trust the live checkout, processes, CI, and PR state over transcript narration.

## Workflow

1. Run `handoff doctor` and `handoff discover claude --since 8h --json` when recovering Claude work.
2. Inspect each candidate's worktree, branch, dirty paths, commits, PRs, processes, and applicable `AGENTS.md` files.
3. Continue bounded fixes, tests, docs, or isolated UI work. Treat auth, tenant isolation, security, secrets, billing, compliance, migrations, dependencies, infrastructure, production operations, destructive changes, and broad refactors as human-review work unless explicitly authorized.
4. Use one branch/worktree per mutable worker. Preserve unrelated changes and never overlap writers.
5. Import with `handoff import claude --session ID --runtime RUNTIME`, or create with `handoff start --goal TEXT --root PATH`. Revalidate every imported claim.
6. Let workers adapt the graph: add specialists or verifiers, supersede obsolete work, retry, or request a human. Do not replace deterministic policy with model judgment.
7. Configure role ladders with `handoff preference set ROLE --candidate runtime:model:effort ...`. Fallback only on recorded quota/rate exhaustion. Never silently route auth, invalid-model, test, or ordinary runtime failures.
8. Run `handoff serve` or `handoff service install --enable`. Observe with the TUI, `status --json`, `events --follow`, and `preference health`.
9. Stop on unexpected paths, exceeded budgets, failed verification, changed PR heads, missing named checks, or ambiguous authority.

## Runtime selection

- Prefer the user's configured ladder. Without one, use Codex Luna/xhigh for general execution.
- Resume exact session IDs; never use a global last-session selector.
- Claude must stay in safe mode with strict declared MCP configuration.
- Pi/OhMyPi require a dedicated worktree and narrowly scoped secrets because they lack an OS sandbox.
- Missing or invalid candidates are errors, not reasons to improvise another provider.

## GitHub boundary

Workers may edit and test. Only an explicitly authorized `finalize` node may commit, push, create a PR, or merge. Require independent pass evidence, changed-file and diff-line budgets, a non-protected branch, exact named CI checks, and `--match-head-commit` semantics.

Read `../../docs/architecture.md` when changing graph or trust semantics, and `../../docs/protocol.md` when constructing machine proposals.
