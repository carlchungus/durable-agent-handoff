---
name: durable-agent-handoff
description: Recover and supervise interrupted Claude Code, Codex, Pi, or other agent work with durable state, exact session resume, safe retries, replies, and checked GitHub merging.
---

# Durable Agent Handoff (Supervisor v2)

Use `handoff` as the one source of state for supervised work. Read its current
state, recorded events, process evidence, tests, and repository policy;
do not infer lifecycle state from a transcript or a legacy store.

## Supported v2 surface

- Start a new or exact-resume execution with `handoff start` or `handoff create`.
  Secret prompts and continuation replies are supplied with `--file -` on
  stdin, never with `--prompt`, `--prompt-file`, `--message`, private prompt
  paths, or any other argv content.
- Use `handoff goal start` for unattended work. Goals are unbounded by default;
  add `--max-turns` only when the human explicitly requests a finite safety
  cap, never as an arbitrary supervisor budget. For periodic supervision, add
  `--wake-interval 10m`; the next automatic continuation is scheduled in the
  journal without occupying a model turn. Do not emulate cadence with polling
  prompts or shell sleeps. Human replies remain immediately runnable.
- Configure an autonomous merge-capable execution when it starts with
  `--finalizer-enabled`, one or more externally hosted `--required-check NAME`,
  and optional `--require-human`. Promotion uses the equivalent flat
  `finalizer_*` JSON fields. The immutable workflow configuration, not a later
  caller override, owns the fixed external check list.
- Start an arca-cloud continuation with
  `handoff execution start --file - --json`. It accepts one strict flat JSON
  object and returns only `workflow_id` and `node_id`.
- Read the same recorded state with `handoff status`, `handoff list`,
  `handoff activity list|read`, `handoff tui --snapshot`, or `handoff events`.
  JSON activity records retain the cloud-compatible active-state fields while
  omitting prompts.
- Run queued work with `handoff run` or `handoff serve`. Use
  `handoff service install [--enable]` for the stable `handoff.service` unit.
  `serve --environment-json FILE` requires a regular mode-0600 JSON object;
  values are transient and service units contain only the file path. The
  installation includes a ten-minute OS watchdog that starts an inactive
  service after a crash or reboot but never restarts healthy live work.
- Configure journaled role/model ladders with `handoff preference set|list|health`.
  Provider fallback creates a child Session with a new native identity; it
  never mutates the original exact runtime Session.
- Stop safely with `handoff execution pause --workflow ID --json`. Pause first
  records exact generation/Attempt fences, then the executor applies controls;
  leases are released only after terminal exit evidence and a durable settle.
- Continue a bound exact Session with `handoff reply`.
- Use `handoff session start` for the quiet terminal-like path. It has no goal,
  evaluator, or mandatory stages; `--check-interval` defaults to 20 minutes
  and only performs exact identity/recovery checks. Use `handoff status SESSION`
  and `handoff tail SESSION --lines 40 [--follow]` for quick operator peeks.
  Session mode does not apply the turn startup deadline; only an explicit
  control stops an exact live harness. POSIX runners persist child exit facts
  for restart adoption, while Windows Job Object teardown does not preserve a
  live tree across service termination.
- Runtime names are open-ended. Codex, Claude, and Pi have named adapters;
  other names use a generic stdin/argv adapter with `--executable` and repeatable
  `--arg=VALUE`. The generic adapter makes no native resume claim and rejects
  read-only authority unless a future named adapter supplies an OS sandbox.
- Treat independently hosted CI and GitHub checks as the merge requirement.
  Worker Result payloads are evidence of work only;
  handoff does not pretend that same-UID workers can authenticate their own
  Results.
- For unattended work allowed to publish, missing optional evidence,
  external CI, or browser authentication lowers confidence but does not erase
  useful work. Publish an honest draft PR with the verification limits and
  continue independent work. Once a PR is handed to repository automation,
  do not spend turns waiting for it to merge. Use `needs_human` only when
  indispensable permission or information blocks the entire workflow and no
  safe partial result can be published.
- Scope open-ended campaign goals with a viable publication outlet and a stop
  condition, not just a count. A goal like "ship 100 PRs" with no terminal
  acceptance and a narrow named surface invites tunnel vision: the worker
  optimizes the objective it is handed (ship another PR) and drifts to
  low-friction, low-value changes. Pair any count goal with a value gate
  (a reviewer, a merge-rate signal, or a max unpublished backlog) and a named
  stop condition ("until the reviewer queue is empty", "until the named
  surface is exhausted"). When the publication outlet is disabled, blocked,
  or repeatedly deferred, the goal evaluator escalates rather than grinding
  out more un-consumable candidates; do not work around that by restating
  "useful work remains." Producing work that cannot reach a consumer is not
  progress.
- Use `handoff github merge` only when merging was authorized at startup. It records
  the prepared exact PR head, named gates, approval, and idempotency key before
  an argv-only `gh` effect, then journals merged or blocked outcome.
- Import old state only with deterministic one-way `handoff execution import-v1`.
  It normalizes workflow-history ledgers only; legacy Session, Activity, and
  team ledgers are not replayed, exact native Session/Activity recovery is
  unsupported and marked unresolved, and the v1 source remains unchanged and
  is never a live write path.

## Identity and durability rules

- A Session owns conversation identity and exact native runtime identity. An
  ordinary new start begins unbound; the first typed `session_bound` milestone
  binds its immutable identity. Promotion and continuation require an exact
  bound identity. Never resume a global last session.
- An Activity is immutable logical work. An Attempt is one OS launch with an
  exact process start token, output identities, runtime, and resolved-worktree
  writer Lease. A `turn_started` milestone consumes task budget; pre-turn
  adapter/provider failures consume launch budget only.
- Controls name the exact Activity generation and Attempt. Stale controls and
  stale milestones fail closed, and each live exact Activity-generation plus
  Attempt can have at most one accepted control. Competing controls are
  rejected without mutation; pause reuses the accepted IDs. Reads and
  polling only read state;
  they do not reconcile, settle, release leases, or append events.
- Drivers receive prompts on stdin and construct provider argv themselves.
  Codex, Claude, and Pi apply workspace/full trust flags inside the driver.
  Full trust still uses direct argv with no shell wrapper. Runtime adapters do
  not receive GitHub merge authority.
- Validate proposed changes against a copy of the current state before one journal
  append. Retry an ambiguous mutation with the same idempotency key and exact
  input; divergent reuse fails closed.

## Recovery checklist

1. Read `AGENTS.md`, `CONTEXT.md`, `docs/architecture.md`, and
   `docs/protocol.md` before changing semantics.
2. Inspect `handoff status --json`, `handoff activity list --json`, and
   `handoff events --after N` from the private state root.
3. Confirm resolved worktree ownership, exact Session/Attempt identity,
   process start tokens, lease state, and named publication gates before any
   retry or stop.
4. Preserve immutable results and continuation generations. Do not invent a
   new Session to resume old work unless the journal records a typed fallback
   child with fewer permissions.
5. Before handoff, run `gofmt`, `go test ./...`, `go test -race ./...`,
   `go vet ./...`, `git diff --check`, and the supported cross-platform builds.

## Explicitly deferred v1/product surfaces

The old `doctor`, `discover`, `import claude`, `preference reset`, `team`,
`activity follow`, `activity stop`, and legacy preference-file commands are not
v2 commands. Do not tell an installed harness to use them. Team stores,
transcript discovery/import, byte-cursor attachment, and the broader Claude
workflow compatibility matrix remain deferred. Session `status` and `tail`
are the intentionally smaller Supervisor-journal observation surface; they do
not create a second live ledger.
