---
name: durable-agent-handoff
description: Recover interrupted Claude Code, Codex, Pi, or other agent work through Supervisor v2's journaled sessions, activities, attempts, leases, controls, runtime fallbacks, replies, and guarded publication effects.
---

# Durable Agent Handoff (Supervisor v2)

Use `handoff` as the one durable control plane. Read live Supervisor
projections, journal events, process evidence, tests, and repository policy;
do not infer lifecycle state from a transcript or a legacy store.

## Supported v2 surface

- Start a new or exact-resume execution with `handoff start` or `handoff create`.
  Secret prompts and continuation replies are supplied with `--file -` on
  stdin, never with `--prompt`, `--prompt-file`, `--message`, private prompt
  paths, or any other argv content.
- Configure an autonomous merge-capable execution at its start boundary with
  `--finalizer-enabled`, one or more `--required-check NAME`,
  `--require-human`, `--require-verifier`, and one or more `--verifier ID`.
  Promotion uses the equivalent flat `finalizer_*` JSON fields. The immutable
  workflow configuration, not a later caller override, owns the required gate
  set.
- Use the arca-cloud promotion envelope with
  `handoff execution start --file - --json`. It accepts one strict flat JSON
  object and returns only `workflow_id` and `node_id`.
- Observe one journal projection with `handoff status`, `handoff list`,
  `handoff activity list|read`, `handoff tui --snapshot`, or `handoff events`.
  JSON activity records retain the cloud-compatible active-state fields while
  omitting prompts.
- Run queued work with `handoff run` or `handoff serve`. Use
  `handoff service install [--enable]` for the stable `handoff.service` unit.
  `serve --environment-json FILE` requires a regular mode-0600 JSON object;
  values are transient and service units contain only the file path.
- Configure journaled role/model ladders with `handoff preference set|list|health`.
  Provider fallback creates a child Session with a new native identity; it
  never mutates the original exact runtime Session.
- Stop safely with `handoff execution pause --workflow ID --json`. Pause first
  records exact generation/Attempt fences, then the executor applies controls;
  leases are released only after terminal exit evidence and a durable settle.
- Continue a bound exact Session with `handoff reply`.
- Record independent verification with `handoff attest`. Pass the exact Result,
  configured verifier identity, verdict, and idempotency key as flags; provide
  the evidence summary only through `--file -` stdin. Worker Result payloads do
  not self-attest. The verifier must differ from the requester, and stale,
  unauthorized, or duplicate verifier/Result pairs fail without mutation.
- Use `handoff github merge` only for authority-owned finalization. It journals
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
  exact process start token, output identities, runtime, and canonical-worktree
  writer Lease. A `turn_started` milestone consumes task budget; pre-turn
  adapter/provider failures consume launch budget only.
- Controls name the exact Activity generation and Attempt. Stale controls and
  stale milestones fail closed, and each live exact Activity-generation plus
  Attempt can have at most one accepted control. Competing controls are
  rejected without mutation; pause reuses the accepted fence. Reads and
  polling are pure projection reads;
  they do not reconcile, settle, release leases, or append events.
- Drivers receive prompts on stdin and construct provider argv themselves.
  Codex, Claude, and Pi apply workspace/full trust flags inside the driver.
  Full trust still uses direct argv with no shell wrapper. Runtime adapters do
  not receive GitHub merge authority.
- Validate proposed mutations against a cloned projection before one journal
  append. Retry an ambiguous mutation with the same idempotency key and exact
  input; divergent reuse fails closed.

## Recovery checklist

1. Read `AGENTS.md`, `CONTEXT.md`, `docs/architecture.md`, and
   `docs/protocol.md` before changing semantics.
2. Inspect `handoff status --json`, `handoff activity list --json`, and
   `handoff events --after N` from the private state root.
3. Confirm canonical worktree ownership, exact Session/Attempt identity,
   process start tokens, lease state, and named publication gates before any
   retry or stop.
4. Preserve immutable results and continuation generations. Do not invent a
   new Session to resume old work unless the journal records a typed fallback
   child with narrowed authority.
5. Before handoff, run `gofmt`, `go test ./...`, `go test -race ./...`,
   `go vet ./...`, `git diff --check`, and the supported cross-platform builds.

## Explicitly deferred v1/product surfaces

The old `doctor`, `discover`, `import claude`, `preference reset`, `team`,
`activity follow`, `activity stop`, and legacy preference-file commands are not
v2 commands. Do not tell an installed harness to use them. Team stores,
transcript discovery/import, byte-cursor output attachment, and the broader
Claude workflow compatibility matrix remain deferred until they have a
Supervisor-journal command and tests; they are not silently emulated by a
second live ledger.
