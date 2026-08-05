# Codex, Pi, and Oh My Pi prior art

This is a clean-room contract study. No upstream source has been copied. The
review pins exact revisions so later changes upstream cannot silently change
the architectural evidence.

| Project | Revision | License | Best idea to reuse |
| --- | --- | --- | --- |
| [OpenAI Codex](https://github.com/openai/codex/tree/9d00bb01c0a712fb7c2f5b002bdf33bcc0fc352c) | `9d00bb0` | Apache-2.0 | canonical JSONL plus rebuildable projections and typed resources |
| [Pi](https://github.com/earendil-works/pi/tree/17f720489fab02413c549c73bb407b8d6ef500c4) | `17f7204` | MIT | append-only conversation trees and revisioned attachments |
| [Oh My Pi](https://github.com/can1357/oh-my-pi/tree/1e492d6ff9b8d4412591942b11fe06e1395ae80f) | `1e492d6` | MIT | immutable launch specs, cold revival, fenced registries, and fallback ladders |

## What we take

### Codex

- A storage-neutral interface that separates create, resume, append, flush,
  read, list, and shutdown ([ThreadStore](https://github.com/openai/codex/blob/9d00bb01c0a712fb7c2f5b002bdf33bcc0fc352c/codex-rs/thread-store/src/store.rs)).
- Inspectable append-only JSONL as canonical truth, with SQLite or snapshots as
  rebuildable projections ([rollout recorder](https://github.com/openai/codex/blob/9d00bb01c0a712fb7c2f5b002bdf33bcc0fc352c/codex-rs/rollout/src/recorder.rs)).
- Typed thread, turn, item, and process resources instead of a CLI that asks
  consumers to infer state from text ([app-server protocol](https://github.com/openai/codex/tree/9d00bb01c0a712fb7c2f5b002bdf33bcc0fc352c/codex-rs/app-server-protocol)).
- Explicit persisted lineage and rejoin of the exact already-running thread.
- OS-backed single-writer locks and a snapshot-then-subscribe operation that
  cannot miss events between the initial read and live follow.

Codex's Unified Exec and app-server process handles are deliberately not used
as durability prior art. Their registries and output buffers are in memory,
some handles are connection-scoped, and process lifetime can end when a handle
or connection disappears ([Unified Exec](https://github.com/openai/codex/tree/9d00bb01c0a712fb7c2f5b002bdf33bcc0fc352c/codex-rs/core/src/unified_exec)).

### Pi

- An append-only conversation tree with stable entry IDs and `parentId`; branch
  navigation moves the active leaf rather than mutating history
  ([session manager](https://github.com/earendil-works/pi/blob/17f720489fab02413c549c73bb407b8d6ef500c4/packages/coding-agent/src/core/session-manager.ts)).
- Separate create, attach, detach, prompt, steer, and abort operations
  ([server sessions](https://github.com/earendil-works/pi/blob/17f720489fab02413c549c73bb407b8d6ef500c4/packages/server/src/sessions.ts)).
- Monotonic snapshot revisions and client-side rejection of stale projections.
- A dynamic turn loop that drains steering messages and lets the agent decide
  its next action rather than forcing global discovery/plan/evaluate gates
  ([agent loop](https://github.com/earendil-works/pi/blob/17f720489fab02413c549c73bb407b8d6ef500c4/packages/agent/src/agent-loop.ts)).

Pi's live runtime registry and ordinary JSONL writer are not a crash-safe
Activity store: the registry is memory-only and the file boundary does not
provide the acknowledgement semantics this harness promises.

### Oh My Pi

- Immutable launch specs, serializable lifecycle snapshots, durable byte
  cursors, and typed start/list/logs/wait/send/stop/restart operations
  ([launch protocol](https://github.com/can1357/oh-my-pi/blob/1e492d6ff9b8d4412591942b11fe06e1395ae80f/packages/coding-agent/src/launch/protocol.ts)).
- Stable child-agent references, terminal tombstones, compare-and-swap attach,
  and exact cold revival from a persisted session file.
- Owner-scoped capacity and wait-for-real-settlement semantics.
- Role/model/provider-specific preference ladders with cooldown, provider-aware
  quota classification, and observable fallback/restoration
  ([fallback chains](https://github.com/can1357/oh-my-pi/blob/1e492d6ff9b8d4412591942b11fe06e1395ae80f/packages/coding-agent/src/session/retry-fallback-chains.ts)).
- One deterministic hub projection for structured agent output and the human
  interface.

Oh My Pi's ordinary async job manager is a process-global singleton. Its launch
broker persists more state, but adoption is PID-only and controls lack the full
generation/process-identity fence required here. We take the contracts, not the
manager implementation.

## Resulting design

```text
Session (conversation, lineage, inbox, policy)
   │ owns/refers to
   ▼
Activity (stable work ID, launch spec, event ledger, output)
   │ contains immutable history of
   ▼
Attempt (runtime/model, PID + start token, generation, result)

Attachment ── reads Activity projection/output at revision + byte cursor
Control    ── mutates only with expected Activity generation + Attempt identity
```

Canonical ledgers are append-only and fsynced at acknowledged boundaries.
Snapshots and query indexes may lag but never lead the ledger. On restart, the
supervisor rebuilds projections, validates the exact process identity, adopts
when safe, otherwise records `lost`, and lets policy decide whether to create a
new Attempt.

## Minimal implementation slice

1. Add a separate Activity store and reducer; do not extend `session.Session`.
2. Persist immutable launch spec, Attempts, output identities, and monotonic
   event sequence.
3. Expose `activity list`, `activity read`, `activity follow`, and
   generation-fenced `activity stop` from the same projection.
4. Prove output reattachment and state recovery by killing and restarting the
   supervisor in a fault test.
5. Only then connect agent turns, model ladders, the animated TUI, and remote
   executors to this resource.

## License boundary

Substantial copied Pi or Oh My Pi code must retain their MIT notices. Derived
Codex code must satisfy Apache-2.0 and NOTICE obligations and mark changes. The
current plan is independent Go implementation of public contracts and concepts,
with this attribution document as design provenance.

