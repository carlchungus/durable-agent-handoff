# ADR 0001: Separate sessions from activities

- Status: accepted
- Date: 2026-08-05

## Context

Claude Code, Codex, Pi, and Oh My Pi expose overlapping notions of sessions,
background work, process handles, attachments, and tasks. Folding all of these
into the existing `session.Session` type would make a conversation's identity
depend on a transient process and would repeat upstream durability gaps.

The harness already has a durable Session ledger for exact conversational
resume and inbox delivery. It needs independently observable and controllable
background execution without changing that contract.

## Decision

Use four distinct resources:

1. **Session** owns conversation identity, lineage, inbox, workspace, and
   capability policy.
2. **Activity** owns a durable work lifecycle, immutable logical work specification,
   output streams, and terminal result.
3. **Attempt** records one immutable process execution for an Activity.
4. **Attachment** is an ephemeral cursor-based reader and never owns lifecycle
   authority.

Stop, signal, adopt, and restart are durable Control intents fenced by Activity
generation and exact Attempt process identity. The event ledger is canonical;
snapshots and indexes are rebuildable projections.

Workflow nodes may reference Sessions and Activities but are not their storage
or lifecycle boundary. Checklist tasks remain a separate UI/domain concept.

## Consequences

- An Activity may continue while its Session has no live runtime process and
  while every observer is disconnected.
- A resumed Session does not silently adopt the most recent or same-PID
  Activity.
- Reattachment uses durable byte cursors and a monotonic snapshot revision.
- Crash recovery validates a process start token and controller generation. If
  exact adoption is unavailable, recovery fails closed and records `lost`
  before policy may create another Attempt.
- The TUI and machine APIs consume the same reducer output.
- Runtime/model fallback creates observable Attempt history instead of
  rewriting Session state.

## Rejected alternatives

- Adding PID, command, output, and task fields to `session.Session`.
- Treating a workflow node as the only durable execution record.
- Using an in-memory process registry as canonical state.
- Copying an upstream all-purpose task executor or compatibility event enum.
