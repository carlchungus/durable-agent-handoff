# Domain context

This repository is a durable control plane for long-running agent work. Its
domain language deliberately separates conversation, execution, observation,
and authority.

## Session

A Session is a durable conversation identity. It owns:

- the exact opaque runtime session ID or session file needed to resume it;
- transcript lineage, parent/fork relationships, and queued messages;
- the workspace and capability envelope in which future turns may run; and
- references to activities launched for the session.

A Session does not own an operating-system process, PID, output pipe, or stop
authority. A conversation can exist while no process is alive.

## Activity

An Activity is independently controllable work: an agent turn, background
command, verifier, or monitor. It owns:

- a stable harness ID and immutable launch specification;
- its owner Session, selected runtime/model, and lifecycle event ledger;
- durable stdout/stderr streams and terminal result; and
- the ordered Attempts made to complete the work.

An Activity can survive the supervisor that launched it. Recovery either
adopts the exact running Attempt or records why it was lost and starts a later
Attempt according to policy.

## Attempt

An Attempt is one immutable process execution. It records its runtime/model,
launch digest, OS PID plus a platform start token, controller generation,
output identities, start/end times, and exit result. Retries create new
Attempts; they never rewrite an old one.

## Attachment

An Attachment is an ephemeral read lease over an Activity projection or output
stream. It is identified by activity, stream, output identity, byte cursor, and
snapshot revision. Disconnecting or detaching does not stop or mutate the
Activity.

## Control intent

A Control intent is a durable request such as stop, signal, adopt, or restart.
It names the expected Activity generation and exact Attempt process identity.
The supervisor acknowledges or rejects it in the event ledger. A stale client,
PID reuse, or old supervisor generation therefore cannot control newer work.

## Projection

The canonical ledgers reduce to one versioned projection. The human TUI,
machine JSON/JSONL, RPC surface, and PR gates consume that same projection; no
view scrapes processes or independently invents lifecycle state.

