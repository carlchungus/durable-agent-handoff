# Domain context

This repository keeps long-running agent work alive and recoverable. It records
conversations, work, processes, and permissions separately because they have
different lifetimes.

## Session

A Session is a durable conversation identity. It owns:

- the exact opaque runtime session ID or session file needed to resume it;
- transcript lineage, parent/fork relationships, and queued messages;
- the workspace and permissions for future turns; and
- references to activities launched for the session.

A Session does not own an operating-system process, PID, output pipe, or stop
authority. A conversation can exist while no process is alive.

## Activity

An Activity is independently controllable work: an agent turn, background
command, release-check, or monitor. It owns:

- a stable harness ID and immutable logical work specification;
- its owner Session, selected runtime/model, and recorded lifecycle events;
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

## Current state

The event journal rebuilds one versioned view of current state. The TUI,
JSON/JSONL commands, RPC calls, and PR checks read that same view; none of them
guess state by scraping processes.
