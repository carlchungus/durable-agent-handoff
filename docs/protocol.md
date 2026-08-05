# Machine protocol

All CLI JSON is UTF-8 on stdout. Diagnostics go to stderr. `events --follow` emits one JSON object per line and resumes with `--after SEQUENCE`.

## Worker result

An agent ends with one object:

```json
{
  "status": "completed",
  "summary": "Implemented the parser fix and ran the focused regression test.",
  "session_id": "exact-runtime-session-id",
  "mutations": [],
  "attestations": []
}
```

`status` is one of `completed`, `continue`, `blocked`, or `needs_human`. `continue` makes the same node ready and resumes the exact stored runtime session. Mutations are applied atomically after policy validation.

Verifier results use an exact source-verdict allowlist. `pass`, `repair`, and `blocked` remain canonical. `fail_blocking` is stored as `blocked`; `pass_with_limit` and `pass_with_runtime_limit` are stored as `repair`, so a qualified pass cannot satisfy a merge gate. A normalized attestation retains the original source value in `raw_verdict` along with its summary and evidence IDs. Unknown verdicts and contradictory canonical/raw pairs reject the proposal atomically.

## Mutations

| Operation | Purpose | Authority |
| --- | --- | --- |
| `add_node` | Add agent, command, observer, human, or extension work | any actor; `merge`/`finalize` are privileged |
| `add_dependency` | Add graph edges | any actor; cycles are rejected |
| `set_state` | Advance or retry a node | transition-checked |
| `supersede` | Mark obsolete work as intentionally replaced | any actor |
| `add_evidence` | Attach a summary, URI, and digest | any actor |
| `attest` | Record `pass`, `repair`, or `blocked` evaluation | any actor; verifier identity is recorded |
| `pause` / `resume` | Stop or restart scheduling | human or supervisor only |
| `set_session` | Persist an exact runtime session ID | supervisor/runtime result |
| `set_runtime` | Persist an observable ladder selection | supervisor only |
| `reopen_agent` | Wake a completed, waiting, or failed agent without changing its exact session ID | human or supervisor only |

## Node kinds

`agent` invokes a runtime adapter. `command` executes an argv vector without a shell. `human` is visible but not scheduled. `finalize` is a privileged deterministic Git/PR operation. Unknown kinds remain pending and observable so extensions fail closed.

`runtime.sandbox` is `read-only` or `workspace-write`. A read-only node cannot propose a write-capable child. Adapters that cannot enforce read-only execution fail closed instead of silently widening authority.

## Compatibility

Fields are additive during the `0.x` series. Consumers must ignore unknown JSON fields and must not infer ordering from object keys. Event sequence numbers are the ordering contract.

Provider health is available through `handoff preference health`. Each entry includes the runtime/model key, classified limit type, redacted reason, observation time, and cooldown deadline.

Preference resolution intersects the node and candidate sandboxes across every ladder step. A provider/model fallback cannot widen `read-only` work to `workspace-write`, even when a configured candidate explicitly requests wider access.

## Activity protocol

`handoff activity list --json` and `handoff activity read ACTIVITY_ID --json` return the canonical Activity projection. An Activity owns a stable logical launch and ordered Attempts; each Attempt records runtime, model, command digest, exact process identity, supervisor ownership, output identities, and terminal result.

`handoff activity follow ACTIVITY_ID --stream stdout|stderr --output OUTPUT_ID --after BYTE --json` emits cursor-addressed chunks. Reconnect clients must retain `output_id` and `end`; a changed output identity fails closed instead of silently attaching to a fallback or restarted Attempt. Attachments have no lifecycle authority.

`handoff activity stop ACTIVITY_ID --if-generation GENERATION --if-attempt ATTEMPT_ID --json` records and applies an exact fenced control intent. Automation must provide both fences. A human may omit them to request a stop against the projection current at command time. PID-only control is never accepted.

## Durable agent session protocol

Background agent identity and inbox state use a separate append-only ledger at `$HANDOFF_HOME/sessions/AGENT_ID/events.jsonl`; a replaceable `state.json` snapshot is rebuilt from that ledger after loss. The stable harness agent ID is derived from workflow and node identity. `runtime_session_id` is the exact opaque runtime identity and is never replaced by a global or most-recent session.

`handoff agents --json` returns both `logical_state` (`working`, `needs_input`, `completed`, or `stopped`) and `process_state` (`starting`, `running`, or `exited`). Consumers must not infer one dimension from the other. `handoff agent inbox WORKFLOW_ID NODE_ID --after N` provides a cursor-readable inbox. `handoff agent reply WORKFLOW_ID NODE_ID --message TEXT` durably queues a human reply and reopens only an exact persisted session.

Messages move `queued` to `dispatched` under a monotonic delivery-attempt fence. This identity never rewinds when a provider-limit transition refunds the node retry counter. Requeue preserves the former delivery attempt; a later dispatch receives a larger one. A queued message with delivery attempt zero has never been dispatched and may repair the crash window between a human reply and node wake-up.

Every agent exit atomically records `agent_attempt_outcome` evidence with `attempt`, `delivery_attempt`, `attempt_outcome`, and `inbox_disposition`. `completed`, `continue`, `needs_human`, `blocked`, and `diff_budget` require `deliver`; `runtime_failure`, `parse_failure`, and `provider_limit` require `requeue`. Reconciliation matches this explicit evidence to a dispatched delivery attempt and applies its disposition exactly once. It never guesses consumption from node state or a legacy evidence name.

## Team protocol

`handoff team apply TEAM_ID` accepts one JSON command on stdin. Commands are `add_member`, `set_member_state`, `set_process`, `add_task`, `claim_task`, `renew_claim`, `complete_task`, `fail_task`, `send_message`, `submit_plan`, `review_plan`, `request_shutdown`, and `respond_shutdown`.

Claims are fenced. A worker must include the exact `claim_generation` returned by `claim_task` when renewing or completing work. Expired claims may be acquired by another member, which increments the generation and prevents the former process from committing a stale completion. `handoff team inbox TEAM_ID --member MEMBER --after SEQUENCE` is a cursor-based machine-readable mailbox.
