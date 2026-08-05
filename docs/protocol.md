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

## Node kinds

`agent` invokes a runtime adapter. `command` executes an argv vector without a shell. `human` is visible but not scheduled. `finalize` is a privileged deterministic Git/PR operation. Unknown kinds remain pending and observable so extensions fail closed.

`runtime.sandbox` is `read-only` or `workspace-write`. A read-only node cannot propose a write-capable child. Adapters that cannot enforce read-only execution fail closed instead of silently widening authority.

## Compatibility

Fields are additive during the `0.x` series. Consumers must ignore unknown JSON fields and must not infer ordering from object keys. Event sequence numbers are the ordering contract.

Provider health is available through `handoff preference health`. Each entry includes the runtime/model key, classified limit type, redacted reason, observation time, and cooldown deadline.

## Team protocol

`handoff team apply TEAM_ID` accepts one JSON command on stdin. Commands are `add_member`, `set_member_state`, `set_process`, `add_task`, `claim_task`, `renew_claim`, `complete_task`, `fail_task`, `send_message`, `submit_plan`, `review_plan`, `request_shutdown`, and `respond_shutdown`.

Claims are fenced. A worker must include the exact `claim_generation` returned by `claim_task` when renewing or completing work. Expired claims may be acquired by another member, which increments the generation and prevents the former process from committing a stale completion. `handoff team inbox TEAM_ID --member MEMBER --after SEQUENCE` is a cursor-based machine-readable mailbox.
