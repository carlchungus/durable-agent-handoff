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

## Compatibility

Fields are additive during the `0.x` series. Consumers must ignore unknown JSON fields and must not infer ordering from object keys. Event sequence numbers are the ordering contract.

Provider health is available through `handoff preference health`. Each entry includes the runtime/model key, classified limit type, redacted reason, observation time, and cooldown deadline.
