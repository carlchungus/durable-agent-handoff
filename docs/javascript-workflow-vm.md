# Sandboxed JavaScript workflow VM

`internal/workflow` contains the embedded orchestration-script kernel. It runs
QuickJS-ng's WASI reactor under wazero; it does not start Node, a browser, a
shell, or a native shared library.

This is a bounded compatibility milestone. It evaluates a script against one
immutable workflow snapshot and returns one policy-validated `core.Proposal`.
`agent()`, `pipeline()`, parallel suspension, saved commands, and the public CLI
surface are separate compatibility work.

## Script API

Scripts are async function bodies, so top-level `await`, loops, branching, and
ordinary JavaScript variables work:

```js
const failed = handoff.workflow.order
  .map(id => handoff.node(id))
  .filter(node => node.state === "failed");

await Promise.resolve();

handoff.propose(
  [{
    op: "add_node",
    node: {
      id: "repair",
      title: "Repair the failed work",
      kind: "agent",
      depends_on: failed.map(node => node.id),
      runtime: {sandbox: "workspace-write"},
    },
  }],
  "A failed node requires a bounded repair worker",
);
```

The only capability is the frozen `handoff` object:

- `handoff.workflow`: immutable JSON snapshot.
- `handoff.args`: immutable structured arguments.
- `handoff.node(id)`: an immutable node or `null`.
- `handoff.evidence(nodeId?)`: a copied, immutable evidence list.
- `handoff.propose(mutations, rationale?)`: exactly one proposed atomic change.

Go assigns the workflow ID and actor. The script cannot grant itself a
different identity. The full proposal is validated against a cloned graph by
the core policy kernel; if any mutation fails, none is returned or applied.

## Isolation and deterministic limits

The WASI module receives no filesystem preopens, environment variables,
sockets, process APIs, stdin, or host clock/random sources. Imports and dynamic
code generation are rejected. `Date`, `performance`, `Math.random`, `eval`, and
function constructors are unavailable to guest code.

Every evaluation has independent limits for:

- source and serialized input bytes;
- QuickJS and WebAssembly memory plus JavaScript stack size;
- deterministic QuickJS/WASM function-entry fuel;
- wall-clock time as a second termination backstop;
- captured output bytes; and
- proposed mutation count.

Limit errors name the failed boundary (`instruction_limit`, `timeout`,
`memory_limit`, or `output_limit`) and the script filename. The fuel unit is a
pinned-engine execution tick, not a claim about JavaScript source statement
count.

## Loading and replay

`LoadScript` resolves symlinks, requires explicit allowed roots, reads one
regular UTF-8 file under a byte cap, and hashes its exact contents. The VM never
receives the resolved path as a filesystem capability.

The evaluation fingerprint binds the pinned engine identity, source hash,
canonical args, immutable workflow snapshot, actor, policy version, and all resource limits. The journal
records `script.started`, followed by either `script.proposed` or
`script.failed`. A completed matching run replays its proposal without starting
the VM. A started run with no durable result executes again from the beginning.
Opening a journal truncates only an incomplete crash tail before the next
append, so a recovered result cannot be concatenated onto corrupt JSON.

## Engine decision

[Zapcode](https://github.com/arcainc/zapcode) is a promising future optional
runtime: its purpose-built TypeScript VM has a strong deny-by-default language
sandbox, compact mid-execution snapshots, and direct bytecode resource
accounting. It is not the default for this compatibility layer today because it
has no Go or WASI-reactor host ABI, its published WebAssembly binding targets a
JavaScript host through `wasm-bindgen`, and its intentionally smaller language
surface rejects valid JavaScript accepted by Claude workflows. Adding it cleanly
would require a stable Rust-to-Go/WASI boundary and differential compatibility
tests, not an implicit Node/Python subprocess.

The current engine sources are the
[QuickJS-ng WASI reactor](https://github.com/quickjs-ng/quickjs/blob/v0.15.1/qjs-wasi-reactor.c)
and [wazero](https://github.com/tetratelabs/wazero). Both are embedded Go
dependencies; no runtime download occurs during evaluation.
