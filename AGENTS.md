# Agent guide

Read `docs/architecture.md` and `docs/protocol.md` before changing workflow semantics.

- Keep the event ledger backward readable.
- Validate proposals against a clone before mutating live state.
- Never grant runtime adapters GitHub merge authority.
- Preserve exact runtime session IDs; never resume a global last session.
- Use argv execution, not a shell, for privileged Git/GitHub paths.
- Add adversarial tests for rejected mutations, path boundaries, budgets, and merge gates.
- Run `go test -race ./...` and `go vet ./...` before handing off.
