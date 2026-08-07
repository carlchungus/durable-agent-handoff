# Testing

Tests are organized around externally observable Supervisor contracts.

## Black-box E2E

Run the process-level suite separately:

```bash
go test -tags=e2e -count=1 ./tests/e2e -v
```

The suite builds and spawns the real `handoff` binary for every operator step.
It uses public stdin/stdout, JSON projections, and persisted state only. The
runtime process is real; only the model-provider executable is replaced with a
fixture that emits the documented Codex stream protocol. This keeps the test
deterministic and free of model cost while exercising the Supervisor,
scheduler, process containment, Driver decoder, and projections together.

Every manual recovery or long-running supervision bug should leave a
deterministic regression here when it can be reproduced through public
commands. Failures must include the command, stdout, and stderr. Use bounded
polling on a public predicate instead of fixed sleeps.

## Unit and invariant tests

```bash
go test ./...
go test -race ./...
go vet ./...
```

Package tests remain the right layer for exhaustive reducer, transaction,
crash-point, fence, budget, and merge-gate invariants. Use E2E when the behavior
must remain true even if all internal packages are rewritten.
