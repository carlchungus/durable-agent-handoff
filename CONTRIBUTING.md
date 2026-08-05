# Contributing

Open an issue before large changes. Small fixes can go directly to a pull request.

Before submitting:

```sh
gofmt -w .
go vet ./...
go test -race ./...
```

Adapter changes must include command-construction tests and document sandbox, credential, event-stream, final-result, and exact-resume behavior. Workflow mutations need reducer tests for both acceptance and atomic rejection. Safety checks should be deterministic and should not be replaced by model judgment.

Please keep dependencies small and portable. Preserve JSON compatibility within the `0.x` line unless the pull request explicitly proposes a protocol change.
