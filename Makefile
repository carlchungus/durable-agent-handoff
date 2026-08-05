.PHONY: build test check

build:
	go build -trimpath -o handoff ./cmd/handoff

test:
	go test -race ./...

check:
	test -z "$$(gofmt -l .)"
	go vet ./...
	go test -race ./...
