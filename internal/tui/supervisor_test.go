package tui

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/carlchungus/durable-agent-handoff/internal/supervisor"
)

func TestSnapshotUsesSupervisorProjection(t *testing.T) {
	stateRoot, worktree := t.TempDir(), t.TempDir()
	if err := os.Chmod(stateRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := supervisor.Open(stateRoot, supervisor.Options{})
	if err != nil {
		t.Fatal(err)
	}
	execution, _, err := store.StartExecution(context.Background(), supervisor.StartExecutionInput{NativeSession: supervisor.NativeSessionIdentity{Runtime: "codex", ID: "exact"}, Prompt: "secret", Runtime: supervisor.RuntimeSpec{Name: "codex", Sandbox: supervisor.SandboxReadOnly}, Root: worktree, Authority: supervisor.AuthoritySpec{RequestedBy: "human", HumanAuthorized: true, Sandbox: supervisor.SandboxReadOnly}, Budget: supervisor.DefaultBudget(), IdempotencyKey: "tui-snapshot-01"})
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := Snapshot(store)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rendered, string(execution.ID)) || !strings.Contains(rendered, "status=queued") || strings.Contains(rendered, "secret") {
		t.Fatalf("rendered=%s", rendered)
	}
}
