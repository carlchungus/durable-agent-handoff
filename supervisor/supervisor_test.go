package supervisor_test

import (
	"context"
	"os"
	"testing"

	"github.com/carlchungus/durable-agent-handoff/supervisor"
)

func TestPublicStartExecutionPromotionSeam(t *testing.T) {
	stateRoot, worktree := t.TempDir(), t.TempDir()
	if err := os.Chmod(stateRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := supervisor.Open(stateRoot, supervisor.Options{})
	if err != nil {
		t.Fatal(err)
	}
	execution, receipt, err := store.StartExecution(context.Background(), supervisor.StartExecutionInput{
		NativeSession: supervisor.NativeSessionIdentity{Runtime: "codex", ID: "external-thread-id"},
		Prompt:        "promote", Runtime: supervisor.RuntimeSpec{Name: "codex", Sandbox: supervisor.SandboxReadOnly}, Root: worktree,
		Authority: supervisor.AuthoritySpec{RequestedBy: "human:external", HumanAuthorized: true, Sandbox: supervisor.SandboxReadOnly},
		Budget:    supervisor.DefaultBudget(), IdempotencyKey: "external-start-01",
	})
	if err != nil || execution == nil || receipt.Existing {
		t.Fatalf("execution=%+v receipt=%+v err=%v", execution, receipt, err)
	}
}
