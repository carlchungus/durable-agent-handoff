package executor

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/carlchungus/durable-agent-handoff/internal/driver"
	"github.com/carlchungus/durable-agent-handoff/internal/supervisor"
)

type testDriver struct{}

func (testDriver) Name() string { return "test" }
func (testDriver) Build(request driver.LaunchRequest) (driver.Launch, error) {
	return driver.Launch{Executable: os.Args[0], Args: []string{"-test.run=TestExecutorHelperProcess", "--", request.Session.ID}, PromptOnStdin: true}, nil
}
func (testDriver) NewDecoder() driver.Decoder { return testDecoder{} }
func (testDriver) Spawned(process supervisor.ProcessIdentity) supervisor.Milestone {
	return supervisor.Milestone{Kind: supervisor.MilestoneProcessSpawned, Process: &process}
}
func (testDriver) StartFailed(err error) supervisor.Milestone {
	return supervisor.Milestone{Kind: supervisor.MilestoneAdapterStartFailed, Failure: err.Error()}
}
func (testDriver) Exited(code int, err error) supervisor.Milestone {
	exit := &supervisor.Exit{Code: code}
	if err != nil {
		exit.Error = err.Error()
	}
	return supervisor.Milestone{Kind: supervisor.MilestoneExit, Exit: exit}
}

type testDecoder struct{}

func (testDecoder) DecodeLine([]byte) ([]supervisor.Milestone, error) {
	return []supervisor.Milestone{{Kind: supervisor.MilestoneTurnStarted}, {Kind: supervisor.MilestoneResult, Result: &supervisor.WorkerResult{Status: "completed", Summary: "helper completed"}}}, nil
}

func TestExecutorHelperProcess(t *testing.T) {
	if !strings.Contains(strings.Join(os.Args, " "), "-test.run=TestExecutorHelperProcess") {
		return
	}
	_, _ = io.ReadAll(os.Stdin)
	_, _ = fmt.Fprintln(os.Stdout, `{}`)
	os.Exit(0)
}

func TestExecutorUsesTypedSupervisorAttemptAndStdinPrompt(t *testing.T) {
	stateRoot, worktree, outputRoot := t.TempDir(), t.TempDir(), t.TempDir()
	if err := os.Chmod(stateRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(outputRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := supervisor.Open(stateRoot, supervisor.Options{})
	if err != nil {
		t.Fatal(err)
	}
	execution, _, err := store.StartExecution(context.Background(), supervisor.StartExecutionInput{NativeSession: supervisor.NativeSessionIdentity{Runtime: "test", ID: "exact-session"}, Prompt: "secret prompt", Runtime: supervisor.RuntimeSpec{Name: "test", Sandbox: supervisor.SandboxWorkspaceWrite}, Root: worktree, Authority: supervisor.AuthoritySpec{RequestedBy: "human", HumanAuthorized: true, Sandbox: supervisor.SandboxWorkspaceWrite}, Budget: supervisor.DefaultBudget(), IdempotencyKey: "executor-start-01"})
	if err != nil {
		t.Fatal(err)
	}
	runner := &Executor{Store: store, OutputRoot: outputRoot, Drivers: func(string) (driver.Driver, error) { return testDriver{}, nil }}
	if err = runner.RunActivity(context.Background(), execution.FirstActivity); err != nil {
		t.Fatal(err)
	}
	state, err := store.Projection()
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Results) != 1 || len(state.Attempts) != 1 {
		t.Fatalf("results=%d attempts=%d", len(state.Results), len(state.Attempts))
	}
	for _, entry := range state.Attempts {
		if entry.CommandDigest == "" || entry.Process == nil {
			t.Fatalf("attempt=%+v", entry)
		}
	}
	entries, err := filepath.Glob(filepath.Join(outputRoot, "*.stdout.jsonl"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("stdout files=%v err=%v", entries, err)
	}
	raw, err := os.ReadFile(entries[0])
	if err != nil || strings.Contains(string(raw), "secret prompt") {
		t.Fatalf("stdout leaked prompt=%q err=%v", raw, err)
	}
}
