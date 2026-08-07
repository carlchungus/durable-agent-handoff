package executor

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

type startupHungDriver struct{ testDriver }

func (startupHungDriver) Build(request driver.LaunchRequest) (driver.Launch, error) {
	return driver.Launch{Executable: os.Args[0], Args: []string{"-test.run=TestExecutorStartupHelperProcess", "--", request.Session.ID}, PromptOnStdin: true}, nil
}

func (startupHungDriver) NewDecoder() driver.Decoder { return emptyDecoder{} }

type emptyDecoder struct{}

func (emptyDecoder) DecodeLine([]byte) ([]supervisor.Milestone, error) { return nil, nil }

func TestExecutorHelperProcess(t *testing.T) {
	if !strings.Contains(strings.Join(os.Args, " "), "-test.run=TestExecutorHelperProcess") {
		return
	}
	_, _ = io.ReadAll(os.Stdin)
	_, _ = fmt.Fprintln(os.Stdout, `{}`)
	os.Exit(0)
}

func TestExecutorStartupHelperProcess(t *testing.T) {
	if !strings.Contains(strings.Join(os.Args, " "), "-test.run=TestExecutorStartupHelperProcess") {
		return
	}
	_, _ = io.ReadAll(os.Stdin)
	time.Sleep(5 * time.Second)
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

func TestExecutorFencesPreTurnStartupDeadlineWithoutBurningTaskBudget(t *testing.T) {
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
	execution, _, err := store.StartExecution(context.Background(), supervisor.StartExecutionInput{NativeSession: supervisor.NativeSessionIdentity{Runtime: "test", ID: "exact-session"}, Prompt: "startup hangs", Runtime: supervisor.RuntimeSpec{Name: "test", Sandbox: supervisor.SandboxWorkspaceWrite}, Root: worktree, Authority: supervisor.AuthoritySpec{RequestedBy: "human", HumanAuthorized: true, Sandbox: supervisor.SandboxWorkspaceWrite}, Budget: supervisor.Budget{MaxTaskAttempts: 1, MaxLaunches: 2}, IdempotencyKey: "executor-startup-deadline"})
	if err != nil {
		t.Fatal(err)
	}
	runner := &Executor{Store: store, OutputRoot: outputRoot, Drivers: func(string) (driver.Driver, error) { return startupHungDriver{}, nil }, StartupDeadline: 100 * time.Millisecond}
	if err = runner.RunActivity(context.Background(), execution.FirstActivity); err == nil {
		t.Fatal("hung pre-turn process unexpectedly completed")
	}
	state, err := store.Projection()
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Attempts) != 1 || len(state.Results) != 0 {
		t.Fatalf("startup timeout produced wrong durable state: attempts=%d results=%d", len(state.Attempts), len(state.Results))
	}
	for _, attempt := range state.Attempts {
		turns := 0
		failed, exited := false, false
		for _, milestone := range attempt.Milestones {
			turns += boolInt(milestone.Kind == supervisor.MilestoneTurnStarted)
			failed = failed || milestone.Kind == supervisor.MilestoneAdapterStartFailed
			exited = exited || milestone.Kind == supervisor.MilestoneExit
		}
		if turns != 0 || !failed || !exited {
			t.Fatalf("startup deadline milestones=%+v", attempt.Milestones)
		}
		lease := state.Leases[attempt.LeaseID]
		if lease == nil || lease.ReleasedAt.IsZero() {
			t.Fatal("startup deadline left writer lease held after exact exit")
		}
	}
	controls := 0
	for _, control := range state.Controls {
		if control.Kind == "stop" && control.AppliedAt.IsZero() {
			t.Fatalf("startup deadline control was not durably applied: %+v", control)
		}
		if control.Kind == "stop" {
			controls++
		}
	}
	if controls != 1 {
		t.Fatalf("startup deadline recorded %d stop controls, want one", controls)
	}
}

func TestSelectRuntimeUsesTypedProviderFallbacksWithoutWideningAuthority(t *testing.T) {
	primary := supervisor.RuntimeSpec{Name: "claude", Model: "sonnet", Sandbox: supervisor.SandboxWorkspaceWrite}
	fallback := supervisor.RuntimeSpec{Name: "codex", Model: "gpt", Sandbox: supervisor.SandboxWorkspaceWrite}
	work := supervisor.WorkSpec{Runtime: primary, Fallbacks: []supervisor.RuntimeSpec{fallback}}
	state := &supervisor.State{Attempts: map[supervisor.AttemptID]*supervisor.Attempt{
		"provider-failed": {ID: "provider-failed", ActivityID: "activity", Runtime: primary, Milestones: []supervisor.Milestone{{Kind: supervisor.MilestoneProviderUnavailable}}},
	}}
	selected, err := selectRuntime(work, state, "activity")
	if err != nil || selected != fallback {
		t.Fatalf("fallback was not selected from typed provider evidence: selected=%+v err=%v", selected, err)
	}
	work.Fallbacks[0].Sandbox = supervisor.SandboxReadOnly
	selected, err = selectRuntime(work, state, "activity")
	if err != nil || selected.Sandbox != supervisor.SandboxReadOnly {
		t.Fatalf("narrow fallback was not preserved: selected=%+v err=%v", selected, err)
	}
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
