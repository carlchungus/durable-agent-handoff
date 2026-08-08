package executor

import (
	"context"
	"encoding/json"
	"errors"
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

func TestResultSchemaRequiresEveryDeclaredProperty(t *testing.T) {
	var schema struct {
		Required   []string                   `json:"required"`
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal([]byte(resultSchema), &schema); err != nil {
		t.Fatal(err)
	}
	required := make(map[string]bool, len(schema.Required))
	for _, name := range schema.Required {
		required[name] = true
	}
	for name := range schema.Properties {
		if !required[name] {
			t.Fatalf("strict response schema property %q is not required", name)
		}
	}
}

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

type buildFailureDriver struct{}

func (buildFailureDriver) Name() string { return "broken" }
func (buildFailureDriver) Build(driver.LaunchRequest) (driver.Launch, error) {
	return driver.Launch{}, fmt.Errorf("provider executable is unavailable")
}
func (buildFailureDriver) NewDecoder() driver.Decoder { return emptyDecoder{} }
func (buildFailureDriver) Spawned(process supervisor.ProcessIdentity) supervisor.Milestone {
	return supervisor.Milestone{Kind: supervisor.MilestoneProcessSpawned, Process: &process}
}
func (buildFailureDriver) StartFailed(err error) supervisor.Milestone {
	return supervisor.Milestone{Kind: supervisor.MilestoneAdapterStartFailed, Failure: err.Error()}
}
func (buildFailureDriver) Exited(code int, err error) supervisor.Milestone {
	exit := &supervisor.Exit{Code: code}
	if err != nil {
		exit.Error = err.Error()
	}
	return supervisor.Milestone{Kind: supervisor.MilestoneExit, Exit: exit}
}

type fallbackDriver struct{ runtime string }

func (d fallbackDriver) Name() string { return d.runtime }
func (d fallbackDriver) Build(request driver.LaunchRequest) (driver.Launch, error) {
	return driver.Launch{Executable: os.Args[0], Args: []string{"-test.run=TestExecutorFallbackHelperProcess", "--", d.runtime}, PromptOnStdin: true}, nil
}
func (d fallbackDriver) NewDecoder() driver.Decoder { return fallbackDecoder{runtime: d.runtime} }
func (fallbackDriver) Spawned(process supervisor.ProcessIdentity) supervisor.Milestone {
	return supervisor.Milestone{Kind: supervisor.MilestoneProcessSpawned, Process: &process}
}
func (fallbackDriver) StartFailed(err error) supervisor.Milestone {
	return supervisor.Milestone{Kind: supervisor.MilestoneAdapterStartFailed, Failure: err.Error()}
}
func (fallbackDriver) Exited(code int, err error) supervisor.Milestone {
	exit := &supervisor.Exit{Code: code}
	if err != nil {
		exit.Error = err.Error()
	}
	return supervisor.Milestone{Kind: supervisor.MilestoneExit, Exit: exit}
}

type fallbackDecoder struct{ runtime string }

func (d fallbackDecoder) DecodeLine(raw []byte) ([]supervisor.Milestone, error) {
	var event struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(raw, &event); err != nil {
		return nil, err
	}
	if event.Kind == "provider" {
		return []supervisor.Milestone{{Kind: supervisor.MilestoneProviderUnavailable, Failure: "provider unavailable"}}, nil
	}
	identity := supervisor.NativeSessionIdentity{Runtime: d.runtime, ID: d.runtime + "-native"}
	return []supervisor.Milestone{
		{Kind: supervisor.MilestoneSessionBound, Session: &identity},
		{Kind: supervisor.MilestoneTurnStarted},
		{Kind: supervisor.MilestoneResult, Result: &supervisor.WorkerResult{Status: "completed", Summary: "fallback completed"}},
	}, nil
}

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

func TestExecutorFallbackHelperProcess(t *testing.T) {
	if !strings.Contains(strings.Join(os.Args, " "), "-test.run=TestExecutorFallbackHelperProcess") {
		return
	}
	_, _ = io.ReadAll(os.Stdin)
	runtimeName := "fallback"
	for index, arg := range os.Args {
		if arg == "--" && index+1 < len(os.Args) {
			runtimeName = os.Args[index+1]
			break
		}
	}
	if runtimeName == "primary" {
		_, _ = fmt.Fprintln(os.Stdout, `{"kind":"provider"}`)
	} else {
		_, _ = fmt.Fprintln(os.Stdout, `{"kind":"success"}`)
	}
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

func TestExecutorBuildFailureJournalsPrelaunchExitAndReleasesLeaseForRetry(t *testing.T) {
	stateRoot, worktree, outputRoot := t.TempDir(), t.TempDir(), t.TempDir()
	for _, path := range []string{stateRoot, outputRoot} {
		if err := os.Chmod(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	store, err := supervisor.Open(stateRoot, supervisor.Options{})
	if err != nil {
		t.Fatal(err)
	}
	execution, _, err := store.StartExecution(context.Background(), supervisor.StartExecutionInput{NativeSession: supervisor.NativeSessionIdentity{Runtime: "broken"}, Prompt: "build fails", Runtime: supervisor.RuntimeSpec{Name: "broken", Sandbox: supervisor.SandboxWorkspaceWrite}, Root: worktree, Authority: supervisor.AuthoritySpec{RequestedBy: "human", HumanAuthorized: true, Sandbox: supervisor.SandboxWorkspaceWrite}, Budget: supervisor.DefaultBudget(), IdempotencyKey: "executor-build-failure"})
	if err != nil {
		t.Fatal(err)
	}
	runner := &Executor{Store: store, OutputRoot: outputRoot, Drivers: func(string) (driver.Driver, error) { return buildFailureDriver{}, nil }}
	if err = runner.RunActivity(context.Background(), execution.FirstActivity); err == nil {
		t.Fatal("Build failure unexpectedly completed")
	}
	state, err := store.Projection()
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Attempts) != 1 {
		t.Fatalf("Build failure was not represented as an Attempt: %d", len(state.Attempts))
	}
	for _, attempt := range state.Attempts {
		if !hasAttemptMilestone(attempt, supervisor.MilestoneAdapterStartFailed) || !hasAttemptMilestone(attempt, supervisor.MilestoneExit) {
			t.Fatalf("prelaunch evidence=%+v", attempt.Milestones)
		}
		if lease := state.Leases[attempt.LeaseID]; lease == nil || lease.ReleasedAt.IsZero() {
			t.Fatal("prelaunch failure retained canonical writer lease")
		}
	}
	if err = runner.RunActivity(context.Background(), execution.FirstActivity); err == nil {
		t.Fatal("retry unexpectedly completed")
	}
	state, _ = store.Projection()
	if len(state.Attempts) != 2 {
		t.Fatalf("retry could not acquire the released writer lease: attempts=%d", len(state.Attempts))
	}
}

func TestExecutorPrepareCommandFailureJournalsPrelaunchExit(t *testing.T) {
	stateRoot, worktree, outputRoot := t.TempDir(), t.TempDir(), t.TempDir()
	for _, path := range []string{stateRoot, outputRoot} {
		if err := os.Chmod(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	store, err := supervisor.Open(stateRoot, supervisor.Options{})
	if err != nil {
		t.Fatal(err)
	}
	execution, _, err := store.StartExecution(context.Background(), supervisor.StartExecutionInput{NativeSession: supervisor.NativeSessionIdentity{Runtime: "test", ID: "prepare-fail"}, Prompt: "prepare fails first", Runtime: supervisor.RuntimeSpec{Name: "test", Sandbox: supervisor.SandboxWorkspaceWrite}, Root: worktree, Authority: supervisor.AuthoritySpec{RequestedBy: "human", HumanAuthorized: true, Sandbox: supervisor.SandboxWorkspaceWrite}, Budget: supervisor.DefaultBudget(), PrepareCommand: "false", IdempotencyKey: "executor-prepare-failure"})
	if err != nil {
		t.Fatal(err)
	}
	runner := &Executor{Store: store, OutputRoot: outputRoot, Drivers: func(string) (driver.Driver, error) { return testDriver{}, nil }}
	if err = runner.RunActivity(context.Background(), execution.FirstActivity); err == nil {
		t.Fatal("prepare failure unexpectedly completed")
	}
	state, err := store.Projection()
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Attempts) != 1 {
		t.Fatalf("prepare failure was not represented as an Attempt: %d", len(state.Attempts))
	}
	for _, attempt := range state.Attempts {
		if !hasAttemptMilestone(attempt, supervisor.MilestoneAdapterStartFailed) || !hasAttemptMilestone(attempt, supervisor.MilestoneExit) {
			t.Fatalf("prepare prelaunch evidence=%+v", attempt.Milestones)
		}
	}
}

func TestExecutorPrepareCommandSuccessProceedsToDriver(t *testing.T) {
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
	execution, _, err := store.StartExecution(context.Background(), supervisor.StartExecutionInput{NativeSession: supervisor.NativeSessionIdentity{Runtime: "test", ID: "prepare-ok"}, Prompt: "prepare then run", Runtime: supervisor.RuntimeSpec{Name: "test", Sandbox: supervisor.SandboxWorkspaceWrite}, Root: worktree, Authority: supervisor.AuthoritySpec{RequestedBy: "human", HumanAuthorized: true, Sandbox: supervisor.SandboxWorkspaceWrite}, Budget: supervisor.DefaultBudget(), PrepareCommand: "true", IdempotencyKey: "executor-prepare-success"})
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
	if len(state.Results) != 1 {
		t.Fatalf("driver did not complete after successful prepare: results=%d", len(state.Results))
	}
}

func TestExecutorHonorsExternallyAppliedControlWithoutSecondStartupControl(t *testing.T) {
	stateRoot, worktree, outputRoot := t.TempDir(), t.TempDir(), t.TempDir()
	for _, path := range []string{stateRoot, outputRoot} {
		if err := os.Chmod(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	store, err := supervisor.Open(stateRoot, supervisor.Options{})
	if err != nil {
		t.Fatal(err)
	}
	execution, _, err := store.StartExecution(context.Background(), supervisor.StartExecutionInput{NativeSession: supervisor.NativeSessionIdentity{Runtime: "test"}, Prompt: "externally stopped", Runtime: supervisor.RuntimeSpec{Name: "test", Sandbox: supervisor.SandboxWorkspaceWrite}, Root: worktree, Authority: supervisor.AuthoritySpec{RequestedBy: "human", HumanAuthorized: true, Sandbox: supervisor.SandboxWorkspaceWrite}, Budget: supervisor.DefaultBudget(), IdempotencyKey: "executor-external-control"})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- (&Executor{Store: store, OutputRoot: outputRoot, Drivers: func(string) (driver.Driver, error) { return startupHungDriver{}, nil }, StartupDeadline: 5 * time.Second}).RunActivity(context.Background(), execution.FirstActivity)
	}()
	var attempt *supervisor.Attempt
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		state, projectionErr := store.Projection()
		if projectionErr == nil {
			for _, candidate := range state.Attempts {
				if hasAttemptMilestone(candidate, supervisor.MilestoneProcessSpawned) {
					attempt = candidate
					break
				}
			}
		}
		if attempt != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if attempt == nil {
		t.Fatal("executor did not spawn before external control")
	}
	control, _, err := store.RequestControl(context.Background(), supervisor.RequestControlInput{ActivityID: execution.FirstActivity, ExpectedGeneration: 1, ExpectedAttemptID: attempt.ID, Kind: "stop", Actor: "cloud", IdempotencyKey: "external-stop-request"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = store.ApplyControl(context.Background(), supervisor.ApplyControlInput{ControlID: control.ID, ActivityID: execution.FirstActivity, ExpectedGeneration: 1, AttemptID: attempt.ID, IdempotencyKey: "external-stop-applied"}); err != nil {
		if !errors.Is(err, supervisor.ErrFenced) {
			t.Fatal(err)
		}
		state, projectionErr := store.Projection()
		var applied *supervisor.Control
		if state != nil {
			applied = state.Controls[control.ID]
		}
		if projectionErr != nil || applied == nil || applied.AppliedAt.IsZero() {
			t.Fatalf("external control was fenced before the exact control was applied: err=%v projection=%v control=%+v", err, projectionErr, applied)
		}
	}
	if err = <-done; err == nil {
		t.Fatal("externally stopped process unexpectedly completed")
	}
	state, err := store.Projection()
	if err != nil {
		t.Fatal(err)
	}
	stopControls := 0
	startupControls := 0
	for _, item := range state.Controls {
		if item.Kind == "stop" {
			stopControls++
			if strings.Contains(item.Actor, "startup") {
				startupControls++
			}
		}
	}
	if stopControls != 1 || startupControls != 0 {
		t.Fatalf("external control was duplicated: stop=%d startup=%d controls=%+v", stopControls, startupControls, state.Controls)
	}
	if !hasAttemptMilestone(state.Attempts[attempt.ID], supervisor.MilestoneExit) || state.Leases[attempt.LeaseID].ReleasedAt.IsZero() {
		t.Fatal("external control did not produce terminal exit and lease release")
	}
}

func TestExecutorCrossRuntimeFallbackCreatesChildSessionLineage(t *testing.T) {
	stateRoot, worktree, outputRoot := t.TempDir(), t.TempDir(), t.TempDir()
	for _, path := range []string{stateRoot, outputRoot} {
		if err := os.Chmod(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	store, err := supervisor.Open(stateRoot, supervisor.Options{})
	if err != nil {
		t.Fatal(err)
	}
	primary := supervisor.RuntimeSpec{Name: "primary", Sandbox: supervisor.SandboxWorkspaceWrite}
	fallback := supervisor.RuntimeSpec{Name: "fallback", Sandbox: supervisor.SandboxWorkspaceWrite}
	execution, _, err := store.StartExecution(context.Background(), supervisor.StartExecutionInput{NativeSession: supervisor.NativeSessionIdentity{Runtime: primary.Name}, Prompt: "cross runtime", Runtime: primary, Fallbacks: []supervisor.RuntimeSpec{fallback}, Root: worktree, Authority: supervisor.AuthoritySpec{RequestedBy: "human", HumanAuthorized: true, Sandbox: supervisor.SandboxWorkspaceWrite}, Budget: supervisor.DefaultBudget(), IdempotencyKey: "executor-cross-runtime"})
	if err != nil {
		t.Fatal(err)
	}
	drivers := func(name string) (driver.Driver, error) {
		return fallbackDriver{runtime: name}, nil
	}
	runner := &Executor{Store: store, OutputRoot: outputRoot, Drivers: drivers}
	_ = runner.RunActivity(context.Background(), execution.FirstActivity)
	if err = runner.RunActivity(context.Background(), execution.FirstActivity); err != nil {
		t.Fatal(err)
	}
	state, err := store.Projection()
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Sessions) != 2 {
		t.Fatalf("fallback did not create a child Session: sessions=%d", len(state.Sessions))
	}
	rootSession := state.Sessions[execution.SessionID]
	if rootSession.Native.ID != "" {
		t.Fatalf("provider fallback mutated original native identity: %+v", rootSession)
	}
	var child *supervisor.Activity
	for _, candidate := range state.Activities {
		if candidate.ParentActivityID != "" {
			child = candidate
		}
	}
	if child == nil || child.SessionID == execution.SessionID || state.Sessions[child.SessionID].Native.ID != "fallback-native" || len(state.Results) != 1 {
		t.Fatalf("fallback lineage/result=%+v sessions=%+v results=%+v", child, state.Sessions, state.Results)
	}
	view, err := store.View(execution.ID, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Queue) != 0 {
		t.Fatalf("fallback parent remained scheduler-eligible: %+v", view.Queue)
	}
}

func hasAttemptMilestone(attempt *supervisor.Attempt, kind supervisor.MilestoneKind) bool {
	if attempt == nil {
		return false
	}
	for _, milestone := range attempt.Milestones {
		if milestone.Kind == kind {
			return true
		}
	}
	return false
}

func TestSelectRuntimeUsesTypedProviderFallbacksWithoutWideningAuthority(t *testing.T) {
	primary := supervisor.RuntimeSpec{Name: "claude", Model: "sonnet", Sandbox: supervisor.SandboxWorkspaceWrite}
	fallback := supervisor.RuntimeSpec{Name: "codex", Model: "gpt", Sandbox: supervisor.SandboxWorkspaceWrite}
	work := supervisor.WorkSpec{Runtime: primary, Fallbacks: []supervisor.RuntimeSpec{fallback}}
	state := &supervisor.State{Attempts: map[supervisor.AttemptID]*supervisor.Attempt{
		"provider-failed": {ID: "provider-failed", ActivityID: "activity", Runtime: primary, Milestones: []supervisor.Milestone{{Kind: supervisor.MilestoneProviderUnavailable}}},
	}}
	selected, err := selectRuntime(work, state, "activity", primary.Name)
	if err != nil || selected != fallback {
		t.Fatalf("fallback was not selected from typed provider evidence: selected=%+v err=%v", selected, err)
	}
	work.Fallbacks[0].Sandbox = supervisor.SandboxReadOnly
	selected, err = selectRuntime(work, state, "activity", primary.Name)
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
