package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/driver"
	"github.com/carlchungus/durable-agent-handoff/internal/evaluator"
	"github.com/carlchungus/durable-agent-handoff/supervisor"
)

type evaluatorFunc func(context.Context, evaluator.Request) (supervisor.TurnDecision, error)

func (f evaluatorFunc) Evaluate(ctx context.Context, request evaluator.Request) (supervisor.TurnDecision, error) {
	return f(ctx, request)
}

func TestInstallV2UnitContainsNoPromptOrEnvironmentValues(t *testing.T) {
	home := t.TempDir()
	path, err := installV2For("linux", home, "/opt/handoff", "/state", "/private/env.json", driver.TrustFull)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if filepath.Base(path) != "handoff.service" || !strings.Contains(text, "Supervisor v2") || !strings.Contains(text, "--trust-mode full") || !strings.Contains(text, "--environment-json /private/env.json") || strings.Contains(text, "prompt") {
		t.Fatalf("unit=%s", text)
	}
}

func TestServeV2GracefulCancellationDrainsActiveExecutor(t *testing.T) {
	stateRoot, worktree, outputRoot := t.TempDir(), t.TempDir(), t.TempDir()
	for _, path := range []string{stateRoot, worktree, outputRoot} {
		if err := os.Chmod(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	store, err := supervisor.Open(stateRoot, supervisor.Options{})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = store.StartExecution(context.Background(), supervisor.StartExecutionInput{
		NativeSession:  supervisor.NativeSessionIdentity{Runtime: "test", ID: "serve-session"},
		Prompt:         "serve drain",
		Runtime:        supervisor.RuntimeSpec{Name: "test", Sandbox: supervisor.SandboxWorkspaceWrite},
		Root:           worktree,
		Authority:      supervisor.AuthoritySpec{RequestedBy: "human", HumanAuthorized: true, Sandbox: supervisor.SandboxWorkspaceWrite},
		Budget:         supervisor.DefaultBudget(),
		IdempotencyKey: "serve-drain-start",
	})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- ServeV2(ctx, store, ServeOptions{
			Interval:   100 * time.Millisecond,
			Workers:    1,
			OutputRoot: outputRoot,
			RunActivity: func(context.Context, supervisor.ActivityID) error {
				close(started)
				<-release
				return nil
			},
		}, nil)
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("ServeV2 did not start the queued executor")
	}
	cancel()
	select {
	case err := <-done:
		t.Fatalf("ServeV2 returned before active executor drained: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ServeV2 did not wait for active executor completion")
	}
}

func TestServeV2DecidesTurnBeforeSchedulingContinuation(t *testing.T) {
	stateRoot, worktree, outputRoot := t.TempDir(), t.TempDir(), t.TempDir()
	for _, path := range []string{stateRoot, worktree, outputRoot} {
		if err := os.Chmod(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	store, err := supervisor.Open(stateRoot, supervisor.Options{})
	if err != nil {
		t.Fatal(err)
	}
	execution, _, err := store.StartExecution(context.Background(), supervisor.StartExecutionInput{
		NativeSession:  supervisor.NativeSessionIdentity{Runtime: "test", ID: "exact-campaign-session"},
		Goal:           "Ship 100 safe changes; skip unsuitable candidates",
		Prompt:         "Find and ship the next safe change",
		Runtime:        supervisor.RuntimeSpec{Name: "test", Sandbox: supervisor.SandboxWorkspaceWrite},
		Root:           worktree,
		Authority:      supervisor.AuthoritySpec{RequestedBy: "human", HumanAuthorized: true, Sandbox: supervisor.SandboxWorkspaceWrite},
		Finalizer:      supervisor.FinalizerSpec{Enabled: true, RequiredChecks: []string{"approve"}},
		Budget:         supervisor.Budget{MaxTaskAttempts: 10, MaxLaunches: 20},
		EvaluatorModel: "fake/evaluator",
		MaxTurns:       100,
		IdempotencyKey: "serve-decision-start",
	})
	if err != nil {
		t.Fatal(err)
	}
	state, _ := store.Projection()
	activity := state.Activities[execution.FirstActivity]
	attempt, _, err := store.PrepareAttempt(context.Background(), supervisor.PrepareAttemptInput{ActivityID: activity.ID, ExpectedGeneration: activity.Generation, CommandDigest: "test-command", Outputs: supervisor.OutputIdentity{Stdout: "test-out", Stderr: "test-err"}, IdempotencyKey: "serve-decision-prepare"})
	if err != nil {
		t.Fatal(err)
	}
	record := func(key string, value supervisor.Milestone) {
		t.Helper()
		if _, recordErr := store.RecordMilestone(context.Background(), supervisor.RecordMilestoneInput{ActivityID: activity.ID, ExpectedGeneration: activity.Generation, AttemptID: attempt.ID, LeaseID: attempt.LeaseID, Milestone: value, IdempotencyKey: key}); recordErr != nil {
			t.Fatal(recordErr)
		}
	}
	record("serve-decision-spawn", supervisor.Milestone{Kind: supervisor.MilestoneProcessSpawned, Process: &supervisor.ProcessIdentity{PID: 901, StartToken: "test-process"}})
	record("serve-decision-turn", supervisor.Milestone{Kind: supervisor.MilestoneTurnStarted})
	record("serve-decision-result", supervisor.Milestone{Kind: supervisor.MilestoneResult, Result: &supervisor.WorkerResult{Status: "needs_human", Summary: "This candidate is unsafe"}})
	record("serve-decision-exit", supervisor.Milestone{Kind: supervisor.MilestoneExit, Exit: &supervisor.Exit{Code: 0}})

	continuationStarted := make(chan supervisor.ActivityID, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- ServeV2(ctx, store, ServeOptions{
			Interval:   100 * time.Millisecond,
			Workers:    1,
			OutputRoot: outputRoot,
			Evaluator: evaluatorFunc(func(_ context.Context, request evaluator.Request) (supervisor.TurnDecision, error) {
				if request.Goal == "" || request.Prompt == "" || request.Turn.Summary != "This candidate is unsafe" || !strings.Contains(request.SupervisorContext, "does not push branches") {
					return supervisor.TurnDecision{}, errors.New("evaluator request lost durable context")
				}
				return supervisor.TurnDecision{Outcome: "continue", Reason: "The local candidate is unsuitable; select another safe candidate.", Model: "fake/evaluator"}, nil
			}),
			RunActivity: func(_ context.Context, id supervisor.ActivityID) error {
				continuationStarted <- id
				return nil
			},
		}, nil)
	}()
	var continuationID supervisor.ActivityID
	select {
	case continuationID = <-continuationStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("evaluator did not schedule an exact-session continuation")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	state, _ = store.Projection()
	continuation := state.Activities[continuationID]
	if continuation == nil || continuation.ParentActivityID != activity.ID || continuation.SessionID != activity.SessionID {
		t.Fatalf("durable decided continuation missing: continuation=%+v", continuation)
	}
}

func TestServeV2BacksOffTransientEvaluatorFailures(t *testing.T) {
	store, outputRoot := pendingGoalTurn(t, "fake/evaluator")
	calls := make(chan struct{}, 10)
	unexpectedActivities := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- ServeV2(ctx, store, ServeOptions{
			Interval:           100 * time.Millisecond,
			Workers:            1,
			OutputRoot:         outputRoot,
			DecisionRetryDelay: time.Second,
			Evaluator: evaluatorFunc(func(context.Context, evaluator.Request) (supervisor.TurnDecision, error) {
				calls <- struct{}{}
				return supervisor.TurnDecision{}, errors.New("transient provider failure")
			}),
			RunActivity: func(context.Context, supervisor.ActivityID) error {
				select {
				case unexpectedActivities <- struct{}{}:
				default:
				}
				return nil
			},
		}, nil)
	}()
	select {
	case <-calls:
	case <-time.After(2 * time.Second):
		t.Fatal("evaluator was never called")
	}
	select {
	case <-calls:
		t.Fatal("transient evaluator failure was retried before its backoff elapsed")
	case <-time.After(350 * time.Millisecond):
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	state, err := store.Projection()
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Results) != 0 {
		t.Fatalf("failed decision created a result: results=%d", len(state.Results))
	}
	select {
	case <-unexpectedActivities:
		t.Fatal("a pending turn scheduled worker continuation")
	default:
	}
}

func TestServeV2BacksOffRuntimeFailures(t *testing.T) {
	stateRoot, worktree, outputRoot := t.TempDir(), t.TempDir(), t.TempDir()
	for _, path := range []string{stateRoot, worktree, outputRoot} {
		if err := os.Chmod(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	store, err := supervisor.Open(stateRoot, supervisor.Options{})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = store.StartExecution(context.Background(), supervisor.StartExecutionInput{
		NativeSession: supervisor.NativeSessionIdentity{Runtime: "test", ID: "runtime-retry-session"},
		Prompt:        "retry safely", Runtime: supervisor.RuntimeSpec{Name: "test", Sandbox: supervisor.SandboxWorkspaceWrite},
		Root: worktree, Authority: supervisor.AuthoritySpec{RequestedBy: "human", HumanAuthorized: true, Sandbox: supervisor.SandboxWorkspaceWrite},
		Budget: supervisor.DefaultBudget(), IdempotencyKey: "runtime-retry-start",
	})
	if err != nil {
		t.Fatal(err)
	}
	calls := make(chan struct{}, 10)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- ServeV2(ctx, store, ServeOptions{
			Interval: 100 * time.Millisecond, Workers: 1, OutputRoot: outputRoot, ActivityRetryDelay: time.Second,
			RunActivity: func(context.Context, supervisor.ActivityID) error {
				calls <- struct{}{}
				return errors.New("persistent runtime failure")
			},
		}, nil)
	}()
	select {
	case <-calls:
	case <-time.After(2 * time.Second):
		t.Fatal("runtime was never called")
	}
	select {
	case <-calls:
		t.Fatal("runtime failure was retried before its backoff elapsed")
	case <-time.After(350 * time.Millisecond):
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func pendingGoalTurn(t *testing.T, model string) (*supervisor.Store, string) {
	t.Helper()
	stateRoot, worktree, outputRoot := t.TempDir(), t.TempDir(), t.TempDir()
	for _, path := range []string{stateRoot, worktree, outputRoot} {
		if err := os.Chmod(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	store, err := supervisor.Open(stateRoot, supervisor.Options{})
	if err != nil {
		t.Fatal(err)
	}
	execution, _, err := store.StartExecution(context.Background(), supervisor.StartExecutionInput{
		NativeSession:  supervisor.NativeSessionIdentity{Runtime: "test", ID: "retry-session"},
		Goal:           "Continue an open-ended campaign",
		Prompt:         "Ship the next safe change",
		Runtime:        supervisor.RuntimeSpec{Name: "test", Sandbox: supervisor.SandboxWorkspaceWrite},
		Root:           worktree,
		Authority:      supervisor.AuthoritySpec{RequestedBy: "human", HumanAuthorized: true, Sandbox: supervisor.SandboxWorkspaceWrite},
		Budget:         supervisor.Budget{MaxTaskAttempts: 10, MaxLaunches: 20},
		EvaluatorModel: model,
		MaxTurns:       100,
		IdempotencyKey: "retry-start",
	})
	if err != nil {
		t.Fatal(err)
	}
	state, err := store.Projection()
	if err != nil {
		t.Fatal(err)
	}
	activity := state.Activities[execution.FirstActivity]
	attempt, _, err := store.PrepareAttempt(context.Background(), supervisor.PrepareAttemptInput{ActivityID: activity.ID, ExpectedGeneration: activity.Generation, CommandDigest: "retry-command", Outputs: supervisor.OutputIdentity{Stdout: "retry-out", Stderr: "retry-err"}, IdempotencyKey: "retry-prepare"})
	if err != nil {
		t.Fatal(err)
	}
	milestones := []supervisor.Milestone{
		{Kind: supervisor.MilestoneProcessSpawned, Process: &supervisor.ProcessIdentity{PID: 902, StartToken: "retry-process"}},
		{Kind: supervisor.MilestoneTurnStarted},
		{Kind: supervisor.MilestoneResult, Result: &supervisor.WorkerResult{Status: "completed", Summary: "I will inspect the next candidate."}},
		{Kind: supervisor.MilestoneExit, Exit: &supervisor.Exit{Code: 0}},
	}
	for index, milestone := range milestones {
		_, err := store.RecordMilestone(context.Background(), supervisor.RecordMilestoneInput{ActivityID: activity.ID, ExpectedGeneration: activity.Generation, AttemptID: attempt.ID, LeaseID: attempt.LeaseID, Milestone: milestone, IdempotencyKey: "retry-milestone-" + string(rune('0'+index))})
		if err != nil {
			t.Fatal(err)
		}
	}
	return store, outputRoot
}
