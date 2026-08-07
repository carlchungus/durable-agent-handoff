package supervisor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/legacyimport"
	"github.com/carlchungus/durable-agent-handoff/internal/processidentity"
)

func safeDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func openTestStore(t *testing.T, options Options) (*Store, string, string) {
	t.Helper()
	stateRoot, worktree := safeDir(t), safeDir(t)
	store, err := Open(stateRoot, options)
	if err != nil {
		t.Fatal(err)
	}
	return store, stateRoot, worktree
}

func startTestExecution(t *testing.T, store *Store, worktree, key string, budget Budget) *Execution {
	t.Helper()
	execution, _, err := store.StartExecution(context.Background(), StartExecutionInput{
		NativeSession: NativeSessionIdentity{Runtime: "claude", ID: "native-session-123"},
		Prompt:        "do durable work", Runtime: RuntimeSpec{Name: "claude", Sandbox: SandboxWorkspaceWrite}, Root: worktree,
		Authority: AuthoritySpec{RequestedBy: "human:test", HumanAuthorized: true, Sandbox: SandboxWorkspaceWrite},
		Budget:    budget, IdempotencyKey: key,
	})
	if err != nil {
		t.Fatal(err)
	}
	return execution
}

func prepareTestAttempt(t *testing.T, store *Store, activityID ActivityID, generation uint64, key string) *Attempt {
	t.Helper()
	attempt, _, err := store.PrepareAttempt(context.Background(), PrepareAttemptInput{ActivityID: activityID, ExpectedGeneration: generation, CommandDigest: "sha256:command", Outputs: OutputIdentity{Stdout: key + "-stdout", Stderr: key + "-stderr"}, IdempotencyKey: key})
	if err != nil {
		t.Fatal(err)
	}
	return attempt
}

func milestone(t *testing.T, store *Store, activity *Activity, attempt *Attempt, key string, value Milestone) {
	t.Helper()
	_, err := store.RecordMilestone(context.Background(), RecordMilestoneInput{ActivityID: activity.ID, ExpectedGeneration: activity.Generation, AttemptID: attempt.ID, LeaseID: attempt.LeaseID, Milestone: value, IdempotencyKey: key})
	if err != nil {
		t.Fatal(err)
	}
}

func hasAttemptMilestone(attempt *Attempt, kind MilestoneKind) bool {
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

func completeActivity(t *testing.T, store *Store, activity *Activity, key string) *Result {
	t.Helper()
	attempt := prepareTestAttempt(t, store, activity.ID, activity.Generation, key+"-prepare")
	milestone(t, store, activity, attempt, key+"-spawned", Milestone{Kind: MilestoneProcessSpawned, Process: &ProcessIdentity{PID: 4242, StartToken: "birth-token"}})
	milestone(t, store, activity, attempt, key+"-turn", Milestone{Kind: MilestoneTurnStarted})
	milestone(t, store, activity, attempt, key+"-result", Milestone{Kind: MilestoneResult, Result: &WorkerResult{Status: "completed", Summary: "immutable result"}})
	milestone(t, store, activity, attempt, key+"-exit", Milestone{Kind: MilestoneExit, Exit: &Exit{Code: 0}})
	state, err := store.Projection()
	if err != nil {
		t.Fatal(err)
	}
	result := resultForActivity(state, activity.ID)
	if result == nil {
		t.Fatal("result was not projected")
	}
	return result
}

func TestStartExecutionIsAtomicAndIdempotent(t *testing.T) {
	store, _, worktree := openTestStore(t, Options{})
	input := StartExecutionInput{NativeSession: NativeSessionIdentity{Runtime: "codex", ID: "thread-exact-1"}, Prompt: "one", Runtime: RuntimeSpec{Name: "codex", Sandbox: SandboxReadOnly}, Root: worktree, Authority: AuthoritySpec{RequestedBy: "human:test", HumanAuthorized: true, Sandbox: SandboxReadOnly}, Budget: DefaultBudget(), IdempotencyKey: "start-key-0001"}
	first, firstReceipt, err := store.StartExecution(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	second, secondReceipt, err := store.StartExecution(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID || !secondReceipt.Existing || firstReceipt.Sequence != secondReceipt.Sequence {
		t.Fatalf("idempotency mismatch: first=%+v second=%+v receipt=%+v", first, second, secondReceipt)
	}
	entries, err := store.Events(0)
	if err != nil || len(entries) != 1 || len(entries[0].Events) != 1 {
		t.Fatalf("one command must produce one journal entry: entries=%d err=%v", len(entries), err)
	}
	input.Prompt = "divergent"
	if _, _, err = store.StartExecution(context.Background(), input); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("divergent reuse got %v", err)
	}
	input.IdempotencyKey = "start-key-0002"
	third, _, err := store.StartExecution(context.Background(), input)
	if err != nil || third.ID == first.ID {
		t.Fatalf("divergent keys silently aliased: third=%+v err=%v", third, err)
	}
}

func TestThreadStartedWithoutTurnRemainsStarting(t *testing.T) {
	store, _, worktree := openTestStore(t, Options{})
	execution := startTestExecution(t, store, worktree, "thread-hang-start", DefaultBudget())
	state, err := store.Projection()
	activity := state.Activities[execution.FirstActivity]
	attempt := prepareTestAttempt(t, store, activity.ID, activity.Generation, "thread-hang-prepare")
	milestone(t, store, activity, attempt, "thread-hang-spawn", Milestone{Kind: MilestoneProcessSpawned, Process: &ProcessIdentity{PID: 9, StartToken: "exact-birth"}})
	identity := NativeSessionIdentity{Runtime: "claude", ID: "native-session-123"}
	milestone(t, store, activity, attempt, "thread-hang-bind", Milestone{Kind: MilestoneSessionBound, Session: &identity, SourceType: "thread.started"})
	view, err := store.View(execution.ID, time.Unix(1000, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Attempts) != 1 || view.Attempts[0].Health != HealthStarting || view.Attempts[0].TurnStarted || view.Attempts[0].TaskAttempt != 0 {
		t.Fatalf("pre-turn process was reported healthy/running: %+v", view.Attempts)
	}
	rendered := RenderText(view)
	if !strings.Contains(rendered, "health=starting") || strings.Contains(rendered, "output") || strings.Contains(rendered, "health=running") {
		t.Fatalf("human view invented lifecycle/progress: %s", rendered)
	}
}

func TestAttemptViewUsesHealthAndTerminalReasonWithoutStateAlias(t *testing.T) {
	store, _, worktree := openTestStore(t, Options{})
	execution := startTestExecution(t, store, worktree, "attempt-view-lifecycle", DefaultBudget())
	state, _ := store.Projection()
	activity := state.Activities[execution.FirstActivity]
	attempt := prepareTestAttempt(t, store, activity.ID, activity.Generation, "attempt-view-failure")
	milestone(t, store, activity, attempt, "attempt-view-start-failure", Milestone{Kind: MilestoneAdapterStartFailed, Failure: "provider executable missing"})
	milestone(t, store, activity, attempt, "attempt-view-failure-exit", Milestone{Kind: MilestoneExit, Exit: &Exit{Code: 127}})
	view, err := store.View(execution.ID, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Attempts) != 1 || view.Attempts[0].Health != HealthExited || view.Attempts[0].TerminalReason != "provider executable missing" {
		t.Fatalf("failure lifecycle was overwritten by exit: %+v", view.Attempts)
	}
	raw, err := json.Marshal(view.Attempts[0])
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), `"state"`) {
		t.Fatalf("AttemptView retained the removed state alias: %s", raw)
	}

	attempt = prepareTestAttempt(t, store, activity.ID, activity.Generation, "attempt-view-result")
	milestone(t, store, activity, attempt, "attempt-view-result-spawn", Milestone{Kind: MilestoneProcessSpawned, Process: &ProcessIdentity{PID: 88, StartToken: "attempt-view-result-process"}})
	milestone(t, store, activity, attempt, "attempt-view-result-turn", Milestone{Kind: MilestoneTurnStarted})
	milestone(t, store, activity, attempt, "attempt-view-result-value", Milestone{Kind: MilestoneResult, Result: &WorkerResult{Status: "completed", Summary: "done"}})
	milestone(t, store, activity, attempt, "attempt-view-result-exit", Milestone{Kind: MilestoneExit, Exit: &Exit{Code: 0}})
	view, err = store.View(execution.ID, time.Now().UTC())
	if err != nil || len(view.Attempts) != 2 || view.Attempts[1].ResultStatus != "completed" {
		t.Fatalf("result status was not projected from immutable Result: attempts=%+v err=%v", view.Attempts, err)
	}
}

func TestStartupReconcileDeadOrphanReleasesLeaseAndQueuesRetry(t *testing.T) {
	store, stateRoot, worktree := openTestStore(t, Options{})
	execution := startTestExecution(t, store, worktree, "dead-orphan-start", DefaultBudget())
	state, _ := store.Projection()
	activity := state.Activities[execution.FirstActivity]
	attempt := prepareTestAttempt(t, store, activity.ID, activity.Generation, "dead-orphan-attempt")
	milestone(t, store, activity, attempt, "dead-orphan-spawn", Milestone{Kind: MilestoneProcessSpawned, Process: &ProcessIdentity{PID: os.Getpid(), StartToken: "stale-process-incarnation"}})
	restarted, err := Open(stateRoot, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err = restarted.ReconcileStartup(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, err = restarted.Projection()
	if err != nil {
		t.Fatal(err)
	}
	recovered := state.Attempts[attempt.ID]
	if !hasAttemptMilestone(recovered, MilestoneExit) || state.Leases[attempt.LeaseID] == nil || state.Leases[attempt.LeaseID].ReleasedAt.IsZero() {
		t.Fatalf("dead orphan was not terminalized and released: attempt=%+v lease=%+v", recovered, state.Leases[attempt.LeaseID])
	}
	view, err := restarted.View(execution.ID, time.Now())
	if err != nil || len(view.Queue) != 1 || view.Queue[0] != activity.ID {
		t.Fatalf("dead orphan did not return immutable Activity to queue: view=%+v err=%v", view, err)
	}
	sequence := state.Sequence
	if err = restarted.ReconcileStartup(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, _ = restarted.Projection()
	if state.Sequence != sequence {
		t.Fatalf("startup reconciliation appended state after all orphans were terminal: before=%d after=%d", sequence, state.Sequence)
	}
	if _, _, err = restarted.PrepareAttempt(context.Background(), PrepareAttemptInput{ActivityID: activity.ID, ExpectedGeneration: activity.Generation, CommandDigest: "retry-after-orphan", Outputs: OutputIdentity{Stdout: "retry-stdout", Stderr: "retry-stderr"}, IdempotencyKey: "dead-orphan-retry"}); err != nil {
		t.Fatalf("released orphan lease did not permit retry: %v", err)
	}
}

func TestStartupReconcilePreparedOrphanIsTerminalizedAndRetryable(t *testing.T) {
	store, stateRoot, worktree := openTestStore(t, Options{})
	execution := startTestExecution(t, store, worktree, "prepared-orphan-start", DefaultBudget())
	state, _ := store.Projection()
	activity := state.Activities[execution.FirstActivity]
	attempt := prepareTestAttempt(t, store, activity.ID, activity.Generation, "prepared-orphan-attempt")
	restarted, err := Open(stateRoot, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err = restarted.ReconcileStartup(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, err = restarted.Projection()
	if err != nil {
		t.Fatal(err)
	}
	recovered := state.Attempts[attempt.ID]
	if len(recovered.Milestones) != 2 || !hasAttemptMilestone(recovered, MilestoneAdapterStartFailed) || !hasAttemptMilestone(recovered, MilestoneExit) || state.Leases[attempt.LeaseID].ReleasedAt.IsZero() {
		t.Fatalf("prepared orphan recovery=%+v lease=%+v", recovered, state.Leases[attempt.LeaseID])
	}
	view, err := restarted.View(execution.ID, time.Now())
	if err != nil || len(view.Queue) != 1 || view.Queue[0] != activity.ID {
		t.Fatalf("prepared orphan did not become retryable: view=%+v err=%v", view, err)
	}
}

func TestSupervisorLiveOrphanHelper(t *testing.T) {
	if os.Getenv("HANDOFF_TEST_LIVE_ORPHAN_HELPER") != "1" {
		return
	}
	ready := os.Getenv("HANDOFF_TEST_LIVE_ORPHAN_READY")
	if ready == "" {
		t.Fatal("live orphan helper readiness path is missing")
	}
	if err := os.WriteFile(ready, []byte("ready"), 0o600); err != nil {
		t.Fatal(err)
	}
	for {
		time.Sleep(time.Hour)
	}
}

func TestStartupReconcileFailsClosedForExactLiveOrphan(t *testing.T) {
	ready := filepath.Join(t.TempDir(), "live-orphan-ready")
	cmd := exec.Command(os.Args[0], "-test.run=^TestSupervisorLiveOrphanHelper$")
	cmd.Env = append(os.Environ(), "HANDOFF_TEST_LIVE_ORPHAN_HELPER=1", "HANDOFF_TEST_LIVE_ORPHAN_READY="+ready)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()
	deadline := time.Now().Add(2 * time.Second)
	token := ""
	for time.Now().Before(deadline) && token == "" {
		if _, readyErr := os.Stat(ready); readyErr == nil {
			token = processidentity.ProcessStartToken(cmd.Process.Pid)
		}
		if token == "" {
			time.Sleep(5 * time.Millisecond)
		}
	}
	if token == "" {
		t.Fatal("live orphan helper did not expose a process start token")
	}
	store, _, worktree := openTestStore(t, Options{})
	execution := startTestExecution(t, store, worktree, "live-orphan-start", DefaultBudget())
	state, err := store.Projection()
	activity := state.Activities[execution.FirstActivity]
	attempt := prepareTestAttempt(t, store, activity.ID, activity.Generation, "live-orphan-attempt")
	milestone(t, store, activity, attempt, "live-orphan-spawn", Milestone{Kind: MilestoneProcessSpawned, Process: &ProcessIdentity{PID: cmd.Process.Pid, StartToken: token}})
	state, err = store.Projection()
	if err != nil {
		t.Fatal(err)
	}
	before := state.Sequence
	if !processidentity.ProcessMatches(cmd.Process.Pid, token) {
		t.Fatal("live orphan helper lost its exact identity before reconciliation")
	}
	if err := store.ReconcileStartup(context.Background()); !errors.Is(err, ErrLiveOrphan) {
		t.Fatalf("exact live orphan did not fail closed: %v", err)
	}
	state, err = store.Projection()
	if err != nil {
		t.Fatal(err)
	}
	if state.Sequence != before || hasAttemptMilestone(state.Attempts[attempt.ID], MilestoneExit) {
		t.Fatalf("live orphan reconciliation mutated or terminalized an exact live process: before=%d after=%d attempt=%+v", before, state.Sequence, state.Attempts[attempt.ID])
	}
	view, err := store.View(execution.ID, time.Now())
	if err != nil || len(view.Queue) != 0 {
		t.Fatalf("live orphan was schedulable after fail-closed recovery: view=%+v err=%v", view, err)
	}
}

func TestFallbackChildIsOnlyQueueEntryBeforeChildPreparation(t *testing.T) {
	store, _, worktree := openTestStore(t, Options{})
	primary := RuntimeSpec{Name: "primary", Sandbox: SandboxWorkspaceWrite}
	fallback := RuntimeSpec{Name: "fallback", Sandbox: SandboxWorkspaceWrite}
	execution, _, err := store.StartExecution(context.Background(), StartExecutionInput{NativeSession: NativeSessionIdentity{Runtime: primary.Name}, Prompt: "fallback", Runtime: primary, Fallbacks: []RuntimeSpec{fallback}, Root: worktree, Authority: AuthoritySpec{RequestedBy: "human", HumanAuthorized: true, Sandbox: SandboxWorkspaceWrite}, Budget: DefaultBudget(), IdempotencyKey: "fallback-queue-start"})
	if err != nil {
		t.Fatal(err)
	}
	state, _ := store.Projection()
	parent := state.Activities[execution.FirstActivity]
	attempt := prepareTestAttempt(t, store, parent.ID, parent.Generation, "fallback-parent-attempt")
	milestone(t, store, parent, attempt, "fallback-parent-spawn", Milestone{Kind: MilestoneProcessSpawned, Process: &ProcessIdentity{PID: 42424, StartToken: "fallback-parent"}})
	milestone(t, store, parent, attempt, "fallback-parent-provider", Milestone{Kind: MilestoneProviderUnavailable, Failure: "primary unavailable"})
	milestone(t, store, parent, attempt, "fallback-parent-exit", Milestone{Kind: MilestoneExit, Exit: &Exit{Code: 1}})
	child, _, err := store.StartFallbackActivity(context.Background(), StartFallbackActivityInput{ParentActivityID: parent.ID, Runtime: fallback, IdempotencyKey: "fallback-child-create"})
	if err != nil {
		t.Fatal(err)
	}
	view, err := store.View(execution.ID, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Queue) != 1 || view.Queue[0] != child.ID {
		t.Fatalf("fallback parent and child queue overlap before child preparation: queue=%v activities=%+v", view.Queue, view.Activities)
	}
	if _, _, err = store.PrepareAttempt(context.Background(), PrepareAttemptInput{ActivityID: parent.ID, ExpectedGeneration: parent.Generation, Runtime: primary, CommandDigest: "parent-after-fallback", Outputs: OutputIdentity{Stdout: "parent-after-fallback-out", Stderr: "parent-after-fallback-err"}, IdempotencyKey: "fallback-parent-rejected"}); !errors.Is(err, ErrFenced) {
		t.Fatalf("superseded fallback parent remained launchable: %v", err)
	}
	childAttempt, _, err := store.PrepareAttempt(context.Background(), PrepareAttemptInput{ActivityID: child.ID, ExpectedGeneration: child.Generation, Runtime: fallback, CommandDigest: "fallback-child", Outputs: OutputIdentity{Stdout: "fallback-child-out", Stderr: "fallback-child-err"}, IdempotencyKey: "fallback-child-prepare"})
	if err != nil {
		t.Fatalf("fallback child could not acquire the released exact writer lease: %v", err)
	}
	state, _ = store.Projection()
	active := 0
	for _, lease := range state.Leases {
		if lease.ReleasedAt.IsZero() {
			active++
		}
	}
	if active != 1 || state.Leases[childAttempt.LeaseID].AttemptID != childAttempt.ID {
		t.Fatalf("fallback preparation did not leave one child-owned writer lease: active=%d leases=%+v", active, state.Leases)
	}
}

func TestPreTurnAdapterDeathsPreserveLaunchesWithoutTaskBudget(t *testing.T) {
	store, _, worktree := openTestStore(t, Options{})
	execution := startTestExecution(t, store, worktree, "startup-deaths-start", Budget{MaxTaskAttempts: 2, MaxLaunches: 6})
	state, _ := store.Projection()
	activity := state.Activities[execution.FirstActivity]
	for i := 0; i < 3; i++ {
		attempt := prepareTestAttempt(t, store, activity.ID, activity.Generation, fmt.Sprintf("startup-%d-prepare", i))
		milestone(t, store, activity, attempt, fmt.Sprintf("startup-%d-failed", i), Milestone{Kind: MilestoneAdapterStartFailed, Failure: "claude adapter died before turn"})
		milestone(t, store, activity, attempt, fmt.Sprintf("startup-%d-exit", i), Milestone{Kind: MilestoneExit, Exit: &Exit{Code: 127}})
	}
	attempt := prepareTestAttempt(t, store, activity.ID, activity.Generation, "startup-live-prepare")
	milestone(t, store, activity, attempt, "startup-live-spawn", Milestone{Kind: MilestoneProcessSpawned, Process: &ProcessIdentity{PID: 10, StartToken: "live-birth"}})
	milestone(t, store, activity, attempt, "startup-live-turn", Milestone{Kind: MilestoneTurnStarted})
	view, err := store.View(execution.ID, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Attempts) != 4 {
		t.Fatalf("OS launch history was lost: %+v", view.Attempts)
	}
	consumed := 0
	for _, item := range view.Attempts {
		if item.TaskAttempt > 0 {
			consumed++
		}
	}
	if consumed != 1 || view.Attempts[3].TaskAttempt != 1 {
		t.Fatalf("pre-turn deaths consumed task budget: %+v", view.Attempts)
	}
}

func TestLaunchEligibilityFencesTaskBudgetAndCanonicalWritersAcrossWorkflows(t *testing.T) {
	store, _, worktree := openTestStore(t, Options{})
	old := startTestExecution(t, store, worktree, "old-live-workflow", Budget{MaxTaskAttempts: 3, MaxLaunches: 6})
	newWorkflow := startTestExecution(t, store, worktree, "new-live-workflow", DefaultBudget())
	state, _ := store.Projection()
	oldActivity := state.Activities[old.FirstActivity]
	newActivity := state.Activities[newWorkflow.FirstActivity]

	first := prepareTestAttempt(t, store, oldActivity.ID, oldActivity.Generation, "old-live-attempt-1")
	milestone(t, store, oldActivity, first, "old-live-spawn-1", Milestone{Kind: MilestoneProcessSpawned, Process: &ProcessIdentity{PID: 9101, StartToken: "old-live-birth-1"}})
	milestone(t, store, oldActivity, first, "old-live-turn-1", Milestone{Kind: MilestoneTurnStarted})
	if _, _, err := store.PrepareAttempt(context.Background(), PrepareAttemptInput{ActivityID: newActivity.ID, ExpectedGeneration: newActivity.Generation, CommandDigest: "new-live", Outputs: OutputIdentity{Stdout: "new-live-out", Stderr: "new-live-err"}, IdempotencyKey: "new-live-attempt-1"}); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("second workflow acquired a live canonical writer: %v", err)
	}
	milestone(t, store, oldActivity, first, "old-live-exit-1", Milestone{Kind: MilestoneExit, Exit: &Exit{Code: 0}})

	for ordinal := 2; ordinal <= 3; ordinal++ {
		attempt := prepareTestAttempt(t, store, oldActivity.ID, oldActivity.Generation, fmt.Sprintf("old-live-attempt-%d", ordinal))
		milestone(t, store, oldActivity, attempt, fmt.Sprintf("old-live-spawn-%d", ordinal), Milestone{Kind: MilestoneProcessSpawned, Process: &ProcessIdentity{PID: 9100 + ordinal, StartToken: fmt.Sprintf("old-live-birth-%d", ordinal)}})
		milestone(t, store, oldActivity, attempt, fmt.Sprintf("old-live-turn-%d", ordinal), Milestone{Kind: MilestoneTurnStarted})
		milestone(t, store, oldActivity, attempt, fmt.Sprintf("old-live-exit-%d", ordinal), Milestone{Kind: MilestoneExit, Exit: &Exit{Code: 0}})
	}
	state, _ = store.Projection()
	before := len(state.Attempts)
	if _, _, err := store.PrepareAttempt(context.Background(), PrepareAttemptInput{ActivityID: oldActivity.ID, ExpectedGeneration: oldActivity.Generation, CommandDigest: "old-live-attempt-4", Outputs: OutputIdentity{Stdout: "old-live-out-4", Stderr: "old-live-err-4"}, IdempotencyKey: "old-live-attempt-4"}); err == nil {
		t.Fatal("task-attempt budget allowed a fourth turn attempt")
	}
	state, _ = store.Projection()
	if len(state.Attempts) != before {
		t.Fatalf("rejected fourth attempt mutated projection: before=%d after=%d", before, len(state.Attempts))
	}
	if _, _, err := store.PrepareAttempt(context.Background(), PrepareAttemptInput{ActivityID: newActivity.ID, ExpectedGeneration: newActivity.Generation, CommandDigest: "new-live", Outputs: OutputIdentity{Stdout: "new-live-out", Stderr: "new-live-err"}, IdempotencyKey: "new-live-attempt-2"}); err != nil {
		t.Fatalf("canonical writer was not released by exact terminal exit: %v", err)
	}
}

func TestNewSessionBindsOnceAndContinuationRequiresExactBoundIdentity(t *testing.T) {
	store, _, worktree := openTestStore(t, Options{})
	execution, _, err := store.StartExecution(context.Background(), StartExecutionInput{NativeSession: NativeSessionIdentity{Runtime: "claude"}, Prompt: "new session", Runtime: RuntimeSpec{Name: "claude", Sandbox: SandboxWorkspaceWrite}, Root: worktree, Authority: AuthoritySpec{RequestedBy: "human:test", HumanAuthorized: true, Sandbox: SandboxWorkspaceWrite}, Budget: DefaultBudget(), IdempotencyKey: "unbound-session-start"})
	if err != nil {
		t.Fatal(err)
	}
	state, _ := store.Projection()
	activity := state.Activities[execution.FirstActivity]
	if state.Sessions[execution.SessionID].Native.ID != "" {
		t.Fatal("ordinary start unexpectedly invented a native session identity")
	}
	if _, _, err := store.ContinueSession(context.Background(), ContinueSessionInput{ExecutionID: execution.ID, SessionID: execution.SessionID, PredecessorActivityID: activity.ID, From: "human", Message: "before bind", IdempotencyKey: "unbound-reply-01"}); err == nil {
		t.Fatal("continuation was allowed before native Session binding")
	}
	attempt := prepareTestAttempt(t, store, activity.ID, activity.Generation, "unbound-attempt-01")
	milestone(t, store, activity, attempt, "unbound-spawn-01", Milestone{Kind: MilestoneProcessSpawned, Process: &ProcessIdentity{PID: 8080, StartToken: "unbound-birth"}})
	identity := NativeSessionIdentity{Runtime: "claude", ID: "new-native-session"}
	milestone(t, store, activity, attempt, "unbound-bind-01", Milestone{Kind: MilestoneSessionBound, Session: &identity})
	milestone(t, store, activity, attempt, "unbound-turn-01", Milestone{Kind: MilestoneTurnStarted})
	milestone(t, store, activity, attempt, "unbound-result-01", Milestone{Kind: MilestoneResult, Result: &WorkerResult{Status: "completed", Summary: "bound"}})
	milestone(t, store, activity, attempt, "unbound-exit-01", Milestone{Kind: MilestoneExit, Exit: &Exit{Code: 0}})
	state, _ = store.Projection()
	if state.Sessions[execution.SessionID].Native != identity {
		t.Fatalf("session binding was not durably persisted: %+v", state.Sessions[execution.SessionID])
	}
	continuation, _, err := store.ContinueSession(context.Background(), ContinueSessionInput{ExecutionID: execution.ID, SessionID: execution.SessionID, PredecessorActivityID: activity.ID, From: "human", Message: "after bind", IdempotencyKey: "bound-reply-01"})
	if err != nil || continuation.ParentActivityID != activity.ID {
		t.Fatalf("bound continuation failed: activity=%+v err=%v", continuation, err)
	}
}

func TestGoalTurnIsDecidedBeforeExactSessionContinuation(t *testing.T) {
	store, _, worktree := openTestStore(t, Options{})
	execution, _, err := store.StartExecution(context.Background(), StartExecutionInput{
		NativeSession:  NativeSessionIdentity{Runtime: "claude", ID: "campaign-session"},
		Goal:           "Ship 100 safe type-hardening pull requests; skip unsuitable candidates",
		Prompt:         "Find and ship the next safe type-hardening change",
		Runtime:        RuntimeSpec{Name: "claude", Sandbox: SandboxWorkspaceWrite},
		Root:           worktree,
		Authority:      AuthoritySpec{RequestedBy: "human:test", HumanAuthorized: true, Sandbox: SandboxWorkspaceWrite},
		Budget:         Budget{MaxTaskAttempts: 20, MaxLaunches: 40},
		EvaluatorModel: "deepseek/deepseek-v4-flash-0731",
		MaxTurns:       100,
		IdempotencyKey: "goal-decision-start",
	})
	if err != nil {
		t.Fatal(err)
	}
	state, _ := store.Projection()
	activity := state.Activities[execution.FirstActivity]
	attempt := prepareTestAttempt(t, store, activity.ID, activity.Generation, "goal-decision-attempt")
	milestone(t, store, activity, attempt, "goal-decision-spawn", Milestone{Kind: MilestoneProcessSpawned, Process: &ProcessIdentity{PID: 701, StartToken: "campaign-process"}})
	milestone(t, store, activity, attempt, "goal-decision-turn", Milestone{Kind: MilestoneTurnStarted})
	milestone(t, store, activity, attempt, "goal-decision-result", Milestone{Kind: MilestoneResult, Result: &WorkerResult{Status: "needs_human", Summary: "This candidate cannot be changed safely"}})
	milestone(t, store, activity, attempt, "goal-decision-exit", Milestone{Kind: MilestoneExit, Exit: &Exit{Code: 0}})

	view, err := store.View(execution.ID, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(view.PendingTurns) != 1 || view.PendingTurns[0] != activity.ID || view.Activities[0].Status != ActivityDeciding || view.Activities[0].ResultID != "" {
		t.Fatalf("pending worker turn became terminal: %+v", view)
	}
	if _, _, err := store.PrepareAttempt(context.Background(), PrepareAttemptInput{ActivityID: activity.ID, ExpectedGeneration: activity.Generation, CommandDigest: "must-not-rerun", Outputs: OutputIdentity{Stdout: "out", Stderr: "err"}, IdempotencyKey: "goal-decision-duplicate-attempt"}); err == nil {
		t.Fatal("a pending turn was allowed to launch a second worker")
	}
	continuation, _, err := store.DecideTurn(context.Background(), DecideTurnInput{
		ActivityID: activity.ID, ExpectedGeneration: activity.Generation, AttemptID: attempt.ID,
		Decision: TurnDecision{
			Outcome: "continue",
			Reason:  "The rejected candidate is local; select another safe candidate for the open-ended campaign.",
			Model:   "deepseek/deepseek-v4-flash-0731",
		},
		IdempotencyKey: "goal-decision-continue",
	})
	if err != nil {
		t.Fatal(err)
	}
	if continuation == nil || continuation.ParentActivityID != activity.ID || continuation.SessionID != activity.SessionID || continuation.Generation != activity.Generation+1 {
		t.Fatalf("decision did not create an exact-session continuation: %+v", continuation)
	}
	state, _ = store.Projection()
	if state.Sessions[continuation.SessionID].Native.ID != "campaign-session" {
		t.Fatalf("continuation lost exact native session: %+v", state.Sessions[continuation.SessionID])
	}
	view, err = store.View(execution.ID, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(view.PendingTurns) != 0 || len(view.Queue) != 1 || view.Queue[0] != continuation.ID {
		t.Fatalf("evaluated continuation was not the only queued work: %+v", view)
	}
	if result := resultForActivity(state, activity.ID); result == nil || result.Status != "continue" {
		t.Fatalf("predecessor did not retain its immutable continuation decision: %+v", result)
	}
}

func TestOldGoalJournalReplaysWithoutRestoringRetiredState(t *testing.T) {
	state := emptyState()
	now := time.Now().UTC()
	workflow := &Workflow{ID: "workflow-old-goal", Root: "/repo", Budget: DefaultBudget(), OldGoal: &oldGoalSettings{Enabled: true, EvaluatorModel: "old/model", MaxTurns: 12}, Nodes: map[NodeID]*Node{}, CreatedAt: now}
	node := &Node{ID: "node-old-goal", WorkflowID: workflow.ID, Title: "finish", Work: WorkSpec{Kind: "agent", Prompt: "work", Root: "/repo", Runtime: RuntimeSpec{Name: "codex", Sandbox: SandboxReadOnly}}, CreatedAt: now}
	session := &Session{ID: "session-old-goal", WorkflowID: workflow.ID, Native: NativeSessionIdentity{Runtime: "codex", ID: "thread-old-goal"}, Root: "/repo", CreatedAt: now}
	activity := &Activity{ID: "activity-old-goal", WorkflowID: workflow.ID, NodeID: node.ID, SessionID: session.ID, Generation: 1, Prompt: "work", CreatedAt: now}
	execution := &Execution{ID: "execution-old-goal", WorkflowID: workflow.ID, RootNodeID: node.ID, SessionID: session.ID, FirstActivity: activity.ID, IdempotencyKey: "old-goal-key", InputDigest: "old-digest", CreatedAt: now}
	if err := applyDomainEvent(state, mustEvent(eventExecutionStarted, executionStartedEvent{Execution: execution, Workflow: workflow, Node: node, Session: session, Activity: activity})); err != nil {
		t.Fatal(err)
	}
	if err := applyDomainEvent(state, DomainEvent{Type: "claim.created", Data: json.RawMessage(`{"claim":{"id":"retired-copy"}}`)}); err != nil {
		t.Fatal(err)
	}
	if err := applyDomainEvent(state, DomainEvent{Type: "evaluation.recorded", Data: json.RawMessage(`{"evaluation":{"id":"retired-copy"}}`)}); err != nil {
		t.Fatal(err)
	}
	replayed := state.Workflows[workflow.ID]
	if replayed.EvaluatorModel != "old/model" || replayed.MaxTurns != 12 || replayed.OldGoal != nil {
		t.Fatalf("old goal settings were not normalized: %+v", replayed)
	}
}

func TestEvaluatorCanAcceptCompletedWorkMislabeledAsPublicationBlocker(t *testing.T) {
	store, _, worktree := openTestStore(t, Options{})
	execution, _, err := store.StartExecution(context.Background(), StartExecutionInput{
		NativeSession: NativeSessionIdentity{Runtime: "claude", ID: "publication-boundary-session"},
		Goal:          "Implement and verify a safe change; the finalizer owns publication",
		Prompt:        "Implement and verify the change",
		Runtime:       RuntimeSpec{Name: "claude", Sandbox: SandboxWorkspaceWrite}, Root: worktree,
		Authority: AuthoritySpec{RequestedBy: "human:test", HumanAuthorized: true, Sandbox: SandboxWorkspaceWrite}, Budget: DefaultBudget(),
		EvaluatorModel: "fake/evaluator", MaxTurns: 10, IdempotencyKey: "publication-boundary-start",
	})
	if err != nil {
		t.Fatal(err)
	}
	state, _ := store.Projection()
	activity := state.Activities[execution.FirstActivity]
	attempt := prepareTestAttempt(t, store, activity.ID, activity.Generation, "publication-boundary-attempt")
	milestone(t, store, activity, attempt, "publication-boundary-spawn", Milestone{Kind: MilestoneProcessSpawned, Process: &ProcessIdentity{PID: 704, StartToken: "publication-boundary-process"}})
	milestone(t, store, activity, attempt, "publication-boundary-turn", Milestone{Kind: MilestoneTurnStarted})
	milestone(t, store, activity, attempt, "publication-boundary-result", Milestone{Kind: MilestoneResult, Result: &WorkerResult{
		Status:  "needs_human",
		Summary: "Implemented and committed 691958899; focused tests, typechecks, format, and the full check passed. Push and PR creation were not performed because publication belongs to the Supervisor finalizer.",
	}})
	milestone(t, store, activity, attempt, "publication-boundary-exit", Milestone{Kind: MilestoneExit, Exit: &Exit{Code: 0}})
	continuation, _, err := store.DecideTurn(context.Background(), DecideTurnInput{
		ActivityID: activity.ID, ExpectedGeneration: activity.Generation, AttemptID: attempt.ID,
		Decision:       TurnDecision{Outcome: "accept", Reason: "The worker Activity is complete; the authorized deterministic finalizer owns publication.", Model: "fake/evaluator"},
		IdempotencyKey: "publication-boundary-accept",
	})
	if err != nil || continuation != nil {
		t.Fatalf("evaluator could not accept terminal worker work: continuation=%+v err=%v", continuation, err)
	}
	state, _ = store.Projection()
	result := resultForActivity(state, activity.ID)
	view, err := store.View(execution.ID, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || result.Status != "completed" || result.Decision != "accept" || len(view.PendingTurns) != 0 || view.Activities[0].Status != ActivityCompleted {
		t.Fatalf("accepted terminal work did not become a completed Result: result=%+v view=%+v", result, view)
	}
}

func TestGoalEscalationRequiresTypedWorkflowWideBlocker(t *testing.T) {
	store, _, worktree := openTestStore(t, Options{})
	execution, _, err := store.StartExecution(context.Background(), StartExecutionInput{
		NativeSession:  NativeSessionIdentity{Runtime: "claude", ID: "blocked-session"},
		Goal:           "Complete the authorized work unless the whole workflow requires a human",
		Prompt:         "Complete the work",
		Runtime:        RuntimeSpec{Name: "claude", Sandbox: SandboxWorkspaceWrite},
		Root:           worktree,
		Authority:      AuthoritySpec{RequestedBy: "human:test", HumanAuthorized: true, Sandbox: SandboxWorkspaceWrite},
		Budget:         DefaultBudget(),
		EvaluatorModel: "fake/evaluator",
		MaxTurns:       10,
		IdempotencyKey: "typed-escalation-start",
	})
	if err != nil {
		t.Fatal(err)
	}
	state, _ := store.Projection()
	activity := state.Activities[execution.FirstActivity]
	attempt := prepareTestAttempt(t, store, activity.ID, activity.Generation, "typed-escalation-attempt")
	milestone(t, store, activity, attempt, "typed-escalation-spawn", Milestone{Kind: MilestoneProcessSpawned, Process: &ProcessIdentity{PID: 702, StartToken: "blocked-process"}})
	milestone(t, store, activity, attempt, "typed-escalation-turn", Milestone{Kind: MilestoneTurnStarted})
	milestone(t, store, activity, attempt, "typed-escalation-result", Milestone{Kind: MilestoneResult, Result: &WorkerResult{Status: "needs_human", Summary: "Production credentials are unavailable"}})
	milestone(t, store, activity, attempt, "typed-escalation-exit", Milestone{Kind: MilestoneExit, Exit: &Exit{Code: 0}})
	state, _ = store.Projection()
	sequence := state.Sequence
	_, _, err = store.DecideTurn(context.Background(), DecideTurnInput{ActivityID: activity.ID, ExpectedGeneration: activity.Generation, AttemptID: attempt.ID, Decision: TurnDecision{Outcome: "escalate", Reason: "The entire workflow needs production credentials.", Model: "fake/evaluator", BlockerKind: "credential"}, IdempotencyKey: "typed-escalation-invalid"})
	if err == nil {
		t.Fatal("untyped human escalation was accepted")
	}
	state, _ = store.Projection()
	if state.Sequence != sequence || resultForActivity(state, activity.ID) != nil {
		t.Fatalf("rejected escalation partially mutated state: sequence=%d", state.Sequence)
	}
	continuation, _, err := store.DecideTurn(context.Background(), DecideTurnInput{ActivityID: activity.ID, ExpectedGeneration: activity.Generation, AttemptID: attempt.ID, Decision: TurnDecision{Outcome: "escalate", Reason: "The entire workflow needs production credentials.", Model: "fake/evaluator", BlockerKind: "credential", Question: "Which approved credential should this workflow use?"}, IdempotencyKey: "typed-escalation-valid"})
	if err != nil || continuation != nil {
		t.Fatalf("typed escalation result=%+v err=%v", continuation, err)
	}
	view, err := store.View(execution.ID, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(view.PendingTurns) != 0 || view.Activities[0].Status != ActivityNeedsHuman || view.Activities[0].BlockerKind != "credential" || view.Activities[0].Question == "" {
		t.Fatalf("typed blocker was not surfaced in the canonical view: %+v", view.Activities[0])
	}
	if rendered := RenderText(view); !strings.Contains(rendered, "blocker=credential") || !strings.Contains(rendered, "Which approved credential") {
		t.Fatalf("human view hid the escalation: %s", rendered)
	}
}

func TestGoalTurnLimitBecomesVisibleHumanEscalation(t *testing.T) {
	store, _, worktree := openTestStore(t, Options{})
	execution, _, err := store.StartExecution(context.Background(), StartExecutionInput{
		NativeSession: NativeSessionIdentity{Runtime: "claude", ID: "budget-session"}, Goal: "Keep trying safely", Prompt: "Try the next candidate",
		Runtime: RuntimeSpec{Name: "claude", Sandbox: SandboxWorkspaceWrite}, Root: worktree,
		Authority: AuthoritySpec{RequestedBy: "human:test", HumanAuthorized: true, Sandbox: SandboxWorkspaceWrite}, Budget: DefaultBudget(),
		EvaluatorModel: "fake/evaluator", MaxTurns: 1, IdempotencyKey: "turn-budget-start",
	})
	if err != nil {
		t.Fatal(err)
	}
	state, _ := store.Projection()
	activity := state.Activities[execution.FirstActivity]
	attempt := prepareTestAttempt(t, store, activity.ID, activity.Generation, "turn-budget-attempt")
	milestone(t, store, activity, attempt, "turn-budget-spawn", Milestone{Kind: MilestoneProcessSpawned, Process: &ProcessIdentity{PID: 703, StartToken: "budget-process"}})
	milestone(t, store, activity, attempt, "turn-budget-turn", Milestone{Kind: MilestoneTurnStarted})
	milestone(t, store, activity, attempt, "turn-budget-result", Milestone{Kind: MilestoneResult, Result: &WorkerResult{Status: "continue", Summary: "Try another candidate"}})
	milestone(t, store, activity, attempt, "turn-budget-exit", Milestone{Kind: MilestoneExit, Exit: &Exit{Code: 0}})
	continuation, _, err := store.DecideTurn(context.Background(), DecideTurnInput{ActivityID: activity.ID, ExpectedGeneration: activity.Generation, AttemptID: attempt.ID, Decision: TurnDecision{Outcome: "continue", Reason: "Another candidate remains available.", Model: "fake/evaluator"}, IdempotencyKey: "turn-budget-resolve"})
	if err != nil || continuation != nil {
		t.Fatalf("budget exhaustion should escalate visibly, not error or continue: continuation=%+v err=%v", continuation, err)
	}
	view, _ := store.View(execution.ID, time.Now().UTC())
	if view.Activities[0].Status != ActivityNeedsHuman || view.Activities[0].BlockerKind != "budget" || view.Activities[0].Question == "" {
		t.Fatalf("turn budget did not become a visible human escalation: %+v", view.Activities[0])
	}
}

func TestUnboundedGoalContinuesWithoutHumanEscalation(t *testing.T) {
	store, _, worktree := openTestStore(t, Options{})
	execution, _, err := store.StartExecution(context.Background(), StartExecutionInput{
		NativeSession: NativeSessionIdentity{Runtime: "claude", ID: "unbounded-session"}, Goal: "Keep supervising until explicitly complete", Prompt: "Check and continue useful work",
		Runtime: RuntimeSpec{Name: "claude", Sandbox: SandboxWorkspaceWrite}, Root: worktree,
		Authority: AuthoritySpec{RequestedBy: "human:test", HumanAuthorized: true, Sandbox: SandboxWorkspaceWrite}, Budget: DefaultBudget(),
		EvaluatorModel: "fake/evaluator", MaxTurns: 0, IdempotencyKey: "unbounded-goal-start",
	})
	if err != nil {
		t.Fatal(err)
	}
	state, _ := store.Projection()
	activity := state.Activities[execution.FirstActivity]
	attempt := prepareTestAttempt(t, store, activity.ID, activity.Generation, "unbounded-attempt")
	milestone(t, store, activity, attempt, "unbounded-spawn", Milestone{Kind: MilestoneProcessSpawned, Process: &ProcessIdentity{PID: 705, StartToken: "unbounded-process"}})
	milestone(t, store, activity, attempt, "unbounded-turn", Milestone{Kind: MilestoneTurnStarted})
	milestone(t, store, activity, attempt, "unbounded-result", Milestone{Kind: MilestoneResult, Result: &WorkerResult{Status: "continue", Summary: "Useful monitoring remains"}})
	milestone(t, store, activity, attempt, "unbounded-exit", Milestone{Kind: MilestoneExit, Exit: &Exit{Code: 0}})
	continuation, _, err := store.DecideTurn(context.Background(), DecideTurnInput{ActivityID: activity.ID, ExpectedGeneration: activity.Generation, AttemptID: attempt.ID, Decision: TurnDecision{Outcome: "continue", Reason: "Continue unattended supervision.", Model: "fake/evaluator"}, IdempotencyKey: "unbounded-goal-continue"})
	if err != nil || continuation == nil {
		t.Fatalf("unbounded goal did not continue: continuation=%+v err=%v", continuation, err)
	}
	view, err := store.View(execution.ID, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Queue) != 1 || view.Queue[0] != continuation.ID || view.Activities[0].BlockerKind != "" {
		t.Fatalf("unbounded goal escalated instead of continuing: %+v", view)
	}
}

func TestGoalWakeIntervalPersistsAndBecomesRunnableWhenDue(t *testing.T) {
	now := time.Date(2026, time.August, 7, 16, 0, 0, 0, time.UTC)
	store, _, worktree := openTestStore(t, Options{Now: func() time.Time { return now }})
	execution, _, err := store.StartExecution(context.Background(), StartExecutionInput{
		NativeSession: NativeSessionIdentity{Runtime: "codex", ID: "scheduled-session"}, Goal: "Periodically supervise active work", Prompt: "Inspect every workstream",
		Runtime: RuntimeSpec{Name: "codex", Sandbox: SandboxWorkspaceWrite}, Root: worktree,
		Authority: AuthoritySpec{RequestedBy: "human:test", HumanAuthorized: true, Sandbox: SandboxWorkspaceWrite}, Budget: DefaultBudget(),
		EvaluatorModel: "fake/evaluator", WakeIntervalSeconds: 600, IdempotencyKey: "scheduled-goal-start",
	})
	if err != nil {
		t.Fatal(err)
	}
	state, _ := store.Projection()
	activity := state.Activities[execution.FirstActivity]
	attempt := prepareTestAttempt(t, store, activity.ID, activity.Generation, "scheduled-attempt")
	milestone(t, store, activity, attempt, "scheduled-spawn", Milestone{Kind: MilestoneProcessSpawned, Process: &ProcessIdentity{PID: 708, StartToken: "scheduled-process"}})
	milestone(t, store, activity, attempt, "scheduled-turn", Milestone{Kind: MilestoneTurnStarted})
	milestone(t, store, activity, attempt, "scheduled-result", Milestone{Kind: MilestoneResult, Result: &WorkerResult{Status: "continue", Summary: "Check again later"}})
	milestone(t, store, activity, attempt, "scheduled-exit", Milestone{Kind: MilestoneExit, Exit: &Exit{Code: 0}})
	continuation, _, err := store.DecideTurn(context.Background(), DecideTurnInput{ActivityID: activity.ID, ExpectedGeneration: activity.Generation, AttemptID: attempt.ID, Decision: TurnDecision{Outcome: "continue", Reason: "Wake for the next audit.", Model: "fake/evaluator"}, IdempotencyKey: "scheduled-goal-continue"})
	if err != nil || continuation == nil {
		t.Fatalf("scheduled goal did not continue: continuation=%+v err=%v", continuation, err)
	}
	wakeAt := now.Add(10 * time.Minute)
	if continuation.NotBefore == nil || !continuation.NotBefore.Equal(wakeAt) {
		t.Fatalf("continuation wake=%s want=%s", continuation.NotBefore, wakeAt)
	}
	view, err := store.View(execution.ID, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Queue) != 0 || view.NextWakeAt == nil || !view.NextWakeAt.Equal(wakeAt) {
		t.Fatalf("scheduled continuation became runnable early: %+v", view)
	}
	var scheduled ActivityView
	for _, candidate := range view.Activities {
		if candidate.ID == continuation.ID {
			scheduled = candidate
			break
		}
	}
	if scheduled.Status != ActivityScheduled {
		t.Fatalf("continuation status=%s want=%s", scheduled.Status, ActivityScheduled)
	}
	if _, _, err = store.PrepareAttempt(context.Background(), PrepareAttemptInput{ActivityID: continuation.ID, ExpectedGeneration: continuation.Generation, CommandDigest: "sha256:early", Outputs: OutputIdentity{Stdout: "early-out", Stderr: "early-err"}, IdempotencyKey: "scheduled-too-early"}); err == nil || !strings.Contains(err.Error(), "scheduled for") {
		t.Fatalf("early execution was not rejected: %v", err)
	}
	now = wakeAt
	view, err = store.View(execution.ID, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Queue) != 1 || view.Queue[0] != continuation.ID || view.NextWakeAt != nil {
		t.Fatalf("due continuation did not enter queue: %+v", view)
	}
	prepareTestAttempt(t, store, continuation.ID, continuation.Generation, "scheduled-due-attempt")
}

func TestGoalContinuationDoesNotStarveQueuedHumanReply(t *testing.T) {
	store, _, worktree := openTestStore(t, Options{})
	execution, _, err := store.StartExecution(context.Background(), StartExecutionInput{
		NativeSession: NativeSessionIdentity{Runtime: "claude", ID: "reply-priority-session"}, Goal: "Keep shipping useful work", Prompt: "Ship the next change",
		Runtime: RuntimeSpec{Name: "claude", Sandbox: SandboxWorkspaceWrite}, Root: worktree,
		Authority: AuthoritySpec{RequestedBy: "human:test", HumanAuthorized: true, Sandbox: SandboxWorkspaceWrite}, Budget: DefaultBudget(),
		EvaluatorModel: "fake/evaluator", IdempotencyKey: "reply-priority-start",
	})
	if err != nil {
		t.Fatal(err)
	}
	state, _ := store.Projection()
	first := state.Activities[execution.FirstActivity]
	firstAttempt := prepareTestAttempt(t, store, first.ID, first.Generation, "reply-priority-first-attempt")
	milestone(t, store, first, firstAttempt, "reply-priority-first-spawn", Milestone{Kind: MilestoneProcessSpawned, Process: &ProcessIdentity{PID: 706, StartToken: "reply-priority-first"}})
	milestone(t, store, first, firstAttempt, "reply-priority-first-turn", Milestone{Kind: MilestoneTurnStarted})
	milestone(t, store, first, firstAttempt, "reply-priority-first-result", Milestone{Kind: MilestoneResult, Result: &WorkerResult{Status: "continue", Summary: "More work remains"}})
	milestone(t, store, first, firstAttempt, "reply-priority-first-exit", Milestone{Kind: MilestoneExit, Exit: &Exit{Code: 0}})
	automatic, _, err := store.DecideTurn(context.Background(), DecideTurnInput{ActivityID: first.ID, ExpectedGeneration: first.Generation, AttemptID: firstAttempt.ID, Decision: TurnDecision{Outcome: "continue", Reason: "Continue the campaign.", Model: "fake/evaluator"}, IdempotencyKey: "reply-priority-first-decision"})
	if err != nil || automatic == nil {
		t.Fatalf("first continuation failed: activity=%+v err=%v", automatic, err)
	}
	human, _, err := store.ContinueSession(context.Background(), ContinueSessionInput{ExecutionID: execution.ID, SessionID: execution.SessionID, PredecessorActivityID: first.ID, From: "human", Message: "Stop polling CI and start the next independent candidate.", IdempotencyKey: "reply-priority-human"})
	if err != nil {
		t.Fatal(err)
	}
	automaticAttempt := prepareTestAttempt(t, store, automatic.ID, automatic.Generation, "reply-priority-auto-attempt")
	milestone(t, store, automatic, automaticAttempt, "reply-priority-auto-spawn", Milestone{Kind: MilestoneProcessSpawned, Process: &ProcessIdentity{PID: 707, StartToken: "reply-priority-auto"}})
	milestone(t, store, automatic, automaticAttempt, "reply-priority-auto-turn", Milestone{Kind: MilestoneTurnStarted})
	milestone(t, store, automatic, automaticAttempt, "reply-priority-auto-result", Milestone{Kind: MilestoneResult, Result: &WorkerResult{Status: "continue", Summary: "CI is still pending"}})
	milestone(t, store, automatic, automaticAttempt, "reply-priority-auto-exit", Milestone{Kind: MilestoneExit, Exit: &Exit{Code: 0}})
	next, _, err := store.DecideTurn(context.Background(), DecideTurnInput{ActivityID: automatic.ID, ExpectedGeneration: automatic.Generation, AttemptID: automaticAttempt.ID, Decision: TurnDecision{Outcome: "continue", Reason: "Keep watching CI.", Model: "fake/evaluator"}, IdempotencyKey: "reply-priority-auto-decision"})
	if err != nil || next == nil || next.ID != human.ID {
		t.Fatalf("evaluator continuation starved human reply: next=%+v human=%+v err=%v", next, human, err)
	}
	state, _ = store.Projection()
	if len(state.Activities) != 3 {
		t.Fatalf("evaluator created a competing continuation: activities=%d", len(state.Activities))
	}
	view, err := store.View(execution.ID, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Queue) != 1 || view.Queue[0] != human.ID {
		t.Fatalf("human reply is not the sole queued continuation: %+v", view.Queue)
	}
}

func TestCompletedPredecessorIsImmutableAndContinuationQueuesBehindSuccessorLease(t *testing.T) {
	store, _, worktree := openTestStore(t, Options{})
	execution := startTestExecution(t, store, worktree, "continuation-start", DefaultBudget())
	state, _ := store.Projection()
	predecessor := state.Activities[execution.FirstActivity]
	predecessorResult := completeActivity(t, store, predecessor, "predecessor")
	successor, _, err := store.AddNode(context.Background(), AddNodeInput{WorkflowID: execution.WorkflowID, NodeID: "successor", Title: "consume immutable predecessor", Work: WorkSpec{Kind: "agent", Prompt: "successor", Root: worktree, Runtime: RuntimeSpec{Name: "claude", Sandbox: SandboxWorkspaceWrite}}, Dependencies: []NodeID{predecessor.NodeID}, Actor: "human:test", IdempotencyKey: "successor-add-0001"})
	if err != nil {
		t.Fatal(err)
	}
	successorActivity, _, err := store.QueueActivity(context.Background(), QueueActivityInput{WorkflowID: execution.WorkflowID, NodeID: successor.ID, SessionID: execution.SessionID, IdempotencyKey: "successor-queue-01"})
	if err != nil {
		t.Fatal(err)
	}
	continuation, _, err := store.ContinueSession(context.Background(), ContinueSessionInput{ExecutionID: execution.ID, SessionID: execution.SessionID, PredecessorActivityID: predecessor.ID, From: "human", Message: "please clarify", IdempotencyKey: "continuation-0001"})
	if err != nil {
		t.Fatal(err)
	}
	if continuation.ParentActivityID != predecessor.ID || continuation.Generation <= predecessor.Generation {
		t.Fatalf("reply did not create a continuation generation: %+v", continuation)
	}
	if len(successorActivity.DependencyBindings) != 1 || successorActivity.DependencyBindings[0].ResultID != predecessorResult.ID {
		t.Fatalf("successor lost its immutable dependency binding: %+v", successorActivity.DependencyBindings)
	}
	prepareTestAttempt(t, store, successorActivity.ID, successorActivity.Generation, "successor-lease-01")
	if _, _, err = store.PrepareAttempt(context.Background(), PrepareAttemptInput{ActivityID: continuation.ID, ExpectedGeneration: continuation.Generation, CommandDigest: "digest", Outputs: OutputIdentity{Stdout: "continuation-out", Stderr: "continuation-err"}, IdempotencyKey: "continuation-lease"}); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("overlapping canonical-worktree writer was not excluded: %v", err)
	}
	state, _ = store.Projection()
	if resultForActivity(state, predecessor.ID).ID != predecessorResult.ID {
		t.Fatal("completed predecessor result was changed by reply")
	}
}

func TestStaleLeaseAndAttemptAreFenced(t *testing.T) {
	store, _, worktree := openTestStore(t, Options{})
	execution := startTestExecution(t, store, worktree, "stale-fence-start", DefaultBudget())
	state, _ := store.Projection()
	activity := state.Activities[execution.FirstActivity]
	old := prepareTestAttempt(t, store, activity.ID, activity.Generation, "old-attempt-prep")
	milestone(t, store, activity, old, "old-attempt-fail", Milestone{Kind: MilestoneAdapterStartFailed, Failure: "dead"})
	milestone(t, store, activity, old, "old-attempt-exit", Milestone{Kind: MilestoneExit, Exit: &Exit{Code: 127}})
	current := prepareTestAttempt(t, store, activity.ID, activity.Generation, "new-attempt-prep")
	_, err := store.RecordMilestone(context.Background(), RecordMilestoneInput{ActivityID: activity.ID, ExpectedGeneration: activity.Generation, AttemptID: old.ID, LeaseID: old.LeaseID, Milestone: Milestone{Kind: MilestoneProcessSpawned, Process: &ProcessIdentity{PID: 1, StartToken: "stale"}}, IdempotencyKey: "stale-event-0001"})
	if !errors.Is(err, ErrFenced) {
		t.Fatalf("stale lease wrote through newer attempt: %v", err)
	}
	milestone(t, store, activity, current, "new-attempt-spawn", Milestone{Kind: MilestoneProcessSpawned, Process: &ProcessIdentity{PID: 2, StartToken: "current"}})
}

func TestPauseFencesActiveAttemptsReleasesOwnershipAndIsIdempotent(t *testing.T) {
	store, _, worktree := openTestStore(t, Options{})
	execution := startTestExecution(t, store, worktree, "pause-fence-start", DefaultBudget())
	state, err := store.Projection()
	if err != nil {
		t.Fatal(err)
	}
	activity := state.Activities[execution.FirstActivity]
	attempt := prepareTestAttempt(t, store, activity.ID, activity.Generation, "pause-fence-prepare")
	milestone(t, store, activity, attempt, "pause-fence-spawn", Milestone{Kind: MilestoneProcessSpawned, Process: &ProcessIdentity{PID: 77, StartToken: "pause-birth"}})
	pause, first, err := store.PauseWorkflow(context.Background(), PauseWorkflowInput{WorkflowID: execution.WorkflowID, RequestedBy: "cloud", IdempotencyKey: "pause-fence-command"})
	if err != nil {
		t.Fatal(err)
	}
	if first.Existing || pause.Phase != PauseDraining || len(pause.FencedAttemptIDs) != 1 || len(pause.ReleasedLeaseIDs) != 0 || !pause.CompletedAt.IsZero() {
		t.Fatalf("pause=%+v receipt=%+v", pause, first)
	}
	state, err = store.Projection()
	if err != nil {
		t.Fatal(err)
	}
	if state.Leases[attempt.LeaseID] == nil || !state.Leases[attempt.LeaseID].ReleasedAt.IsZero() {
		t.Fatal("pause released cloud writer ownership before terminal exit")
	}
	if _, err = store.RecordMilestone(context.Background(), RecordMilestoneInput{ActivityID: activity.ID, ExpectedGeneration: activity.Generation, AttemptID: attempt.ID, LeaseID: attempt.LeaseID, Milestone: Milestone{Kind: MilestoneTurnStarted}, IdempotencyKey: "pause-stale-turn"}); !errors.Is(err, ErrFenced) {
		t.Fatalf("paused attempt accepted a stale milestone: %v", err)
	}
	milestone(t, store, activity, attempt, "pause-exit", Milestone{Kind: MilestoneExit, Exit: &Exit{Code: 143}})
	settled, _, err := store.SettlePause(context.Background(), SettlePauseInput{WorkflowID: execution.WorkflowID, IdempotencyKey: "pause-settle-command"})
	if err != nil || settled.Phase != PauseCompleted || settled.CompletedAt.IsZero() || len(settled.ReleasedLeaseIDs) != 1 {
		t.Fatalf("pause was not settled after exact exit: pause=%+v err=%v", settled, err)
	}
	_, second, err := store.PauseWorkflow(context.Background(), PauseWorkflowInput{WorkflowID: execution.WorkflowID, RequestedBy: "cloud", IdempotencyKey: "pause-fence-command"})
	if err != nil || !second.Existing || second.Sequence != first.Sequence {
		t.Fatalf("pause retry was not idempotent: receipt=%+v err=%v", second, err)
	}
}

func TestExactAttemptAcceptsOnlyOneControlAndPauseReusesIt(t *testing.T) {
	store, _, worktree := openTestStore(t, Options{})
	execution := startTestExecution(t, store, worktree, "control-unique-start", DefaultBudget())
	state, err := store.Projection()
	if err != nil {
		t.Fatal(err)
	}
	activity := state.Activities[execution.FirstActivity]
	attempt := prepareTestAttempt(t, store, activity.ID, activity.Generation, "control-unique-attempt")
	first, _, err := store.RequestControl(context.Background(), RequestControlInput{ActivityID: activity.ID, ExpectedGeneration: activity.Generation, ExpectedAttemptID: attempt.ID, Kind: "stop", Actor: "cloud:a", IdempotencyKey: "control-unique-first"})
	if err != nil || first == nil || !first.Accepted {
		t.Fatalf("first control was not accepted: control=%+v err=%v", first, err)
	}
	entriesBefore, err := store.Events(0)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = store.RequestControl(context.Background(), RequestControlInput{ActivityID: activity.ID, ExpectedGeneration: activity.Generation, ExpectedAttemptID: attempt.ID, Kind: "stop", Actor: "cloud:b", IdempotencyKey: "control-unique-second"}); !errors.Is(err, ErrControlAlreadyAccepted) {
		t.Fatalf("competing control was not deterministically rejected: %v", err)
	}
	entriesAfter, _ := store.Events(0)
	if len(entriesAfter) != len(entriesBefore) {
		t.Fatal("competing control mutated the journal")
	}

	pause, _, err := store.PauseWorkflow(context.Background(), PauseWorkflowInput{WorkflowID: execution.WorkflowID, RequestedBy: "human:pause", IdempotencyKey: "control-unique-pause"})
	if err != nil {
		t.Fatal(err)
	}
	if len(pause.FencedAttemptIDs) != 1 {
		t.Fatalf("pause did not fence the existing accepted control: %+v", pause)
	}
	entriesBefore, err = store.Events(0)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = store.RequestControl(context.Background(), RequestControlInput{ActivityID: activity.ID, ExpectedGeneration: activity.Generation, ExpectedAttemptID: attempt.ID, Kind: "stop", Actor: "cloud:c", IdempotencyKey: "control-unique-after-pause"}); !errors.Is(err, ErrControlAlreadyAccepted) {
		t.Fatalf("post-pause competing control was not rejected by the existing fence: %v", err)
	}
	entriesAfter, _ = store.Events(0)
	if len(entriesAfter) != len(entriesBefore) {
		t.Fatal("post-pause competing control mutated the journal")
	}
	state, _ = store.Projection()
	accepted := 0
	for _, control := range state.Controls {
		if control.Accepted && control.ActivityID == activity.ID && control.ExpectedAttemptID == attempt.ID {
			accepted++
		}
	}
	if accepted != 1 {
		t.Fatalf("pause created a competing accepted control: count=%d controls=%+v", accepted, state.Controls)
	}
}

func TestCompetingControlsConcurrentAcceptExactlyOne(t *testing.T) {
	store, _, worktree := openTestStore(t, Options{})
	execution := startTestExecution(t, store, worktree, "control-concurrent-start", DefaultBudget())
	state, _ := store.Projection()
	activity := state.Activities[execution.FirstActivity]
	attempt := prepareTestAttempt(t, store, activity.ID, activity.Generation, "control-concurrent-attempt")
	type result struct {
		accepted *Control
		err      error
	}
	results := make(chan result, 2)
	for _, key := range []string{"control-concurrent-a", "control-concurrent-b"} {
		go func(key string) {
			control, _, err := store.RequestControl(context.Background(), RequestControlInput{ActivityID: activity.ID, ExpectedGeneration: activity.Generation, ExpectedAttemptID: attempt.ID, Kind: "stop", Actor: key, IdempotencyKey: key})
			results <- result{accepted: control, err: err}
		}(key)
	}
	accepted := 0
	conflicts := 0
	for range 2 {
		item := <-results
		if item.err == nil && item.accepted != nil && item.accepted.Accepted {
			accepted++
		} else if errors.Is(item.err, ErrControlAlreadyAccepted) {
			conflicts++
		} else {
			t.Fatalf("unexpected concurrent control result: %+v", item)
		}
	}
	if accepted != 1 || conflicts != 1 {
		t.Fatalf("concurrent controls accepted=%d conflicts=%d", accepted, conflicts)
	}
}

func TestAcceptedControlProjectionSelectionIsDeterministic(t *testing.T) {
	state := &State{Controls: map[ControlID]*Control{
		"z": {ID: "z", Accepted: true, ActivityID: "activity", ExpectedGeneration: 1, ExpectedAttemptID: "attempt", CreatedAt: time.Unix(10, 0)},
		"a": {ID: "a", Accepted: true, ActivityID: "activity", ExpectedGeneration: 1, ExpectedAttemptID: "attempt", CreatedAt: time.Unix(10, 0)},
	}}
	control := AcceptedControlForAttempt(state, &Activity{ID: "activity", Generation: 1}, &Attempt{ID: "attempt", ActivityGeneration: 1})
	if control == nil || control.ID != "a" {
		t.Fatalf("control selection depended on map order: %+v", control)
	}
}

func TestCrashInjectionAtEveryTransactionBoundaryReplaysExactlyOnce(t *testing.T) {
	for _, boundary := range []Boundary{BoundaryAfterValidation, BoundaryAfterAppend, BoundaryAfterSnapshot} {
		t.Run(string(boundary), func(t *testing.T) {
			fired := false
			store, stateRoot, worktree := openTestStore(t, Options{Fault: func(at Boundary) error {
				if !fired && at == boundary {
					fired = true
					return fmt.Errorf("injected %s", boundary)
				}
				return nil
			}})
			input := StartExecutionInput{NativeSession: NativeSessionIdentity{Runtime: "claude", ID: "crash-session"}, Prompt: "crash", Runtime: RuntimeSpec{Name: "claude", Sandbox: SandboxWorkspaceWrite}, Root: worktree, Authority: AuthoritySpec{RequestedBy: "human:test", HumanAuthorized: true, Sandbox: SandboxWorkspaceWrite}, Budget: DefaultBudget(), IdempotencyKey: "crash-boundary-01"}
			_, _, firstErr := store.StartExecution(context.Background(), input)
			if firstErr == nil {
				t.Fatal("fault injection did not fire")
			}
			reopened, err := Open(stateRoot, Options{})
			if err != nil {
				t.Fatal(err)
			}
			state, err := reopened.Projection()
			if err != nil {
				t.Fatal(err)
			}
			want := 1
			if boundary == BoundaryAfterValidation {
				want = 0
			}
			if len(state.Executions) != want {
				t.Fatalf("boundary %s executions=%d want=%d", boundary, len(state.Executions), want)
			}
			_, receipt, err := reopened.StartExecution(context.Background(), input)
			if err != nil {
				t.Fatal(err)
			}
			if want == 1 && !receipt.Existing {
				t.Fatal("post-append retry duplicated instead of returning committed execution")
			}
			entries, _ := reopened.Events(0)
			if len(entries) != 1 {
				t.Fatalf("replay produced %d entries", len(entries))
			}
		})
	}
}

func TestConcurrentIdempotentStartHasOneCommit(t *testing.T) {
	stateRoot, worktree := safeDir(t), safeDir(t)
	stores := make([]*Store, 8)
	for i := range stores {
		var err error
		stores[i], err = Open(stateRoot, Options{})
		if err != nil {
			t.Fatal(err)
		}
	}
	input := StartExecutionInput{NativeSession: NativeSessionIdentity{Runtime: "codex", ID: "concurrent-thread"}, Prompt: "concurrent", Runtime: RuntimeSpec{Name: "codex", Sandbox: SandboxReadOnly}, Root: worktree, Authority: AuthoritySpec{RequestedBy: "human:test", HumanAuthorized: true, Sandbox: SandboxReadOnly}, Budget: DefaultBudget(), IdempotencyKey: "concurrent-start"}
	var wg sync.WaitGroup
	errs := make(chan error, len(stores))
	for _, store := range stores {
		wg.Add(1)
		go func(store *Store) {
			defer wg.Done()
			_, _, err := store.StartExecution(context.Background(), input)
			errs <- err
		}(store)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	entries, err := stores[0].Events(0)
	if err != nil || len(entries) != 1 {
		t.Fatalf("concurrent idempotency commits=%d err=%v", len(entries), err)
	}
}

func TestSymlinkAliasesShareOneWriterLeaseAndReadsDoNotAppend(t *testing.T) {
	store, _, worktree := openTestStore(t, Options{})
	aliasParent := safeDir(t)
	alias := filepath.Join(aliasParent, "alias")
	if err := os.Symlink(worktree, alias); err != nil {
		t.Fatal(err)
	}
	first := startTestExecution(t, store, worktree, "root-real-start", DefaultBudget())
	second := startTestExecution(t, store, alias, "root-alias-start", DefaultBudget())
	state, _ := store.Projection()
	firstActivity, secondActivity := state.Activities[first.FirstActivity], state.Activities[second.FirstActivity]
	prepareTestAttempt(t, store, firstActivity.ID, firstActivity.Generation, "root-real-attempt")
	if _, _, err := store.PrepareAttempt(context.Background(), PrepareAttemptInput{ActivityID: secondActivity.ID, ExpectedGeneration: secondActivity.Generation, CommandDigest: "digest", Outputs: OutputIdentity{Stdout: "alias-out", Stderr: "alias-err"}, IdempotencyKey: "root-alias-attempt"}); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("symlink alias bypassed writer lease: %v", err)
	}
	before, _ := store.Events(0)
	for i := 0; i < 5; i++ {
		if _, err := store.Projection(); err != nil {
			t.Fatal(err)
		}
		if _, err := store.View(first.ID, time.Unix(int64(i), 0)); err != nil {
			t.Fatal(err)
		}
	}
	after, _ := store.Events(0)
	if len(after) != len(before) {
		t.Fatalf("observation mutated journal: before=%d after=%d", len(before), len(after))
	}
}

func TestConcurrentCrossWorkflowWritersAcquireExactlyOneLease(t *testing.T) {
	store, stateRoot, worktree := openTestStore(t, Options{})
	other, err := Open(stateRoot, Options{})
	if err != nil {
		t.Fatal(err)
	}
	first := startTestExecution(t, store, worktree, "writer-race-first", DefaultBudget())
	second := startTestExecution(t, store, worktree, "writer-race-second", DefaultBudget())
	state, _ := store.Projection()
	inputs := []PrepareAttemptInput{
		{ActivityID: first.FirstActivity, ExpectedGeneration: state.Activities[first.FirstActivity].Generation, CommandDigest: "first", Outputs: OutputIdentity{Stdout: "first-out", Stderr: "first-err"}, IdempotencyKey: "writer-race-attempt-one"},
		{ActivityID: second.FirstActivity, ExpectedGeneration: state.Activities[second.FirstActivity].Generation, CommandDigest: "second", Outputs: OutputIdentity{Stdout: "second-out", Stderr: "second-err"}, IdempotencyKey: "writer-race-attempt-two"},
	}
	stores := []*Store{store, other}
	start := make(chan struct{})
	errs := make(chan error, 2)
	for index := range inputs {
		go func(index int) {
			<-start
			_, _, err := stores[index].PrepareAttempt(context.Background(), inputs[index])
			errs <- err
		}(index)
	}
	close(start)
	firstErr, secondErr := <-errs, <-errs
	successes, fenced := 0, 0
	for _, err := range []error{firstErr, secondErr} {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrLeaseHeld):
			fenced++
		default:
			t.Fatalf("unexpected writer result: %v", err)
		}
	}
	if successes != 1 || fenced != 1 {
		t.Fatalf("successes=%d fenced=%d errors=%v,%v", successes, fenced, firstErr, secondErr)
	}
	state, _ = store.Projection()
	active := 0
	for _, lease := range state.Leases {
		if lease.ReleasedAt.IsZero() {
			active++
		}
	}
	if active != 1 {
		t.Fatalf("active writer leases=%d", active)
	}
}

func TestProjectionReplaysWhenSnapshotIsDeleted(t *testing.T) {
	store, stateRoot, worktree := openTestStore(t, Options{})
	execution := startTestExecution(t, store, worktree, "snapshot-replay-start", DefaultBudget())
	before, err := store.Projection()
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(filepath.Join(stateRoot, "supervisor-v2", "canonical", "state.json")); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(stateRoot, Options{})
	if err != nil {
		t.Fatal(err)
	}
	after, err := reopened.Projection()
	if err != nil {
		t.Fatal(err)
	}
	if before.Sequence != after.Sequence || after.Executions[execution.ID] == nil {
		t.Fatalf("snapshot loss changed replay: before=%d after=%d", before.Sequence, after.Sequence)
	}
}

func TestRetiredPublicationEventRemainsReplayReadable(t *testing.T) {
	state := emptyState()
	entry := JournalEntry{
		SchemaVersion: SchemaVersion,
		Sequence:      1,
		At:            time.Unix(100, 0).UTC(),
		Events: []DomainEvent{{
			Type: "attestation.recorded",
			Data: json.RawMessage(`{"attestation":{"id":"legacy","result_id":"missing"}}`),
		}},
	}
	if err := applyEntry(state, entry); err != nil {
		t.Fatalf("retired publication event broke journal replay: %v", err)
	}
	if state.Sequence != entry.Sequence || len(state.Results) != 0 {
		t.Fatalf("retired publication event changed active state: sequence=%d results=%d", state.Sequence, len(state.Results))
	}
}

func TestStartExecutionRejectsRootOutsideAuthorityWithoutMutation(t *testing.T) {
	store, _, worktree := openTestStore(t, Options{})
	authorized := safeDir(t)
	nested := filepath.Join(authorized, "nested-repository")
	if err := os.Mkdir(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	accepted, _, err := store.StartExecution(context.Background(), StartExecutionInput{NativeSession: NativeSessionIdentity{Runtime: "claude", ID: "nested-path-session"}, Prompt: "nested path", Runtime: RuntimeSpec{Name: "claude", Sandbox: SandboxWorkspaceWrite}, Root: nested, Authority: AuthoritySpec{RequestedBy: "human:test", HumanAuthorized: true, Sandbox: SandboxWorkspaceWrite, AllowedRoots: []string{authorized}}, Budget: DefaultBudget(), IdempotencyKey: "path-nested-01"})
	if err != nil || accepted == nil {
		t.Fatalf("authorized nested root was rejected: execution=%+v err=%v", accepted, err)
	}
	outside := safeDir(t)
	entriesBefore, err := store.Events(0)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = store.StartExecution(context.Background(), StartExecutionInput{NativeSession: NativeSessionIdentity{Runtime: "claude", ID: "path-session"}, Prompt: "path", Runtime: RuntimeSpec{Name: "claude", Sandbox: SandboxWorkspaceWrite}, Root: worktree, Authority: AuthoritySpec{RequestedBy: "human:test", HumanAuthorized: true, Sandbox: SandboxWorkspaceWrite, AllowedRoots: []string{outside}}, Budget: DefaultBudget(), IdempotencyKey: "path-boundary-01"})
	if err == nil {
		t.Fatal("out-of-authority root was accepted")
	}
	entries, _ := store.Events(0)
	if len(entries) != len(entriesBefore) {
		t.Fatal("rejected command mutated the journal")
	}
}

func TestLegacyLedgerImportIsDeterministicOneWayAndPreservesContinuationHistory(t *testing.T) {
	sourceRoot, worktree := safeDir(t), safeDir(t)
	workflow := &legacyimport.Workflow{ID: "wf_legacy_import", Goal: "legacy goal", Root: worktree, Status: legacyimport.WorkflowActive, Budget: legacyimport.Budget{MaxNodes: 8, MaxConcurrent: 1, MaxAttempts: 3, MaxChangedFiles: 10, MaxDiffLines: 100, RequireAttestation: true}, Nodes: map[string]*legacyimport.Node{}, CreatedAt: time.Unix(100, 0).UTC(), UpdatedAt: time.Unix(100, 0).UTC()}
	created := *workflow
	created.Nodes = map[string]*legacyimport.Node{}
	legacyEvents := []legacyimport.Event{{Sequence: 1, ID: "created", WorkflowID: workflow.ID, Type: "workflow.created", Actor: "human", At: workflow.CreatedAt, Data: &created}}
	appendProposal := func(sequence uint64, proposal legacyimport.Proposal, at time.Time) {
		t.Helper()
		if err := legacyimport.ApplyProposal(workflow, proposal, at); err != nil {
			t.Fatal(err)
		}
		legacyEvents = append(legacyEvents, legacyimport.Event{Sequence: sequence, ID: fmt.Sprintf("proposal-%d", sequence), WorkflowID: workflow.ID, Type: "proposal.applied", Actor: proposal.Actor, At: at, Data: proposal})
	}
	node := &legacyimport.Node{ID: "lead", Title: "legacy lead", Kind: "agent", Prompt: "legacy prompt", Worktree: worktree, Runtime: legacyimport.RuntimeSpec{Name: "claude", Sandbox: "workspace-write"}, SessionID: "legacy-exact-session", MaxAttempts: 3}
	appendProposal(2, legacyimport.Proposal{WorkflowID: workflow.ID, Actor: "human", Mutations: []legacyimport.Mutation{{Op: "add_node", Node: node}}}, time.Unix(101, 0).UTC())
	appendProposal(3, legacyimport.Proposal{WorkflowID: workflow.ID, Actor: "supervisor", Mutations: []legacyimport.Mutation{{Op: "set_state", NodeID: node.ID, State: legacyimport.NodeRunning}}}, time.Unix(102, 0).UTC())
	appendProposal(4, legacyimport.Proposal{WorkflowID: workflow.ID, Actor: node.ID, Mutations: []legacyimport.Mutation{{Op: "set_state", NodeID: node.ID, State: legacyimport.NodeCompleted}}}, time.Unix(103, 0).UTC())
	appendProposal(5, legacyimport.Proposal{WorkflowID: workflow.ID, Actor: "human", Mutations: []legacyimport.Mutation{{Op: "attest", Attestation: &legacyimport.Attestation{ID: "legacy-attestation", NodeID: node.ID, Verifier: "legacy-ci", Verdict: "pass", Summary: "historical release gate"}}}}, time.Unix(104, 0).UTC())
	appendProposal(6, legacyimport.Proposal{WorkflowID: workflow.ID, Actor: "human", Mutations: []legacyimport.Mutation{{Op: "reopen_agent", NodeID: node.ID}}}, time.Unix(105, 0).UTC())
	legacyPath := filepath.Join(sourceRoot, "workflows", workflow.ID, "events.jsonl")
	if err := os.MkdirAll(filepath.Dir(legacyPath), 0o700); err != nil {
		t.Fatal(err)
	}
	var encoded []byte
	for _, event := range legacyEvents {
		raw, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		encoded = append(encoded, raw...)
		encoded = append(encoded, '\n')
	}
	if err := os.WriteFile(legacyPath, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(legacyPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, namespace := range []string{"sessions", "activities", "teams"} {
		path := filepath.Join(sourceRoot, namespace, "unreplayed", "events.jsonl")
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("non-workflow history is not replayed\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	target, _, _ := openTestStore(t, Options{})
	receipt, err := target.ImportV1(context.Background(), ImportV1Input{SourceRoot: sourceRoot, IdempotencyKey: "legacy-import-01"})
	if err != nil {
		t.Fatal(err)
	}
	state, err := target.Projection()
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Workflows) != 1 || len(state.Results) != 1 || len(state.Activities) != 2 || len(state.LegacyImports) != 1 {
		t.Fatalf("unexpected imported projection: workflows=%d results=%d activities=%d imports=%d", len(state.Workflows), len(state.Results), len(state.Activities), len(state.LegacyImports))
	}
	serializedState, err := json.Marshal(state)
	if err != nil || strings.Contains(string(serializedState), "attestation") {
		t.Fatalf("legacy attestation leaked into Supervisor v2 state: err=%v state=%s", err, serializedState)
	}
	for _, session := range state.Sessions {
		if !session.ImportedUnresolved || session.Native.ID != "" {
			t.Fatalf("legacy session was presented as exact recoverable identity: %+v", session)
		}
	}
	importRecord, ok := state.LegacyImports[receipt.ResourceID]
	if !ok || len(importRecord.Files) == 0 {
		t.Fatalf("workflow import record is missing file inventory: receipt=%+v imports=%+v", receipt, state.LegacyImports)
	}
	for path := range importRecord.Files {
		if strings.HasPrefix(path, "sessions/") || strings.HasPrefix(path, "activities/") || strings.HasPrefix(path, "teams/") {
			t.Fatalf("non-workflow ledger was incorrectly inventoried: %s", path)
		}
	}
	var predecessor, continuation *Activity
	for _, activity := range state.Activities {
		if activity.Generation == 1 {
			predecessor = activity
		} else if activity.Generation == 2 {
			continuation = activity
		}
	}
	if predecessor == nil || continuation == nil || continuation.ParentActivityID != predecessor.ID || resultForActivity(state, predecessor.ID) == nil || resultForActivity(state, continuation.ID) != nil {
		t.Fatalf("legacy reopen was not normalized to immutable predecessor plus queued continuation: predecessor=%+v continuation=%+v", predecessor, continuation)
	}
	var executionID ExecutionID
	for id := range state.Executions {
		executionID = id
		break
	}
	view, err := target.View(executionID, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	var continuationView *ActivityView
	for index := range view.Activities {
		if view.Activities[index].ID == continuation.ID {
			continuationView = &view.Activities[index]
		}
	}
	if len(view.Queue) != 0 || continuationView == nil || continuationView.Status != ActivityNeedsHuman {
		t.Fatalf("unresolved imported continuation was schedulable: queue=%v activities=%+v", view.Queue, view.Activities)
	}
	retry, err := target.ImportV1(context.Background(), ImportV1Input{SourceRoot: sourceRoot, IdempotencyKey: "legacy-import-01"})
	if err != nil || !retry.Existing || retry.Sequence != receipt.Sequence {
		t.Fatalf("repeat import was not idempotent: receipt=%+v err=%v", retry, err)
	}
	if _, err = target.ImportV1(context.Background(), ImportV1Input{SourceRoot: sourceRoot, IdempotencyKey: "legacy-import-02"}); err == nil {
		t.Fatal("same legacy bytes were imported a second time under a divergent key")
	}
	after, err := os.ReadFile(legacyPath)
	if err != nil || string(after) != string(before) {
		t.Fatalf("one-way importer modified legacy ledger: err=%v", err)
	}
}

func TestLegacyAttestationImportDoesNotCreateV2PublicationAuthority(t *testing.T) {
	sourceRoot, worktree := safeDir(t), safeDir(t)
	createdAt := time.Unix(200, 0).UTC()
	node := &legacyimport.Node{ID: "legacy-node", Title: "legacy node", Kind: "agent", State: legacyimport.NodeCompleted, Worktree: worktree, Runtime: legacyimport.RuntimeSpec{Name: "claude", Sandbox: "workspace-write"}, SessionID: "legacy-session", MaxAttempts: 2, CreatedAt: createdAt, UpdatedAt: createdAt}
	workflow := &legacyimport.Workflow{ID: "wf_legacy_attestation", Goal: "legacy release gate", Root: worktree, Status: legacyimport.WorkflowWaiting, Budget: legacyimport.Budget{MaxNodes: 2, MaxConcurrent: 1, MaxAttempts: 2, RequireAttestation: true}, Nodes: map[string]*legacyimport.Node{node.ID: node}, Order: []string{node.ID}, CreatedAt: createdAt, UpdatedAt: createdAt}
	created := *workflow
	proposal := legacyimport.Proposal{WorkflowID: workflow.ID, Actor: "human", Mutations: []legacyimport.Mutation{{Op: "attest", Attestation: &legacyimport.Attestation{ID: "legacy-attestation", NodeID: node.ID, Verifier: "legacy-ci", Verdict: "pass", Summary: "historical release gate"}}}}
	if err := legacyimport.ApplyProposal(workflow, proposal, createdAt.Add(time.Second)); err != nil {
		t.Fatalf("legacy attestation proposal was not replayable: %v", err)
	}
	legacyEvents := []legacyimport.Event{
		{Sequence: 1, ID: "created", WorkflowID: workflow.ID, Type: "workflow.created", Actor: "human", At: createdAt, Data: &created},
		{Sequence: 2, ID: "proposal-2", WorkflowID: workflow.ID, Type: "proposal.applied", Actor: proposal.Actor, At: createdAt.Add(time.Second), Data: proposal},
	}
	legacyPath := filepath.Join(sourceRoot, "workflows", workflow.ID, "events.jsonl")
	if err := os.MkdirAll(filepath.Dir(legacyPath), 0o700); err != nil {
		t.Fatal(err)
	}
	var encoded []byte
	for _, event := range legacyEvents {
		raw, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		encoded = append(encoded, raw...)
		encoded = append(encoded, '\n')
	}
	if err := os.WriteFile(legacyPath, encoded, 0o600); err != nil {
		t.Fatal(err)
	}

	target, _, _ := openTestStore(t, Options{})
	if _, err := target.ImportV1(context.Background(), ImportV1Input{SourceRoot: sourceRoot, IdempotencyKey: "legacy-import-authority-01"}); err != nil {
		t.Fatalf("historical attestation ledger did not import: %v", err)
	}
	state, err := target.Projection()
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Workflows) != 1 || len(state.Results) != 1 {
		t.Fatalf("historical attestation import did not produce the expected v2 projection: workflows=%d results=%d", len(state.Workflows), len(state.Results))
	}
	var imported *Workflow
	for _, candidate := range state.Workflows {
		imported = candidate
	}
	if imported == nil || imported.Finalizer.Enabled || len(imported.Finalizer.RequiredChecks) != 0 || imported.Finalizer.RequireHuman {
		t.Fatalf("legacy attestation created v2 publication authority: workflow=%+v", imported)
	}
	serializedState, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	serialized := string(serializedState)
	if strings.Contains(serialized, `"attestation"`) || strings.Contains(serialized, `"attestations"`) || strings.Contains(serialized, `"verifier"`) || strings.Contains(serialized, `"verifiers"`) {
		t.Fatalf("legacy attestation or verifier data leaked into Supervisor v2 state: %s", serializedState)
	}
}
