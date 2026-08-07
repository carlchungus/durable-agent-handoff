package supervisor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/core"
	"github.com/carlchungus/durable-agent-handoff/internal/runstate"
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
	if !strings.Contains(strings.Join(os.Args, " "), "supervisor-live-orphan-helper") {
		return
	}
	for {
		time.Sleep(time.Hour)
	}
}

func TestStartupReconcileFailsClosedForExactLiveOrphan(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=TestSupervisorLiveOrphanHelper", "--supervisor-live-orphan-helper")
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
		token = runstate.ProcessStartToken(cmd.Process.Pid)
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
	legacy, err := core.OpenStore(sourceRoot)
	if err != nil {
		t.Fatal(err)
	}
	workflow, err := legacy.Create("legacy goal", worktree, core.Budget{MaxNodes: 8, MaxConcurrent: 1, MaxAttempts: 3, MaxChangedFiles: 10, MaxDiffLines: 100})
	if err != nil {
		t.Fatal(err)
	}
	node := &core.Node{ID: "lead", Title: "legacy lead", Kind: "agent", Prompt: "legacy prompt", Worktree: worktree, Runtime: core.RuntimeSpec{Name: "claude", Sandbox: "workspace-write"}, SessionID: "legacy-exact-session", MaxAttempts: 3}
	workflow, err = legacy.Apply(core.Proposal{WorkflowID: workflow.ID, Actor: "human", Mutations: []core.Mutation{{Op: "add_node", Node: node}}})
	if err != nil {
		t.Fatal(err)
	}
	workflow, err = legacy.Apply(core.Proposal{WorkflowID: workflow.ID, Actor: "supervisor", Mutations: []core.Mutation{{Op: "set_state", NodeID: node.ID, State: core.NodeRunning}}})
	if err != nil {
		t.Fatal(err)
	}
	workflow, err = legacy.Apply(core.Proposal{WorkflowID: workflow.ID, Actor: node.ID, Mutations: []core.Mutation{{Op: "set_state", NodeID: node.ID, State: core.NodeCompleted}}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = legacy.Apply(core.Proposal{WorkflowID: workflow.ID, Actor: "human", Mutations: []core.Mutation{{Op: "reopen_agent", NodeID: node.ID}}})
	if err != nil {
		t.Fatal(err)
	}
	legacyPath := filepath.Join(sourceRoot, "workflows", workflow.ID, "events.jsonl")
	before, err := os.ReadFile(legacyPath)
	if err != nil {
		t.Fatal(err)
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
