package supervisor

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

type finalizerRunner struct {
	responses [][]byte
	errs      []error
	calls     [][]string
}

func (r *finalizerRunner) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	r.calls = append(r.calls, append([]string{name}, args...))
	index := len(r.calls) - 1
	var err error
	if index < len(r.errs) {
		err = r.errs[index]
	}
	if index >= len(r.responses) {
		return nil, err
	}
	return r.responses[index], err
}

func TestFinalizerUsesPurePublicationAndExactUnchangedHeadGate(t *testing.T) {
	store, _, worktree := openTestStore(t, Options{})
	execution, _, err := store.StartExecution(context.Background(), StartExecutionInput{
		NativeSession: NativeSessionIdentity{Runtime: "claude", ID: "finalizer-session"}, Prompt: "finalize", Goal: "ship",
		Runtime: RuntimeSpec{Name: "claude", Sandbox: SandboxWorkspaceWrite}, Root: worktree,
		Authority: AuthoritySpec{RequestedBy: "human", HumanAuthorized: true, Sandbox: SandboxWorkspaceWrite},
		Finalizer: FinalizerSpec{Enabled: true, RequiredChecks: []string{"verify"}, RequireHuman: true},
		Budget:    DefaultBudget(), IdempotencyKey: "finalizer-start-01",
	})
	if err != nil {
		t.Fatal(err)
	}
	state, _ := store.Projection()
	activity := state.Activities[execution.FirstActivity]
	completeActivity(t, store, activity, "finalizer-result")
	runner := &finalizerRunner{responses: [][]byte{
		[]byte(`{"number":7,"url":"https://example/pr/7","headRefOid":"abc123","mergeStateStatus":"CLEAN","statusCheckRollup":[{"name":"verify","status":"COMPLETED","conclusion":"SUCCESS"}]}`),
		[]byte(`{"number":7,"url":"https://example/pr/7","headRefOid":"abc123","mergeStateStatus":"CLEAN","statusCheckRollup":[{"name":"verify","status":"COMPLETED","conclusion":"SUCCESS"}]}`),
		[]byte("ok"),
	}}
	finalized, err := store.Finalize(context.Background(), FinalizationRequest{ExecutionID: execution.ID, Repository: "o/r", PullRequest: "7", HumanApproved: true, IdempotencyKey: "finalize-publication-01"}, runner)
	if err != nil || !finalized.Merged || finalized.HeadSHA != "abc123" {
		t.Fatalf("finalization=%+v err=%v", finalized, err)
	}
	want := []string{"gh", "pr", "merge", "7", "--repo", "o/r", "--match-head-commit", "abc123", "--squash"}
	if !reflect.DeepEqual(runner.calls[2], want) {
		t.Fatalf("merge argv=%v", runner.calls[2])
	}
}

func TestFinalizerAllowsOptionalHumanApprovalAfterResultAndExternalChecks(t *testing.T) {
	store, _, worktree := openTestStore(t, Options{})
	execution, _, err := store.StartExecution(context.Background(), StartExecutionInput{
		NativeSession: NativeSessionIdentity{Runtime: "codex", ID: "optional-human-session"}, Prompt: "finalize", Goal: "ship",
		Runtime: RuntimeSpec{Name: "codex", Sandbox: SandboxWorkspaceWrite}, Root: worktree,
		Authority: AuthoritySpec{RequestedBy: "human", HumanAuthorized: true, Sandbox: SandboxWorkspaceWrite},
		Finalizer: FinalizerSpec{Enabled: true, RequiredChecks: []string{"lint", "verify"}},
		Budget:    DefaultBudget(), IdempotencyKey: "optional-human-start",
	})
	if err != nil {
		t.Fatal(err)
	}
	view, err := store.View(execution.ID, time.Unix(100, 0).UTC())
	if err != nil || view.Publication != PublicationAwaitingResult {
		t.Fatalf("publication was not held until an immutable Result: view=%+v err=%v", view, err)
	}
	if _, _, err = store.PrepareFinalization(context.Background(), PrepareFinalizationInput{
		ExecutionID: execution.ID, Repository: "o/r", PullRequest: "7", Gates: []string{"lint", "verify"}, Method: "squash", HeadSHA: "abc123", IdempotencyKey: "optional-human-before-result",
	}); err == nil {
		t.Fatal("publication was prepared before an immutable Result")
	}
	state, _ := store.Projection()
	completeActivity(t, store, state.Activities[execution.FirstActivity], "optional-human-result")
	view, err = store.View(execution.ID, time.Unix(101, 0).UTC())
	if err != nil || view.Publication != PublicationEligible {
		t.Fatalf("publication did not become eligible after the Result without human policy: view=%+v err=%v", view, err)
	}
	runner := &finalizerRunner{responses: [][]byte{
		[]byte(`{"number":7,"url":"https://example/pr/7","headRefOid":"abc123","mergeStateStatus":"CLEAN","statusCheckRollup":[{"name":"lint","status":"COMPLETED","conclusion":"SUCCESS"},{"name":"verify","status":"COMPLETED","conclusion":"SUCCESS"}]}`),
		[]byte(`{"number":7,"url":"https://example/pr/7","headRefOid":"abc123","mergeStateStatus":"CLEAN","statusCheckRollup":[{"name":"lint","status":"COMPLETED","conclusion":"SUCCESS"},{"name":"verify","status":"COMPLETED","conclusion":"SUCCESS"}]}`),
		[]byte("ok"),
	}}
	finalized, err := store.Finalize(context.Background(), FinalizationRequest{ExecutionID: execution.ID, Repository: "o/r", PullRequest: "7", IdempotencyKey: "optional-human-publication"}, runner)
	if err != nil || !finalized.Merged {
		t.Fatalf("optional human finalization failed after independent checks: result=%+v err=%v", finalized, err)
	}
}

func prepareEligibleFinalization(t *testing.T, store *Store, worktree, key string) (*Execution, *Finalization) {
	t.Helper()
	execution, _, err := store.StartExecution(context.Background(), StartExecutionInput{
		NativeSession: NativeSessionIdentity{Runtime: "claude", ID: key + "-session"}, Prompt: "finalize", Goal: "ship",
		Runtime: RuntimeSpec{Name: "claude", Sandbox: SandboxWorkspaceWrite}, Root: worktree,
		Authority: AuthoritySpec{RequestedBy: "human", HumanAuthorized: true, Sandbox: SandboxWorkspaceWrite},
		Finalizer: FinalizerSpec{Enabled: true, RequiredChecks: []string{"verify"}, RequireHuman: true},
		Budget:    DefaultBudget(), IdempotencyKey: key + "-start",
	})
	if err != nil {
		t.Fatal(err)
	}
	state, _ := store.Projection()
	completeActivity(t, store, state.Activities[execution.FirstActivity], key+"-result")
	prepared, _, err := store.PrepareFinalization(context.Background(), PrepareFinalizationInput{ExecutionID: execution.ID, Repository: "o/r", PullRequest: "7", Gates: []string{"verify"}, Method: "squash", HumanApproved: true, HeadSHA: "abc123", PRURL: "https://example/pr/7", IdempotencyKey: key + "-publication"})
	if err != nil {
		t.Fatal(err)
	}
	return execution, prepared
}

func TestFinalizerRequiredChecksCannotBeOverridden(t *testing.T) {
	store, _, worktree := openTestStore(t, Options{})
	checks := []string{"lint", "verify"}
	execution, _, err := store.StartExecution(context.Background(), StartExecutionInput{
		NativeSession: NativeSessionIdentity{Runtime: "claude", ID: "finalizer-check-policy-session"}, Prompt: "finalize", Goal: "ship",
		Runtime: RuntimeSpec{Name: "claude", Sandbox: SandboxWorkspaceWrite}, Root: worktree,
		Authority: AuthoritySpec{RequestedBy: "human", HumanAuthorized: true, Sandbox: SandboxWorkspaceWrite},
		Finalizer: FinalizerSpec{Enabled: true, RequiredChecks: checks, RequireHuman: true},
		Budget:    DefaultBudget(), IdempotencyKey: "finalizer-check-policy-start",
	})
	if err != nil {
		t.Fatal(err)
	}
	state, _ := store.Projection()
	completeActivity(t, store, state.Activities[execution.FirstActivity], "finalizer-check-policy-result")

	cases := []struct {
		name  string
		gates []string
	}{
		{name: "omitted", gates: nil},
		{name: "substituted", gates: []string{"lint", "other"}},
		{name: "duplicate", gates: []string{"lint", "verify", "lint"}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := store.PrepareFinalization(context.Background(), PrepareFinalizationInput{
				ExecutionID: execution.ID, Repository: "o/r", PullRequest: "7", Gates: test.gates, Method: "squash", HumanApproved: true, HeadSHA: "abc123", PRURL: "https://example/pr/7", IdempotencyKey: "finalizer-check-policy-" + test.name,
			})
			if err == nil {
				t.Fatalf("weaker check set was accepted: %v", test.gates)
			}
		})
	}

	prepared, _, err := store.PrepareFinalization(context.Background(), PrepareFinalizationInput{
		ExecutionID: execution.ID, Repository: "o/r", PullRequest: "7", Gates: []string{"verify", "lint"}, Method: "squash", HumanApproved: true, HeadSHA: "abc123", PRURL: "https://example/pr/7", IdempotencyKey: "finalizer-check-policy-exact",
	})
	if err != nil || !reflect.DeepEqual(prepared.Gates, checks) {
		t.Fatalf("canonical configured checks were not retained: prepared=%+v err=%v", prepared, err)
	}

	runner := &finalizerRunner{}
	_, err = store.Finalize(context.Background(), FinalizationRequest{ExecutionID: execution.ID, Repository: "o/r", PullRequest: "7", Gates: []string{"verify"}, HumanApproved: true, IdempotencyKey: "finalizer-check-policy-weaker"}, runner)
	if err == nil || len(runner.calls) != 0 {
		t.Fatalf("Finalize accepted or inspected with weaker caller gates: err=%v calls=%v", err, runner.calls)
	}
}

func TestFinalizerCrashAfterPreparedSettlementIsIdempotent(t *testing.T) {
	store, stateRoot, worktree := openTestStore(t, Options{})
	execution, prepared := prepareEligibleFinalization(t, store, worktree, "finalizer-crash")
	fired := false
	crashed, err := Open(stateRoot, Options{Fault: func(boundary Boundary) error {
		if boundary == BoundaryAfterAppend && !fired {
			fired = true
			return errors.New("crash after settlement append")
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = crashed.SettleFinalization(context.Background(), SettleFinalizationInput{FinalizationID: prepared.ID, State: FinalizationMerged, Summary: "merged", PRURL: prepared.PRURL, IdempotencyKey: "finalizer-crash-settle"}); err == nil {
		t.Fatal("fault injection did not make settlement ambiguous")
	}
	reopened, err := Open(stateRoot, Options{})
	if err != nil {
		t.Fatal(err)
	}
	state, err := reopened.Projection()
	if err != nil || state.Finalizations[prepared.ID].State != FinalizationMerged {
		t.Fatalf("crash replay lost terminal publication: state=%+v err=%v", state.Finalizations[prepared.ID], err)
	}
	result, err := reopened.Finalize(context.Background(), FinalizationRequest{ExecutionID: execution.ID, Repository: "o/r", PullRequest: "7", HumanApproved: true, IdempotencyKey: "finalizer-crash-publication"}, &finalizerRunner{})
	if err != nil || !result.Merged {
		t.Fatalf("terminal retry was not idempotent: result=%+v err=%v", result, err)
	}
}

func TestFinalizerChangedHeadSettlesBlockedAndRejectsDivergentRetry(t *testing.T) {
	store, _, worktree := openTestStore(t, Options{})
	execution, _ := prepareEligibleFinalization(t, store, worktree, "finalizer-head")
	runner := &finalizerRunner{responses: [][]byte{
		[]byte(`{"number":7,"url":"https://example/pr/7","headRefOid":"def456","mergeStateStatus":"CLEAN","statusCheckRollup":[{"name":"verify","status":"COMPLETED","conclusion":"SUCCESS"}]}`),
	}}
	result, err := store.Finalize(context.Background(), FinalizationRequest{ExecutionID: execution.ID, Repository: "o/r", PullRequest: "7", HumanApproved: true, IdempotencyKey: "finalizer-head-publication"}, runner)
	if err == nil || result.State != FinalizationBlocked || result.Merged {
		t.Fatalf("changed head was not durably blocked: result=%+v err=%v", result, err)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("changed head should be fenced before merge: calls=%v", runner.calls)
	}
	_, retryErr := store.Finalize(context.Background(), FinalizationRequest{ExecutionID: execution.ID, Repository: "other/repo", PullRequest: "7", HumanApproved: true, IdempotencyKey: "finalizer-head-publication"}, &finalizerRunner{})
	if !errors.Is(retryErr, ErrIdempotencyConflict) {
		t.Fatalf("divergent finalization reuse was not rejected: %v", retryErr)
	}
}

func TestFinalizerRetriesUnknownGitHubEffectByInspectingMergedState(t *testing.T) {
	store, _, worktree := openTestStore(t, Options{})
	execution, _ := prepareEligibleFinalization(t, store, worktree, "finalizer-effect")
	open := []byte(`{"number":7,"url":"https://example/pr/7","headRefOid":"abc123","mergeStateStatus":"CLEAN","statusCheckRollup":[{"name":"verify","status":"COMPLETED","conclusion":"SUCCESS"}]}`)
	first := &finalizerRunner{responses: [][]byte{open, []byte("response lost")}, errs: []error{nil, errors.New("connection lost after merge")}}
	request := FinalizationRequest{ExecutionID: execution.ID, Repository: "o/r", PullRequest: "7", HumanApproved: true, IdempotencyKey: "finalizer-effect-publication"}
	prepared, err := store.Finalize(context.Background(), request, first)
	if err == nil || prepared.State != FinalizationPrepared {
		t.Fatalf("unknown GitHub effect was made terminal: result=%+v err=%v", prepared, err)
	}
	merged := &finalizerRunner{responses: [][]byte{[]byte(`{"number":7,"url":"https://example/pr/7","headRefOid":"abc123","state":"MERGED","mergedAt":"2026-08-07T00:00:00Z","mergeStateStatus":"CLEAN","statusCheckRollup":[{"name":"verify","status":"COMPLETED","conclusion":"SUCCESS"}]}`)}}
	settled, err := store.Finalize(context.Background(), request, merged)
	if err != nil || settled.State != FinalizationMerged || !settled.Merged {
		t.Fatalf("retry did not settle observed merged outcome: result=%+v err=%v", settled, err)
	}
}
