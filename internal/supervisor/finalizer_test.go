package supervisor

import (
	"context"
	"errors"
	"reflect"
	"testing"
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
		Finalizer: FinalizerSpec{Enabled: true, RequiredChecks: []string{"verify"}, RequireHuman: true, RequireVerifier: true, Verifiers: []string{"independent"}},
		Budget:    DefaultBudget(), IdempotencyKey: "finalizer-start-01",
	})
	if err != nil {
		t.Fatal(err)
	}
	state, _ := store.Projection()
	activity := state.Activities[execution.FirstActivity]
	result := completeActivity(t, store, activity, "finalizer-result")
	if _, _, err = store.RecordAttestation(context.Background(), RecordAttestationInput{ResultID: result.ID, Verifier: "independent", Verdict: "pass", Summary: "independent pass", IdempotencyKey: "finalizer-attestation-01"}); err != nil {
		t.Fatal(err)
	}
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

func prepareEligibleFinalization(t *testing.T, store *Store, worktree, key string) (*Execution, *Finalization) {
	t.Helper()
	execution, _, err := store.StartExecution(context.Background(), StartExecutionInput{
		NativeSession: NativeSessionIdentity{Runtime: "claude", ID: key + "-session"}, Prompt: "finalize", Goal: "ship",
		Runtime: RuntimeSpec{Name: "claude", Sandbox: SandboxWorkspaceWrite}, Root: worktree,
		Authority: AuthoritySpec{RequestedBy: "human", HumanAuthorized: true, Sandbox: SandboxWorkspaceWrite},
		Finalizer: FinalizerSpec{Enabled: true, RequiredChecks: []string{"verify"}, RequireHuman: true, RequireVerifier: true, Verifiers: []string{"independent"}},
		Budget:    DefaultBudget(), IdempotencyKey: key + "-start",
	})
	if err != nil {
		t.Fatal(err)
	}
	state, _ := store.Projection()
	result := completeActivity(t, store, state.Activities[execution.FirstActivity], key+"-result")
	if _, _, err = store.RecordAttestation(context.Background(), RecordAttestationInput{ResultID: result.ID, Verifier: "independent", Verdict: "pass", Summary: "independent pass", IdempotencyKey: key + "-attestation"}); err != nil {
		t.Fatal(err)
	}
	prepared, _, err := store.PrepareFinalization(context.Background(), PrepareFinalizationInput{ExecutionID: execution.ID, Repository: "o/r", PullRequest: "7", Gates: []string{"verify"}, Method: "squash", HumanApproved: true, HeadSHA: "abc123", PRURL: "https://example/pr/7", IdempotencyKey: key + "-publication"})
	if err != nil {
		t.Fatal(err)
	}
	return execution, prepared
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
