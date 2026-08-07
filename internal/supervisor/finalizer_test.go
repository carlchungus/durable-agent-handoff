package supervisor

import (
	"context"
	"reflect"
	"testing"
)

type finalizerRunner struct {
	responses [][]byte
	calls     [][]string
}

func (r *finalizerRunner) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	r.calls = append(r.calls, append([]string{name}, args...))
	return r.responses[len(r.calls)-1], nil
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
	finalized, err := store.Finalize(context.Background(), FinalizationRequest{ExecutionID: execution.ID, Repository: "o/r", PullRequest: "7", HumanApproved: true}, runner)
	if err != nil || !finalized.Merged || finalized.HeadSHA != "abc123" {
		t.Fatalf("finalization=%+v err=%v", finalized, err)
	}
	want := []string{"gh", "pr", "merge", "7", "--repo", "o/r", "--match-head-commit", "abc123", "--squash"}
	if !reflect.DeepEqual(runner.calls[2], want) {
		t.Fatalf("merge argv=%v", runner.calls[2])
	}
}
