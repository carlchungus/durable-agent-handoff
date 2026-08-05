package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/core"
	"github.com/carlchungus/durable-agent-handoff/internal/finalize"
	"github.com/carlchungus/durable-agent-handoff/internal/runstate"
)

func TestResultSchemaDefinesArrayItems(t *testing.T) {
	var schema struct {
		Properties map[string]struct {
			Type  string          `json:"type"`
			Items json.RawMessage `json:"items"`
		} `json:"properties"`
	}
	if err := json.Unmarshal([]byte(resultSchema), &schema); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"mutations", "attestations"} {
		property := schema.Properties[name]
		if property.Type != "array" || len(property.Items) == 0 {
			t.Fatalf("%s must define array items: %s", name, resultSchema)
		}
	}
}

func TestResultSchemaConstrainsVerifierVerdicts(t *testing.T) {
	var schema struct {
		Properties map[string]struct {
			Items struct {
				Properties map[string]struct {
					Enum []string `json:"enum"`
				} `json:"properties"`
			} `json:"items"`
		} `json:"properties"`
	}
	if err := json.Unmarshal([]byte(resultSchema), &schema); err != nil {
		t.Fatal(err)
	}
	got := schema.Properties["attestations"].Items.Properties["verdict"].Enum
	want := []string{"pass", "repair", "blocked", "pass_with_limit", "pass_with_runtime_limit", "fail_blocking"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("verdict enum=%v, want %v", got, want)
	}
}

func TestDiffBudgetStopsFurtherAutonomy(t *testing.T) {
	budget := core.DefaultBudget()
	if exceedsDiffBudget(budget, finalize.DiffStats{Files: budget.MaxChangedFiles, Lines: budget.MaxDiffLines}) {
		t.Fatal("the exact authorized budget should be allowed")
	}
	if !exceedsDiffBudget(budget, finalize.DiffStats{Files: budget.MaxChangedFiles + 1}) {
		t.Fatal("changed-file overflow must stop autonomy")
	}
	if !exceedsDiffBudget(budget, finalize.DiffStats{Lines: budget.MaxDiffLines + 1}) {
		t.Fatal("diff-line overflow must stop autonomy")
	}
}

func TestResultAcceptsObjectAndEncodedMutations(t *testing.T) {
	for _, input := range []string{
		`{"status":"continue","summary":"adapt","mutations":[{"op":"set_state","node_id":"next","state":"ready"}]}`,
		`{"status":"continue","summary":"adapt","mutations":["{\"op\":\"set_state\",\"node_id\":\"next\",\"state\":\"ready\"}"]}`,
	} {
		var result Result
		if err := json.Unmarshal([]byte(input), &result); err != nil {
			t.Fatal(err)
		}
		if len(result.Mutations) != 1 || result.Mutations[0].NodeID != "next" {
			t.Fatalf("unexpected mutations: %#v", result.Mutations)
		}
	}
}

func TestResultTranslatesTaskShapedAddNodeMutation(t *testing.T) {
	input := `{"status":"continue","summary":"more work","session_id":"lead","mutations":["{\"kind\":\"add_node\",\"node_id\":\"query_plan\",\"parent_id\":\"lead\",\"priority\":\"P1\",\"task\":\"Verify the access query plan.\",\"authority\":\"worktree_only\"}"],"attestations":[]}`
	var result Result
	if err := json.Unmarshal([]byte(input), &result); err != nil {
		t.Fatal(err)
	}
	mutation := result.Mutations[0].Mutation
	if mutation.Op != "add_node" || mutation.Node == nil || mutation.Node.ID != "query_plan" || mutation.Node.Prompt != "Verify the access query plan." {
		t.Fatalf("mutation=%+v", mutation)
	}
	if len(mutation.Node.DependsOn) != 0 || mutation.Node.Metadata["parent_id"] != "lead" {
		t.Fatalf("dependencies=%v metadata=%v", mutation.Node.DependsOn, mutation.Node.Metadata)
	}
}

func TestApplyResultPrefersRuntimeSessionAndNormalizesLimitedAttestation(t *testing.T) {
	st, _ := core.OpenStore(t.TempDir())
	w, _ := st.Create("continue safely", t.TempDir(), core.DefaultBudget())
	n := &core.Node{ID: "lead", Title: "lead", Kind: "agent", Runtime: core.RuntimeSpec{Name: "codex", Model: "gpt-test"}}
	w, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []core.Mutation{{Op: "add_node", Node: n}}})
	w, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: []core.Mutation{{Op: "set_state", NodeID: "lead", State: core.NodeRunning}}})
	result := Result{
		Status:    "continue",
		Summary:   "spawn verifier",
		SessionID: "lead",
		Mutations: []encodedMutation{{Mutation: core.Mutation{
			Op: "add_node",
			Node: &core.Node{
				ID: "verify", Title: "verify", Kind: "agent", DependsOn: []string{"lead"},
			},
		}}},
		Attestations: []core.Attestation{{Verifier: "test", Verdict: "pass_with_runtime_limit", Summary: "partial"}},
	}
	if err := (&Engine{Store: st}).applyAgentResult(w, w.Nodes["lead"], result, "019fd182-2edc-78a0-b9c0-d4968a5a5cbb", 1); err != nil {
		t.Fatal(err)
	}
	got, _ := st.Load(w.ID)
	if got.Nodes["lead"].SessionID != "019fd182-2edc-78a0-b9c0-d4968a5a5cbb" {
		t.Fatalf("session=%q", got.Nodes["lead"].SessionID)
	}
	if got.Nodes["verify"].Runtime.Name != "codex" || got.Attestations[0].Verdict != "repair" {
		t.Fatalf("verify=%+v attestations=%+v", got.Nodes["verify"], got.Attestations)
	}
}

func runningVerifierWorkflow(t *testing.T, runtimeName string) (*core.Store, *core.Workflow) {
	t.Helper()
	st, err := core.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	w, err := st.Create("verify safely", t.TempDir(), core.DefaultBudget())
	if err != nil {
		t.Fatal(err)
	}
	n := &core.Node{ID: "verify", Title: "verify", Kind: "agent", Runtime: core.RuntimeSpec{Name: runtimeName, Sandbox: "read-only"}, MaxAttempts: 1}
	w, err = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []core.Mutation{{Op: "add_node", Node: n}}})
	if err != nil {
		t.Fatal(err)
	}
	w, err = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: []core.Mutation{{Op: "set_state", NodeID: "verify", State: core.NodeRunning}}})
	if err != nil {
		t.Fatal(err)
	}
	return st, w
}

func TestApplyResultNormalizesBlockingAttestationAndPreservesRawEvidence(t *testing.T) {
	st, w := runningVerifierWorkflow(t, "codex")

	result := Result{
		Status:  "completed",
		Summary: "unsafe to merge",
		Attestations: []core.Attestation{{
			Verifier:    "fresh-reviewer",
			Verdict:     "fail_blocking",
			Summary:     "tenant boundary is not enforced",
			EvidenceIDs: []string{"test-output", "diff-review"},
		}},
	}
	if err := (&Engine{Store: st}).applyAgentResult(w, w.Nodes["verify"], result, "", 1); err != nil {
		t.Fatal(err)
	}

	got, _ := st.Load(w.ID)
	if got.Nodes["verify"].State != core.NodeCompleted || len(got.Attestations) != 1 {
		t.Fatalf("node=%+v attestations=%+v evidence=%+v", got.Nodes["verify"], got.Attestations, got.Evidence)
	}
	attestationJSON, err := json.Marshal(got.Attestations[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"verdict":"blocked"`, `"raw_verdict":"fail_blocking"`, `"summary":"tenant boundary is not enforced"`, `"evidence_ids":["test-output","diff-review"]`} {
		if !strings.Contains(string(attestationJSON), want) {
			t.Fatalf("attestation lost %s: %s", want, attestationJSON)
		}
	}
}

func TestApplyResultTreatsLimitedPassAsRepair(t *testing.T) {
	st, w := runningVerifierWorkflow(t, "codex")

	result := Result{Status: "completed", Summary: "verification was incomplete", Attestations: []core.Attestation{{
		Verifier: "fresh-reviewer", Verdict: "pass_with_limit", Summary: "unit tests passed but the required browser check could not run", EvidenceIDs: []string{"unit-tests"},
	}}}
	if err := (&Engine{Store: st}).applyAgentResult(w, w.Nodes["verify"], result, "", 1); err != nil {
		t.Fatal(err)
	}

	got, _ := st.Load(w.ID)
	if len(got.Attestations) != 1 {
		t.Fatalf("attestations=%+v evidence=%+v", got.Attestations, got.Evidence)
	}
	a := got.Attestations[0]
	if a.Verdict != "repair" || a.RawVerdict != "pass_with_limit" || a.Summary != "unit tests passed but the required browser check could not run" || len(a.EvidenceIDs) != 1 || a.EvidenceIDs[0] != "unit-tests" {
		t.Fatalf("attestation=%+v", a)
	}
	if got.Status == core.WorkflowCompleted {
		t.Fatalf("qualified pass must not satisfy the merge attestation gate: workflow=%s", got.Status)
	}
}

func TestApplyResultRejectsUnknownSemanticAttestation(t *testing.T) {
	st, w := runningVerifierWorkflow(t, "exec")

	result := Result{Status: "completed", Summary: "invented a favorable verdict", Attestations: []core.Attestation{{
		Verifier: "fresh-reviewer", Verdict: "pass_probably", Summary: "not a recognized attestation",
	}}}
	if err := (&Engine{Store: st}).applyAgentResult(w, w.Nodes["verify"], result, "", 1); err != nil {
		t.Fatal(err)
	}

	got, _ := st.Load(w.ID)
	if got.Nodes["verify"].State != core.NodeFailed || len(got.Attestations) != 0 || got.Status == core.WorkflowCompleted {
		t.Fatalf("unknown verdict escaped fail-closed handling: node=%+v workflow=%s attestations=%+v", got.Nodes["verify"], got.Status, got.Attestations)
	}
	if len(got.Evidence) == 0 || !strings.Contains(got.Evidence[len(got.Evidence)-1].Summary, "attestation verdict must be pass, repair, or blocked") {
		t.Fatalf("rejection evidence=%+v", got.Evidence)
	}
}

func TestApplyResultDoesNotTrustRuntimeAuthoredRawVerdict(t *testing.T) {
	st, w := runningVerifierWorkflow(t, "exec")

	result := Result{Status: "completed", Summary: "canonical pass", Attestations: []core.Attestation{{
		Verifier: "fresh-reviewer", Verdict: "pass", RawVerdict: "fail_blocking", Summary: "canonical source verdict is authoritative",
	}}}
	if err := (&Engine{Store: st}).applyAgentResult(w, w.Nodes["verify"], result, "", 1); err != nil {
		t.Fatal(err)
	}

	got, _ := st.Load(w.ID)
	if len(got.Attestations) != 1 || got.Attestations[0].Verdict != "pass" || got.Attestations[0].RawVerdict != "" {
		t.Fatalf("runtime supplied derived provenance: %+v", got.Attestations)
	}
}

func TestRuntimeEventObserverReadsDirectChildOutput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	stop := make(chan struct{})
	session := make(chan string, 1)
	done := observeRuntimeEvents(path, stop, func(id string) { session <- id })

	line := []byte("{\"type\":\"thread.started\",\"thread_id\":\"019fd17f-f95a-76e2-b0fe-35efee5fabda\"}\n")
	if _, err = f.Write(line); err != nil {
		t.Fatal(err)
	}
	onDisk, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(onDisk) != string(line) {
		t.Fatalf("runtime stream was not visible before close: %q", onDisk)
	}
	select {
	case sessionID := <-session:
		if sessionID != "019fd17f-f95a-76e2-b0fe-35efee5fabda" {
			t.Fatalf("session callback=%q", sessionID)
		}
	case <-time.After(time.Second):
		t.Fatal("observer did not report the live session id")
	}
	close(stop)
	<-done
}

func TestEngineRunsGenericAgentAndPersistsSession(t *testing.T) {
	if os.Getenv("GO_WANT_HANDOFF_HELPER") == "1" {
		fmt.Println(`{"thread_id":"session-abcdef"}`)
		fmt.Println(`{"status":"completed","summary":"implemented safely","session_id":"session-abcdef","attestations":[{"verifier":"helper","verdict":"pass","summary":"checked"}]}`)
		os.Exit(0)
	}
	t.Setenv("GO_WANT_HANDOFF_HELPER", "1")
	st, err := core.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	w, err := st.Create("test generic runtime", t.TempDir(), core.DefaultBudget())
	if err != nil {
		t.Fatal(err)
	}
	n := &core.Node{ID: "lead", Title: "lead", Kind: "agent", Runtime: core.RuntimeSpec{Name: "exec", Executable: os.Args[0], Args: []string{"-test.run=TestEngineRunsGenericAgentAndPersistsSession"}}}
	if _, err = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []core.Mutation{{Op: "add_node", Node: n}}}); err != nil {
		t.Fatal(err)
	}
	eng := Engine{Store: st}
	if _, err = eng.RunOne(context.Background(), w.ID); err != nil {
		t.Fatal(err)
	}
	got, err := st.Load(w.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Nodes["lead"].SessionID != "session-abcdef" {
		t.Fatalf("session=%q", got.Nodes["lead"].SessionID)
	}
	if got.Nodes["lead"].State != core.NodeCompleted || got.Status != core.WorkflowCompleted {
		t.Fatalf("node=%s workflow=%s", got.Nodes["lead"].State, got.Status)
	}
}

func TestInvalidAgentMutationFailsClosed(t *testing.T) {
	if os.Getenv("GO_WANT_BAD_HANDOFF_HELPER") == "1" {
		fmt.Println(`{"status":"completed","summary":"tried to escalate","mutations":[{"op":"add_node","node":{"id":"merge-now","title":"merge","kind":"merge"}}]}`)
		os.Exit(0)
	}
	t.Setenv("GO_WANT_BAD_HANDOFF_HELPER", "1")
	st, _ := core.OpenStore(t.TempDir())
	w, _ := st.Create("test rejection", t.TempDir(), core.DefaultBudget())
	n := &core.Node{ID: "lead", Title: "lead", Kind: "agent", Runtime: core.RuntimeSpec{Name: "exec", Executable: os.Args[0], Args: []string{"-test.run=TestInvalidAgentMutationFailsClosed"}}, MaxAttempts: 1}
	_, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []core.Mutation{{Op: "add_node", Node: n}}})
	err := func() error { _, err := (&Engine{Store: st}).RunOne(context.Background(), w.ID); return err }()
	if err != nil {
		t.Fatal(err)
	}
	got, _ := st.Load(w.ID)
	if got.Nodes["lead"].State != core.NodeFailed {
		t.Fatalf("state=%s", got.Nodes["lead"].State)
	}
	if len(got.Evidence) == 0 || !strings.Contains(got.Evidence[len(got.Evidence)-1].Summary, "invalid workflow mutation") {
		t.Fatalf("evidence=%#v", got.Evidence)
	}
}

func TestReconcileResumesOnlyExactPersistedSession(t *testing.T) {
	state := t.TempDir()
	st, _ := core.OpenStore(state)
	w, _ := st.Create("recover interrupted work", t.TempDir(), core.DefaultBudget())
	n := &core.Node{ID: "lead", Title: "lead", Kind: "agent", Runtime: core.RuntimeSpec{Name: "codex"}}
	_, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []core.Mutation{{Op: "add_node", Node: n}}})
	_, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: []core.Mutation{{Op: "set_state", NodeID: "lead", State: core.NodeRunning}}})
	dir := filepath.Join(state, "workflows", w.ID, "runs", "lead", "1")
	_, err := runstate.Create(filepath.Join(dir, "attempt.json"), runstate.Manifest{ID: "lead/1", WorkflowID: w.ID, NodeID: "lead", Attempt: 1, Runtime: "codex", SessionID: "019fd17f-f95a-76e2-b0fe-35efee5fabda", PID: 999999, ProcessStartToken: "gone"})
	if err != nil {
		t.Fatal(err)
	}
	if err = (&Engine{Store: st}).Reconcile(context.Background(), w.ID); err != nil {
		t.Fatal(err)
	}
	got, _ := st.Load(w.ID)
	if got.Nodes["lead"].State != core.NodeReady || got.Nodes["lead"].SessionID != "019fd17f-f95a-76e2-b0fe-35efee5fabda" {
		t.Fatalf("node=%+v", got.Nodes["lead"])
	}
}

func TestRecoverAttemptReappliesRejectedCompletedResult(t *testing.T) {
	state := t.TempDir()
	st, _ := core.OpenStore(state)
	w, _ := st.Create("recover rejected result", t.TempDir(), core.DefaultBudget())
	n := &core.Node{ID: "lead", Title: "lead", Kind: "agent", Runtime: core.RuntimeSpec{Name: "codex", Model: "gpt-test"}}
	_, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []core.Mutation{{Op: "add_node", Node: n}}})
	_, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: []core.Mutation{{Op: "set_state", NodeID: "lead", State: core.NodeRunning}}})
	_, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: []core.Mutation{{Op: "set_state", NodeID: "lead", State: core.NodeReady}}})
	dir := filepath.Join(state, "workflows", w.ID, "runs", "lead", "1")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	result := `{"status":"continue","summary":"delegate follow-up","session_id":"lead","mutations":["{\"kind\":\"add_node\",\"node_id\":\"query_plan\",\"parent_id\":\"lead\",\"priority\":\"P1\",\"task\":\"Verify the query plan.\",\"authority\":\"worktree_only\"}"],"attestations":[]}`
	if err := os.WriteFile(filepath.Join(dir, "last-message.json"), []byte(result), 0o600); err != nil {
		t.Fatal(err)
	}
	events := "{\"type\":\"thread.started\",\"thread_id\":\"019fd182-2edc-78a0-b9c0-d4968a5a5cbb\"}\n"
	if err := os.WriteFile(filepath.Join(dir, "events.jsonl"), []byte(events), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := (&Engine{Store: st}).RecoverAttempt(w.ID, "lead", 1); err != nil {
		t.Fatal(err)
	}
	got, _ := st.Load(w.ID)
	if got.Nodes["lead"].SessionID != "019fd182-2edc-78a0-b9c0-d4968a5a5cbb" || got.Nodes["query_plan"].State != core.NodeReady {
		t.Fatalf("lead=%+v query=%+v", got.Nodes["lead"], got.Nodes["query_plan"])
	}
}

func TestReconcileStopsOnAmbiguousNonIdempotentAttempt(t *testing.T) {
	state := t.TempDir()
	st, _ := core.OpenStore(state)
	w, _ := st.Create("do not duplicate side effects", t.TempDir(), core.DefaultBudget())
	n := &core.Node{ID: "lead", Title: "lead", Kind: "agent", Runtime: core.RuntimeSpec{Name: "exec"}}
	_, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []core.Mutation{{Op: "add_node", Node: n}}})
	_, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: []core.Mutation{{Op: "set_state", NodeID: "lead", State: core.NodeRunning}}})
	if err := (&Engine{Store: st}).Reconcile(context.Background(), w.ID); err != nil {
		t.Fatal(err)
	}
	got, _ := st.Load(w.ID)
	if got.Nodes["lead"].State != core.NodeWaiting || got.Status != core.WorkflowNeedsHuman {
		t.Fatalf("node=%s workflow=%s", got.Nodes["lead"].State, got.Status)
	}
}

func TestRuntimeChildSurvivesSupervisorCrashAndReconciles(t *testing.T) {
	if os.Getenv("GO_WANT_CRASH_RUNTIME") == "1" && len(os.Args) > 2 {
		fmt.Println(`{"type":"thread.started","thread_id":"019fd17f-f95a-76e2-b0fe-35efee5fabda"}`)
		_ = os.Stdout.Sync()
		time.Sleep(350 * time.Millisecond)
		fmt.Println(`{"status":"completed","summary":"survived supervisor crash","session_id":"019fd17f-f95a-76e2-b0fe-35efee5fabda","mutations":[],"attestations":[{"verifier":"crash-test","verdict":"pass","summary":"child completed","evidence_ids":[]}]}`)
		_ = os.Stdout.Sync()
		os.Exit(0)
	}
	if os.Getenv("GO_WANT_CRASH_SUPERVISOR") == "1" {
		st, err := core.OpenStore(os.Getenv("HANDOFF_TEST_STATE"))
		if err != nil {
			os.Exit(2)
		}
		_, _ = (&Engine{Store: st}).RunOne(context.Background(), os.Getenv("HANDOFF_TEST_WORKFLOW"))
		os.Exit(0)
	}
	if runtime.GOOS == "windows" {
		t.Skip("process adoption is covered by the Windows service integration suite")
	}

	state := t.TempDir()
	st, _ := core.OpenStore(state)
	w, _ := st.Create("survive a supervisor crash", t.TempDir(), core.DefaultBudget())
	n := &core.Node{ID: "lead", Title: "lead", Kind: "agent", Runtime: core.RuntimeSpec{Name: "exec", Executable: os.Args[0], Args: []string{"-test.run=TestRuntimeChildSurvivesSupervisorCrashAndReconciles"}}}
	_, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []core.Mutation{{Op: "add_node", Node: n}}})

	supervisor := exec.Command(os.Args[0], "-test.run=TestRuntimeChildSurvivesSupervisorCrashAndReconciles")
	supervisor.Env = append(os.Environ(), "GO_WANT_CRASH_SUPERVISOR=1", "GO_WANT_CRASH_RUNTIME=1", "HANDOFF_TEST_STATE="+state, "HANDOFF_TEST_WORKFLOW="+w.ID)
	if err := supervisor.Start(); err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(state, "workflows", w.ID, "runs", "lead", "1", "attempt.json")
	var manifest runstate.Manifest
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		manifest, _ = runstate.Load(manifestPath)
		if manifest.Status == "running" && manifest.PID > 0 && manifest.SessionID != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if manifest.PID == 0 || manifest.SessionID == "" {
		_ = supervisor.Process.Kill()
		t.Fatalf("worker identity was not persisted before crash: %+v", manifest)
	}
	if err := supervisor.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_, _ = supervisor.Process.Wait()

	eventsPath := filepath.Join(state, "workflows", w.ID, "runs", "lead", "1", "events.jsonl")
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		b, _ := os.ReadFile(eventsPath)
		if strings.Contains(string(b), "survived supervisor crash") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if runstate.ProcessMatches(manifest) {
		process, _ := os.FindProcess(manifest.PID)
		_ = process.Kill()
	}
	if err := (&Engine{Store: st}).Reconcile(context.Background(), w.ID); err != nil {
		t.Fatal(err)
	}
	got, _ := st.Load(w.ID)
	if got.Nodes["lead"].State != core.NodeCompleted || got.Status != core.WorkflowCompleted {
		t.Fatalf("reconciled node=%s workflow=%s evidence=%+v", got.Nodes["lead"].State, got.Status, got.Evidence)
	}
}
