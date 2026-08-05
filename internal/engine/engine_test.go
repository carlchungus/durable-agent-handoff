package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/activity"
	"github.com/carlchungus/durable-agent-handoff/internal/core"
	"github.com/carlchungus/durable-agent-handoff/internal/finalize"
	"github.com/carlchungus/durable-agent-handoff/internal/runstate"
	agentsession "github.com/carlchungus/durable-agent-handoff/internal/session"
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
	activities, err := eng.Activities.List()
	if err != nil || len(activities) != 1 {
		t.Fatalf("activities=%+v err=%v", activities, err)
	}
	agent, err := eng.Sessions.LoadByNode(w.ID, "lead")
	if err != nil {
		t.Fatal(err)
	}
	tracked := activities[0]
	if tracked.OwnerSessionID != agent.ID || tracked.State != activity.StateCompleted || len(tracked.Attempts) != 1 || tracked.Attempts[0].CommandDigest == "" {
		t.Fatalf("activity=%+v agent=%+v", tracked, agent)
	}
	chunk, err := eng.Activities.ReadOutput(tracked.ID, activity.OutputCursor{AttemptID: tracked.Attempts[0].ID, Stream: activity.StreamStdout, OutputID: tracked.Attempts[0].Stdout.ID}, 64<<10)
	if err != nil || !strings.Contains(string(chunk.Data), "implemented safely") {
		t.Fatalf("durable activity output=%q err=%v", chunk.Data, err)
	}
}

func TestDelegatedAgentTurnGetsParentSessionAndOwnActivity(t *testing.T) {
	if os.Getenv("GO_WANT_SUBAGENT_ACTIVITY_HELPER") == "1" {
		fmt.Println(`{"status":"completed","summary":"child-safe","attestations":[{"verifier":"helper","verdict":"pass","summary":"checked"}]}`)
		os.Exit(0)
	}
	t.Setenv("GO_WANT_SUBAGENT_ACTIVITY_HELPER", "1")
	state := t.TempDir()
	st, _ := core.OpenStore(state)
	w, _ := st.Create("run delegated agent", t.TempDir(), core.DefaultBudget())
	runtimeSpec := core.RuntimeSpec{Name: "exec", Executable: os.Args[0], Args: []string{"-test.run=TestDelegatedAgentTurnGetsParentSessionAndOwnActivity"}}
	parent := &core.Node{ID: "lead", Title: "lead", Kind: "agent", Runtime: runtimeSpec}
	child := &core.Node{ID: "child", Title: "child", Kind: "agent", Runtime: runtimeSpec, DependsOn: []string{"lead"}, Metadata: map[string]string{"parent_id": "lead"}}
	w, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []core.Mutation{{Op: "add_node", Node: parent}, {Op: "add_node", Node: child}}})
	eng := &Engine{Store: st}
	if _, err := eng.RunOne(context.Background(), w.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.RunOne(context.Background(), w.ID); err != nil {
		t.Fatal(err)
	}
	childSession, err := eng.Sessions.LoadByNode(w.ID, "child")
	if err != nil {
		t.Fatal(err)
	}
	if childSession.ParentAgentID != "lead" {
		t.Fatalf("child session=%+v", childSession)
	}
	childActivity, err := eng.Activities.Load(activity.StableID(w.ID, "child", "1"))
	if err != nil {
		t.Fatal(err)
	}
	if childActivity.OwnerSessionID != childSession.ID || childActivity.State != activity.StateCompleted {
		t.Fatalf("child activity=%+v session=%+v", childActivity, childSession)
	}
}

func TestEngineDeliversDurableReplyToExactResumedAttempt(t *testing.T) {
	if os.Getenv("GO_WANT_REPLY_HELPER") == "1" {
		prompt := os.Args[len(os.Args)-1]
		if !strings.Contains(prompt, "message-1") || !strings.Contains(prompt, "use blue") {
			fmt.Fprintln(os.Stderr, "durable reply missing from prompt")
			os.Exit(2)
		}
		fmt.Println(`{"thread_id":"session-exact-123"}`)
		fmt.Println(`{"status":"completed","summary":"reply applied","session_id":"session-exact-123"}`)
		os.Exit(0)
	}
	t.Setenv("GO_WANT_REPLY_HELPER", "1")
	state := t.TempDir()
	st, _ := core.OpenStore(state)
	sessions, _ := agentsession.OpenStore(state)
	w, _ := st.Create("continue exact session", t.TempDir(), core.DefaultBudget())
	n := &core.Node{ID: "lead", Title: "lead", Kind: "agent", SessionID: "session-exact-123", Runtime: core.RuntimeSpec{Name: "exec", Executable: os.Args[0], Args: []string{"-test.run=TestEngineDeliversDurableReplyToExactResumedAttempt"}}}
	w, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []core.Mutation{{Op: "add_node", Node: n}}})
	agent, err := sessions.Ensure(agentsession.Descriptor{WorkflowID: w.ID, NodeID: n.ID, Runtime: "exec", RuntimeSessionID: n.SessionID, Worktree: w.Root})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = sessions.Queue(agent.ID, "human", "use blue"); err != nil {
		t.Fatal(err)
	}
	if _, err = (&Engine{Store: st, Sessions: sessions}).RunOne(context.Background(), w.ID); err != nil {
		t.Fatal(err)
	}
	got, err := sessions.Load(agent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.RuntimeSessionID != "session-exact-123" || len(got.Inbox) != 1 || got.Inbox[0].State != agentsession.MessageDelivered || got.Inbox[0].DeliveryAttempt != 1 {
		t.Fatalf("session=%+v", got)
	}
	if got.LogicalState != agentsession.LogicalCompleted || got.ProcessState != agentsession.ProcessExited {
		t.Fatalf("logical=%s process=%s", got.LogicalState, got.ProcessState)
	}
}

func TestAgentFailurePathsRecordRejectedAttemptOutcome(t *testing.T) {
	if os.Getenv("GO_WANT_ATTEMPT_FAILURE_HELPER") == "1" {
		if strings.Contains(strings.Join(os.Args, " "), "runtime-failure-mode") {
			fmt.Fprintln(os.Stderr, "runtime exploded")
			os.Exit(2)
		}
		fmt.Println("not a result")
		os.Exit(0)
	}
	t.Setenv("GO_WANT_ATTEMPT_FAILURE_HELPER", "1")
	for _, tc := range []struct{ mode, outcome string }{{"runtime-failure-mode", "runtime_failure"}, {"parse-failure-mode", "parse_failure"}} {
		t.Run(tc.outcome, func(t *testing.T) {
			state := t.TempDir()
			st, _ := core.OpenStore(state)
			sessions, _ := agentsession.OpenStore(state)
			w, _ := st.Create("record rejected attempt", t.TempDir(), core.DefaultBudget())
			n := &core.Node{ID: "lead", Title: "lead", Kind: "agent", Runtime: core.RuntimeSpec{Name: "exec", Executable: os.Args[0], Args: []string{"-test.run=TestAgentFailurePathsRecordRejectedAttemptOutcome", tc.mode}}}
			_, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []core.Mutation{{Op: "add_node", Node: n}}})
			agent, _ := sessions.Ensure(agentsession.Descriptor{WorkflowID: w.ID, NodeID: n.ID})
			_, _ = sessions.Queue(agent.ID, "human", "retry me")
			if _, err := (&Engine{Store: st, Sessions: sessions}).RunOne(context.Background(), w.ID); err != nil {
				t.Fatal(err)
			}
			got, _ := st.Load(w.ID)
			outcomes := attemptOutcomesForNode(got, n.ID)
			if len(outcomes) != 1 || outcomes[0].AttemptOutcome != tc.outcome || outcomes[0].InboxDisposition != "requeue" || outcomes[0].DeliveryAttempt != 1 {
				t.Fatalf("outcomes=%+v", outcomes)
			}
			agent, _ = sessions.Load(agent.ID)
			if agent.Inbox[0].State != agentsession.MessageQueued || agent.Inbox[0].DeliveryAttempt != 1 {
				t.Fatalf("session=%+v", agent)
			}
		})
	}
}

func TestDiffBudgetRecordsAcceptedAttemptOutcome(t *testing.T) {
	if os.Getenv("GO_WANT_DIFF_BUDGET_HELPER") == "1" {
		fmt.Println(`{"status":"completed","summary":"implemented","attestations":[]}`)
		os.Exit(0)
	}
	t.Setenv("GO_WANT_DIFF_BUDGET_HELPER", "1")
	root := t.TempDir()
	for _, args := range [][]string{{"init"}, {"config", "user.email", "test@example.com"}, {"config", "user.name", "Test"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "base.txt"), []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"add", "base.txt"}, {"commit", "-m", "base"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "extra.txt"), []byte("over budget\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	state := t.TempDir()
	st, _ := core.OpenStore(state)
	sessions, _ := agentsession.OpenStore(state)
	budget := core.DefaultBudget()
	budget.MaxChangedFiles = 0
	w, _ := st.Create("stop at diff budget", root, budget)
	n := &core.Node{ID: "lead", Title: "lead", Kind: "agent", Runtime: core.RuntimeSpec{Name: "exec", Executable: os.Args[0], Args: []string{"-test.run=TestDiffBudgetRecordsAcceptedAttemptOutcome"}}}
	_, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []core.Mutation{{Op: "add_node", Node: n}}})
	agent, _ := sessions.Ensure(agentsession.Descriptor{WorkflowID: w.ID, NodeID: n.ID})
	_, _ = sessions.Queue(agent.ID, "human", "apply this")
	if _, err := (&Engine{Store: st, Sessions: sessions}).RunOne(context.Background(), w.ID); err != nil {
		t.Fatal(err)
	}
	got, _ := st.Load(w.ID)
	outcomes := attemptOutcomesForNode(got, n.ID)
	if got.Nodes[n.ID].State != core.NodeWaiting || len(outcomes) != 1 || outcomes[0].AttemptOutcome != "diff_budget" || outcomes[0].InboxDisposition != "deliver" {
		t.Fatalf("node=%+v outcomes=%+v", got.Nodes[n.ID], outcomes)
	}
	agent, _ = sessions.Load(agent.ID)
	if agent.Inbox[0].State != agentsession.MessageDelivered || agent.Inbox[0].DeliveryAttempt != 1 {
		t.Fatalf("session=%+v", agent)
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

func TestReconcileReopensQueuedReplyAfterSupervisorCrash(t *testing.T) {
	state := t.TempDir()
	st, _ := core.OpenStore(state)
	sessions, _ := agentsession.OpenStore(state)
	w, _ := st.Create("wake after crash", t.TempDir(), core.DefaultBudget())
	n := &core.Node{ID: "lead", Title: "lead", Kind: "agent", Runtime: core.RuntimeSpec{Name: "claude"}, SessionID: "session-exact-123"}
	w, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []core.Mutation{{Op: "add_node", Node: n}}})
	w, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: []core.Mutation{{Op: "set_state", NodeID: n.ID, State: core.NodeRunning}}})
	w, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: n.ID, Mutations: []core.Mutation{{Op: "set_state", NodeID: n.ID, State: core.NodeCompleted}}})
	agent, _ := sessions.Ensure(agentsession.Descriptor{WorkflowID: w.ID, NodeID: n.ID, RuntimeSessionID: n.SessionID})
	_, _ = sessions.Queue(agent.ID, "human", "continue")
	if err := (&Engine{Store: st, Sessions: sessions}).Reconcile(context.Background(), w.ID); err != nil {
		t.Fatal(err)
	}
	w, _ = st.Load(w.ID)
	if w.Nodes[n.ID].State != core.NodeReady || w.Nodes[n.ID].SessionID != "session-exact-123" {
		t.Fatalf("node=%+v", w.Nodes[n.ID])
	}
}

func TestReconcileRequeuesReplyFromInterruptedRuntimeAttempt(t *testing.T) {
	state := t.TempDir()
	st, _ := core.OpenStore(state)
	sessions, _ := agentsession.OpenStore(state)
	w, _ := st.Create("resume interrupted reply", t.TempDir(), core.DefaultBudget())
	n := &core.Node{ID: "lead", Title: "lead", Kind: "agent", Runtime: core.RuntimeSpec{Name: "claude"}, SessionID: "session-exact-123"}
	w, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []core.Mutation{{Op: "add_node", Node: n}}})
	w, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: []core.Mutation{{Op: "set_state", NodeID: n.ID, State: core.NodeRunning}}})
	agent, _ := sessions.Ensure(agentsession.Descriptor{WorkflowID: w.ID, NodeID: n.ID, RuntimeSessionID: n.SessionID})
	_, _ = sessions.Queue(agent.ID, "human", "continue")
	_, _ = sessions.Dispatch(agent.ID, 1)
	dir := filepath.Join(state, "workflows", w.ID, "runs", n.ID, "1")
	_, err := runstate.Create(filepath.Join(dir, "attempt.json"), runstate.Manifest{ID: "lead/1", WorkflowID: w.ID, NodeID: n.ID, Attempt: 1, Runtime: "claude", SessionID: n.SessionID, PID: 999999, ProcessStartToken: "gone"})
	if err != nil {
		t.Fatal(err)
	}
	if err = (&Engine{Store: st, Sessions: sessions}).Reconcile(context.Background(), w.ID); err != nil {
		t.Fatal(err)
	}
	agent, _ = sessions.Load(agent.ID)
	if agent.Inbox[0].State != agentsession.MessageQueued || agent.Inbox[0].DeliveryAttempt != 1 || agent.ProcessState != agentsession.ProcessExited {
		t.Fatalf("session=%+v", agent)
	}
}

func TestReconcileAppliesExplicitAttemptOutcomeExactlyOnce(t *testing.T) {
	cases := []struct {
		name        string
		outcome     string
		disposition string
		state       core.NodeState
		refund      bool
		logical     agentsession.LogicalState
	}{
		{name: "completed", outcome: "completed", disposition: "deliver", state: core.NodeCompleted, logical: agentsession.LogicalCompleted},
		{name: "continue", outcome: "continue", disposition: "deliver", state: core.NodeReady, logical: agentsession.LogicalWorking},
		{name: "needs-human", outcome: "needs_human", disposition: "deliver", state: core.NodeWaiting, logical: agentsession.LogicalNeedsInput},
		{name: "blocked", outcome: "blocked", disposition: "deliver", state: core.NodeFailed, logical: agentsession.LogicalCompleted},
		{name: "diff-budget", outcome: "diff_budget", disposition: "deliver", state: core.NodeWaiting, logical: agentsession.LogicalNeedsInput},
		{name: "runtime-failure", outcome: "runtime_failure", disposition: "requeue", state: core.NodeReady, logical: agentsession.LogicalWorking},
		{name: "parse-failure", outcome: "parse_failure", disposition: "requeue", state: core.NodeFailed, logical: agentsession.LogicalCompleted},
		{name: "provider-fallback-refunded", outcome: "provider_limit", disposition: "requeue", state: core.NodeReady, refund: true, logical: agentsession.LogicalWorking},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := t.TempDir()
			st, _ := core.OpenStore(state)
			sessions, _ := agentsession.OpenStore(state)
			w, _ := st.Create("recover attempt outcome", t.TempDir(), core.DefaultBudget())
			n := &core.Node{ID: "lead", Title: "lead", Kind: "agent", Runtime: core.RuntimeSpec{Name: "claude"}, SessionID: "session-exact-123"}
			w, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []core.Mutation{{Op: "add_node", Node: n}}})
			w, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: []core.Mutation{{Op: "set_state", NodeID: n.ID, State: core.NodeRunning}}})
			agent, _ := sessions.Ensure(agentsession.Descriptor{WorkflowID: w.ID, NodeID: n.ID, RuntimeSessionID: n.SessionID})
			_, _ = sessions.Queue(agent.ID, "human", "continue")
			deliveryAttempt := 1
			if tc.refund {
				deliveryAttempt = 2
			}
			_, _ = sessions.Dispatch(agent.ID, deliveryAttempt)
			mutations := []core.Mutation{attemptOutcomeMutation(n, 1, deliveryAttempt, tc.outcome, tc.disposition, tc.name)}
			if tc.refund {
				mutations = append(mutations, core.Mutation{Op: "refund_attempt", NodeID: n.ID})
			}
			mutations = append(mutations, core.Mutation{Op: "set_state", NodeID: n.ID, State: tc.state})
			_, err := st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: mutations})
			if err != nil {
				t.Fatal(err)
			}
			eng := &Engine{Store: st, Sessions: sessions}
			for i := 0; i < 2; i++ {
				if err = eng.Reconcile(context.Background(), w.ID); err != nil {
					t.Fatal(err)
				}
			}
			agent, _ = sessions.Load(agent.ID)
			wantMessageState := agentsession.MessageDelivered
			if tc.disposition == "requeue" {
				wantMessageState = agentsession.MessageQueued
			}
			if agent.Inbox[0].State != wantMessageState || agent.Inbox[0].DeliveryAttempt != deliveryAttempt {
				t.Fatalf("case=%s session=%+v", tc.name, agent)
			}
			if agent.LogicalState != tc.logical || agent.ProcessState != agentsession.ProcessExited {
				t.Fatalf("case=%s logical=%s process=%s", tc.name, agent.LogicalState, agent.ProcessState)
			}
			got, _ := st.Load(w.ID)
			if tc.refund && got.Nodes[n.ID].Attempt != 0 {
				t.Fatalf("refunded node attempt=%d", got.Nodes[n.ID].Attempt)
			}
			if tc.state == core.NodeFailed && got.Nodes[n.ID].State != core.NodeFailed {
				t.Fatalf("requeued failure was unexpectedly reopened: %+v", got.Nodes[n.ID])
			}
		})
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

func TestReconcileUsesActivityBeforeConflictingLegacyManifest(t *testing.T) {
	state := t.TempDir()
	st, _ := core.OpenStore(state)
	w, _ := st.Create("prefer activity authority", t.TempDir(), core.DefaultBudget())
	n := &core.Node{ID: "lead", Title: "lead", Kind: "agent", Runtime: core.RuntimeSpec{Name: "exec"}}
	_, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []core.Mutation{{Op: "add_node", Node: n}}})
	w, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: []core.Mutation{{Op: "set_state", NodeID: "lead", State: core.NodeRunning}}})
	n = w.Nodes["lead"]
	activities, _ := activity.OpenStore(state)
	tracked, _ := activities.Create(activity.Descriptor{ID: activity.StableID(w.ID, n.ID, "1"), Work: activity.WorkSpec{Kind: "agent", Cwd: w.Root, Intent: w.ID + "/lead"}})
	attempt, stdout, stderr, _ := activities.PrepareAttempt(tracked.ID, tracked.Generation, activity.AttemptStart{CommandDigest: "exact"})
	_, _ = stdout.WriteString(`{"status":"completed","summary":"activity won","mutations":[],"attestations":[]}`)
	_ = stdout.Close()
	_ = stderr.Close()
	attempt, _ = activities.MarkRunning(tracked.ID, tracked.Generation, attempt.ID, activity.ProcessIdentity{PID: 8888, ProcessStartToken: "dead", SupervisorID: "dead:owner", SupervisorGeneration: 1})
	if err := activities.FinishAttempt(tracked.ID, tracked.Generation, activity.AttemptIdentity{ID: attempt.ID, PID: attempt.PID, ProcessStartToken: attempt.ProcessStartToken, SupervisorID: attempt.SupervisorID, SupervisorGeneration: attempt.SupervisorGeneration}, activity.ExitResult{State: activity.StateCompleted}); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(state, "workflows", w.ID, "runs", "lead", "1")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	legacy, err := runstate.Create(filepath.Join(dir, "attempt.json"), runstate.Manifest{ID: "legacy", WorkflowID: w.ID, NodeID: "lead", Attempt: 1, Status: "running", PID: os.Getpid(), ProcessStartToken: runstate.ProcessStartToken(os.Getpid()), SupervisorID: runstate.SupervisorIdentity(), SupervisorGeneration: 1})
	if err != nil {
		t.Fatal(err)
	}
	_ = legacy
	if err = (&Engine{Store: st, Activities: activities}).Reconcile(context.Background(), w.ID); err != nil {
		t.Fatal(err)
	}
	got, _ := st.Load(w.ID)
	if got.Nodes["lead"].State != core.NodeCompleted || len(got.Evidence) == 0 || got.Evidence[len(got.Evidence)-1].Summary != "activity won" {
		t.Fatalf("workflow did not reduce authoritative Activity: state=%s evidence=%+v", got.Nodes["lead"].State, got.Evidence)
	}
}

func TestReconcileDoesNotApplyValidOutputFromFailedActivity(t *testing.T) {
	state := t.TempDir()
	st, _ := core.OpenStore(state)
	w, _ := st.Create("reject stale failed output", t.TempDir(), core.DefaultBudget())
	n := &core.Node{ID: "lead", Title: "lead", Kind: "agent", Runtime: core.RuntimeSpec{Name: "exec"}}
	_, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []core.Mutation{{Op: "add_node", Node: n}}})
	w, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: []core.Mutation{{Op: "set_state", NodeID: "lead", State: core.NodeRunning}}})
	n = w.Nodes["lead"]
	activities, _ := activity.OpenStore(state)
	tracked, _ := activities.Create(activity.Descriptor{ID: activity.StableID(w.ID, n.ID, "1"), Work: activity.WorkSpec{Kind: "agent", Cwd: w.Root, Intent: w.ID + "/lead"}})
	attempt, stdout, stderr, _ := activities.PrepareAttempt(tracked.ID, tracked.Generation, activity.AttemptStart{})
	_, _ = stdout.WriteString(`{"status":"completed","summary":"must not apply","mutations":[],"attestations":[]}`)
	_ = stdout.Close()
	_ = stderr.Close()
	attempt, _ = activities.MarkRunning(tracked.ID, tracked.Generation, attempt.ID, activity.ProcessIdentity{PID: 7777, ProcessStartToken: "dead", SupervisorID: "dead:owner", SupervisorGeneration: 1})
	code := 1
	identity := activity.AttemptIdentity{ID: attempt.ID, PID: attempt.PID, ProcessStartToken: attempt.ProcessStartToken, SupervisorID: attempt.SupervisorID, SupervisorGeneration: attempt.SupervisorGeneration}
	_ = activities.FinishAttempt(tracked.ID, tracked.Generation, identity, activity.ExitResult{State: activity.StateFailed, ExitCode: &code, Error: "exit status 1"})
	if err := (&Engine{Store: st, Activities: activities}).Reconcile(context.Background(), w.ID); err != nil {
		t.Fatal(err)
	}
	got, _ := st.Load(w.ID)
	if got.Nodes["lead"].State == core.NodeCompleted {
		t.Fatalf("failed Activity output was applied: evidence=%+v", got.Evidence)
	}
	for _, evidence := range got.Evidence {
		if evidence.Summary == "must not apply" {
			t.Fatalf("stale failed result was reduced: %+v", got.Evidence)
		}
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
	activities, err := activity.OpenStore(state)
	if err != nil {
		t.Fatal(err)
	}
	activityID := activity.StableID(w.ID, "lead", "1")
	var tracked *activity.Activity
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		tracked, _ = activities.Load(activityID)
		current, _ := st.Load(w.ID)
		if tracked != nil && tracked.State == activity.StateRunning && len(tracked.Attempts) == 1 && tracked.Attempts[0].PID > 0 && current.Nodes["lead"].SessionID != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if tracked == nil || len(tracked.Attempts) != 1 || tracked.Attempts[0].PID == 0 {
		_ = supervisor.Process.Kill()
		t.Fatalf("worker identity was not persisted before crash: %+v", tracked)
	}
	workerAttempt := tracked.Attempts[0]
	if !workerIsDetached(supervisor.Process.Pid, workerAttempt.PID) {
		_ = supervisor.Process.Kill()
		t.Fatalf("worker %d shares supervisor %d process group", workerAttempt.PID, supervisor.Process.Pid)
	}
	if err := supervisor.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_, _ = supervisor.Process.Wait()
	if err := (&Engine{Store: st}).Reconcile(context.Background(), w.ID); err != nil {
		t.Fatal(err)
	}
	adopted, err := activities.Load(activityID)
	if err != nil || adopted.Generation != 2 || adopted.Attempts[0].SupervisorGeneration != 2 {
		t.Fatalf("live runner was not adopted exactly once: activity=%+v err=%v", adopted, err)
	}

	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		chunk, _ := activities.ReadOutput(activityID, activity.OutputCursor{AttemptID: workerAttempt.ID, Stream: activity.StreamStdout, OutputID: workerAttempt.Stdout.ID}, 64<<10)
		if strings.Contains(string(chunk.Data), "survived supervisor crash") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	var got *core.Workflow
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if err := (&Engine{Store: st}).Reconcile(context.Background(), w.ID); err != nil {
			t.Fatal(err)
		}
		got, _ = st.Load(w.ID)
		if got.Nodes["lead"].State == core.NodeCompleted {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got.Nodes["lead"].State != core.NodeCompleted || got.Status != core.WorkflowCompleted {
		t.Fatalf("reconciled node=%s workflow=%s evidence=%+v", got.Nodes["lead"].State, got.Status, got.Evidence)
	}
	if _, err = os.Stat(filepath.Join(state, "workflows", w.ID, "runs", "lead", "1", "attempt.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("new agent turn wrote legacy process authority: %v", err)
	}
}
