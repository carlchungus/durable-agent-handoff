package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/core"
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
