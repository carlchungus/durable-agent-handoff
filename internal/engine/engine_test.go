package engine

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/carlchungus/durable-agent-handoff/internal/core"
)

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
