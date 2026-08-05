package engine

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/carlchungus/durable-agent-handoff/internal/core"
	"github.com/carlchungus/durable-agent-handoff/internal/preferences"
)

func TestUsageLimitFallsThroughConfiguredLadder(t *testing.T) {
	if os.Getenv("GO_WANT_LADDER_HELPER") == "1" {
		joined := strings.Join(os.Args, " ")
		if strings.Contains(joined, "limit-mode") {
			fmt.Fprintln(os.Stderr, "You've hit your session limit · resets 8:40am (local)")
			os.Exit(1)
		}
		fmt.Println(`{"status":"completed","summary":"backup completed","attestations":[{"verifier":"backup","verdict":"pass","summary":"verified"}]}`)
		os.Exit(0)
	}
	t.Setenv("GO_WANT_LADDER_HELPER", "1")
	state := t.TempDir()
	prefs := preferences.Open(state)
	primary := core.RuntimeSpec{Name: "exec", Model: "primary", Executable: os.Args[0], Args: []string{"-test.run=TestUsageLimitFallsThroughConfiguredLadder", "limit-mode"}}
	backup := core.RuntimeSpec{Name: "exec", Model: "backup", Executable: os.Args[0], Args: []string{"-test.run=TestUsageLimitFallsThroughConfiguredLadder", "success-mode"}}
	if err := prefs.Set("planner", []core.RuntimeSpec{primary, backup}); err != nil {
		t.Fatal(err)
	}
	st, _ := core.OpenStore(state)
	w, _ := st.Create("fallback", t.TempDir(), core.DefaultBudget())
	n := &core.Node{ID: "plan", Title: "plan", Kind: "agent", Role: "planner", Runtime: primary}
	_, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []core.Mutation{{Op: "add_node", Node: n}}})
	eng := Engine{Store: st, Preferences: prefs}
	if _, err := eng.RunOne(context.Background(), w.ID); err != nil {
		t.Fatal(err)
	}
	afterLimit, _ := st.Load(w.ID)
	if afterLimit.Nodes["plan"].Runtime.Model != "backup" || afterLimit.Nodes["plan"].State != core.NodeReady {
		t.Fatalf("after limit=%#v", afterLimit.Nodes["plan"])
	}
	if _, err := eng.RunOne(context.Background(), w.ID); err != nil {
		t.Fatal(err)
	}
	done, _ := st.Load(w.ID)
	if done.Status != core.WorkflowCompleted {
		t.Fatalf("status=%s evidence=%#v", done.Status, done.Evidence)
	}
}
