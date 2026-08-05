package tui

import (
	"strings"
	"testing"

	"github.com/carlchungus/durable-agent-handoff/internal/core"
)

func TestSnapshotIsReadableAndHasNoANSI(t *testing.T) {
	st, err := core.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	w, err := st.Create("repair the frobnicator", t.TempDir(), core.DefaultBudget())
	if err != nil {
		t.Fatal(err)
	}
	proposal := core.Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []core.Mutation{
		{Op: "add_node", Node: &core.Node{ID: "lead", Title: "inspect and repair", Kind: "agent", Runtime: core.RuntimeSpec{Name: "codex"}}},
	}}
	if _, err = st.Apply(proposal); err != nil {
		t.Fatal(err)
	}
	view, err := Snapshot(st)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"handoff", "repair the frobnicator", "inspect and repair", "agent API"} {
		if !strings.Contains(view, want) {
			t.Errorf("missing %q in\n%s", want, view)
		}
	}
	if strings.Contains(view, "\x1b[") {
		t.Fatal("snapshot contains ANSI escapes")
	}
}
