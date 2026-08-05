package tui

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/carlchungus/durable-agent-handoff/internal/core"
	"github.com/carlchungus/durable-agent-handoff/internal/runstate"
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

func TestSnapshotShowsDurableAttemptHealth(t *testing.T) {
	state := t.TempDir()
	st, _ := core.OpenStore(state)
	w, _ := st.Create("observe live worker", t.TempDir(), core.DefaultBudget())
	_, _ = st.Apply(core.Proposal{
		WorkflowID: w.ID,
		Actor:      "human",
		Mutations: []core.Mutation{{
			Op: "add_node",
			Node: &core.Node{
				ID: "lead", Title: "working", Kind: "agent", Runtime: core.RuntimeSpec{Name: "codex"},
			},
		}},
	})
	_, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: []core.Mutation{{Op: "set_state", NodeID: "lead", State: core.NodeRunning}}})
	path := filepath.Join(state, "workflows", w.ID, "runs", "lead", "1", "attempt.json")
	_, err := runstate.Create(path, runstate.Manifest{ID: "lead/1", WorkflowID: w.ID, NodeID: "lead", Attempt: 1, Status: "running", PID: 4242, SessionID: "session-observable", EventOffset: 4096})
	if err != nil {
		t.Fatal(err)
	}
	view, err := Snapshot(st)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"pid 4242", "heartbeat", "session-obse…", "4.0 KB"} {
		if !strings.Contains(view, want) {
			t.Errorf("missing %q in\n%s", want, view)
		}
	}
}
