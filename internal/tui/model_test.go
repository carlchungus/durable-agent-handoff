package tui

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/carlchungus/durable-agent-handoff/internal/activity"
	"github.com/carlchungus/durable-agent-handoff/internal/core"
	"github.com/carlchungus/durable-agent-handoff/internal/runstate"
	coord "github.com/carlchungus/durable-agent-handoff/internal/team"
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

func TestSnapshotShowsActivityBackedAttemptWithoutLegacyManifest(t *testing.T) {
	state := t.TempDir()
	st, _ := core.OpenStore(state)
	w, _ := st.Create("observe activity worker", t.TempDir(), core.DefaultBudget())
	_, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []core.Mutation{{Op: "add_node", Node: &core.Node{ID: "lead", Title: "working", Kind: "agent", Runtime: core.RuntimeSpec{Name: "codex"}}}}})
	w, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: []core.Mutation{{Op: "set_state", NodeID: "lead", State: core.NodeRunning}, {Op: "set_session", NodeID: "lead", Reason: "session-activity"}}})
	activities, _ := activity.OpenStore(state)
	tracked, _ := activities.Create(activity.Descriptor{ID: activity.StableID(w.ID, "lead", "1"), Work: activity.WorkSpec{Kind: "agent", Cwd: w.Root, Intent: w.ID + "/lead"}})
	attempt, stdout, stderr, _ := activities.PrepareAttempt(tracked.ID, tracked.Generation, activity.AttemptStart{})
	_, _ = stdout.WriteString(strings.Repeat("x", 2048))
	_ = stdout.Close()
	_ = stderr.Close()
	_, _ = activities.MarkRunning(tracked.ID, tracked.Generation, attempt.ID, activity.ProcessIdentity{PID: 4343, ProcessStartToken: "birth", SupervisorID: "owner", SupervisorGeneration: 2})
	view, err := Snapshot(st)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"pid 4343", "supervisor g2", "session-", "acti…", "2.0 KB"} {
		if !strings.Contains(view, want) {
			t.Errorf("missing %q in\n%s", want, view)
		}
	}
}

func TestSnapshotDoesNotFallBackWhenActivityLedgerIsCorrupt(t *testing.T) {
	state := t.TempDir()
	st, _ := core.OpenStore(state)
	w, _ := st.Create("reject split authority", t.TempDir(), core.DefaultBudget())
	_, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []core.Mutation{{Op: "add_node", Node: &core.Node{ID: "lead", Title: "working", Kind: "agent", Runtime: core.RuntimeSpec{Name: "codex"}}}}})
	w, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: []core.Mutation{{Op: "set_state", NodeID: "lead", State: core.NodeRunning}}})
	activities, _ := activity.OpenStore(state)
	tracked, _ := activities.Create(activity.Descriptor{ID: activity.StableID(w.ID, "lead", "1"), Work: activity.WorkSpec{Kind: "agent", Cwd: w.Root, Intent: w.ID + "/lead"}})
	legacyPath := filepath.Join(state, "workflows", w.ID, "runs", "lead", "1", "attempt.json")
	_, _ = runstate.Create(legacyPath, runstate.Manifest{ID: "legacy", WorkflowID: w.ID, NodeID: "lead", Attempt: 1, Status: "running", PID: 4242})
	activityDir := filepath.Join(state, "activities", tracked.ID)
	if err := os.WriteFile(filepath.Join(activityDir, "events.jsonl"), []byte("not-json\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(activityDir, "state.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := Snapshot(st); err == nil {
		t.Fatal("corrupt Activity was hidden by legacy attempt fallback")
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
	for _, want := range []string{"pid 4242", "supervisor g1", "runtime", "session-", "obse…", "4.0 KB"} {
		if !strings.Contains(view, want) {
			t.Errorf("missing %q in\n%s", want, view)
		}
	}
}

func TestTeamViewShowsPeerTaskAndMailboxState(t *testing.T) {
	state := t.TempDir()
	st, _ := core.OpenStore(state)
	teamStore, _ := coord.OpenStore(state)
	tm, err := teamStore.Create("schema-review", "wf_review", coord.Member{ID: "lead", Name: "Lead"})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = teamStore.Apply(tm.ID, coord.Command{Op: "add_member", Actor: "lead", Member: &coord.Member{ID: "db-reviewer", Name: "DB Reviewer", Plan: coord.PlanApproved}})
	_, _ = teamStore.Apply(tm.ID, coord.Command{Op: "add_task", Actor: "lead", Task: &coord.Task{ID: "inspect", Title: "Inspect schema"}})
	_, _ = teamStore.Apply(tm.ID, coord.Command{Op: "claim_task", Actor: "db-reviewer", TaskID: "inspect"})
	_, _ = teamStore.Apply(tm.ID, coord.Command{Op: "send_message", Actor: "db-reviewer", To: "lead", Body: "membership key needs repair"})
	teams, _ := teamStore.List()
	m := New(st)
	m.mode, m.teams = "teams", teams
	view := m.RenderPlain()
	for _, want := range []string{"schema-review", "DB Reviewer", "Inspect schema", "db-reviewer/g1", "membership key needs repair"} {
		if !strings.Contains(view, want) {
			t.Errorf("missing %q in\n%s", want, view)
		}
	}
}
