package main

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/carlchungus/durable-agent-handoff/internal/activity"
	"github.com/carlchungus/durable-agent-handoff/internal/core"
	agentsession "github.com/carlchungus/durable-agent-handoff/internal/session"
	"github.com/carlchungus/durable-agent-handoff/internal/team"
)

func TestStartAndStatusJSONContract(t *testing.T) {
	state := t.TempDir()
	root := t.TempDir()
	var out, errOut bytes.Buffer
	if err := run([]string{"start", "--state", state, "--goal", "test the CLI", "--root", root, "--runtime", "codex", "--json"}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	var created core.Workflow
	if err := json.Unmarshal(out.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.ID == "" || created.Nodes["lead"] == nil {
		t.Fatalf("created=%#v", created)
	}
	out.Reset()
	if err := run([]string{"status", "--state", state, created.ID, "--json"}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	var loaded core.Workflow
	if err := json.Unmarshal(out.Bytes(), &loaded); err != nil {
		t.Fatal(err)
	}
	if loaded.ID != created.ID || loaded.Goal != "test the CLI" {
		t.Fatalf("loaded=%#v", loaded)
	}
}

func TestFinalizationRequiresExplicitGate(t *testing.T) {
	var out, errOut bytes.Buffer
	err := run([]string{"start", "--state", t.TempDir(), "--goal", "ship", "--root", t.TempDir(), "--finalize-repo", "owner/repo"}, &out, &errOut)
	if err == nil {
		t.Fatal("expected missing gate error")
	}
}

func TestPreferenceCLIStoresOrderedLadder(t *testing.T) {
	state := t.TempDir()
	var out, errOut bytes.Buffer
	err := run([]string{"preference", "set", "--state", state, "planner", "--candidate", "claude:opus:xhigh", "--candidate", "codex:gpt-5.6-sol:xhigh"}, &out, &errOut)
	if err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err = run([]string{"preference", "list", "--state", state}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(out.Bytes(), []byte(`"opus"`)) || !bytes.Contains(out.Bytes(), []byte(`"gpt-5.6-sol"`)) {
		t.Fatalf("output=%s", out.String())
	}
}

func TestTeamCLIProducesMachineReadableState(t *testing.T) {
	state := t.TempDir()
	var out, errOut bytes.Buffer
	if err := run([]string{"team", "create", "--state", state, "--name", "review", "--workflow", "wf_1", "--lead", "captain"}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	var created team.Team
	if err := json.Unmarshal(out.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.ID == "" || created.LeadID != "captain" || created.WorkflowID != "wf_1" {
		t.Fatalf("created=%+v", created)
	}
	out.Reset()
	if err := run([]string{"team", "status", "--state", state, created.ID}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	var loaded team.Team
	if err := json.Unmarshal(out.Bytes(), &loaded); err != nil {
		t.Fatal(err)
	}
	if loaded.ID != created.ID || loaded.Members["captain"] == nil {
		t.Fatalf("loaded=%+v", loaded)
	}
}

func TestAgentReplyInboxAndViewAreMachineReadable(t *testing.T) {
	state := t.TempDir()
	st, _ := core.OpenStore(state)
	w, _ := st.Create("continue work", t.TempDir(), core.DefaultBudget())
	n := &core.Node{ID: "lead", Title: "lead", Kind: "agent", Runtime: core.RuntimeSpec{Name: "claude"}, SessionID: "session-exact-123"}
	w, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []core.Mutation{{Op: "add_node", Node: n}}})
	w, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: []core.Mutation{{Op: "set_state", NodeID: n.ID, State: core.NodeRunning}}})
	w, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: n.ID, Mutations: []core.Mutation{{Op: "set_state", NodeID: n.ID, State: core.NodeCompleted}}})
	sessions, _ := agentsession.OpenStore(state)
	_, _ = sessions.Ensure(agentsession.Descriptor{WorkflowID: w.ID, NodeID: n.ID, Runtime: "claude", RuntimeSessionID: n.SessionID, LogicalState: agentsession.LogicalNeedsInput, ProcessState: agentsession.ProcessExited})

	var out, errOut bytes.Buffer
	if err := run([]string{"agent", "reply", "--state", state, w.ID, n.ID, "--message", "use blue"}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	var message agentsession.Message
	if err := json.Unmarshal(out.Bytes(), &message); err != nil {
		t.Fatal(err)
	}
	if message.State != agentsession.MessageQueued || message.Sequence != 1 {
		t.Fatalf("message=%+v", message)
	}
	w, _ = st.Load(w.ID)
	if w.Nodes[n.ID].State != core.NodeReady || w.Nodes[n.ID].SessionID != "session-exact-123" {
		t.Fatalf("node=%+v", w.Nodes[n.ID])
	}
	out.Reset()
	if err := run([]string{"agent", "inbox", "--state", state, w.ID, n.ID, "--after", "0"}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	var inbox []agentsession.Message
	if err := json.Unmarshal(out.Bytes(), &inbox); err != nil || len(inbox) != 1 || inbox[0].Body != "use blue" {
		t.Fatalf("inbox=%+v err=%v", inbox, err)
	}
	out.Reset()
	if err := run([]string{"agents", "--state", state, "--workflow", w.ID, "--json"}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	var view []*agentsession.Session
	if err := json.Unmarshal(out.Bytes(), &view); err != nil || len(view) != 1 {
		t.Fatalf("view=%+v err=%v", view, err)
	}
	if view[0].LogicalState != agentsession.LogicalNeedsInput || view[0].ProcessState != agentsession.ProcessExited || view[0].RuntimeSessionID != "session-exact-123" {
		t.Fatalf("view=%+v", view[0])
	}
	out.Reset()
	if err := run([]string{"agents", "--state", state, "--workflow", w.ID}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if json.Valid(out.Bytes()) || !bytes.Contains(out.Bytes(), []byte("LOGICAL")) || !bytes.Contains(out.Bytes(), []byte("needs_input")) || !bytes.Contains(out.Bytes(), []byte("session-exact-123")) {
		t.Fatalf("human agent view=%q", out.String())
	}
}

func TestActivityCLIUsesTheDurableProjectionAndOutputCursor(t *testing.T) {
	state := t.TempDir()
	store, err := activity.OpenStore(state)
	if err != nil {
		t.Fatal(err)
	}
	item, err := store.Create(activity.Descriptor{ID: "activity_abcdefabcdefabcdefabcdef", Launch: activity.LaunchSpec{Kind: "command", Argv: []string{"tool"}, Cwd: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	attempt, stdout, stderr, err := store.PrepareAttempt(item.ID, item.Generation, activity.AttemptStart{Runtime: "exec"})
	if err != nil {
		t.Fatal(err)
	}
	attempt, err = store.MarkRunning(item.ID, item.Generation, attempt.ID, activity.ProcessIdentity{PID: 123, ProcessStartToken: "exact", SupervisorGeneration: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = stdout.WriteString("hello\n"); err == nil {
		err = stdout.Sync()
	}
	_ = stdout.Close()
	_ = stderr.Close()
	if err != nil {
		t.Fatal(err)
	}
	identity := activity.AttemptIdentity{ID: attempt.ID, PID: attempt.PID, ProcessStartToken: attempt.ProcessStartToken, SupervisorGeneration: attempt.SupervisorGeneration}
	if err = store.FinishAttempt(item.ID, item.Generation, identity, activity.ExitResult{State: activity.StateCompleted}); err != nil {
		t.Fatal(err)
	}

	var out, errOut bytes.Buffer
	if err = run([]string{"activity", "list", "--state", state, "--json"}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	var listed []*activity.Activity
	if err = json.Unmarshal(out.Bytes(), &listed); err != nil || len(listed) != 1 || listed[0].ID != item.ID {
		t.Fatalf("listed=%+v err=%v", listed, err)
	}
	out.Reset()
	if err = run([]string{"activity", "read", "--state", state, item.ID, "--json"}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	var read activity.Activity
	if err = json.Unmarshal(out.Bytes(), &read); err != nil || read.State != activity.StateCompleted {
		t.Fatalf("read=%+v err=%v", read, err)
	}
	out.Reset()
	if err = run([]string{"activity", "follow", "--state", state, item.ID, "--stream", "stdout", "--output", attempt.Stdout.ID, "--after", "0"}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if out.String() != "hello\n" {
		t.Fatalf("follow=%q", out.String())
	}
}
