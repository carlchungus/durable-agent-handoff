package engine

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/carlchungus/durable-agent-handoff/internal/activity"
	"github.com/carlchungus/durable-agent-handoff/internal/core"
	"github.com/carlchungus/durable-agent-handoff/internal/preferences"
	agentsession "github.com/carlchungus/durable-agent-handoff/internal/session"
)

func TestUsageLimitFallsThroughConfiguredLadder(t *testing.T) {
	if os.Getenv("GO_WANT_LADDER_HELPER") == "1" {
		joined := strings.Join(os.Args, " ")
		if strings.Contains(joined, "limit-mode") {
			fmt.Fprint(os.Stdout, strings.Repeat("large initialization event ", 100))
			fmt.Fprintln(os.Stdout, "You've hit your session limit · resets 8:40am (local)")
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
	sessions, _ := agentsession.OpenStore(state)
	w, _ := st.Create("fallback", t.TempDir(), core.DefaultBudget())
	primary.Sandbox = "workspace-write"
	n := &core.Node{ID: "plan", Title: "plan", Kind: "agent", Role: "planner", Runtime: primary}
	_, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []core.Mutation{{Op: "add_node", Node: n}}})
	agent, _ := sessions.Ensure(agentsession.Descriptor{WorkflowID: w.ID, NodeID: n.ID})
	_, _ = sessions.Queue(agent.ID, "human", "keep this reply")
	eng := Engine{Store: st, Preferences: prefs, Sessions: sessions}
	if _, err := eng.RunOne(context.Background(), w.ID); err != nil {
		t.Fatal(err)
	}
	afterLimit, _ := st.Load(w.ID)
	if afterLimit.Nodes["plan"].Runtime.Model != "backup" || afterLimit.Nodes["plan"].Runtime.Sandbox != "workspace-write" || afterLimit.Nodes["plan"].State != core.NodeReady {
		t.Fatalf("after limit=%#v", afterLimit.Nodes["plan"])
	}
	if afterLimit.Nodes["plan"].Attempt != 0 {
		t.Fatalf("provider routing consumed a task attempt: %d", afterLimit.Nodes["plan"].Attempt)
	}
	agent, _ = sessions.Load(agent.ID)
	if agent.Inbox[0].State != agentsession.MessageQueued || agent.Inbox[0].DeliveryAttempt != 1 {
		t.Fatalf("provider fallback did not preserve rejected delivery: %+v", agent.Inbox)
	}
	if _, err := eng.RunOne(context.Background(), w.ID); err != nil {
		t.Fatal(err)
	}
	done, _ := st.Load(w.ID)
	if done.Status != core.WorkflowCompleted {
		t.Fatalf("status=%s evidence=%#v", done.Status, done.Evidence)
	}
	if done.Nodes["plan"].Attempt != 1 {
		t.Fatalf("successful backup should be the first task attempt: %d", done.Nodes["plan"].Attempt)
	}
	agent, _ = sessions.Load(agent.ID)
	if agent.Inbox[0].State != agentsession.MessageDelivered || agent.Inbox[0].DeliveryAttempt != 2 {
		t.Fatalf("refunded node attempt reused inbox fence: %+v", agent.Inbox)
	}
}

func TestCrashRecoveryFallbackRequeuesTheExactInboxDelivery(t *testing.T) {
	state := t.TempDir()
	prefs := preferences.Open(state)
	primary := core.RuntimeSpec{Name: "exec", Model: "primary", Executable: "primary"}
	backup := core.RuntimeSpec{Name: "exec", Model: "backup", Executable: "backup"}
	if err := prefs.Set("planner", []core.RuntimeSpec{primary, backup}); err != nil {
		t.Fatal(err)
	}
	st, _ := core.OpenStore(state)
	w, _ := st.Create("recover provider fallback", t.TempDir(), core.DefaultBudget())
	n := &core.Node{ID: "plan", Title: "plan", Kind: "agent", Role: "planner", Runtime: primary}
	_, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []core.Mutation{{Op: "add_node", Node: n}}})
	w, _ = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: []core.Mutation{{Op: "set_state", NodeID: n.ID, State: core.NodeRunning}}})
	n = w.Nodes[n.ID]
	sessions, _ := agentsession.OpenStore(state)
	agent, _ := sessions.Ensure(agentsession.Descriptor{WorkflowID: w.ID, NodeID: n.ID})
	_, _ = sessions.Queue(agent.ID, "human", "preserve me")
	dispatched, _ := sessions.Dispatch(agent.ID, 1)
	if len(dispatched) != 1 {
		t.Fatalf("dispatched=%+v", dispatched)
	}
	activities, _ := activity.OpenStore(state)
	tracked, _ := activities.Create(activity.Descriptor{ID: activity.StableID(w.ID, n.ID, "1"), OwnerSessionID: agent.ID, Work: activity.WorkSpec{Kind: "agent", Cwd: w.Root, Intent: w.ID + "/" + n.ID}})
	attempt, stdout, stderr, _ := activities.PrepareAttempt(tracked.ID, tracked.Generation, activity.AttemptStart{Runtime: "exec", Model: "primary", CommandDigest: "digest"})
	_, _ = stderr.WriteString("You've hit your usage limit")
	_ = stdout.Close()
	_ = stderr.Close()
	attempt, _ = activities.MarkRunning(tracked.ID, tracked.Generation, attempt.ID, activity.ProcessIdentity{PID: 12345, ProcessStartToken: "dead", SupervisorID: "dead:owner", SupervisorGeneration: 1})
	code := 1
	identity := activity.AttemptIdentity{ID: attempt.ID, PID: attempt.PID, ProcessStartToken: attempt.ProcessStartToken, SupervisorID: attempt.SupervisorID, SupervisorGeneration: attempt.SupervisorGeneration}
	if err := activities.FinishAttempt(tracked.ID, tracked.Generation, identity, activity.ExitResult{State: activity.StateFailed, ExitCode: &code, Error: "exit status 1"}); err != nil {
		t.Fatal(err)
	}
	eng := &Engine{Store: st, Preferences: prefs, Sessions: sessions, Activities: activities}
	if err := eng.Reconcile(context.Background(), w.ID); err != nil {
		t.Fatal(err)
	}
	after, _ := st.Load(w.ID)
	if after.Nodes[n.ID].State != core.NodeReady || after.Nodes[n.ID].Runtime.Model != "backup" || after.Nodes[n.ID].Attempt != 0 {
		t.Fatalf("node=%+v", after.Nodes[n.ID])
	}
	agent, _ = sessions.Load(agent.ID)
	if agent.Inbox[0].State != agentsession.MessageQueued || agent.Inbox[0].DeliveryAttempt != 1 {
		t.Fatalf("inbox delivery was not requeued exactly: %+v", agent.Inbox)
	}
}
