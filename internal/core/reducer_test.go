package core

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func fixtureWorkflow(t *testing.T) *Workflow {
	t.Helper()
	root := t.TempDir()
	now := time.Now().UTC()
	return &Workflow{ID: "wf_test", Goal: "ship safely", Root: root, Status: WorkflowActive, Budget: DefaultBudget(), Nodes: map[string]*Node{}, CreatedAt: now, UpdatedAt: now}
}

func TestDynamicGraphCanGrowAndCompleteWithAttestation(t *testing.T) {
	w := fixtureWorkflow(t)
	proposal := Proposal{WorkflowID: w.ID, Actor: "lead", Mutations: []Mutation{
		{Op: "add_node", Node: &Node{ID: "implement", Title: "implement", Kind: "agent"}},
		{Op: "add_node", Node: &Node{ID: "review", Title: "review independently", Kind: "agent", DependsOn: []string{"implement"}}},
	}}
	if err := ApplyProposal(w, proposal, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if w.Nodes["implement"].State != NodeReady || w.Nodes["review"].State != NodePending {
		t.Fatalf("unexpected states: %#v", w.Nodes)
	}
	if err := ApplyProposal(w, Proposal{WorkflowID: w.ID, Actor: "implement", Mutations: []Mutation{{Op: "set_state", NodeID: "implement", State: NodeRunning}, {Op: "set_state", NodeID: "implement", State: NodeCompleted}}}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if w.Nodes["review"].State != NodeReady {
		t.Fatalf("review did not become ready: %s", w.Nodes["review"].State)
	}
	att := &Attestation{ID: "a1", NodeID: "review", Verifier: "fresh-agent", Verdict: "pass", Summary: "diff and tests verified"}
	if err := ApplyProposal(w, Proposal{WorkflowID: w.ID, Actor: "review", Mutations: []Mutation{{Op: "set_state", NodeID: "review", State: NodeRunning}, {Op: "attest", Attestation: att}, {Op: "set_state", NodeID: "review", State: NodeCompleted}}}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if w.Status != WorkflowCompleted {
		t.Fatalf("status = %s", w.Status)
	}
}

func TestRejectedCycleIsAtomic(t *testing.T) {
	w := fixtureWorkflow(t)
	proposal := Proposal{WorkflowID: w.ID, Actor: "lead", Mutations: []Mutation{
		{Op: "add_node", Node: &Node{ID: "a", Title: "a", Kind: "agent"}},
		{Op: "add_node", Node: &Node{ID: "b", Title: "b", Kind: "agent", DependsOn: []string{"a"}}},
	}}
	if err := ApplyProposal(w, proposal, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	err := ApplyProposal(w, Proposal{WorkflowID: w.ID, Actor: "lead", Mutations: []Mutation{{Op: "add_dependency", NodeID: "a", DependsOn: []string{"b"}}}}, time.Now().UTC())
	if err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("expected cycle error, got %v", err)
	}
	if len(w.Nodes["a"].DependsOn) != 0 {
		t.Fatal("rejected proposal mutated live state")
	}
}

func TestPolicyAuthorityAndRootBoundary(t *testing.T) {
	w := fixtureWorkflow(t)
	err := ApplyProposal(w, Proposal{WorkflowID: w.ID, Actor: "agent", Mutations: []Mutation{{Op: "add_node", Node: &Node{ID: "merge", Title: "merge", Kind: "merge"}}}}, time.Now().UTC())
	if err == nil || !strings.Contains(err.Error(), "human or supervisor") {
		t.Fatalf("unexpected error: %v", err)
	}
	err = ApplyProposal(w, Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []Mutation{{Op: "add_node", Node: &Node{ID: "escape", Title: "escape", Kind: "agent", Worktree: filepath.Dir(w.Root)}}}}, time.Now().UTC())
	if err == nil || !strings.Contains(err.Error(), "outside workflow root") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReadOnlyWorkerCannotEscalateChildCapability(t *testing.T) {
	w := fixtureWorkflow(t)
	if err := ApplyProposal(w, Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []Mutation{{Op: "add_node", Node: &Node{ID: "reviewer", Title: "review", Kind: "agent", Runtime: RuntimeSpec{Name: "codex", Sandbox: "read-only"}}}}}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	err := ApplyProposal(w, Proposal{WorkflowID: w.ID, Actor: "reviewer", Mutations: []Mutation{{Op: "add_node", Node: &Node{ID: "writer", Title: "write", Kind: "agent", Runtime: RuntimeSpec{Name: "codex", Sandbox: "workspace-write"}}}}}, time.Now().UTC())
	if err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("unexpected escalation result: %v", err)
	}
	if w.Nodes["writer"] != nil {
		t.Fatal("rejected escalation mutated workflow")
	}
	if err = ApplyProposal(w, Proposal{WorkflowID: w.ID, Actor: "reviewer", Mutations: []Mutation{{Op: "add_node", Node: &Node{ID: "reader", Title: "read", Kind: "agent", Runtime: RuntimeSpec{Name: "codex", Sandbox: "read-only"}}}}}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
}

func TestCompletionWaitsForAttestation(t *testing.T) {
	w := fixtureWorkflow(t)
	if err := ApplyProposal(w, Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []Mutation{{Op: "add_node", Node: &Node{ID: "one", Title: "one", Kind: "agent"}}}}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := ApplyProposal(w, Proposal{WorkflowID: w.ID, Actor: "one", Mutations: []Mutation{{Op: "set_state", NodeID: "one", State: NodeRunning}, {Op: "set_state", NodeID: "one", State: NodeCompleted}}}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if w.Status != WorkflowWaiting {
		t.Fatalf("status=%s", w.Status)
	}
}
