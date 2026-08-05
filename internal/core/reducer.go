package core

import (
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

var identifier = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)

type Policy struct{}

func (Policy) ValidateProposal(w *Workflow, p Proposal) error {
	if w == nil {
		return errors.New("workflow is required")
	}
	if p.WorkflowID != w.ID {
		return fmt.Errorf("proposal targets %q, expected %q", p.WorkflowID, w.ID)
	}
	if strings.TrimSpace(p.Actor) == "" {
		return errors.New("actor is required")
	}
	if len(p.Mutations) == 0 {
		return errors.New("proposal has no mutations")
	}
	clone := CloneWorkflow(w)
	for i, m := range p.Mutations {
		if err := validateMutation(clone, p.Actor, m); err != nil {
			return fmt.Errorf("mutation %d: %w", i, err)
		}
		applyMutation(clone, m, time.Now().UTC())
	}
	if len(clone.Nodes) > clone.Budget.MaxNodes {
		return fmt.Errorf("node budget exceeded: %d > %d", len(clone.Nodes), clone.Budget.MaxNodes)
	}
	if cycle := findCycle(clone); len(cycle) > 0 {
		return fmt.Errorf("dependency cycle: %s", strings.Join(cycle, " -> "))
	}
	return nil
}

func ApplyProposal(w *Workflow, p Proposal, at time.Time) error {
	if err := (Policy{}).ValidateProposal(w, p); err != nil {
		return err
	}
	for _, m := range p.Mutations {
		applyMutation(w, m, at)
	}
	w.UpdatedAt = at
	recompute(w)
	return nil
}

func validateMutation(w *Workflow, actor string, m Mutation) error {
	switch m.Op {
	case "pause", "resume":
		if actor != "human" && actor != "supervisor" {
			return errors.New("only a human or supervisor may pause or resume")
		}
	case "add_node":
		if m.Node == nil {
			return errors.New("add_node requires node")
		}
		if !identifier.MatchString(m.Node.ID) {
			return fmt.Errorf("invalid node id %q", m.Node.ID)
		}
		if _, exists := w.Nodes[m.Node.ID]; exists {
			return fmt.Errorf("node %q already exists", m.Node.ID)
		}
		if strings.TrimSpace(m.Node.Title) == "" || strings.TrimSpace(m.Node.Kind) == "" {
			return errors.New("node title and kind are required")
		}
		if (m.Node.Kind == "merge" || m.Node.Kind == "finalize") && actor != "human" && actor != "supervisor" {
			return errors.New("only a human or supervisor may add a merge or finalize node")
		}
		if err := validateWorktree(w.Root, m.Node.Worktree); err != nil {
			return err
		}
		for _, dep := range m.Node.DependsOn {
			if _, ok := w.Nodes[dep]; !ok {
				return fmt.Errorf("dependency %q does not exist", dep)
			}
		}
	case "add_dependency":
		if w.Nodes[m.NodeID] == nil {
			return fmt.Errorf("node %q does not exist", m.NodeID)
		}
		for _, dep := range m.DependsOn {
			if w.Nodes[dep] == nil {
				return fmt.Errorf("dependency %q does not exist", dep)
			}
		}
	case "set_state", "supersede", "set_session", "set_runtime":
		n := w.Nodes[m.NodeID]
		if n == nil {
			return fmt.Errorf("node %q does not exist", m.NodeID)
		}
		if m.Op == "set_state" && !validTransition(n.State, m.State) {
			return fmt.Errorf("invalid state transition %s -> %s", n.State, m.State)
		}
		if m.Op == "set_runtime" && actor != "supervisor" {
			return errors.New("only the supervisor may route runtimes")
		}
		if m.Op == "set_runtime" && (m.Runtime == nil || m.Runtime.Name == "") {
			return errors.New("set_runtime requires a runtime")
		}
	case "add_evidence":
		if m.Evidence == nil || w.Nodes[m.Evidence.NodeID] == nil {
			return errors.New("evidence requires an existing node")
		}
	case "attest":
		if m.Attestation == nil || w.Nodes[m.Attestation.NodeID] == nil {
			return errors.New("attestation requires an existing node")
		}
		if m.Attestation.Verdict != "pass" && m.Attestation.Verdict != "repair" && m.Attestation.Verdict != "blocked" {
			return errors.New("attestation verdict must be pass, repair, or blocked")
		}
	default:
		return fmt.Errorf("unknown mutation %q", m.Op)
	}
	return nil
}

func validateWorktree(root, worktree string) error {
	if worktree == "" {
		return nil
	}
	r, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	w, err := filepath.Abs(worktree)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(r, w)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("worktree %q is outside workflow root %q", worktree, root)
	}
	return nil
}

func applyMutation(w *Workflow, m Mutation, at time.Time) {
	switch m.Op {
	case "pause":
		w.Paused = true
	case "resume":
		w.Paused = false
	case "add_node":
		n := *m.Node
		if n.State == "" {
			n.State = NodePending
		}
		if n.MaxAttempts == 0 {
			n.MaxAttempts = w.Budget.MaxAttempts
		}
		n.CreatedAt, n.UpdatedAt = at, at
		w.Nodes[n.ID] = &n
		w.Order = append(w.Order, n.ID)
	case "add_dependency":
		n := w.Nodes[m.NodeID]
		seen := map[string]bool{}
		for _, id := range n.DependsOn {
			seen[id] = true
		}
		for _, id := range m.DependsOn {
			if !seen[id] {
				n.DependsOn = append(n.DependsOn, id)
				seen[id] = true
			}
		}
		n.UpdatedAt = at
	case "set_state":
		n := w.Nodes[m.NodeID]
		n.State, n.UpdatedAt = m.State, at
		if m.State == NodeRunning {
			n.Attempt++
		}
	case "supersede":
		n := w.Nodes[m.NodeID]
		n.State, n.UpdatedAt = NodeSuperseded, at
	case "set_session":
		n := w.Nodes[m.NodeID]
		n.SessionID, n.UpdatedAt = m.Reason, at
	case "set_runtime":
		n := w.Nodes[m.NodeID]
		n.Runtime = *m.Runtime
		n.CandidateIndex = m.CandidateIndex
		n.UpdatedAt = at
	case "add_evidence":
		e := *m.Evidence
		if e.CreatedAt.IsZero() {
			e.CreatedAt = at
		}
		w.Evidence = append(w.Evidence, e)
	case "attest":
		a := *m.Attestation
		if a.CreatedAt.IsZero() {
			a.CreatedAt = at
		}
		w.Attestations = append(w.Attestations, a)
	}
}

func validTransition(from, to NodeState) bool {
	allowed := map[NodeState]map[NodeState]bool{
		NodePending: {NodeReady: true, NodeRunning: true, NodeWaiting: true, NodeFailed: true},
		NodeReady:   {NodeRunning: true, NodeWaiting: true, NodeFailed: true},
		NodeRunning: {NodeCompleted: true, NodeWaiting: true, NodeFailed: true, NodeReady: true},
		NodeWaiting: {NodeReady: true, NodeRunning: true, NodeFailed: true},
		NodeFailed:  {NodeReady: true},
	}
	return from == to || allowed[from][to]
}

func recompute(w *Workflow) {
	for _, id := range w.Order {
		n := w.Nodes[id]
		if n == nil || n.State != NodePending {
			continue
		}
		ready := true
		for _, dep := range n.DependsOn {
			if w.Nodes[dep] == nil || (w.Nodes[dep].State != NodeCompleted && w.Nodes[dep].State != NodeSuperseded) {
				ready = false
				break
			}
		}
		if ready {
			n.State = NodeReady
		}
	}
	active, waiting, failed := false, false, false
	for _, n := range w.Nodes {
		switch n.State {
		case NodePending, NodeReady, NodeRunning:
			active = true
		case NodeWaiting:
			waiting = true
		case NodeFailed:
			failed = true
		}
	}
	switch {
	case w.Paused:
		w.Status = WorkflowWaiting
	case active:
		w.Status = WorkflowActive
	case waiting:
		w.Status = WorkflowNeedsHuman
	case failed:
		w.Status = WorkflowFailed
	case w.Budget.RequireAttestation && !hasPassingAttestation(w):
		w.Status = WorkflowWaiting
	default:
		w.Status = WorkflowCompleted
	}
}

func hasPassingAttestation(w *Workflow) bool {
	for _, a := range w.Attestations {
		if a.Verdict == "pass" {
			return true
		}
	}
	return false
}

func findCycle(w *Workflow) []string {
	state := map[string]int{}
	stack := []string{}
	var visit func(string) []string
	visit = func(id string) []string {
		if state[id] == 1 {
			for i, v := range stack {
				if v == id {
					return append(append([]string{}, stack[i:]...), id)
				}
			}
		}
		if state[id] == 2 {
			return nil
		}
		state[id] = 1
		stack = append(stack, id)
		for _, dep := range w.Nodes[id].DependsOn {
			if c := visit(dep); len(c) > 0 {
				return c
			}
		}
		stack = stack[:len(stack)-1]
		state[id] = 2
		return nil
	}
	ids := append([]string{}, w.Order...)
	sort.Strings(ids)
	for _, id := range ids {
		if c := visit(id); len(c) > 0 {
			return c
		}
	}
	return nil
}

func CloneWorkflow(w *Workflow) *Workflow {
	c := *w
	c.Order = append([]string{}, w.Order...)
	c.Nodes = make(map[string]*Node, len(w.Nodes))
	for id, n := range w.Nodes {
		nn := *n
		nn.DependsOn = append([]string{}, n.DependsOn...)
		c.Nodes[id] = &nn
	}
	c.Evidence = append([]Evidence{}, w.Evidence...)
	c.Attestations = append([]Attestation{}, w.Attestations...)
	return &c
}
