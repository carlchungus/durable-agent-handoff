package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/activity"
	"github.com/carlchungus/durable-agent-handoff/internal/core"
	"github.com/carlchungus/durable-agent-handoff/internal/finalize"
	"github.com/carlchungus/durable-agent-handoff/internal/preferences"
	"github.com/carlchungus/durable-agent-handoff/internal/runstate"
	hruntime "github.com/carlchungus/durable-agent-handoff/internal/runtime"
	agentsession "github.com/carlchungus/durable-agent-handoff/internal/session"
)

type Result struct {
	Status       string             `json:"status"`
	Summary      string             `json:"summary"`
	SessionID    string             `json:"session_id,omitempty"`
	Mutations    []encodedMutation  `json:"mutations,omitempty"`
	Attestations []core.Attestation `json:"attestations,omitempty"`
}

// encodedMutation accepts ordinary mutation objects from runtimes without
// strict structured output and JSON-encoded object strings from Codex. Codex's
// strict response schemas cannot express the intentionally dynamic mutation
// union without making every nested operation shape required.
type encodedMutation struct{ core.Mutation }

func (m *encodedMutation) UnmarshalJSON(data []byte) error {
	if len(data) > 0 && data[0] == '"' {
		var encoded string
		if err := json.Unmarshal(data, &encoded); err != nil {
			return err
		}
		data = []byte(encoded)
	}
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(data, &probe); err != nil {
		return err
	}
	if _, ok := probe["op"]; ok {
		return json.Unmarshal(data, &m.Mutation)
	}
	// Agent models commonly describe the intuitive task mutation even when the
	// prompt names the lower-level core mutation. Accept that narrow shape and
	// translate it at the protocol boundary; unknown kinds still fail closed.
	var task struct {
		Kind      string `json:"kind"`
		NodeID    string `json:"node_id"`
		ParentID  string `json:"parent_id"`
		Priority  string `json:"priority"`
		Task      string `json:"task"`
		Authority string `json:"authority"`
	}
	if err := json.Unmarshal(data, &task); err != nil {
		return err
	}
	if task.Kind != "add_node" || task.NodeID == "" || task.Task == "" {
		return fmt.Errorf("unsupported encoded mutation kind %q", task.Kind)
	}
	m.Mutation = core.Mutation{Op: "add_node", Node: &core.Node{ID: task.NodeID, Title: task.Task, Kind: "agent", Prompt: task.Task, Metadata: map[string]string{"parent_id": task.ParentID, "priority": task.Priority, "authority": task.Authority}}}
	return nil
}

type Engine struct {
	Store       *core.Store
	Preferences *preferences.Manager
	Sessions    *agentsession.Store
	Activities  *activity.Store
}

func (e *Engine) RunOne(ctx context.Context, id string) (*core.Node, error) {
	w, err := e.Store.Load(id)
	if err != nil {
		return nil, err
	}
	if w.Paused {
		return nil, errors.New("workflow is paused")
	}
	var node *core.Node
	for _, nid := range w.Order {
		n := w.Nodes[nid]
		if n.State == core.NodeReady && n.Kind != "human" && n.Kind != "merge" {
			node = n
			break
		}
	}
	if node == nil {
		return nil, errors.New("no runnable node")
	}
	if e.Preferences != nil {
		routed, index, routeErr := e.Preferences.Resolve(node.Role, node.Runtime)
		if routeErr != nil {
			return nil, routeErr
		}
		if routed.Sandbox == "" {
			routed.Sandbox = node.Runtime.Sandbox
		}
		if !reflect.DeepEqual(routed, node.Runtime) || index != node.CandidateIndex {
			_, err = e.Store.Apply(core.Proposal{WorkflowID: id, Actor: "supervisor", Mutations: []core.Mutation{
				{Op: "set_runtime", NodeID: node.ID, Runtime: &routed, CandidateIndex: index},
				{Op: "add_evidence", Evidence: &core.Evidence{ID: fmt.Sprintf("route-%s-%d", node.ID, node.Attempt+1), NodeID: node.ID, Kind: "routing", Summary: fmt.Sprintf("selected preference %d: %s", index+1, preferences.Key(routed))}},
			}})
			if err != nil {
				return nil, err
			}
			node.Runtime, node.CandidateIndex = routed, index
		}
	}
	if node.Kind == "agent" {
		agent, sessionErr := e.ensureAgentSession(w, node)
		if sessionErr != nil {
			return nil, sessionErr
		}
		if sessionErr = e.sessionStoreObserve(agent.ID, agentsession.Observation{LogicalState: agentsession.LogicalWorking, ProcessState: agentsession.ProcessStarting}); sessionErr != nil {
			return nil, sessionErr
		}
	}
	if _, err = e.Store.Apply(core.Proposal{WorkflowID: id, Actor: "supervisor", Mutations: []core.Mutation{{Op: "set_state", NodeID: node.ID, State: core.NodeRunning}}}); err != nil {
		return nil, err
	}
	if node.Kind == "command" {
		return node, e.runCommand(ctx, w, node)
	}
	if node.Kind == "finalize" {
		return node, e.runFinalize(ctx, w, node)
	}
	return node, e.runAgent(ctx, w, node)
}

// Reconcile repairs nodes left in running state after a supervisor restart.
// It never guesses from a PID alone and never reruns ambiguous side effects.
func (e *Engine) Reconcile(_ context.Context, id string) error {
	w, err := e.Store.Load(id)
	if err != nil {
		return err
	}
	if e.Activities == nil {
		e.Activities, err = activity.OpenStore(e.Store.Dir())
		if err != nil {
			return err
		}
	}
	if _, err = (&activity.Supervisor{Store: e.Activities}).Recover(); err != nil {
		return err
	}
	if e.Sessions == nil {
		e.Sessions, err = agentsession.OpenStore(e.Store.Dir())
		if err != nil {
			return err
		}
	}
	for _, nodeID := range w.Order {
		n := w.Nodes[nodeID]
		if n == nil || n.Kind != "agent" || n.State == core.NodeRunning {
			continue
		}
		agent, loadErr := e.Sessions.LoadByNode(w.ID, n.ID)
		if errors.Is(loadErr, os.ErrNotExist) {
			continue
		}
		if loadErr != nil {
			return loadErr
		}
		outcomes := attemptOutcomesForNode(w, n.ID)
		for _, message := range agent.Inbox {
			if message.State != agentsession.MessageDispatched {
				continue
			}
			outcome, outcomeErr := outcomeForDelivery(outcomes, message.DeliveryAttempt)
			if outcomeErr != nil {
				return outcomeErr
			}
			if outcome == nil {
				continue
			}
			if outcome.InboxDisposition == "deliver" {
				err = e.Sessions.Deliver(agent.ID, message.DeliveryAttempt)
			} else {
				err = e.Sessions.Requeue(agent.ID, message.DeliveryAttempt)
			}
			if err != nil {
				return err
			}
			if err = e.Sessions.Observe(agent.ID, agentsession.Observation{LogicalState: logicalStateForNode(n.State), ProcessState: agentsession.ProcessExited}); err != nil {
				return err
			}
			agent, err = e.Sessions.Load(agent.ID)
			if err != nil {
				return err
			}
			break
		}
		queued := false
		for _, message := range agent.Inbox {
			queued = queued || (message.State == agentsession.MessageQueued && message.DeliveryAttempt == 0)
		}
		if queued && n.SessionID != "" && (n.State == core.NodeCompleted || n.State == core.NodeWaiting || n.State == core.NodeFailed) {
			if _, err = e.Store.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: []core.Mutation{{Op: "reopen_agent", NodeID: n.ID}}, Rationale: "recover queued durable reply after supervisor interruption"}); err != nil {
				return err
			}
		}
	}
	w, err = e.Store.Load(id)
	if err != nil {
		return err
	}
	for _, nodeID := range w.Order {
		n := w.Nodes[nodeID]
		if n == nil || n.State != core.NodeRunning {
			continue
		}
		activityID := activity.StableID(w.ID, n.ID, fmt.Sprint(n.Attempt))
		tracked, activityErr := e.Activities.Load(activityID)
		if activityErr == nil {
			handled, reconcileErr := e.reconcileActivity(w, n, tracked)
			if reconcileErr != nil {
				return reconcileErr
			}
			if handled {
				continue
			}
		} else if !errors.Is(activityErr, os.ErrNotExist) {
			return activityErr
		}
		dir := filepath.Join(e.Store.Dir(), "workflows", w.ID, "runs", n.ID, fmt.Sprint(n.Attempt))
		manifest, manifestErr := runstate.Load(filepath.Join(dir, "attempt.json"))
		if manifestErr == nil && runstate.ProcessMatches(manifest) {
			_, claimed, claimErr := runstate.ClaimSupervisor(filepath.Join(dir, "attempt.json"), runstate.SupervisorIdentity(), runstate.SupervisorLeaseDuration)
			if claimErr != nil {
				return claimErr
			}
			if claimed && n.SessionID == "" {
				if stream, readErr := os.ReadFile(manifestOutputPath(manifest, dir, true)); readErr == nil {
					if sessionID := extractSessionID(stream); sessionID != "" {
						_, _ = e.Store.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: []core.Mutation{{Op: "set_session", NodeID: n.ID, Reason: sessionID}}, Rationale: "adopt live attempt and preserve exact runtime session"})
					}
				}
			}
			continue
		}

		outputPath := filepath.Join(dir, "last-message.json")
		b, readErr := os.ReadFile(outputPath)
		if readErr != nil || len(bytes.TrimSpace(b)) == 0 {
			b, _ = os.ReadFile(manifestOutputPath(manifest, dir, true))
		}
		if result, parseErr := parseResult(b); parseErr == nil {
			sessionID := result.SessionID
			if sessionID == "" && manifestErr == nil {
				sessionID = manifest.SessionID
			}
			return e.applyAgentResult(w, n, result, sessionID, n.Attempt)
		}

		sessionID := n.SessionID
		if sessionID == "" && manifestErr == nil {
			sessionID = manifest.SessionID
		}
		restartSafe := manifestErr == nil && manifest.RestartSafe
		return e.reconcileInterruptedAgent(w, n, sessionID, restartSafe)
	}
	return nil
}

// reconcileActivity makes the Activity ledger authoritative for all new agent
// attempts. A legacy attempt.json is consulted only when no Activity exists.
func (e *Engine) reconcileActivity(w *core.Workflow, n *core.Node, tracked *activity.Activity) (bool, error) {
	if tracked.State == activity.StateRunning || tracked.State == activity.StateStopping {
		return true, nil
	}
	if len(tracked.Attempts) == 0 {
		return true, fmt.Errorf("activity %s has no attempt", tracked.ID)
	}
	attempt := tracked.Attempts[len(tracked.Attempts)-1]
	dir := filepath.Join(e.Store.Dir(), "workflows", w.ID, "runs", n.ID, fmt.Sprint(n.Attempt))
	stdout, _ := os.ReadFile(attempt.Stdout.Path)
	b, readErr := readActivityAttemptResult(dir, attempt)
	if readErr != nil || len(bytes.TrimSpace(b)) == 0 {
		b = stdout
	}
	if (tracked.State == activity.StateCompleted || tracked.State == activity.StateLost) && len(bytes.TrimSpace(b)) > 0 {
		result, parseErr := parseResult(b)
		if parseErr != nil {
			return true, e.reconcileInterruptedAgent(w, n, extractSessionID(stdout), n.Metadata["restart_safe"] == "true")
		}
		if tracked.State == activity.StateLost {
			code := 0
			identity := activity.AttemptIdentity{ID: attempt.ID, PID: attempt.PID, ProcessStartToken: attempt.ProcessStartToken, ProcessTreeID: attempt.ProcessTreeID, SupervisorID: attempt.SupervisorID, SupervisorGeneration: attempt.SupervisorGeneration}
			if err := e.Activities.ResolveLost(tracked.ID, tracked.Generation, identity, activity.ExitResult{State: activity.StateCompleted, ExitCode: &code}); err != nil && !errors.Is(err, activity.ErrFenced) {
				return true, err
			}
		}
		return true, e.applyAgentResult(w, n, result, extractSessionID(stdout), n.Attempt)
	}
	stderr, _ := os.ReadFile(attempt.Stderr.Path)
	if tracked.State == activity.StateFailed {
		deliveryAttempt, deliveryErr := e.dispatchedDeliveryAttempt(w.ID, n.ID)
		if deliveryErr != nil {
			return true, deliveryErr
		}
		if e.routeAfterLimit(w, n, n.Attempt, deliveryAttempt, attempt.Error, string(stderr), string(stdout)) {
			return true, e.requeueDelivery(w.ID, n.ID, deliveryAttempt)
		}
		failure := attempt.Error + ": " + truncate(string(stderr), 1000) + " " + truncate(string(stdout), 1000)
		if failErr := e.failAgentAttempt(w, n, n.Attempt, deliveryAttempt, "runtime_failure", failure); failErr != nil {
			return true, failErr
		}
		return true, e.requeueDelivery(w.ID, n.ID, deliveryAttempt)
	}
	sessionID := n.SessionID
	if sessionID == "" {
		sessionID = extractSessionID(stdout)
	}
	return true, e.reconcileInterruptedAgent(w, n, sessionID, n.Metadata["restart_safe"] == "true")
}

func (e *Engine) requeueDelivery(workflowID, nodeID string, deliveryAttempt int) error {
	if deliveryAttempt == 0 {
		return nil
	}
	agent, err := e.Sessions.LoadByNode(workflowID, nodeID)
	if err != nil {
		return err
	}
	return e.Sessions.Requeue(agent.ID, deliveryAttempt)
}

func (e *Engine) reconcileInterruptedAgent(w *core.Workflow, n *core.Node, sessionID string, restartSafe bool) error {
	state := core.NodeWaiting
	summary := "supervisor lost the worker before a resumable session identity was persisted; human review is required before retrying"
	if sessionID != "" {
		state = core.NodeReady
		summary = "worker process ended after supervisor interruption; resuming the exact persisted runtime session"
	} else if restartSafe {
		state = core.NodeReady
		summary = "restart-safe worker ended after supervisor interruption; scheduling a fresh attempt"
	}
	deliveryAttempt := 0
	var err error
	if n.Kind == "agent" {
		deliveryAttempt, err = e.dispatchedDeliveryAttempt(w.ID, n.ID)
		if err != nil {
			return err
		}
	}
	mutations := []core.Mutation{{Op: "add_evidence", Evidence: &core.Evidence{ID: fmt.Sprintf("recovery-%s-%d", n.ID, n.Attempt), NodeID: n.ID, Kind: "recovery", Summary: summary}}}
	if n.Kind == "agent" {
		mutations = append(mutations, attemptOutcomeMutation(n, n.Attempt, deliveryAttempt, "runtime_failure", "requeue", summary))
	}
	mutations = append(mutations, core.Mutation{Op: "set_state", NodeID: n.ID, State: state})
	if sessionID != "" && sessionID != n.SessionID {
		mutations = append([]core.Mutation{{Op: "set_session", NodeID: n.ID, Reason: sessionID}}, mutations...)
	}
	_, err = e.Store.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: mutations, Rationale: "reconcile interrupted runtime attempt"})
	if err != nil || n.Kind != "agent" {
		return err
	}
	agent, loadErr := e.Sessions.LoadByNode(w.ID, n.ID)
	if errors.Is(loadErr, os.ErrNotExist) {
		return nil
	}
	if loadErr != nil {
		return loadErr
	}
	if deliveryAttempt > 0 {
		if err = e.Sessions.Requeue(agent.ID, deliveryAttempt); err != nil {
			return err
		}
	}
	return e.Sessions.Observe(agent.ID, agentsession.Observation{LogicalState: logicalStateForNode(state), ProcessState: agentsession.ProcessExited})
}

func attemptOutcomesForNode(w *core.Workflow, nodeID string) []core.Evidence {
	var outcomes []core.Evidence
	for _, evidence := range w.Evidence {
		if evidence.NodeID == nodeID && evidence.Kind == "agent_attempt_outcome" {
			outcomes = append(outcomes, evidence)
		}
	}
	return outcomes
}

func outcomeForDelivery(outcomes []core.Evidence, attempt int) (*core.Evidence, error) {
	var matched *core.Evidence
	for i := range outcomes {
		if outcomes[i].DeliveryAttempt != attempt {
			continue
		}
		if matched != nil && (matched.AttemptOutcome != outcomes[i].AttemptOutcome || matched.InboxDisposition != outcomes[i].InboxDisposition) {
			return nil, fmt.Errorf("conflicting outcomes for inbox delivery attempt %d", attempt)
		}
		matched = &outcomes[i]
	}
	return matched, nil
}

func logicalStateForNode(state core.NodeState) agentsession.LogicalState {
	switch state {
	case core.NodeWaiting:
		return agentsession.LogicalNeedsInput
	case core.NodeFailed, core.NodeCompleted:
		return agentsession.LogicalCompleted
	default:
		return agentsession.LogicalWorking
	}
}

// RecoverAttempt reapplies a completed runtime result that an older or crashed
// supervisor failed to reduce. It is explicit and attempt-addressed so recovery
// never guesses which concurrent session produced the result.
func (e *Engine) RecoverAttempt(id, nodeID string, attempt int) error {
	w, err := e.Store.Load(id)
	if err != nil {
		return err
	}
	n := w.Nodes[nodeID]
	if n == nil {
		return fmt.Errorf("node %q does not exist", nodeID)
	}
	if attempt < 1 || attempt > n.Attempt {
		return fmt.Errorf("attempt %d is outside recorded range 1..%d", attempt, n.Attempt)
	}
	dir := filepath.Join(e.Store.Dir(), "workflows", w.ID, "runs", n.ID, fmt.Sprint(attempt))
	if e.Activities == nil {
		e.Activities, err = activity.OpenStore(e.Store.Dir())
		if err != nil {
			return err
		}
	}
	tracked, activityErr := e.Activities.Load(activity.StableID(w.ID, n.ID, fmt.Sprint(attempt)))
	if activityErr == nil {
		if len(tracked.Attempts) == 0 {
			return fmt.Errorf("activity %s has no attempt", tracked.ID)
		}
		activityAttempt := tracked.Attempts[len(tracked.Attempts)-1]
		b, readErr := readActivityAttemptResult(dir, activityAttempt)
		stdout, _ := os.ReadFile(activityAttempt.Stdout.Path)
		if readErr != nil || len(bytes.TrimSpace(b)) == 0 {
			b = stdout
		}
		result, parseErr := parseResult(b)
		if parseErr != nil {
			return parseErr
		}
		if tracked.State == activity.StateLost {
			code := 0
			identity := activity.AttemptIdentity{ID: activityAttempt.ID, PID: activityAttempt.PID, ProcessStartToken: activityAttempt.ProcessStartToken, ProcessTreeID: activityAttempt.ProcessTreeID, SupervisorID: activityAttempt.SupervisorID, SupervisorGeneration: activityAttempt.SupervisorGeneration}
			if err = e.Activities.ResolveLost(tracked.ID, tracked.Generation, identity, activity.ExitResult{State: activity.StateCompleted, ExitCode: &code}); err != nil && !errors.Is(err, activity.ErrFenced) {
				return err
			}
		}
		return e.applyAgentResult(w, n, result, extractSessionID(stdout), attempt)
	}
	if !errors.Is(activityErr, os.ErrNotExist) {
		return activityErr
	}
	b, readErr := os.ReadFile(filepath.Join(dir, "last-message.json"))
	if readErr != nil || len(bytes.TrimSpace(b)) == 0 {
		manifest, _ := runstate.Load(filepath.Join(dir, "attempt.json"))
		b, readErr = os.ReadFile(manifestOutputPath(manifest, dir, true))
	}
	if readErr != nil {
		return readErr
	}
	result, err := parseResult(b)
	if err != nil {
		return err
	}
	manifest, _ := runstate.Load(filepath.Join(dir, "attempt.json"))
	stream, _ := os.ReadFile(manifestOutputPath(manifest, dir, true))
	return e.applyAgentResult(w, n, result, extractSessionID(stream), attempt)
}

func (e *Engine) runFinalize(ctx context.Context, w *core.Workflow, n *core.Node) error {
	ctx, cancel := context.WithTimeout(ctx, w.Budget.MaxRuntime)
	defer cancel()
	result, err := finalize.Execute(ctx, finalize.ExecRunner{Dir: workdir(w, n)}, w, n)
	if errors.Is(err, finalize.ErrGatesPending) {
		_, applyErr := e.Store.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: []core.Mutation{
			{Op: "add_evidence", Evidence: &core.Evidence{ID: fmt.Sprintf("pr-wait-%s-%d", n.ID, n.Attempt+1), NodeID: n.ID, Kind: "github", Summary: result.Summary, URI: result.PRURL}},
			{Op: "set_state", NodeID: n.ID, State: core.NodeReady},
		}})
		return applyErr
	}
	if err != nil {
		return e.fail(w, n, err.Error())
	}
	_, err = e.Store.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: []core.Mutation{
		{Op: "add_evidence", Evidence: &core.Evidence{ID: fmt.Sprintf("merged-%s-%d", n.ID, n.Attempt+1), NodeID: n.ID, Kind: "github", Summary: result.Summary, URI: result.PRURL, Digest: result.HeadSHA}},
		{Op: "set_state", NodeID: n.ID, State: core.NodeCompleted},
	}})
	return err
}

func (e *Engine) runCommand(ctx context.Context, w *core.Workflow, n *core.Node) error {
	ctx, cancel := context.WithTimeout(ctx, w.Budget.MaxRuntime)
	defer cancel()
	if n.Runtime.Executable == "" {
		return e.fail(w, n, "command executable is empty")
	}
	cmd := exec.CommandContext(ctx, n.Runtime.Executable, n.Runtime.Args...)
	cmd.Dir = workdir(w, n)
	out, err := cmd.CombinedOutput()
	ev := core.Evidence{ID: "evidence-" + n.ID + fmt.Sprint(n.Attempt+1), NodeID: n.ID, Kind: "command", Summary: truncate(string(out), 4000)}
	state := core.NodeCompleted
	if err != nil {
		state = core.NodeFailed
		ev.Summary = fmt.Sprintf("%v\n%s", err, ev.Summary)
	}
	_, applyErr := e.Store.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: []core.Mutation{{Op: "add_evidence", Evidence: &ev}, {Op: "set_state", NodeID: n.ID, State: state}}})
	return applyErr
}

func (e *Engine) runAgent(ctx context.Context, w *core.Workflow, n *core.Node) error {
	ctx, cancel := context.WithTimeout(ctx, w.Budget.MaxRuntime)
	defer cancel()
	attempt := n.Attempt + 1
	baseDir := filepath.Join(e.Store.Dir(), "workflows", w.ID, "runs", n.ID, fmt.Sprint(attempt))
	agent, err := e.ensureAgentSession(w, n)
	if err != nil {
		return e.failAgentAttempt(w, n, attempt, 0, "runtime_failure", err.Error())
	}
	defer func() {
		_ = e.Sessions.Observe(agent.ID, agentsession.Observation{ProcessState: agentsession.ProcessExited})
	}()
	dispatched, err := e.Sessions.Dispatch(agent.ID, attempt)
	if err != nil {
		deliveryAttempt, _ := e.dispatchedDeliveryAttempt(w.ID, n.ID)
		return e.failAgentAttempt(w, n, attempt, deliveryAttempt, "runtime_failure", err.Error())
	}
	deliveryAttempt := 0
	if len(dispatched) > 0 {
		deliveryAttempt = dispatched[0].DeliveryAttempt
		defer func() { _ = e.Sessions.Requeue(agent.ID, deliveryAttempt) }()
	}
	prompt := contract(w, n, dispatched)
	if e.Activities == nil {
		e.Activities, err = activity.OpenStore(e.Store.Dir())
		if err != nil {
			return e.failAgentAttempt(w, n, attempt, deliveryAttempt, "runtime_failure", err.Error())
		}
	}
	activityID := activity.StableID(w.ID, n.ID, fmt.Sprint(attempt))
	activityRecord, err := e.Activities.Ensure(activity.Descriptor{
		ID:             activityID,
		OwnerSessionID: agent.ID,
		Work: activity.WorkSpec{
			Kind: "agent", Cwd: workdir(w, n), Intent: w.ID + "/" + n.ID,
		},
	})
	if err != nil {
		return e.failAgentAttempt(w, n, attempt, deliveryAttempt, "runtime_failure", err.Error())
	}
	expectedOrdinal := len(activityRecord.Attempts) + 1
	dir := filepath.Join(baseDir, fmt.Sprintf("activity-attempt-%d", expectedOrdinal))
	if err = os.MkdirAll(dir, 0o700); err != nil {
		return e.failAgentAttempt(w, n, attempt, deliveryAttempt, "runtime_failure", err.Error())
	}
	schema := filepath.Join(dir, "result.schema.json")
	output := filepath.Join(dir, "last-message.json")
	if err = os.WriteFile(schema, []byte(resultSchema), 0o600); err != nil {
		return e.failAgentAttempt(w, n, attempt, deliveryAttempt, "runtime_failure", err.Error())
	}
	c, err := hruntime.Build(n.Runtime, workdir(w, n), prompt, n.SessionID, schema, output)
	if err != nil {
		return e.failAgentAttempt(w, n, attempt, deliveryAttempt, "runtime_failure", err.Error())
	}
	activityAttempt, stdout, stderr, err := e.Activities.PrepareAttempt(activityRecord.ID, activityRecord.Generation, activity.AttemptStart{
		Runtime: n.Runtime.Name, Model: n.Runtime.Model, CommandDigest: runstate.CommandDigest(c.Name, c.Args),
	})
	if err != nil {
		return e.failAgentAttempt(w, n, attempt, deliveryAttempt, "runtime_failure", err.Error())
	}
	if activityAttempt.Ordinal != expectedOrdinal {
		_ = stdout.Close()
		_ = stderr.Close()
		failure := fmt.Sprintf("activity attempt ordinal changed concurrently: expected %d, got %d", expectedOrdinal, activityAttempt.Ordinal)
		_ = e.Activities.FailPrepared(activityRecord.ID, activityRecord.Generation, activityAttempt.ID, failure)
		return e.failAgentAttempt(w, n, attempt, deliveryAttempt, "runtime_failure", failure)
	}
	var stdin []byte
	if c.PromptOnStdin {
		stdin = []byte(prompt)
	}
	gated, err := activity.PrepareGatedCommand(append([]string{c.Name}, c.Args...), workdir(w, n), nil, stdin)
	if err != nil {
		_ = stdout.Close()
		_ = stderr.Close()
		_ = e.Activities.FailPrepared(activityRecord.ID, activityRecord.Generation, activityAttempt.ID, err.Error())
		return e.failAgentAttempt(w, n, attempt, deliveryAttempt, "runtime_failure", err.Error())
	}
	cmd := gated.Command
	stdoutPath := activityAttempt.Stdout.Path
	stderrPath := activityAttempt.Stderr.Path
	defer stdout.Close()
	defer stderr.Close()
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err = cmd.Start(); err != nil {
		gated.Abort()
		_ = e.Activities.FailPrepared(activityRecord.ID, activityRecord.Generation, activityAttempt.ID, err.Error())
		return e.failAgentAttempt(w, n, attempt, deliveryAttempt, "runtime_failure", err.Error())
	}
	processToken := waitForProcessStartToken(cmd.Process.Pid, 2*time.Second)
	if processToken == "" {
		gated.Abort()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		failure := "could not establish exact process start token"
		_ = e.Activities.FailPrepared(activityRecord.ID, activityRecord.Generation, activityAttempt.ID, failure)
		return e.failAgentAttempt(w, n, attempt, deliveryAttempt, "runtime_failure", failure)
	}
	treeID, treeErr := gated.BindProcessTree(cmd.Process.Pid, processToken)
	if treeErr != nil {
		gated.Abort()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		_ = e.Activities.FailPrepared(activityRecord.ID, activityRecord.Generation, activityAttempt.ID, treeErr.Error())
		return e.failAgentAttempt(w, n, attempt, deliveryAttempt, "runtime_failure", treeErr.Error())
	}
	activityAttempt, err = e.Activities.MarkRunning(activityRecord.ID, activityRecord.Generation, activityAttempt.ID, activity.ProcessIdentity{PID: cmd.Process.Pid, ProcessStartToken: processToken, ProcessTreeID: treeID, SupervisorID: runstate.SupervisorIdentity(), SupervisorGeneration: 1})
	if err != nil {
		gated.Abort()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return e.failAgentAttempt(w, n, attempt, deliveryAttempt, "runtime_failure", err.Error())
	}
	activityIdentity := activity.AttemptIdentity{ID: activityAttempt.ID, PID: activityAttempt.PID, ProcessStartToken: activityAttempt.ProcessStartToken, ProcessTreeID: activityAttempt.ProcessTreeID, SupervisorID: activityAttempt.SupervisorID, SupervisorGeneration: activityAttempt.SupervisorGeneration}
	gated.CompleteActivity(e.Activities.Root(), activityRecord.ID, activityRecord.Generation, activityIdentity)
	if err = gated.Release(); err != nil {
		_, _ = (&activity.Supervisor{Store: e.Activities}).StopExpected(activityRecord.ID, activity.ControlRequest{ExpectedGeneration: activityRecord.Generation, ExpectedAttempt: activityIdentity})
		_ = cmd.Wait()
		return e.failAgentAttempt(w, n, attempt, deliveryAttempt, "runtime_failure", "release gated activity command: "+err.Error())
	}
	stopOnContext := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_, _ = (&activity.Supervisor{Store: e.Activities}).StopExpected(activityRecord.ID, activity.ControlRequest{ExpectedGeneration: activityRecord.Generation, ExpectedAttempt: activityIdentity})
		case <-stopOnContext:
		}
	}()
	_ = e.Sessions.Observe(agent.ID, agentsession.Observation{ProcessState: agentsession.ProcessRunning})
	stopObserve := make(chan struct{})
	observed := observeRuntimeEvents(stdoutPath, stopObserve, func(sessionID string) {
		if sessionID == "" || sessionID == n.SessionID {
			return
		}
		_, _ = e.Store.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: []core.Mutation{{Op: "set_session", NodeID: n.ID, Reason: sessionID}}, Rationale: "persist runtime session identity before the process exits"})
		_ = e.Sessions.Observe(agent.ID, agentsession.Observation{RuntimeSessionID: sessionID})
	})
	err = cmd.Wait()
	close(stopOnContext)
	_ = e.Sessions.Observe(agent.ID, agentsession.Observation{ProcessState: agentsession.ProcessExited})
	close(stopObserve)
	<-observed
	_ = stdout.Sync()
	_ = stderr.Sync()
	stdoutBytes, _ := os.ReadFile(stdoutPath)
	stderrBytes, _ := os.ReadFile(stderrPath)
	activityState := activity.StateCompleted
	activityError := ""
	if err != nil {
		activityState = activity.StateFailed
		activityError = err.Error()
	}
	activityExitCode := 0
	if cmd.ProcessState != nil {
		activityExitCode = cmd.ProcessState.ExitCode()
	}
	finishActivityErr := e.Activities.FinishAttempt(activityRecord.ID, activityRecord.Generation, activityIdentity, activity.ExitResult{State: activityState, ExitCode: &activityExitCode, Error: activityError})
	if errors.Is(finishActivityErr, activity.ErrFenced) {
		if ctx.Err() != nil {
			return e.failAgentAttempt(w, n, attempt, deliveryAttempt, "runtime_failure", "runtime deadline exceeded: "+ctx.Err().Error())
		}
		current, loadErr := e.Activities.Load(activityRecord.ID)
		if loadErr != nil || (current.State != activity.StateCompleted && current.State != activity.StateFailed) {
			return nil
		}
		finishActivityErr = nil
	}
	if finishActivityErr != nil {
		return finishActivityErr
	}
	if err != nil {
		failure := fmt.Sprintf("runtime failed: %v: %s %s", err, truncate(string(stderrBytes), 1000), truncate(string(stdoutBytes), 1000))
		if e.routeAfterLimit(w, n, attempt, deliveryAttempt, failure, string(stderrBytes), string(stdoutBytes)) {
			return nil
		}
		return e.failAgentAttempt(w, n, attempt, deliveryAttempt, "runtime_failure", failure)
	}
	b := stdoutBytes
	if data, readErr := os.ReadFile(output); readErr == nil && len(bytes.TrimSpace(data)) > 0 {
		b = data
	}
	result, err := parseResult(b)
	if err != nil {
		failure := err.Error() + ": " + truncate(string(stderrBytes), 1000) + " " + truncate(string(stdoutBytes), 1000)
		if e.routeAfterLimit(w, n, attempt, deliveryAttempt, failure, string(stderrBytes), string(stdoutBytes)) {
			return nil
		}
		return e.failAgentAttempt(w, n, attempt, deliveryAttempt, "parse_failure", failure)
	}
	stats, inspectErr := finalize.InspectDiff(ctx, finalize.ExecRunner{Dir: workdir(w, n)}, workdir(w, n))
	if inspectErr == nil && exceedsDiffBudget(w.Budget, stats) {
		summary := fmt.Sprintf("worker stopped at deterministic diff budget: %d files / %d lines exceeds %d files / %d lines; result: %s", stats.Files, stats.Lines, w.Budget.MaxChangedFiles, w.Budget.MaxDiffLines, truncate(result.Summary, 1000))
		_, applyErr := e.Store.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: []core.Mutation{
			{Op: "add_evidence", Evidence: &core.Evidence{ID: fmt.Sprintf("budget-%s-%d", n.ID, n.Attempt+1), NodeID: n.ID, Kind: "budget", Summary: summary}},
			attemptOutcomeMutation(n, attempt, deliveryAttempt, "diff_budget", "deliver", summary),
			{Op: "set_state", NodeID: n.ID, State: core.NodeWaiting},
		}, Rationale: "deterministic changed-file and diff-line budget"})
		if applyErr == nil {
			_ = e.Sessions.Observe(agent.ID, agentsession.Observation{LogicalState: agentsession.LogicalNeedsInput, ProcessState: agentsession.ProcessExited})
			if len(dispatched) > 0 {
				applyErr = e.Sessions.Deliver(agent.ID, deliveryAttempt)
			}
		}
		return applyErr
	}
	return e.applyAgentResult(w, n, result, extractSessionID(stdoutBytes), n.Attempt+1)
}

func readActivityAttemptResult(baseDir string, attempt activity.Attempt) ([]byte, error) {
	path := filepath.Join(baseDir, fmt.Sprintf("activity-attempt-%d", attempt.Ordinal), "last-message.json")
	b, err := os.ReadFile(path)
	if (err != nil || len(bytes.TrimSpace(b)) == 0) && attempt.Ordinal == 1 {
		// Compatibility for Activities created by pre-v0.4 development builds.
		return os.ReadFile(filepath.Join(baseDir, "last-message.json"))
	}
	return b, err
}

func exceedsDiffBudget(budget core.Budget, stats finalize.DiffStats) bool {
	return stats.Files > budget.MaxChangedFiles || stats.Lines > budget.MaxDiffLines
}

func (e *Engine) applyAgentResult(w *core.Workflow, n *core.Node, result Result, streamSessionID string, attempt int) error {
	deliveryAttempt, deliveryErr := e.dispatchedDeliveryAttempt(w.ID, n.ID)
	if deliveryErr != nil {
		return deliveryErr
	}
	if result.Status != "completed" && result.Status != "continue" && result.Status != "needs_human" && result.Status != "blocked" {
		return e.failAgentAttempt(w, n, attempt, deliveryAttempt, "parse_failure", fmt.Sprintf("unsupported agent result status %q", result.Status))
	}
	mut := make([]core.Mutation, 0, len(result.Mutations)+4)
	for _, proposed := range result.Mutations {
		mutation := proposed.Mutation
		if mutation.Op == "add_evidence" && mutation.Evidence != nil && mutation.Evidence.Kind == "agent_attempt_outcome" {
			return e.failAgentAttempt(w, n, attempt, deliveryAttempt, "parse_failure", "runtime attempted to forge reserved attempt outcome evidence")
		}
		if mutation.Op == "add_node" && mutation.Node != nil && mutation.Node.Runtime.Name == "" {
			node := *mutation.Node
			node.Runtime, node.Role = n.Runtime, n.Role
			mutation.Node = &node
		}
		mut = append(mut, mutation)
	}
	// Runtime protocol events, unlike the model-authored result, are the source
	// of truth for exact resume identity.
	sessionID := streamSessionID
	if sessionID == "" {
		sessionID = result.SessionID
	}
	if sessionID != "" && sessionID != n.SessionID {
		mut = append(mut, core.Mutation{Op: "set_session", NodeID: n.ID, Reason: sessionID})
	}
	for i := range result.Attestations {
		a := normalizeAttestation(result.Attestations[i])
		if a.NodeID == "" {
			a.NodeID = n.ID
		}
		if a.ID == "" {
			a.ID = fmt.Sprintf("attest-%s-%d", n.ID, i+1)
		}
		mut = append(mut, core.Mutation{Op: "attest", Attestation: &a})
	}
	state := core.NodeCompleted
	if result.Status == "continue" {
		state = core.NodeReady
	}
	if result.Status == "needs_human" {
		state = core.NodeWaiting
	}
	if result.Status == "blocked" {
		state = core.NodeFailed
	}
	mut = append(mut,
		core.Mutation{Op: "add_evidence", Evidence: &core.Evidence{ID: fmt.Sprintf("result-%s-%d", n.ID, attempt), NodeID: n.ID, Kind: "agent", Summary: result.Summary}},
		attemptOutcomeMutation(n, attempt, deliveryAttempt, result.Status, "deliver", result.Summary),
		core.Mutation{Op: "set_state", NodeID: n.ID, State: state},
	)
	_, err := e.Store.Apply(core.Proposal{WorkflowID: w.ID, Actor: n.ID, Mutations: mut, Rationale: result.Summary})
	if err != nil {
		return e.failAgentAttempt(w, n, attempt, deliveryAttempt, "parse_failure", "runtime proposed an invalid workflow mutation: "+err.Error())
	}
	if n.Kind == "agent" {
		agent, sessionErr := e.ensureAgentSession(w, n)
		if sessionErr != nil {
			return sessionErr
		}
		logical := logicalStateForResult(result.Status)
		if sessionErr = e.Sessions.Observe(agent.ID, agentsession.Observation{RuntimeSessionID: sessionID, LogicalState: logical, ProcessState: agentsession.ProcessExited}); sessionErr != nil {
			return sessionErr
		}
		if current, loadErr := e.Sessions.Load(agent.ID); loadErr == nil {
			for _, message := range current.Inbox {
				if message.State == agentsession.MessageDispatched && message.DeliveryAttempt == deliveryAttempt {
					return e.Sessions.Deliver(agent.ID, deliveryAttempt)
				}
			}
		}
	}
	return nil
}

func (e *Engine) ensureAgentSession(w *core.Workflow, n *core.Node) (*agentsession.Session, error) {
	if e.Sessions == nil {
		var err error
		e.Sessions, err = agentsession.OpenStore(e.Store.Dir())
		if err != nil {
			return nil, err
		}
	}
	agent, err := e.Sessions.Ensure(agentsession.Descriptor{
		WorkflowID: w.ID, NodeID: n.ID, ParentAgentID: n.Metadata["parent_id"], Name: n.Title,
		Runtime: n.Runtime.Name, RuntimeSessionID: n.SessionID, Worktree: workdir(w, n),
	})
	if err != nil {
		return nil, err
	}
	if err = e.Sessions.Observe(agent.ID, agentsession.Observation{Runtime: n.Runtime.Name, RuntimeSessionID: n.SessionID, Worktree: workdir(w, n)}); err != nil {
		return nil, err
	}
	return e.Sessions.Load(agent.ID)
}

func (e *Engine) sessionStoreObserve(id string, observation agentsession.Observation) error {
	if e.Sessions == nil {
		return errors.New("agent session store is not initialized")
	}
	return e.Sessions.Observe(id, observation)
}

func logicalStateForResult(status string) agentsession.LogicalState {
	switch status {
	case "completed", "blocked":
		return agentsession.LogicalCompleted
	case "needs_human":
		return agentsession.LogicalNeedsInput
	default:
		return agentsession.LogicalWorking
	}
}

func normalizeAttestation(a core.Attestation) core.Attestation {
	sourceVerdict := a.Verdict
	a.RawVerdict = ""
	switch sourceVerdict {
	case "fail_blocking":
		a.RawVerdict = sourceVerdict
		a.Verdict = "blocked"
	case "pass_with_limit", "pass_with_runtime_limit":
		a.RawVerdict = sourceVerdict
		a.Verdict = "repair"
	}
	return a
}

// observeRuntimeEvents tails a file that the child process writes directly.
// Direct file descriptors matter: if the supervisor crashes, the child keeps
// its descriptor and can finish emitting output instead of losing a Go pipe.
func observeRuntimeEvents(path string, stop <-chan struct{}, onSession func(string)) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		f, err := os.Open(path)
		if err != nil {
			return
		}
		defer f.Close()
		buf := make([]byte, 32<<10)
		pending := make([]byte, 0, 32<<10)
		seen := false
		stopping := false
		for {
			n, readErr := f.Read(buf)
			if n > 0 && !seen {
				pending = append(pending, buf[:n]...)
				if id := extractSessionID(pending); id != "" {
					seen = true
					onSession(id)
				}
			}
			if readErr != nil && !errors.Is(readErr, io.EOF) {
				return
			}
			if n > 0 {
				continue
			}
			if stopping {
				return
			}
			select {
			case <-stop:
				stopping = true
			case <-time.After(50 * time.Millisecond):
			}
		}
	}()
	return done
}

func manifestOutputPath(manifest runstate.Manifest, legacyDir string, stdout bool) string {
	if stdout && manifest.StdoutPath != "" {
		return manifest.StdoutPath
	}
	if !stdout && manifest.StderrPath != "" {
		return manifest.StderrPath
	}
	if stdout {
		return filepath.Join(legacyDir, "events.jsonl")
	}
	return filepath.Join(legacyDir, "stderr.log")
}

func waitForProcessStartToken(pid int, timeout time.Duration) string {
	deadline := time.Now().Add(timeout)
	for {
		if token := runstate.ProcessStartToken(pid); token != "" {
			return token
		}
		if time.Now().After(deadline) {
			return ""
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (e *Engine) routeAfterLimit(w *core.Workflow, n *core.Node, attempt, deliveryAttempt int, failure string, rawOutput ...string) bool {
	if e.Preferences == nil || n.Role == "" {
		return false
	}
	class := preferences.ClassifyFailure(strings.Join(append([]string{failure}, rawOutput...), " "))
	if class == "runtime_error" {
		return false
	}
	if e.Preferences.Record(n.Runtime, class, failure) != nil {
		return false
	}
	next, index, routeErr := e.Preferences.Resolve(n.Role, n.Runtime)
	mutations := []core.Mutation{
		{Op: "add_evidence", Evidence: &core.Evidence{ID: fmt.Sprintf("limit-%s-%d", n.ID, n.Attempt+1), NodeID: n.ID, Kind: "provider_limit", Summary: fmt.Sprintf("%s hit %s; %s", preferences.Key(n.Runtime), class, truncate(failure, 500))}},
		attemptOutcomeMutation(n, attempt, deliveryAttempt, "provider_limit", "requeue", failure),
		{Op: "refund_attempt", NodeID: n.ID},
		{Op: "set_state", NodeID: n.ID, State: core.NodeReady},
	}
	if routeErr == nil {
		mutations = append([]core.Mutation{{Op: "set_runtime", NodeID: n.ID, Runtime: &next, CandidateIndex: index}}, mutations...)
	}
	_, err := e.Store.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: mutations, Rationale: "observable preference-ladder fallback"})
	return err == nil
}

func attemptOutcomeMutation(n *core.Node, attempt, deliveryAttempt int, outcome, disposition, summary string) core.Mutation {
	return core.Mutation{Op: "add_evidence", Evidence: &core.Evidence{
		ID: fmt.Sprintf("attempt-outcome-%s-r%d-d%d-%s", n.ID, attempt, deliveryAttempt, outcome), NodeID: n.ID,
		Kind: "agent_attempt_outcome", Summary: truncate(summary, 1000), Attempt: attempt,
		DeliveryAttempt: deliveryAttempt, AttemptOutcome: outcome, InboxDisposition: disposition,
	}}
}

func (e *Engine) dispatchedDeliveryAttempt(workflowID, nodeID string) (int, error) {
	if e.Sessions == nil {
		var err error
		e.Sessions, err = agentsession.OpenStore(e.Store.Dir())
		if err != nil {
			return 0, err
		}
	}
	agent, err := e.Sessions.LoadByNode(workflowID, nodeID)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	attempt := 0
	for _, message := range agent.Inbox {
		if message.State != agentsession.MessageDispatched {
			continue
		}
		if attempt != 0 && attempt != message.DeliveryAttempt {
			return 0, errors.New("agent session has multiple dispatched inbox attempts")
		}
		attempt = message.DeliveryAttempt
	}
	return attempt, nil
}

func (e *Engine) failAgentAttempt(w *core.Workflow, n *core.Node, attempt, deliveryAttempt int, outcome, reason string) error {
	state := core.NodeFailed
	if attempt < n.MaxAttempts {
		state = core.NodeReady
	}
	_, err := e.Store.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: []core.Mutation{
		{Op: "add_evidence", Evidence: &core.Evidence{ID: fmt.Sprintf("error-%s-%d", n.ID, attempt), NodeID: n.ID, Kind: "runtime_error", Summary: reason}},
		attemptOutcomeMutation(n, attempt, deliveryAttempt, outcome, "requeue", reason),
		{Op: "set_state", NodeID: n.ID, State: state},
	}})
	return err
}

func (e *Engine) fail(w *core.Workflow, n *core.Node, reason string) error {
	state := core.NodeFailed
	if n.Attempt+1 < n.MaxAttempts {
		state = core.NodeReady
	}
	_, err := e.Store.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: []core.Mutation{{Op: "add_evidence", Evidence: &core.Evidence{ID: fmt.Sprintf("error-%s-%d", n.ID, n.Attempt+1), NodeID: n.ID, Kind: "runtime_error", Summary: reason}}, {Op: "set_state", NodeID: n.ID, State: state}}})
	return err
}

func workdir(w *core.Workflow, n *core.Node) string {
	if n.Worktree != "" {
		return n.Worktree
	}
	return w.Root
}
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
func parseResult(b []byte) (Result, error) {
	var r Result
	if json.Unmarshal(bytes.TrimSpace(b), &r) == nil && r.Status != "" {
		return r, nil
	}
	lines := bytes.Split(b, []byte("\n"))
	for i := len(lines) - 1; i >= 0; i-- {
		var direct Result
		if json.Unmarshal(lines[i], &direct) == nil && direct.Status != "" {
			return direct, nil
		}
		var v any
		if json.Unmarshal(lines[i], &v) != nil {
			continue
		}
		found := findResult(v)
		if found != nil {
			return *found, nil
		}
	}
	return r, errors.New("runtime did not emit a valid result object")
}
func findResult(v any) *Result {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	if x, ok := m["result"]; ok {
		if s, ok := x.(string); ok {
			var r Result
			if json.Unmarshal([]byte(s), &r) == nil && r.Status != "" {
				return &r
			}
		}
	}
	for _, x := range m {
		if r := findResult(x); r != nil {
			return r
		}
	}
	return nil
}
func extractSessionID(b []byte) string {
	lines := bytes.Split(b, []byte("\n"))
	for _, line := range lines {
		var v any
		if json.Unmarshal(line, &v) == nil {
			if id := findSessionID(v); id != "" {
				return id
			}
		}
	}
	return ""
}
func findSessionID(v any) string {
	switch x := v.(type) {
	case map[string]any:
		for _, key := range []string{"thread_id", "threadId", "session_id", "sessionId", "session_file", "sessionFile", "session_path", "sessionPath"} {
			if id, ok := x[key].(string); ok && len(id) >= 8 {
				return id
			}
		}
		for _, child := range x {
			if id := findSessionID(child); id != "" {
				return id
			}
		}
	case []any:
		for _, child := range x {
			if id := findSessionID(child); id != "" {
				return id
			}
		}
	}
	return ""
}
func contract(w *core.Workflow, n *core.Node, messages []agentsession.Message) string {
	inbox := ""
	if len(messages) > 0 {
		var b strings.Builder
		b.WriteString("\n\nDurable inbox messages for this exact attempt:\n")
		for _, message := range messages {
			fmt.Fprintf(&b, "[%s from %s] %s\n", message.ID, message.From, message.Body)
		}
		inbox = b.String()
	}
	return fmt.Sprintf(`Goal: %s

Current dynamic task: %s
Task id: %s

%s

You own how to accomplish and verify this task. Inspect live state, adapt the plan when evidence changes, and propose new nodes or independent verifier work when useful. Do not push, merge, access production, or expand authority. End with one JSON result matching the supplied schema.%s`, w.Goal, n.Title, n.ID, n.Prompt, inbox)
}

const resultSchema = `{"type":"object","required":["status","summary","session_id","mutations","attestations"],"properties":{"status":{"enum":["completed","continue","blocked","needs_human"]},"summary":{"type":"string"},"session_id":{"type":"string"},"mutations":{"type":"array","description":"Workflow mutations. Each item is a JSON-encoded core Mutation object. Use an empty array when no graph change is needed.","items":{"type":"string"}},"attestations":{"type":"array","items":{"type":"object","required":["verifier","verdict","summary","evidence_ids"],"properties":{"verifier":{"type":"string"},"verdict":{"enum":["pass","repair","blocked","pass_with_limit","pass_with_runtime_limit","fail_blocking"]},"summary":{"type":"string"},"evidence_ids":{"type":"array","items":{"type":"string"}}},"additionalProperties":false}}},"additionalProperties":false}`
