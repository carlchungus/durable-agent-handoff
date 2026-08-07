package supervisor

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	eventExecutionStarted     = "execution.started"
	eventNodeAdded            = "node.added"
	eventNodeSuperseded       = "node.superseded"
	eventActivityQueued       = "activity.queued"
	eventAttemptPrepared      = "attempt.prepared"
	eventMilestone            = "attempt.milestone"
	eventResultCreated        = "result.created"
	eventMessageQueued        = "message.queued"
	eventMessageDispatched    = "message.dispatched"
	eventMessageSettled       = "message.settled"
	eventLeaseReleased        = "lease.released"
	eventControlRecorded      = "control.recorded"
	eventControlApplied       = "control.applied"
	eventWorkflowPaused       = "workflow.paused"
	eventPauseSettled         = "workflow.pause_settled"
	eventFallbackQueued       = "activity.fallback_queued"
	eventRoleLadderSet        = "role_ladder.set"
	eventFinalizationPrepared = "finalization.prepared"
	eventFinalizationSettled  = "finalization.settled"
	eventLegacyImported       = "legacy.imported"
)

type executionStartedEvent struct {
	Execution *Execution `json:"execution"`
	Workflow  *Workflow  `json:"workflow"`
	Node      *Node      `json:"node"`
	Session   *Session   `json:"session"`
	Activity  *Activity  `json:"activity"`
}

type nodeAddedEvent struct {
	Node *Node `json:"node"`
}

type nodeSupersededEvent struct {
	WorkflowID WorkflowID `json:"workflow_id"`
	NodeID     NodeID     `json:"node_id"`
	At         time.Time  `json:"at"`
}

type activityQueuedEvent struct {
	Activity *Activity `json:"activity"`
}

type fallbackQueuedEvent struct {
	Session  *Session  `json:"session"`
	Activity *Activity `json:"activity"`
}

type roleLadderSetEvent struct {
	Role       string        `json:"role"`
	Candidates []RuntimeSpec `json:"candidates"`
}

type finalizationPreparedEvent struct {
	Finalization *Finalization `json:"finalization"`
}

type finalizationSettledEvent struct {
	ID          string            `json:"id"`
	State       FinalizationState `json:"state"`
	Summary     string            `json:"summary"`
	CompletedAt time.Time         `json:"completed_at"`
	PRURL       string            `json:"pr_url,omitempty"`
}

type attemptPreparedEvent struct {
	Attempt *Attempt `json:"attempt"`
	Lease   *Lease   `json:"lease"`
}

type milestoneEvent struct {
	AttemptID AttemptID `json:"attempt_id"`
	Milestone Milestone `json:"milestone"`
}

type resultCreatedEvent struct {
	Result *Result `json:"result"`
}

type messageEvent struct {
	Message *Message `json:"message"`
}

type messageDispatchEvent struct {
	MessageID          MessageID `json:"message_id"`
	AttemptID          AttemptID `json:"attempt_id"`
	DeliveryGeneration uint64    `json:"delivery_generation"`
}

type messageSettledEvent struct {
	MessageID MessageID  `json:"message_id"`
	Delivered bool       `json:"delivered"`
	At        timeMarker `json:"at"`
}

// timeMarker keeps event payload validation explicit without coupling reducer
// operations to a clock. It has the same JSON representation as time.Time.
type timeMarker = time.Time

type leaseReleasedEvent struct {
	LeaseID LeaseID   `json:"lease_id"`
	At      time.Time `json:"at"`
}

type controlRecordedEvent struct {
	Control *Control `json:"control"`
}

type controlAppliedEvent struct {
	ControlID ControlID `json:"control_id"`
	At        time.Time `json:"at"`
}

type workflowPausedEvent struct {
	Pause *Pause `json:"pause"`
}

type pauseSettledEvent struct {
	WorkflowID  WorkflowID `json:"workflow_id"`
	CompletedAt time.Time  `json:"completed_at"`
}

type legacyImportedEvent struct {
	Import     LegacyImport `json:"import"`
	Executions []*Execution `json:"executions,omitempty"`
	Workflows  []*Workflow  `json:"workflows,omitempty"`
	Sessions   []*Session   `json:"sessions,omitempty"`
	Activities []*Activity  `json:"activities,omitempty"`
	Attempts   []*Attempt   `json:"attempts,omitempty"`
	Results    []*Result    `json:"results,omitempty"`
}

func applyEntry(state *State, entry JournalEntry) error {
	if state == nil {
		return errors.New("supervisor projection is required")
	}
	if entry.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported supervisor journal schema %d", entry.SchemaVersion)
	}
	if entry.Sequence != 0 && entry.Sequence != state.Sequence+1 {
		return fmt.Errorf("supervisor journal sequence %d followed %d", entry.Sequence, state.Sequence)
	}
	for _, domain := range entry.Events {
		if err := applyDomainEvent(state, domain); err != nil {
			return fmt.Errorf("reduce %s: %w", domain.Type, err)
		}
	}
	if entry.Sequence != 0 {
		state.Sequence = entry.Sequence
	}
	if entry.IdempotencyKey != "" {
		if prior, ok := state.Idempotency[entry.IdempotencyKey]; ok && (prior.CommandType != entry.CommandType || prior.InputDigest != entry.InputDigest || prior.ResourceID != entry.ResourceID) {
			return fmt.Errorf("journal contains divergent idempotency record %q", entry.IdempotencyKey)
		}
		state.Idempotency[entry.IdempotencyKey] = IdempotencyRecord{Key: entry.IdempotencyKey, CommandType: entry.CommandType, InputDigest: entry.InputDigest, ResourceID: entry.ResourceID, Sequence: entry.Sequence}
	}
	return nil
}

func applyDomainEvent(state *State, domain DomainEvent) error {
	switch domain.Type {
	case eventExecutionStarted:
		var data executionStartedEvent
		if err := json.Unmarshal(domain.Data, &data); err != nil {
			return err
		}
		if data.Execution == nil || data.Workflow == nil || data.Node == nil || data.Session == nil || data.Activity == nil {
			return errors.New("execution start event is incomplete")
		}
		if state.Executions[data.Execution.ID] != nil || state.Workflows[data.Workflow.ID] != nil || state.Sessions[data.Session.ID] != nil || state.Activities[data.Activity.ID] != nil {
			return errors.New("execution start event reuses a durable identity")
		}
		workflow := cloneWorkflow(data.Workflow)
		if workflow.EvaluatorModel == "" && workflow.OldGoal != nil && workflow.OldGoal.Enabled {
			workflow.EvaluatorModel = workflow.OldGoal.EvaluatorModel
			workflow.MaxTurns = workflow.OldGoal.MaxTurns
		}
		workflow.OldGoal = nil
		workflow.Nodes[data.Node.ID] = cloneNode(data.Node)
		workflow.Order = append(workflow.Order, data.Node.ID)
		state.Executions[data.Execution.ID] = cloneExecution(data.Execution)
		state.Workflows[data.Workflow.ID] = workflow
		state.Sessions[data.Session.ID] = cloneSession(data.Session)
		state.Activities[data.Activity.ID] = cloneActivity(data.Activity)
	case eventNodeAdded:
		var data nodeAddedEvent
		if err := json.Unmarshal(domain.Data, &data); err != nil {
			return err
		}
		if data.Node == nil {
			return errors.New("node event is incomplete")
		}
		workflow := state.Workflows[data.Node.WorkflowID]
		if workflow == nil || workflow.Nodes[data.Node.ID] != nil {
			return errors.New("node event targets an unknown workflow or duplicate node")
		}
		workflow.Nodes[data.Node.ID] = cloneNode(data.Node)
		workflow.Order = append(workflow.Order, data.Node.ID)
	case eventNodeSuperseded:
		var data nodeSupersededEvent
		if err := json.Unmarshal(domain.Data, &data); err != nil {
			return err
		}
		node := lookupNode(state, data.WorkflowID, data.NodeID)
		if node == nil {
			return errors.New("supersede event targets an unknown node")
		}
		if !node.SupersededAt.IsZero() {
			return errors.New("node is already superseded")
		}
		if data.At.IsZero() {
			return errors.New("supersede event omitted immutable timestamp")
		}
		node.SupersededAt = data.At
	case eventActivityQueued:
		var data activityQueuedEvent
		if err := json.Unmarshal(domain.Data, &data); err != nil {
			return err
		}
		if data.Activity == nil || state.Activities[data.Activity.ID] != nil {
			return errors.New("activity event is incomplete or duplicate")
		}
		state.Activities[data.Activity.ID] = cloneActivity(data.Activity)
	case eventFallbackQueued:
		var data fallbackQueuedEvent
		if err := json.Unmarshal(domain.Data, &data); err != nil {
			return err
		}
		if data.Session == nil || data.Activity == nil || state.Sessions[data.Session.ID] != nil || state.Activities[data.Activity.ID] != nil {
			return errors.New("fallback event is incomplete or duplicate")
		}
		workflow := state.Workflows[data.Activity.WorkflowID]
		parent := state.Activities[data.Activity.ParentActivityID]
		if workflow == nil || parent == nil || parent.WorkflowID != workflow.ID || data.Session.WorkflowID != workflow.ID || data.Activity.SessionID != data.Session.ID || data.Activity.ParentActivityID == data.Activity.ID {
			return errors.New("fallback event has broken lineage")
		}
		state.Sessions[data.Session.ID] = cloneSession(data.Session)
		state.Activities[data.Activity.ID] = cloneActivity(data.Activity)
	case eventRoleLadderSet:
		var data roleLadderSetEvent
		if err := json.Unmarshal(domain.Data, &data); err != nil {
			return err
		}
		if data.Role == "" || len(data.Candidates) == 0 {
			return errors.New("role ladder event is incomplete")
		}
		state.RoleLadders[data.Role] = append([]RuntimeSpec(nil), data.Candidates...)
	case eventFinalizationPrepared:
		var data finalizationPreparedEvent
		if err := json.Unmarshal(domain.Data, &data); err != nil {
			return err
		}
		if data.Finalization == nil || state.Finalizations[data.Finalization.ID] != nil {
			return errors.New("finalization preparation is incomplete or duplicate")
		}
		execution := state.Executions[data.Finalization.ExecutionID]
		if execution == nil || execution.WorkflowID != data.Finalization.WorkflowID || strings.TrimSpace(data.Finalization.Repository) == "" || strings.TrimSpace(data.Finalization.PullRequest) == "" || strings.TrimSpace(data.Finalization.HeadSHA) == "" || len(data.Finalization.Gates) == 0 || data.Finalization.State != FinalizationPrepared || data.Finalization.PreparedAt.IsZero() || !data.Finalization.CompletedAt.IsZero() {
			return errors.New("finalization preparation is incomplete")
		}
		state.Finalizations[data.Finalization.ID] = cloneFinalization(data.Finalization)
	case eventFinalizationSettled:
		var data finalizationSettledEvent
		if err := json.Unmarshal(domain.Data, &data); err != nil {
			return err
		}
		finalization := state.Finalizations[data.ID]
		if finalization == nil || finalization.State != FinalizationPrepared || (data.State != FinalizationMerged && data.State != FinalizationBlocked) || strings.TrimSpace(data.Summary) == "" || data.CompletedAt.IsZero() {
			return errors.New("finalization settlement targets an invalid preparation")
		}
		finalization.State = data.State
		finalization.Summary = data.Summary
		if strings.TrimSpace(data.PRURL) != "" {
			finalization.PRURL = data.PRURL
		}
		finalization.CompletedAt = data.CompletedAt
	case eventAttemptPrepared:
		var data attemptPreparedEvent
		if err := json.Unmarshal(domain.Data, &data); err != nil {
			return err
		}
		if data.Attempt == nil || data.Lease == nil || state.Attempts[data.Attempt.ID] != nil || state.Leases[data.Lease.ID] != nil {
			return errors.New("attempt preparation is incomplete or duplicate")
		}
		state.Attempts[data.Attempt.ID] = cloneAttempt(data.Attempt)
		state.Leases[data.Lease.ID] = cloneLease(data.Lease)
	case eventMilestone:
		var data milestoneEvent
		if err := json.Unmarshal(domain.Data, &data); err != nil {
			return err
		}
		attempt := state.Attempts[data.AttemptID]
		if attempt == nil {
			return errors.New("milestone targets an unknown attempt")
		}
		attempt.Milestones = append(attempt.Milestones, cloneMilestone(data.Milestone))
		if data.Milestone.Kind == MilestoneProcessSpawned {
			process := *data.Milestone.Process
			attempt.Process = &process
		}
		if data.Milestone.Kind == MilestoneSessionBound {
			activity := state.Activities[attempt.ActivityID]
			if activity == nil || data.Milestone.Session == nil {
				return errors.New("session binding targets an invalid activity")
			}
			session := state.Sessions[activity.SessionID]
			if session == nil {
				return errors.New("session binding targets an unknown session")
			}
			if session.Native.ID != "" && session.Native != *data.Milestone.Session {
				return errors.New("session binding changes an immutable native identity")
			}
			session.Native = *data.Milestone.Session
		}
	case eventResultCreated:
		var data resultCreatedEvent
		if err := json.Unmarshal(domain.Data, &data); err != nil {
			return err
		}
		if data.Result == nil || state.Results[data.Result.ID] != nil {
			return errors.New("result event is incomplete or duplicate")
		}
		state.Results[data.Result.ID] = cloneResult(data.Result)
	case "claim.created", "evaluation.recorded":
		// Releases before v1.2 duplicated turn data into two extra records. The
		// attempt milestone and result events remain sufficient to rebuild them.
	case "attestation.recorded":
		// Retired v2 publication events remain readable as historical facts, but
		// they no longer create state or publication authority.
	case eventMessageQueued:
		var data messageEvent
		if err := json.Unmarshal(domain.Data, &data); err != nil {
			return err
		}
		if data.Message == nil || state.Messages[data.Message.ID] != nil {
			return errors.New("message event is incomplete or duplicate")
		}
		state.Messages[data.Message.ID] = cloneMessage(data.Message)
	case eventMessageDispatched:
		var data messageDispatchEvent
		if err := json.Unmarshal(domain.Data, &data); err != nil {
			return err
		}
		message := state.Messages[data.MessageID]
		if message == nil || message.State != MessageQueued {
			return errors.New("dispatch targets a non-queued message")
		}
		message.State = MessageDispatched
		message.AttemptID = data.AttemptID
		message.DeliveryGeneration = data.DeliveryGeneration
	case eventMessageSettled:
		var data messageSettledEvent
		if err := json.Unmarshal(domain.Data, &data); err != nil {
			return err
		}
		message := state.Messages[data.MessageID]
		if message == nil || message.State != MessageDispatched {
			return errors.New("settlement targets a non-dispatched message")
		}
		message.SettledAt = data.At
		if data.Delivered {
			message.State = MessageDelivered
		} else {
			message.State = MessageQueued
			message.AttemptID = ""
		}
	case eventLeaseReleased:
		var data leaseReleasedEvent
		if err := json.Unmarshal(domain.Data, &data); err != nil {
			return err
		}
		lease := state.Leases[data.LeaseID]
		if lease == nil || !lease.ReleasedAt.IsZero() {
			return errors.New("lease release targets an inactive lease")
		}
		lease.ReleasedAt = data.At
		for _, pause := range state.Pauses {
			for _, attemptID := range pause.FencedAttemptIDs {
				if attemptID == lease.AttemptID && !containsLease(pause.ReleasedLeaseIDs, lease.ID) {
					pause.ReleasedLeaseIDs = append(pause.ReleasedLeaseIDs, lease.ID)
				}
			}
		}
	case eventControlRecorded:
		var data controlRecordedEvent
		if err := json.Unmarshal(domain.Data, &data); err != nil {
			return err
		}
		if data.Control == nil || state.Controls[data.Control.ID] != nil {
			return errors.New("control event is incomplete or duplicate")
		}
		state.Controls[data.Control.ID] = cloneControl(data.Control)
	case eventControlApplied:
		var data controlAppliedEvent
		if err := json.Unmarshal(domain.Data, &data); err != nil {
			return err
		}
		control := state.Controls[data.ControlID]
		if control == nil || !control.Accepted || !control.AppliedAt.IsZero() || data.At.IsZero() {
			return errors.New("control application targets an invalid control")
		}
		control.AppliedAt = data.At
	case eventWorkflowPaused:
		var data workflowPausedEvent
		if err := json.Unmarshal(domain.Data, &data); err != nil {
			return err
		}
		// V2 journals written before draining pauses had no Phase field and
		// released leases in this event. Preserve replay readability by treating
		// their completed timestamp as the old terminal pause state.
		if data.Pause != nil && data.Pause.Phase == "" {
			if data.Pause.CompletedAt.IsZero() {
				data.Pause.Phase = PauseDraining
			} else {
				data.Pause.Phase = PauseCompleted
			}
		}
		if data.Pause == nil || data.Pause.WorkflowID == "" || data.Pause.Phase == "" || state.Workflows[data.Pause.WorkflowID] == nil || state.Pauses[data.Pause.WorkflowID] != nil {
			return errors.New("pause event is incomplete, unknown, or duplicate")
		}
		state.Pauses[data.Pause.WorkflowID] = clonePause(data.Pause)
	case eventPauseSettled:
		var data pauseSettledEvent
		if err := json.Unmarshal(domain.Data, &data); err != nil {
			return err
		}
		pause := state.Pauses[data.WorkflowID]
		if pause == nil || pause.Phase == PauseCompleted || data.CompletedAt.IsZero() {
			return errors.New("pause settlement targets an invalid pause")
		}
		pause.Phase = PauseCompleted
		pause.CompletedAt = data.CompletedAt
	case eventLegacyImported:
		var data legacyImportedEvent
		if err := json.Unmarshal(domain.Data, &data); err != nil {
			return err
		}
		if _, exists := state.LegacyImports[data.Import.SourceDigest]; exists {
			return errors.New("legacy source digest was already imported")
		}
		for _, execution := range data.Executions {
			state.Executions[execution.ID] = cloneExecution(execution)
		}
		for _, workflow := range data.Workflows {
			state.Workflows[workflow.ID] = cloneWorkflow(workflow)
		}
		for _, session := range data.Sessions {
			state.Sessions[session.ID] = cloneSession(session)
		}
		for _, activity := range data.Activities {
			state.Activities[activity.ID] = cloneActivity(activity)
		}
		for _, attempt := range data.Attempts {
			state.Attempts[attempt.ID] = cloneAttempt(attempt)
		}
		for _, result := range data.Results {
			state.Results[result.ID] = cloneResult(result)
		}
		state.LegacyImports[data.Import.SourceDigest] = data.Import
	default:
		return fmt.Errorf("unknown supervisor domain event %q", domain.Type)
	}
	return nil
}

func validateState(state *State) error {
	activeRoots := map[string]LeaseID{}
	for id, lease := range state.Leases {
		attempt := state.Attempts[lease.AttemptID]
		activity := state.Activities[lease.ActivityID]
		if attempt == nil || activity == nil || attempt.LeaseID != id || attempt.ActivityID != activity.ID || activity.Generation != lease.ActivityGeneration {
			return fmt.Errorf("lease %s is not fenced to its exact activity generation and attempt", id)
		}
		if lease.ReleasedAt.IsZero() {
			if prior := activeRoots[lease.CanonicalWorktree]; prior != "" {
				return fmt.Errorf("canonical worktree %q has active leases %s and %s", lease.CanonicalWorktree, prior, id)
			}
			activeRoots[lease.CanonicalWorktree] = id
		}
	}
	for id, activity := range state.Activities {
		workflow := state.Workflows[activity.WorkflowID]
		if workflow == nil || workflow.Nodes[activity.NodeID] == nil || state.Sessions[activity.SessionID] == nil {
			return fmt.Errorf("activity %s has broken workflow, node, or session identity", id)
		}
		for _, binding := range activity.DependencyBindings {
			result := state.Results[binding.ResultID]
			if result == nil || result.NodeID != binding.DependencyNodeID {
				return fmt.Errorf("activity %s has invalid immutable result binding", id)
			}
		}
	}
	for id, attempt := range state.Attempts {
		activity := state.Activities[attempt.ActivityID]
		if activity == nil || activity.Generation != attempt.ActivityGeneration {
			return fmt.Errorf("attempt %s has invalid activity generation", id)
		}
		turns, results := 0, 0
		for _, milestone := range attempt.Milestones {
			if milestone.Kind == MilestoneTurnStarted {
				turns++
			}
			if milestone.Kind == MilestoneResult {
				results++
			}
		}
		if turns > 1 || results > 1 {
			return fmt.Errorf("attempt %s contains duplicate turn or result milestones", id)
		}
	}
	for id, pause := range state.Pauses {
		if state.Workflows[id] == nil || pause.WorkflowID != id || (pause.Phase != PauseRequested && pause.Phase != PauseDraining && pause.Phase != PauseCompleted) || pause.Phase == PauseCompleted && pause.CompletedAt.IsZero() || pause.Phase != PauseCompleted && !pause.CompletedAt.IsZero() {
			return fmt.Errorf("pause %s has broken workflow identity", id)
		}
		for _, attemptID := range pause.FencedAttemptIDs {
			attempt := state.Attempts[attemptID]
			if attempt == nil {
				return fmt.Errorf("pause %s fences unknown attempt %s", id, attemptID)
			}
			activity := state.Activities[attempt.ActivityID]
			if activity == nil || activity.WorkflowID != id {
				return fmt.Errorf("pause %s fences attempt %s from another workflow", id, attemptID)
			}
		}
	}
	for id, result := range state.Results {
		activity := state.Activities[result.ActivityID]
		attempt := state.Attempts[result.AttemptID]
		if activity == nil || attempt == nil || result.NodeID != activity.NodeID || attempt.ActivityID != activity.ID || result.Generation != activity.Generation {
			return fmt.Errorf("result %s has broken immutable provenance", id)
		}
	}
	for id, finalization := range state.Finalizations {
		execution := (*Execution)(nil)
		if finalization != nil {
			execution = state.Executions[finalization.ExecutionID]
		}
		if finalization == nil || finalization.ID != id || execution == nil || execution.WorkflowID != finalization.WorkflowID || state.Workflows[finalization.WorkflowID] == nil || strings.TrimSpace(finalization.IdempotencyKey) == "" || strings.TrimSpace(finalization.Repository) == "" || strings.TrimSpace(finalization.PullRequest) == "" || (finalization.Method != "merge" && finalization.Method != "squash" && finalization.Method != "rebase") || finalization.State == "" || (finalization.State != FinalizationPrepared && finalization.State != FinalizationMerged && finalization.State != FinalizationBlocked) || strings.TrimSpace(finalization.HeadSHA) == "" || len(finalization.Gates) == 0 {
			return fmt.Errorf("finalization %s has broken durable publication identity", id)
		}
		if finalization.State == FinalizationPrepared && !finalization.CompletedAt.IsZero() || finalization.State != FinalizationPrepared && finalization.CompletedAt.IsZero() {
			return fmt.Errorf("finalization %s has broken terminal state", id)
		}
	}
	for role, candidates := range state.RoleLadders {
		if role == "" || len(candidates) == 0 {
			return fmt.Errorf("role ladder %q is empty", role)
		}
		seenNames := map[string]bool{}
		for _, candidate := range candidates {
			if err := validateRuntime(candidate); err != nil {
				return fmt.Errorf("role ladder %q: %w", role, err)
			}
			if seenNames[candidate.Name] {
				return fmt.Errorf("role ladder %q reuses provider identity %q", role, candidate.Name)
			}
			seenNames[candidate.Name] = true
		}
	}
	for id, workflow := range state.Workflows {
		if err := validateDAG(workflow); err != nil {
			return fmt.Errorf("workflow %s: %w", id, err)
		}
		for _, node := range workflow.Nodes {
			if !withinRoot(filepath.Clean(workflow.Root), filepath.Clean(node.Work.Root)) {
				return fmt.Errorf("node %s root is outside its canonical workflow root", node.ID)
			}
		}
	}
	return nil
}

func validateRecordedWorkerResult(result WorkerResult) error {
	if result.Status != "completed" && result.Status != "continue" && result.Status != "needs_human" && result.Status != "blocked" {
		return fmt.Errorf("unsupported worker result %q", result.Status)
	}
	if strings.TrimSpace(result.Summary) == "" {
		return errors.New("worker result summary is empty")
	}
	if (result.BlockerKind == "") != (result.Question == "") {
		return errors.New("worker result blocker kind and question must be paired")
	}
	if result.Status != "needs_human" && (result.BlockerKind != "" || result.Question != "") {
		return errors.New("only needs_human may carry blocker fields")
	}
	return nil
}

func validateDAG(workflow *Workflow) error {
	visiting, visited := map[NodeID]bool{}, map[NodeID]bool{}
	var visit func(NodeID) error
	visit = func(id NodeID) error {
		if visiting[id] {
			return fmt.Errorf("dependency cycle at %s", id)
		}
		if visited[id] {
			return nil
		}
		node := workflow.Nodes[id]
		if node == nil {
			return fmt.Errorf("unknown node %s", id)
		}
		visiting[id] = true
		for _, dependency := range node.Dependencies {
			if workflow.Nodes[dependency] == nil {
				return fmt.Errorf("node %s depends on unknown node %s", id, dependency)
			}
			if err := visit(dependency); err != nil {
				return err
			}
		}
		delete(visiting, id)
		visited[id] = true
		return nil
	}
	ids := make([]string, 0, len(workflow.Nodes))
	for id := range workflow.Nodes {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	for _, id := range ids {
		if err := visit(NodeID(id)); err != nil {
			return err
		}
	}
	return nil
}

func lookupNode(state *State, workflowID WorkflowID, nodeID NodeID) *Node {
	workflow := state.Workflows[workflowID]
	if workflow == nil {
		return nil
	}
	return workflow.Nodes[nodeID]
}

func cloneExecution(v *Execution) *Execution { c := *v; return &c }
func cloneNode(v *Node) *Node {
	c := *v
	c.Dependencies = append([]NodeID(nil), v.Dependencies...)
	c.Work.Fallbacks = append([]RuntimeSpec(nil), v.Work.Fallbacks...)
	return &c
}
func cloneWorkflow(v *Workflow) *Workflow {
	c := *v
	c.Authority.AllowedRoots = append([]string(nil), v.Authority.AllowedRoots...)
	c.Finalizer.RequiredChecks = append([]string(nil), v.Finalizer.RequiredChecks...)
	c.Nodes = make(map[NodeID]*Node, len(v.Nodes))
	for id, node := range v.Nodes {
		c.Nodes[id] = cloneNode(node)
	}
	c.Order = append([]NodeID(nil), v.Order...)
	return &c
}
func cloneSession(v *Session) *Session { c := *v; return &c }
func cloneActivity(v *Activity) *Activity {
	c := *v
	c.DependencyBindings = append([]ResultBinding(nil), v.DependencyBindings...)
	return &c
}
func cloneAttempt(v *Attempt) *Attempt {
	c := *v
	c.Milestones = append([]Milestone(nil), v.Milestones...)
	if v.Process != nil {
		p := *v.Process
		c.Process = &p
	}
	return &c
}
func cloneMilestone(v Milestone) Milestone {
	c := v
	if v.Session != nil {
		x := *v.Session
		c.Session = &x
	}
	if v.Process != nil {
		x := *v.Process
		c.Process = &x
	}
	if v.Result != nil {
		x := *v.Result
		c.Result = &x
	}
	if v.Exit != nil {
		x := *v.Exit
		c.Exit = &x
	}
	return c
}
func cloneResult(v *Result) *Result {
	c := *v
	return &c
}

func cloneFinalization(v *Finalization) *Finalization {
	c := *v
	c.Gates = append([]string(nil), v.Gates...)
	return &c
}
func cloneMessage(v *Message) *Message { c := *v; return &c }
func cloneLease(v *Lease) *Lease       { c := *v; return &c }
func cloneControl(v *Control) *Control { c := *v; return &c }

func clonePause(v *Pause) *Pause {
	c := *v
	c.FencedAttemptIDs = append([]AttemptID(nil), v.FencedAttemptIDs...)
	c.ReleasedLeaseIDs = append([]LeaseID(nil), v.ReleasedLeaseIDs...)
	return &c
}

func containsLease(values []LeaseID, wanted LeaseID) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
