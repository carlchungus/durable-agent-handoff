package supervisor

import (
	"os"
	"sort"
	"strings"
	"time"
)

type ActivityStatus string

const (
	ActivityQueued     ActivityStatus = "queued"
	ActivityScheduled  ActivityStatus = "scheduled"
	ActivityStarting   ActivityStatus = "starting"
	ActivityRunning    ActivityStatus = "running"
	ActivityDeciding   ActivityStatus = "deciding"
	ActivityRetryable  ActivityStatus = "retryable"
	ActivityCompleted  ActivityStatus = "completed"
	ActivityNeedsHuman ActivityStatus = "needs_human"
	ActivityBlocked    ActivityStatus = "blocked"
	ActivityPaused     ActivityStatus = "paused"
	ActivityFailed     ActivityStatus = "failed"
)

type ProcessHealth string

const (
	HealthNotLaunched ProcessHealth = "not_launched"
	HealthStarting    ProcessHealth = "starting"
	HealthRunning     ProcessHealth = "running"
	HealthExited      ProcessHealth = "exited"
)

type PublicationState string

const (
	PublicationDisabled       PublicationState = "disabled"
	PublicationAwaitingResult PublicationState = "awaiting_result"
	PublicationAwaitingHuman  PublicationState = "awaiting_human"
	PublicationEligible       PublicationState = "eligible"
)

type Overhead struct {
	PrepareToSpawn time.Duration `json:"prepare_to_spawn"`
	SpawnToTurn    time.Duration `json:"spawn_to_turn"`
	TurnToProgress time.Duration `json:"turn_to_progress"`
}

type AttemptView struct {
	ID                  AttemptID     `json:"id"`
	ActivityID          ActivityID    `json:"activity_id"`
	ActivityGeneration  uint64        `json:"activity_generation"`
	Health              ProcessHealth `json:"health"`
	TurnStarted         bool          `json:"turn_started"`
	TaskAttempt         int           `json:"task_attempt,omitempty"`
	MeaningfulProgress  string        `json:"meaningful_progress,omitempty"`
	ProviderUnavailable bool          `json:"provider_unavailable,omitempty"`
	TerminalReason      string        `json:"terminal_reason,omitempty"`
	ResultStatus        string        `json:"result_status,omitempty"`
	Overhead            Overhead      `json:"orchestration_overhead"`
}

type ActivityView struct {
	ID                 ActivityID      `json:"id"`
	NodeID             NodeID          `json:"node_id"`
	SessionID          SessionID       `json:"session_id"`
	Generation         uint64          `json:"generation"`
	ParentActivityID   ActivityID      `json:"parent_activity_id,omitempty"`
	Status             ActivityStatus  `json:"status"`
	DependencyBindings []ResultBinding `json:"dependency_bindings,omitempty"`
	ResultID           ResultID        `json:"result_id,omitempty"`
	ResultSummary      string          `json:"result_summary,omitempty"`
	BlockerKind        string          `json:"blocker_kind,omitempty"`
	Question           string          `json:"question,omitempty"`
	NotBefore          *time.Time      `json:"not_before,omitempty"`
}

type NodeStatus string

const (
	NodeWaitingDependencies NodeStatus = "waiting_dependencies"
	NodeEligible            NodeStatus = "eligible"
	NodeQueued              NodeStatus = "queued"
	NodeScheduled           NodeStatus = "scheduled"
	NodeStarting            NodeStatus = "starting"
	NodeRunning             NodeStatus = "running"
	NodeDeciding            NodeStatus = "deciding"
	NodeCompleted           NodeStatus = "completed"
	NodeSuperseded          NodeStatus = "superseded"
)

type NodeView struct {
	ID             NodeID       `json:"id"`
	Status         NodeStatus   `json:"status"`
	BoundResultIDs []ResultID   `json:"bound_result_ids,omitempty"`
	ActivityIDs    []ActivityID `json:"activity_ids,omitempty"`
}

type ExecutionStatus string

const (
	ExecutionWaiting    ExecutionStatus = "waiting"
	ExecutionQueued     ExecutionStatus = "queued"
	ExecutionScheduled  ExecutionStatus = "scheduled"
	ExecutionStarting   ExecutionStatus = "starting"
	ExecutionRunning    ExecutionStatus = "running"
	ExecutionDeciding   ExecutionStatus = "deciding"
	ExecutionNeedsHuman ExecutionStatus = "needs_human"
	ExecutionBlocked    ExecutionStatus = "blocked"
	ExecutionCompleted  ExecutionStatus = "completed"
	ExecutionPaused     ExecutionStatus = "paused"
	ExecutionFailed     ExecutionStatus = "failed"
)

type ExecutionView struct {
	ID           ExecutionID      `json:"id"`
	WorkflowID   WorkflowID       `json:"workflow_id"`
	Title        string           `json:"title,omitempty"`
	Status       ExecutionStatus  `json:"status"`
	Active       bool             `json:"active"`
	Summary      string           `json:"summary,omitempty"`
	UpdatedAt    time.Time        `json:"updated_at"`
	Nodes        []NodeView       `json:"nodes"`
	Activities   []ActivityView   `json:"activities"`
	Attempts     []AttemptView    `json:"attempts"`
	Queue        []ActivityID     `json:"queue"`
	PendingTurns []ActivityID     `json:"pending_turns,omitempty"`
	Publication  PublicationState `json:"publication"`
	NextWakeAt   *time.Time       `json:"next_wake_at,omitempty"`
	AsOf         time.Time        `json:"as_of"`
}

// View is a pure read. asOf is explicit so polling cannot manufacture events
// or smuggle wall-clock state into the canonical reducer.
func (s *Store) View(executionID ExecutionID, asOf time.Time) (*ExecutionView, error) {
	state, err := s.Projection()
	if err != nil {
		return nil, err
	}
	return ProjectExecution(state, executionID, asOf)
}

func ProjectExecution(state *State, executionID ExecutionID, asOf time.Time) (*ExecutionView, error) {
	execution := state.Executions[executionID]
	if execution == nil {
		return nil, os.ErrNotExist
	}
	workflow := state.Workflows[execution.WorkflowID]
	view := &ExecutionView{ID: execution.ID, WorkflowID: workflow.ID, UpdatedAt: execution.CreatedAt.UTC(), AsOf: asOf.UTC()}
	if root := workflow.Nodes[execution.RootNodeID]; root != nil {
		view.Title = root.Title
	}
	for _, activity := range orderedActivities(state, workflow.ID) {
		item := projectActivity(state, activity, asOf)
		view.Activities = append(view.Activities, item)
		if (item.Status == ActivityQueued || item.Status == ActivityRetryable) && fallbackChildForActivity(state, activity.ID) == nil {
			view.Queue = append(view.Queue, item.ID)
		}
		if item.Status == ActivityScheduled && item.NotBefore != nil && (view.NextWakeAt == nil || item.NotBefore.Before(*view.NextWakeAt)) {
			view.NextWakeAt = item.NotBefore
		}
		if item.Status == ActivityDeciding {
			view.PendingTurns = append(view.PendingTurns, activity.ID)
		}
	}
	taskOrdinals := map[NodeID]int{}
	for _, attempt := range orderedAttempts(state, workflow.ID) {
		item := projectAttempt(state, attempt)
		activity := state.Activities[attempt.ActivityID]
		if item.TurnStarted && !hasProviderUnavailable(state.Attempts[item.ID]) {
			taskOrdinals[activity.NodeID]++
			item.TaskAttempt = taskOrdinals[activity.NodeID]
		}
		view.Attempts = append(view.Attempts, item)
	}
	for _, nodeID := range workflow.Order {
		node := workflow.Nodes[nodeID]
		item := NodeView{ID: node.ID}
		if !node.SupersededAt.IsZero() {
			item.Status = NodeSuperseded
		} else {
			item.Status = NodeEligible
			for _, dependency := range node.Dependencies {
				result := latestResultForNode(state, workflow.ID, dependency)
				if result == nil {
					item.Status = NodeWaitingDependencies
					break
				}
				item.BoundResultIDs = append(item.BoundResultIDs, result.ID)
			}
		}
		for _, activity := range view.Activities {
			if activity.NodeID != node.ID {
				continue
			}
			item.ActivityIDs = append(item.ActivityIDs, activity.ID)
			switch activity.Status {
			case ActivityRunning:
				item.Status = NodeRunning
			case ActivityDeciding:
				if item.Status != NodeRunning {
					item.Status = NodeDeciding
				}
			case ActivityStarting:
				if item.Status != NodeRunning {
					item.Status = NodeStarting
				}
			case ActivityQueued, ActivityRetryable:
				if item.Status != NodeRunning && item.Status != NodeStarting {
					item.Status = NodeQueued
				}
			case ActivityScheduled:
				if item.Status != NodeRunning && item.Status != NodeStarting && item.Status != NodeQueued {
					item.Status = NodeScheduled
				}
			case ActivityCompleted, ActivityNeedsHuman, ActivityBlocked, ActivityPaused:
				if item.Status != NodeRunning && item.Status != NodeStarting && item.Status != NodeQueued {
					item.Status = NodeCompleted
				}
			}
		}
		view.Nodes = append(view.Nodes, item)
	}
	view.Publication = ProjectPublication(workflow, state)
	if state.Pauses[workflow.ID] != nil {
		view.Status, view.Active = ExecutionPaused, false
	} else {
		view.Status, view.Active = projectExecutionStatus(view)
	}
	view.Summary, view.UpdatedAt = projectExecutionSummary(state, workflow.ID, view.UpdatedAt)
	return view, nil
}

func projectExecutionStatus(view *ExecutionView) (ExecutionStatus, bool) {
	latest := make(map[NodeID]ActivityView)
	for _, activity := range view.Activities {
		current, ok := latest[activity.NodeID]
		if !ok || activity.Generation >= current.Generation {
			latest[activity.NodeID] = activity
		}
	}

	statuses := make(map[ActivityStatus]bool)
	waiting := false
	for _, node := range view.Nodes {
		if node.Status == NodeSuperseded {
			continue
		}
		if activity, ok := latest[node.ID]; ok {
			statuses[activity.Status] = true
			continue
		}
		if node.Status == NodeEligible || node.Status == NodeWaitingDependencies {
			waiting = true
		}
	}

	for _, candidate := range []struct {
		activity ActivityStatus
		status   ExecutionStatus
	}{
		{ActivityNeedsHuman, ExecutionNeedsHuman},
		{ActivityBlocked, ExecutionBlocked},
		{ActivityRunning, ExecutionRunning},
		{ActivityStarting, ExecutionStarting},
		{ActivityDeciding, ExecutionDeciding},
		{ActivityQueued, ExecutionQueued},
		{ActivityRetryable, ExecutionQueued},
		{ActivityScheduled, ExecutionScheduled},
	} {
		if statuses[candidate.activity] {
			return candidate.status, true
		}
	}
	if waiting {
		return ExecutionWaiting, true
	}
	if statuses[ActivityFailed] {
		return ExecutionFailed, false
	}
	if statuses[ActivityPaused] {
		return ExecutionPaused, false
	}
	return ExecutionCompleted, false
}

func projectExecutionSummary(state *State, workflowID WorkflowID, updatedAt time.Time) (string, time.Time) {
	summary := ""
	var summaryAt time.Time
	consider := func(at time.Time, value string) {
		if at.After(updatedAt) {
			updatedAt = at.UTC()
		}
		if strings.TrimSpace(value) != "" && (summaryAt.IsZero() || !at.Before(summaryAt)) {
			summary = strings.Join(strings.Fields(value), " ")
			summaryAt = at
		}
	}
	for _, activity := range orderedActivities(state, workflowID) {
		consider(activity.CreatedAt, "")
	}
	for _, attempt := range orderedAttempts(state, workflowID) {
		consider(attempt.CreatedAt, "")
		for _, milestone := range attempt.Milestones {
			consider(milestone.At, milestone.Progress)
		}
	}
	results := make([]*Result, 0, len(state.Results))
	for _, result := range state.Results {
		if result.WorkflowID == workflowID {
			results = append(results, result)
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].CreatedAt.Before(results[j].CreatedAt) || results[i].CreatedAt.Equal(results[j].CreatedAt) && results[i].ID < results[j].ID
	})
	for _, result := range results {
		consider(result.CreatedAt, result.Summary)
	}
	if pause := state.Pauses[workflowID]; pause != nil {
		consider(pause.RequestedAt, "")
		consider(pause.CompletedAt, "")
	}
	return summary, updatedAt
}

func projectActivity(state *State, activity *Activity, asOf time.Time) ActivityView {
	view := ActivityView{ID: activity.ID, NodeID: activity.NodeID, SessionID: activity.SessionID, Generation: activity.Generation, ParentActivityID: activity.ParentActivityID, Status: ActivityQueued, DependencyBindings: append([]ResultBinding(nil), activity.DependencyBindings...)}
	if activity.NotBefore != nil && !activity.NotBefore.IsZero() {
		view.NotBefore = activity.NotBefore
	}
	// A provider-unavailable parent is superseded by its durable child Session.
	// Keep the parent visible for lineage, but never return it to the scheduler.
	if child := fallbackChildForActivity(state, activity.ID); child != nil {
		childView := projectActivity(state, child, asOf)
		view.Status = childView.Status
		view.ResultID = childView.ResultID
		return view
	}
	result := resultForActivity(state, activity.ID)
	if result != nil {
		view.ResultID = result.ID
		view.ResultSummary = result.Summary
		view.BlockerKind = result.BlockerKind
		view.Question = result.Question
		switch result.Status {
		case "needs_human":
			view.Status = ActivityNeedsHuman
		case "blocked":
			view.Status = ActivityBlocked
		default:
			view.Status = ActivityCompleted
		}
		return view
	}
	if runsAsGoal(state.Workflows[activity.WorkflowID]) {
		if attempt := workerResultAttemptForActivity(state, activity.ID); attempt != nil {
			if attemptHasExit(attempt) {
				view.Status = ActivityDeciding
			} else {
				view.Status = ActivityRunning
			}
			return view
		}
	}
	if session := state.Sessions[activity.SessionID]; session != nil && session.ImportedUnresolved {
		// Workflow history can be normalized, but an exact native Session or
		// Activity cannot be recovered from it. Keep the continuation visible
		// for human promotion and fail closed before it reaches Queue.
		view.Status = ActivityNeedsHuman
		return view
	}
	if state.Pauses[activity.WorkflowID] != nil {
		view.Status = ActivityPaused
		return view
	}
	if view.NotBefore != nil && view.NotBefore.After(asOf) {
		view.Status = ActivityScheduled
		return view
	}
	for _, attempt := range orderedAttemptsForActivity(state, activity.ID) {
		if !attemptTerminal(attempt) {
			if hasMilestone(attempt, MilestoneTurnStarted) {
				view.Status = ActivityRunning
			} else {
				view.Status = ActivityStarting
			}
			return view
		}
	}
	workflow := state.Workflows[activity.WorkflowID]
	launches, turns := attemptCountsForActivity(state, activity.ID)
	switch {
	case launches == 0:
		view.Status = ActivityQueued
	case launches < workflow.Budget.MaxLaunches && turns < workflow.Budget.MaxTaskAttempts:
		view.Status = ActivityRetryable
	default:
		view.Status = ActivityFailed
	}
	return view
}

func workerResultAttemptForActivity(state *State, activityID ActivityID) *Attempt {
	for _, attempt := range orderedAttemptsForActivity(state, activityID) {
		if workerResultForAttempt(attempt) != nil {
			return attempt
		}
	}
	return nil
}

func projectAttempt(state *State, attempt *Attempt) AttemptView {
	view := AttemptView{ID: attempt.ID, ActivityID: attempt.ActivityID, ActivityGeneration: attempt.ActivityGeneration, Health: HealthStarting}
	var spawned, turn, progress time.Time
	for _, milestone := range attempt.Milestones {
		switch milestone.Kind {
		case MilestoneProcessSpawned:
			spawned = milestone.At
		case MilestoneTurnStarted:
			view.TurnStarted, view.Health, turn = true, HealthRunning, milestone.At
		case MilestoneMeaningfulProgress:
			view.MeaningfulProgress = milestone.Progress
			if progress.IsZero() {
				progress = milestone.At
			}
		case MilestoneProviderUnavailable:
			view.ProviderUnavailable = true
			view.Health, view.TerminalReason = HealthExited, milestone.Failure
		case MilestoneAdapterStartFailed:
			view.Health, view.TerminalReason = HealthExited, milestone.Failure
		case MilestoneExit:
			view.Health = HealthExited
			if milestone.Exit != nil && strings.TrimSpace(milestone.Exit.Error) != "" {
				view.TerminalReason = milestone.Exit.Error
			}
		}
	}
	if result := resultForActivity(state, attempt.ActivityID); result != nil && result.AttemptID == attempt.ID {
		view.ResultStatus = result.Status
	}
	if attempt.Process == nil && len(attempt.Milestones) == 0 {
		view.Health = HealthStarting
	}
	if !spawned.IsZero() {
		view.Overhead.PrepareToSpawn = spawned.Sub(attempt.CreatedAt)
	}
	if !spawned.IsZero() && !turn.IsZero() {
		view.Overhead.SpawnToTurn = turn.Sub(spawned)
	}
	if !turn.IsZero() && !progress.IsZero() {
		view.Overhead.TurnToProgress = progress.Sub(turn)
	}
	return view
}

func fallbackChildForActivity(state *State, parentID ActivityID) *Activity {
	parent := state.Activities[parentID]
	if parent == nil {
		return nil
	}
	var child *Activity
	for _, candidate := range state.Activities {
		if candidate.ParentActivityID != parentID || candidate.SessionID == parent.SessionID {
			continue
		}
		if child == nil || candidate.CreatedAt.Before(child.CreatedAt) || candidate.CreatedAt.Equal(child.CreatedAt) && candidate.ID < child.ID {
			child = candidate
		}
	}
	return child
}

// ProjectPublication derives the durable publication outlet state for a
// workflow from immutable configuration and the latest result. It is the
// single computation shared by execution projections and by goal
// turn-decision requests, so the decision-maker sees the same outlet signal
// a human reading status would. PublicationDisabled means there is no
// consumable outlet for new work; an open-ended goal that keeps producing
// candidates with no outlet is grinding, not progressing.
func ProjectPublication(workflow *Workflow, state *State) PublicationState {
	if !workflow.Finalizer.Enabled {
		return PublicationDisabled
	}
	var latest *Result
	for nodeID := range workflow.Nodes {
		node := workflow.Nodes[nodeID]
		if node != nil && !node.SupersededAt.IsZero() {
			continue
		}
		result := latestResultForNode(state, workflow.ID, nodeID)
		if result == nil {
			return PublicationAwaitingResult
		}
		if result != nil && (latest == nil || result.CreatedAt.After(latest.CreatedAt)) {
			latest = result
		}
	}
	if latest == nil {
		return PublicationAwaitingResult
	}
	if workflow.Finalizer.RequireHuman {
		return PublicationAwaitingHuman
	}
	return PublicationEligible
}

func hasMilestone(attempt *Attempt, kind MilestoneKind) bool {
	for _, m := range attempt.Milestones {
		if m.Kind == kind {
			return true
		}
	}
	return false
}
func orderedActivities(state *State, workflowID WorkflowID) []*Activity {
	var out []*Activity
	for _, a := range state.Activities {
		if a.WorkflowID == workflowID {
			out = append(out, a)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.Before(out[j].CreatedAt) || out[i].CreatedAt.Equal(out[j].CreatedAt) && out[i].ID < out[j].ID
	})
	return out
}
func orderedAttemptsForActivity(state *State, activityID ActivityID) []*Attempt {
	var out []*Attempt
	for _, a := range state.Attempts {
		if a.ActivityID == activityID {
			out = append(out, a)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ordinal < out[j].Ordinal })
	return out
}
func orderedAttempts(state *State, workflowID WorkflowID) []*Attempt {
	var out []*Attempt
	for _, a := range state.Attempts {
		activity := state.Activities[a.ActivityID]
		if activity != nil && activity.WorkflowID == workflowID {
			out = append(out, a)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.Before(out[j].CreatedAt) || out[i].CreatedAt.Equal(out[j].CreatedAt) && out[i].ID < out[j].ID
	})
	return out
}

// AcceptedControlForAttempt returns the one accepted control for an exact
// Activity generation and Attempt. Sorting makes projections deterministic even
// when a legacy journal contains duplicate accepted controls.
func AcceptedControlForAttempt(state *State, activity *Activity, attempt *Attempt) *Control {
	if state == nil || activity == nil || attempt == nil {
		return nil
	}
	controls := make([]*Control, 0, len(state.Controls))
	for _, control := range state.Controls {
		if control.Accepted && control.ActivityID == activity.ID && control.ExpectedGeneration == activity.Generation && control.ExpectedAttemptID == attempt.ID && attempt.ActivityGeneration == activity.Generation {
			controls = append(controls, control)
		}
	}
	sort.Slice(controls, func(i, j int) bool {
		return controls[i].CreatedAt.Before(controls[j].CreatedAt) || controls[i].CreatedAt.Equal(controls[j].CreatedAt) && controls[i].ID < controls[j].ID
	})
	if len(controls) == 0 {
		return nil
	}
	copy := *controls[0]
	return &copy
}
