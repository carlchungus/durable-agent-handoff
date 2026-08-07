package supervisor

import (
	"os"
	"sort"
	"time"
)

type ActivityStatus string

const (
	ActivityQueued     ActivityStatus = "queued"
	ActivityStarting   ActivityStatus = "starting"
	ActivityRunning    ActivityStatus = "running"
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

type VerificationState string

const (
	VerificationPending VerificationState = "pending"
	VerificationPass    VerificationState = "pass"
	VerificationRepair  VerificationState = "repair"
	VerificationBlocked VerificationState = "blocked"
)

type PublicationState string

const (
	PublicationDisabled         PublicationState = "disabled"
	PublicationAwaitingResult   PublicationState = "awaiting_result"
	PublicationAwaitingVerifier PublicationState = "awaiting_verifier"
	PublicationAwaitingHuman    PublicationState = "awaiting_human"
	PublicationEligible         PublicationState = "eligible"
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
	Overhead            Overhead      `json:"orchestration_overhead"`
}

type ActivityView struct {
	ID                 ActivityID        `json:"id"`
	NodeID             NodeID            `json:"node_id"`
	SessionID          SessionID         `json:"session_id"`
	Generation         uint64            `json:"generation"`
	ParentActivityID   ActivityID        `json:"parent_activity_id,omitempty"`
	Status             ActivityStatus    `json:"status"`
	DependencyBindings []ResultBinding   `json:"dependency_bindings,omitempty"`
	AttemptIDs         []AttemptID       `json:"attempt_ids,omitempty"`
	ResultID           ResultID          `json:"result_id,omitempty"`
	QueuePosition      int               `json:"queue_position,omitempty"`
	Verification       VerificationState `json:"verification"`
}

type NodeStatus string

const (
	NodeWaitingDependencies NodeStatus = "waiting_dependencies"
	NodeEligible            NodeStatus = "eligible"
	NodeQueued              NodeStatus = "queued"
	NodeStarting            NodeStatus = "starting"
	NodeRunning             NodeStatus = "running"
	NodeCompleted           NodeStatus = "completed"
	NodeSuperseded          NodeStatus = "superseded"
)

type NodeView struct {
	ID             NodeID       `json:"id"`
	Status         NodeStatus   `json:"status"`
	BoundResultIDs []ResultID   `json:"bound_result_ids,omitempty"`
	ActivityIDs    []ActivityID `json:"activity_ids,omitempty"`
}

type ExecutionView struct {
	ID          ExecutionID      `json:"id"`
	WorkflowID  WorkflowID       `json:"workflow_id"`
	Nodes       []NodeView       `json:"nodes"`
	Activities  []ActivityView   `json:"activities"`
	Attempts    []AttemptView    `json:"attempts"`
	Queue       []ActivityID     `json:"queue"`
	Publication PublicationState `json:"publication"`
	AsOf        time.Time        `json:"as_of"`
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
	view := &ExecutionView{ID: execution.ID, WorkflowID: workflow.ID, AsOf: asOf.UTC()}
	activityViews := make(map[ActivityID]*ActivityView)
	for _, activity := range orderedActivities(state, workflow.ID) {
		item := projectActivity(state, activity)
		activityViews[activity.ID] = &item
		view.Activities = append(view.Activities, item)
		if item.Status == ActivityQueued || item.Status == ActivityRetryable {
			view.Queue = append(view.Queue, item.ID)
		}
	}
	for index, id := range view.Queue {
		activityViews[id].QueuePosition = index + 1
		for i := range view.Activities {
			if view.Activities[i].ID == id {
				view.Activities[i].QueuePosition = index + 1
			}
		}
	}
	taskOrdinals := map[NodeID]int{}
	for _, attempt := range orderedAttempts(state, workflow.ID) {
		item := projectAttempt(attempt)
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
			case ActivityStarting:
				if item.Status != NodeRunning {
					item.Status = NodeStarting
				}
			case ActivityQueued, ActivityRetryable:
				if item.Status != NodeRunning && item.Status != NodeStarting {
					item.Status = NodeQueued
				}
			case ActivityCompleted, ActivityNeedsHuman, ActivityBlocked, ActivityPaused:
				if item.Status != NodeRunning && item.Status != NodeStarting && item.Status != NodeQueued {
					item.Status = NodeCompleted
				}
			}
		}
		view.Nodes = append(view.Nodes, item)
	}
	view.Publication = projectPublication(workflow, state)
	return view, nil
}

func projectActivity(state *State, activity *Activity) ActivityView {
	view := ActivityView{ID: activity.ID, NodeID: activity.NodeID, SessionID: activity.SessionID, Generation: activity.Generation, ParentActivityID: activity.ParentActivityID, DependencyBindings: append([]ResultBinding(nil), activity.DependencyBindings...), Verification: VerificationPending}
	result := resultForActivity(state, activity.ID)
	if result != nil {
		view.ResultID = result.ID
		view.Verification = verificationFor(state, result.ID)
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
	if state.Pauses[activity.WorkflowID] != nil {
		view.Status = ActivityPaused
		return view
	}
	for _, attempt := range orderedAttemptsForActivity(state, activity.ID) {
		view.AttemptIDs = append(view.AttemptIDs, attempt.ID)
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
	launches, turns := attemptCounts(state, activity.WorkflowID, activity.NodeID)
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

func projectAttempt(attempt *Attempt) AttemptView {
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
			if milestone.Exit != nil {
				view.TerminalReason = milestone.Exit.Error
			}
		}
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

func verificationFor(state *State, resultID ResultID) VerificationState {
	verification := VerificationPending
	for _, attestation := range state.Attestations {
		if attestation.ResultID != resultID {
			continue
		}
		switch attestation.Verdict {
		case "blocked":
			return VerificationBlocked
		case "repair":
			verification = VerificationRepair
		case "pass":
			if verification == VerificationPending {
				verification = VerificationPass
			}
		}
	}
	return verification
}

func projectPublication(workflow *Workflow, state *State) PublicationState {
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
	if workflow.Finalizer.RequireVerifier {
		for nodeID := range workflow.Nodes {
			node := workflow.Nodes[nodeID]
			if node != nil && !node.SupersededAt.IsZero() {
				continue
			}
			result := latestResultForNode(state, workflow.ID, nodeID)
			if result == nil || verificationFor(state, result.ID) != VerificationPass {
				return PublicationAwaitingVerifier
			}
		}
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
