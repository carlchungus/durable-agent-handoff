// Package supervisor is the public Supervisor v2 promotion seam.
//
// StartExecution is the idempotent, human-authorized entry point intended for
// arca-cloud and other control planes. All returned projections come from one
// canonical append-only journal; callers must never write legacy workflow,
// session, or Activity ledgers alongside it.
package supervisor

import internal "github.com/carlchungus/durable-agent-handoff/internal/supervisor"

type (
	Store                      = internal.Store
	Options                    = internal.Options
	Boundary                   = internal.Boundary
	Receipt                    = internal.Receipt
	JournalEntry               = internal.JournalEntry
	DomainEvent                = internal.DomainEvent
	ExecutionID                = internal.ExecutionID
	WorkflowID                 = internal.WorkflowID
	NodeID                     = internal.NodeID
	SessionID                  = internal.SessionID
	ActivityID                 = internal.ActivityID
	AttemptID                  = internal.AttemptID
	ResultID                   = internal.ResultID
	MessageID                  = internal.MessageID
	ControlID                  = internal.ControlID
	LeaseID                    = internal.LeaseID
	Sandbox                    = internal.Sandbox
	ExecutionMode              = internal.ExecutionMode
	RuntimeSpec                = internal.RuntimeSpec
	NativeSessionIdentity      = internal.NativeSessionIdentity
	AuthoritySpec              = internal.AuthoritySpec
	FinalizerSpec              = internal.FinalizerSpec
	Budget                     = internal.Budget
	WorkSpec                   = internal.WorkSpec
	Execution                  = internal.Execution
	Workflow                   = internal.Workflow
	Node                       = internal.Node
	Session                    = internal.Session
	Activity                   = internal.Activity
	Attempt                    = internal.Attempt
	Result                     = internal.Result
	TurnDecision               = internal.TurnDecision
	ResultBinding              = internal.ResultBinding
	ProcessIdentity            = internal.ProcessIdentity
	OutputIdentity             = internal.OutputIdentity
	MilestoneKind              = internal.MilestoneKind
	Milestone                  = internal.Milestone
	WorkerResult               = internal.WorkerResult
	Exit                       = internal.Exit
	Message                    = internal.Message
	Lease                      = internal.Lease
	Control                    = internal.Control
	Pause                      = internal.Pause
	PausePhase                 = internal.PausePhase
	State                      = internal.State
	StartExecutionInput        = internal.StartExecutionInput
	SetRoleLadderInput         = internal.SetRoleLadderInput
	StartFallbackActivityInput = internal.StartFallbackActivityInput
	AddNodeInput               = internal.AddNodeInput
	QueueActivityInput         = internal.QueueActivityInput
	ContinueSessionInput       = internal.ContinueSessionInput
	PrepareAttemptInput        = internal.PrepareAttemptInput
	RecordMilestoneInput       = internal.RecordMilestoneInput
	DecideTurnInput            = internal.DecideTurnInput
	RequestControlInput        = internal.RequestControlInput
	PauseWorkflowInput         = internal.PauseWorkflowInput
	SettlePauseInput           = internal.SettlePauseInput
	ApplyControlInput          = internal.ApplyControlInput
	ImportV1Input              = internal.ImportV1Input
	ExecutionView              = internal.ExecutionView
	ExecutionStatus            = internal.ExecutionStatus
	NodeView                   = internal.NodeView
	ActivityView               = internal.ActivityView
	AttemptView                = internal.AttemptView
	ActivityStatus             = internal.ActivityStatus
	ProcessHealth              = internal.ProcessHealth
	PublicationState           = internal.PublicationState
	FinalizationRequest        = internal.FinalizationRequest
	FinalizationResult         = internal.FinalizationResult
	FinalizationState          = internal.FinalizationState
	PrepareFinalizationInput   = internal.PrepareFinalizationInput
	SettleFinalizationInput    = internal.SettleFinalizationInput
	GateRunner                 = internal.GateRunner
)

const (
	SchemaVersion = internal.SchemaVersion

	BoundaryAfterValidation = internal.BoundaryAfterValidation
	BoundaryAfterAppend     = internal.BoundaryAfterAppend
	BoundaryAfterSnapshot   = internal.BoundaryAfterSnapshot

	SandboxReadOnly       = internal.SandboxReadOnly
	SandboxWorkspaceWrite = internal.SandboxWorkspaceWrite
	ExecutionModeTurn     = internal.ExecutionModeTurn
	ExecutionModeSession  = internal.ExecutionModeSession

	MilestoneProcessSpawned      = internal.MilestoneProcessSpawned
	MilestoneSessionBound        = internal.MilestoneSessionBound
	MilestoneTurnStarted         = internal.MilestoneTurnStarted
	MilestoneEffectStarted       = internal.MilestoneEffectStarted
	MilestoneMeaningfulProgress  = internal.MilestoneMeaningfulProgress
	MilestoneResult              = internal.MilestoneResult
	MilestoneProviderUnavailable = internal.MilestoneProviderUnavailable
	MilestoneAdapterStartFailed  = internal.MilestoneAdapterStartFailed
	MilestoneExit                = internal.MilestoneExit

	HealthNotLaunched  = internal.HealthNotLaunched
	HealthStarting     = internal.HealthStarting
	HealthRunning      = internal.HealthRunning
	HealthExited       = internal.HealthExited
	ActivityDeciding   = internal.ActivityDeciding
	ActivityNeedsHuman = internal.ActivityNeedsHuman
	ExecutionPaused    = internal.ExecutionPaused

	PublicationDisabled       = internal.PublicationDisabled
	PublicationAwaitingResult = internal.PublicationAwaitingResult
	PublicationAwaitingHuman  = internal.PublicationAwaitingHuman
	PublicationEligible       = internal.PublicationEligible

	PauseRequested       = internal.PauseRequested
	PauseDraining        = internal.PauseDraining
	PauseCompleted       = internal.PauseCompleted
	FinalizationPrepared = internal.FinalizationPrepared
	FinalizationMerged   = internal.FinalizationMerged
	FinalizationBlocked  = internal.FinalizationBlocked
)

var (
	Open                   = internal.Open
	DefaultBudget          = internal.DefaultBudget
	ProjectExecution       = internal.ProjectExecution
	ProjectPublication     = internal.ProjectPublication
	RenderText             = internal.RenderText
	ErrIdempotencyConflict = internal.ErrIdempotencyConflict
	ErrFenced              = internal.ErrFenced
	ErrLeaseHeld           = internal.ErrLeaseHeld
	ErrPausePending        = internal.ErrPausePending
	ErrLiveOrphan          = internal.ErrLiveOrphan
)
