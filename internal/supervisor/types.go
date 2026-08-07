// Package supervisor is the single durable command and projection boundary for
// workflow execution. Domain identities stay distinct, but every mutation is
// ordered by one append-only journal.
package supervisor

import "time"

const SchemaVersion = 2

type (
	ExecutionID   string
	WorkflowID    string
	NodeID        string
	SessionID     string
	ActivityID    string
	AttemptID     string
	ResultID      string
	AttestationID string
	MessageID     string
	ControlID     string
	LeaseID       string
)

type Sandbox string

const (
	SandboxReadOnly       Sandbox = "read-only"
	SandboxWorkspaceWrite Sandbox = "workspace-write"
)

// RuntimeSpec is durable launch intent. Prompt-bearing argv is deliberately
// absent: a Driver constructs it at launch time.
type RuntimeSpec struct {
	Name       string  `json:"name"`
	Executable string  `json:"executable,omitempty"`
	Model      string  `json:"model,omitempty"`
	Effort     string  `json:"effort,omitempty"`
	Sandbox    Sandbox `json:"sandbox"`
}

type NativeSessionIdentity struct {
	Runtime string `json:"runtime"`
	ID      string `json:"id"`
}

type AuthoritySpec struct {
	RequestedBy     string   `json:"requested_by"`
	HumanAuthorized bool     `json:"human_authorized"`
	Sandbox         Sandbox  `json:"sandbox"`
	AllowedRoots    []string `json:"allowed_roots,omitempty"`
}

type FinalizerSpec struct {
	Enabled         bool     `json:"enabled"`
	RequiredChecks  []string `json:"required_checks,omitempty"`
	RequireHuman    bool     `json:"require_human,omitempty"`
	RequireVerifier bool     `json:"require_verifier,omitempty"`
	Verifiers       []string `json:"verifiers,omitempty"`
}

type Budget struct {
	// MaxTaskAttempts counts Attempts that reached turn_started. Adapter launch
	// failures and provider startup failures do not consume it.
	MaxTaskAttempts int `json:"max_task_attempts"`
	// MaxLaunches bounds repeated pre-turn process failures independently.
	MaxLaunches int `json:"max_launches"`
}

func DefaultBudget() Budget { return Budget{MaxTaskAttempts: 3, MaxLaunches: 12} }

type WorkSpec struct {
	Kind    string      `json:"kind"`
	Prompt  string      `json:"prompt"`
	Root    string      `json:"root"`
	Runtime RuntimeSpec `json:"runtime"`
}

type Execution struct {
	ID             ExecutionID `json:"id"`
	WorkflowID     WorkflowID  `json:"workflow_id"`
	RootNodeID     NodeID      `json:"root_node_id"`
	SessionID      SessionID   `json:"session_id"`
	FirstActivity  ActivityID  `json:"first_activity_id"`
	IdempotencyKey string      `json:"idempotency_key"`
	InputDigest    string      `json:"input_digest"`
	CreatedAt      time.Time   `json:"created_at"`
}

type Workflow struct {
	ID        WorkflowID       `json:"id"`
	Root      string           `json:"root"`
	Authority AuthoritySpec    `json:"authority"`
	Finalizer FinalizerSpec    `json:"finalizer"`
	Budget    Budget           `json:"budget"`
	Nodes     map[NodeID]*Node `json:"nodes"`
	Order     []NodeID         `json:"order"`
	CreatedAt time.Time        `json:"created_at"`
}

// Node is desired work. It intentionally has no attempt, session, process,
// readiness, running, or completion fields.
type Node struct {
	ID           NodeID     `json:"id"`
	WorkflowID   WorkflowID `json:"workflow_id"`
	Title        string     `json:"title"`
	Work         WorkSpec   `json:"work"`
	Dependencies []NodeID   `json:"dependencies,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	SupersededAt time.Time  `json:"superseded_at,omitempty"`
}

// Session owns only durable conversation identity and lineage.
type Session struct {
	ID                 SessionID             `json:"id"`
	WorkflowID         WorkflowID            `json:"workflow_id"`
	Native             NativeSessionIdentity `json:"native"`
	ImportedUnresolved bool                  `json:"imported_unresolved,omitempty"`
	ParentID           SessionID             `json:"parent_id,omitempty"`
	Root               string                `json:"root"`
	CreatedAt          time.Time             `json:"created_at"`
}

type ResultBinding struct {
	DependencyNodeID NodeID   `json:"dependency_node_id"`
	ResultID         ResultID `json:"result_id"`
}

// Activity is one immutable generation of logical work. A reply creates a new
// Activity; it never mutates or reopens its predecessor.
type Activity struct {
	ID                 ActivityID      `json:"id"`
	WorkflowID         WorkflowID      `json:"workflow_id"`
	NodeID             NodeID          `json:"node_id"`
	SessionID          SessionID       `json:"session_id"`
	Generation         uint64          `json:"generation"`
	ParentActivityID   ActivityID      `json:"parent_activity_id,omitempty"`
	Prompt             string          `json:"prompt"`
	DependencyBindings []ResultBinding `json:"dependency_bindings,omitempty"`
	CreatedAt          time.Time       `json:"created_at"`
}

type ProcessIdentity struct {
	PID        int    `json:"pid"`
	StartToken string `json:"start_token"`
	TreeID     string `json:"tree_id,omitempty"`
}

type OutputIdentity struct {
	Stdout string `json:"stdout"`
	Stderr string `json:"stderr"`
	Result string `json:"result,omitempty"`
}

// Attempt is one OS launch. Its creation record is immutable; later facts are
// appended as typed Milestones and reduced into this projection.
type Attempt struct {
	ID                 AttemptID        `json:"id"`
	ActivityID         ActivityID       `json:"activity_id"`
	ActivityGeneration uint64           `json:"activity_generation"`
	Ordinal            int              `json:"ordinal"`
	Runtime            RuntimeSpec      `json:"runtime"`
	CommandDigest      string           `json:"command_digest"`
	Outputs            OutputIdentity   `json:"outputs"`
	LeaseID            LeaseID          `json:"lease_id"`
	CreatedAt          time.Time        `json:"created_at"`
	Process            *ProcessIdentity `json:"process,omitempty"`
	Milestones         []Milestone      `json:"milestones,omitempty"`
}

type MilestoneKind string

const (
	MilestoneProcessSpawned      MilestoneKind = "process_spawned"
	MilestoneSessionBound        MilestoneKind = "session_bound"
	MilestoneTurnStarted         MilestoneKind = "turn_started"
	MilestoneEffectStarted       MilestoneKind = "effect_started"
	MilestoneMeaningfulProgress  MilestoneKind = "meaningful_progress"
	MilestoneResult              MilestoneKind = "result"
	MilestoneProviderUnavailable MilestoneKind = "provider_unavailable"
	MilestoneAdapterStartFailed  MilestoneKind = "adapter_start_failed"
	MilestoneExit                MilestoneKind = "exit"
)

type WorkerResult struct {
	Status  string `json:"status"`
	Summary string `json:"summary"`
}

type Exit struct {
	Code  int    `json:"code"`
	Error string `json:"error,omitempty"`
}

type Milestone struct {
	Kind       MilestoneKind          `json:"kind"`
	At         time.Time              `json:"at"`
	Session    *NativeSessionIdentity `json:"session,omitempty"`
	Process    *ProcessIdentity       `json:"process,omitempty"`
	Effect     string                 `json:"effect,omitempty"`
	Progress   string                 `json:"progress,omitempty"`
	Result     *WorkerResult          `json:"result,omitempty"`
	Failure    string                 `json:"failure,omitempty"`
	Exit       *Exit                  `json:"exit,omitempty"`
	SourceType string                 `json:"source_type,omitempty"`
}

type Attestation struct {
	ID          AttestationID `json:"id"`
	ResultID    ResultID      `json:"result_id"`
	Verifier    string        `json:"verifier"`
	Verdict     string        `json:"verdict"`
	RawVerdict  string        `json:"raw_verdict,omitempty"`
	Summary     string        `json:"summary"`
	EvidenceIDs []string      `json:"evidence_ids,omitempty"`
	At          time.Time     `json:"at"`
}

type Result struct {
	ID         ResultID   `json:"id"`
	WorkflowID WorkflowID `json:"workflow_id"`
	NodeID     NodeID     `json:"node_id"`
	ActivityID ActivityID `json:"activity_id"`
	AttemptID  AttemptID  `json:"attempt_id"`
	Generation uint64     `json:"generation"`
	Status     string     `json:"status"`
	Summary    string     `json:"summary"`
	CreatedAt  time.Time  `json:"created_at"`
}

type Pause struct {
	WorkflowID       WorkflowID  `json:"workflow_id"`
	RequestedBy      string      `json:"requested_by"`
	FencedAttemptIDs []AttemptID `json:"fenced_attempt_ids,omitempty"`
	ReleasedLeaseIDs []LeaseID   `json:"released_lease_ids,omitempty"`
	RequestedAt      time.Time   `json:"requested_at"`
	CompletedAt      time.Time   `json:"completed_at,omitempty"`
}

type MessageState string

const (
	MessageQueued     MessageState = "queued"
	MessageDispatched MessageState = "dispatched"
	MessageDelivered  MessageState = "delivered"
)

type Message struct {
	ID                 MessageID    `json:"id"`
	SessionID          SessionID    `json:"session_id"`
	ActivityID         ActivityID   `json:"activity_id"`
	AttemptID          AttemptID    `json:"attempt_id,omitempty"`
	Body               string       `json:"body"`
	From               string       `json:"from"`
	State              MessageState `json:"state"`
	DeliveryGeneration uint64       `json:"delivery_generation"`
	CreatedAt          time.Time    `json:"created_at"`
	SettledAt          time.Time    `json:"settled_at,omitempty"`
}

type Lease struct {
	ID                 LeaseID    `json:"id"`
	CanonicalWorktree  string     `json:"canonical_worktree"`
	ActivityID         ActivityID `json:"activity_id"`
	ActivityGeneration uint64     `json:"activity_generation"`
	AttemptID          AttemptID  `json:"attempt_id"`
	AcquiredAt         time.Time  `json:"acquired_at"`
	ReleasedAt         time.Time  `json:"released_at,omitempty"`
}

type Control struct {
	ID                 ControlID  `json:"id"`
	Kind               string     `json:"kind"`
	ActivityID         ActivityID `json:"activity_id"`
	ExpectedGeneration uint64     `json:"expected_generation"`
	ExpectedAttemptID  AttemptID  `json:"expected_attempt_id"`
	Accepted           bool       `json:"accepted"`
	Reason             string     `json:"reason,omitempty"`
	CreatedAt          time.Time  `json:"created_at"`
}

type IdempotencyRecord struct {
	Key         string `json:"key"`
	CommandType string `json:"command_type"`
	InputDigest string `json:"input_digest"`
	ResourceID  string `json:"resource_id"`
	Sequence    uint64 `json:"sequence"`
}

type LegacyImport struct {
	SourceDigest string            `json:"source_digest"`
	SourceRoot   string            `json:"source_root"`
	ImportedAt   time.Time         `json:"imported_at"`
	Records      int               `json:"records"`
	Files        map[string]string `json:"files"`
}

// State is a rebuildable reducer projection. Its maps are indexes over one
// journal, not independently writable stores.
type State struct {
	Version       int                            `json:"version"`
	Sequence      uint64                         `json:"sequence"`
	Executions    map[ExecutionID]*Execution     `json:"executions"`
	Workflows     map[WorkflowID]*Workflow       `json:"workflows"`
	Sessions      map[SessionID]*Session         `json:"sessions"`
	Activities    map[ActivityID]*Activity       `json:"activities"`
	Attempts      map[AttemptID]*Attempt         `json:"attempts"`
	Results       map[ResultID]*Result           `json:"results"`
	Attestations  map[AttestationID]*Attestation `json:"attestations"`
	Messages      map[MessageID]*Message         `json:"messages"`
	Leases        map[LeaseID]*Lease             `json:"leases"`
	Controls      map[ControlID]*Control         `json:"controls"`
	Pauses        map[WorkflowID]*Pause          `json:"pauses"`
	Idempotency   map[string]IdempotencyRecord   `json:"idempotency"`
	LegacyImports map[string]LegacyImport        `json:"legacy_imports"`
}

func emptyState() *State {
	return &State{
		Version:    SchemaVersion,
		Executions: make(map[ExecutionID]*Execution), Workflows: make(map[WorkflowID]*Workflow),
		Sessions: make(map[SessionID]*Session), Activities: make(map[ActivityID]*Activity),
		Attempts: make(map[AttemptID]*Attempt), Results: make(map[ResultID]*Result), Attestations: make(map[AttestationID]*Attestation),
		Messages: make(map[MessageID]*Message), Leases: make(map[LeaseID]*Lease),
		Controls: make(map[ControlID]*Control), Pauses: make(map[WorkflowID]*Pause), Idempotency: make(map[string]IdempotencyRecord),
		LegacyImports: make(map[string]LegacyImport),
	}
}
