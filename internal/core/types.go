package core

import "time"

type WorkflowStatus string

const (
	WorkflowActive     WorkflowStatus = "active"
	WorkflowWaiting    WorkflowStatus = "waiting"
	WorkflowNeedsHuman WorkflowStatus = "needs_human"
	WorkflowCompleted  WorkflowStatus = "completed"
	WorkflowFailed     WorkflowStatus = "failed"
)

type NodeState string

const (
	NodePending    NodeState = "pending"
	NodeReady      NodeState = "ready"
	NodeRunning    NodeState = "running"
	NodeWaiting    NodeState = "waiting"
	NodeCompleted  NodeState = "completed"
	NodeFailed     NodeState = "failed"
	NodeSuperseded NodeState = "superseded"
)

type Budget struct {
	MaxNodes           int           `json:"max_nodes"`
	MaxConcurrent      int           `json:"max_concurrent"`
	MaxAttempts        int           `json:"max_attempts"`
	MaxRuntime         time.Duration `json:"max_runtime"`
	MaxChangedFiles    int           `json:"max_changed_files"`
	MaxDiffLines       int           `json:"max_diff_lines"`
	RequireAttestation bool          `json:"require_attestation"`
}

func DefaultBudget() Budget {
	return Budget{MaxNodes: 32, MaxConcurrent: 2, MaxAttempts: 3, MaxRuntime: 45 * time.Minute, MaxChangedFiles: 20, MaxDiffLines: 1200, RequireAttestation: true}
}

type RuntimeSpec struct {
	Name       string   `json:"name"`
	Executable string   `json:"executable,omitempty"`
	Model      string   `json:"model,omitempty"`
	Effort     string   `json:"effort,omitempty"`
	Sandbox    string   `json:"sandbox,omitempty"`
	Args       []string `json:"args,omitempty"`
}

type Node struct {
	ID             string            `json:"id"`
	Title          string            `json:"title"`
	Kind           string            `json:"kind"`
	State          NodeState         `json:"state"`
	DependsOn      []string          `json:"depends_on,omitempty"`
	Prompt         string            `json:"prompt,omitempty"`
	Worktree       string            `json:"worktree,omitempty"`
	Runtime        RuntimeSpec       `json:"runtime,omitempty"`
	Role           string            `json:"role,omitempty"`
	CandidateIndex int               `json:"candidate_index,omitempty"`
	SessionID      string            `json:"session_id,omitempty"`
	Attempt        int               `json:"attempt"`
	MaxAttempts    int               `json:"max_attempts"`
	CreatedAt      time.Time         `json:"created_at"`
	UpdatedAt      time.Time         `json:"updated_at"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

type Evidence struct {
	ID               string    `json:"id"`
	NodeID           string    `json:"node_id"`
	Kind             string    `json:"kind"`
	Summary          string    `json:"summary"`
	URI              string    `json:"uri,omitempty"`
	Digest           string    `json:"digest,omitempty"`
	Attempt          int       `json:"attempt,omitempty"`
	DeliveryAttempt  int       `json:"delivery_attempt,omitempty"`
	AttemptOutcome   string    `json:"attempt_outcome,omitempty"`
	InboxDisposition string    `json:"inbox_disposition,omitempty"`
	CreatedAt        time.Time `json:"created_at"`
}

type Attestation struct {
	ID          string    `json:"id"`
	NodeID      string    `json:"node_id"`
	Verifier    string    `json:"verifier"`
	Verdict     string    `json:"verdict"`
	RawVerdict  string    `json:"raw_verdict,omitempty"`
	Summary     string    `json:"summary"`
	EvidenceIDs []string  `json:"evidence_ids,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

type Workflow struct {
	ID           string            `json:"id"`
	Goal         string            `json:"goal"`
	Root         string            `json:"root"`
	Status       WorkflowStatus    `json:"status"`
	Paused       bool              `json:"paused,omitempty"`
	Budget       Budget            `json:"budget"`
	Nodes        map[string]*Node  `json:"nodes"`
	Order        []string          `json:"order"`
	Evidence     []Evidence        `json:"evidence,omitempty"`
	Attestations []Attestation     `json:"attestations,omitempty"`
	CreatedAt    time.Time         `json:"created_at"`
	UpdatedAt    time.Time         `json:"updated_at"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

type Mutation struct {
	Op             string       `json:"op"`
	Node           *Node        `json:"node,omitempty"`
	NodeID         string       `json:"node_id,omitempty"`
	DependsOn      []string     `json:"depends_on,omitempty"`
	State          NodeState    `json:"state,omitempty"`
	Reason         string       `json:"reason,omitempty"`
	Evidence       *Evidence    `json:"evidence,omitempty"`
	Attestation    *Attestation `json:"attestation,omitempty"`
	Runtime        *RuntimeSpec `json:"runtime,omitempty"`
	CandidateIndex int          `json:"candidate_index,omitempty"`
}

type Proposal struct {
	WorkflowID string     `json:"workflow_id"`
	Actor      string     `json:"actor"`
	Mutations  []Mutation `json:"mutations"`
	Rationale  string     `json:"rationale,omitempty"`
}

type Event struct {
	Sequence   uint64    `json:"sequence"`
	ID         string    `json:"id"`
	WorkflowID string    `json:"workflow_id"`
	Type       string    `json:"type"`
	Actor      string    `json:"actor"`
	At         time.Time `json:"at"`
	Data       any       `json:"data,omitempty"`
}
