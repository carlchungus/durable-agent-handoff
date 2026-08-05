package team

import (
	"encoding/json"
	"time"
)

type MemberState string

const (
	MemberStarting   MemberState = "starting"
	MemberWorking    MemberState = "working"
	MemberIdle       MemberState = "idle"
	MemberNeedsInput MemberState = "needs_input"
	MemberStopped    MemberState = "stopped"
	MemberExited     MemberState = "exited"
)

type ProcessState string

const (
	ProcessUnknown ProcessState = "unknown"
	ProcessLive    ProcessState = "live"
	ProcessExited  ProcessState = "exited"
)

type TaskState string

const (
	TaskPending    TaskState = "pending"
	TaskInProgress TaskState = "in_progress"
	TaskCompleted  TaskState = "completed"
	TaskFailed     TaskState = "failed"
)

type PlanState string

const (
	PlanNotRequired PlanState = "not_required"
	PlanAwaiting    PlanState = "awaiting_approval"
	PlanApproved    PlanState = "approved"
	PlanRejected    PlanState = "rejected"
)

type Member struct {
	ID                string            `json:"id"`
	Name              string            `json:"name"`
	Role              string            `json:"role,omitempty"`
	Runtime           string            `json:"runtime,omitempty"`
	Model             string            `json:"model,omitempty"`
	SessionID         string            `json:"session_id,omitempty"`
	State             MemberState       `json:"state"`
	Process           ProcessState      `json:"process"`
	NeedsInputReason  string            `json:"needs_input_reason,omitempty"`
	Plan              PlanState         `json:"plan"`
	PlanText          string            `json:"plan_text,omitempty"`
	PlanReview        string            `json:"plan_review,omitempty"`
	ShutdownRequested bool              `json:"shutdown_requested,omitempty"`
	Metadata          map[string]string `json:"metadata,omitempty"`
	CreatedAt         time.Time         `json:"created_at"`
	UpdatedAt         time.Time         `json:"updated_at"`
}

type Claim struct {
	MemberID   string    `json:"member_id"`
	Generation uint64    `json:"generation"`
	ClaimedAt  time.Time `json:"claimed_at"`
	ExpiresAt  time.Time `json:"expires_at,omitempty"`
}

type Task struct {
	ID          string            `json:"id"`
	Title       string            `json:"title"`
	Description string            `json:"description,omitempty"`
	State       TaskState         `json:"state"`
	BlockedBy   []string          `json:"blocked_by,omitempty"`
	Claim       *Claim            `json:"claim,omitempty"`
	Result      string            `json:"result,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

type MessageKind string

const (
	MessageDirect           MessageKind = "direct"
	MessageBroadcast        MessageKind = "broadcast"
	MessageIdle             MessageKind = "idle"
	MessageShutdownRequest  MessageKind = "shutdown_request"
	MessageShutdownResponse MessageKind = "shutdown_response"
	MessagePlanSubmitted    MessageKind = "plan_submitted"
	MessagePlanReviewed     MessageKind = "plan_reviewed"
)

type Message struct {
	Sequence  uint64      `json:"sequence"`
	ID        string      `json:"id"`
	Kind      MessageKind `json:"kind"`
	From      string      `json:"from"`
	To        string      `json:"to,omitempty"`
	Body      string      `json:"body,omitempty"`
	ReplyTo   string      `json:"reply_to,omitempty"`
	CreatedAt time.Time   `json:"created_at"`
}

type Team struct {
	ID         string             `json:"id"`
	WorkflowID string             `json:"workflow_id,omitempty"`
	Name       string             `json:"name"`
	LeadID     string             `json:"lead_id"`
	Members    map[string]*Member `json:"members"`
	Tasks      map[string]*Task   `json:"tasks"`
	TaskOrder  []string           `json:"task_order"`
	Messages   []Message          `json:"messages,omitempty"`
	CreatedAt  time.Time          `json:"created_at"`
	UpdatedAt  time.Time          `json:"updated_at"`
}

type Command struct {
	Op              string        `json:"op"`
	Actor           string        `json:"actor"`
	Member          *Member       `json:"member,omitempty"`
	MemberID        string        `json:"member_id,omitempty"`
	State           MemberState   `json:"state,omitempty"`
	Process         ProcessState  `json:"process,omitempty"`
	Reason          string        `json:"reason,omitempty"`
	SessionID       string        `json:"session_id,omitempty"`
	Task            *Task         `json:"task,omitempty"`
	TaskID          string        `json:"task_id,omitempty"`
	ClaimGeneration uint64        `json:"claim_generation,omitempty"`
	Lease           time.Duration `json:"lease,omitempty"`
	Result          string        `json:"result,omitempty"`
	To              string        `json:"to,omitempty"`
	Body            string        `json:"body,omitempty"`
	ReplyTo         string        `json:"reply_to,omitempty"`
	Approved        *bool         `json:"approved,omitempty"`
}

type Event struct {
	Sequence uint64          `json:"sequence"`
	ID       string          `json:"id"`
	TeamID   string          `json:"team_id"`
	Type     string          `json:"type"`
	Actor    string          `json:"actor"`
	At       time.Time       `json:"at"`
	Data     json.RawMessage `json:"data"`
}
