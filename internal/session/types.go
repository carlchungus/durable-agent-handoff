package session

import "time"

const Version = 1

type LogicalState string

const (
	LogicalWorking    LogicalState = "working"
	LogicalNeedsInput LogicalState = "needs_input"
	LogicalCompleted  LogicalState = "completed"
	LogicalStopped    LogicalState = "stopped"
)

type ProcessState string

const (
	ProcessStarting ProcessState = "starting"
	ProcessRunning  ProcessState = "running"
	ProcessExited   ProcessState = "exited"
)

type MessageState string

const (
	MessageQueued     MessageState = "queued"
	MessageDispatched MessageState = "dispatched"
	MessageDelivered  MessageState = "delivered"
)

type Descriptor struct {
	WorkflowID       string
	NodeID           string
	ParentAgentID    string
	Name             string
	Runtime          string
	RuntimeSessionID string
	Worktree         string
	LogicalState     LogicalState
	ProcessState     ProcessState
}

type Observation struct {
	Runtime          string       `json:"runtime,omitempty"`
	RuntimeSessionID string       `json:"runtime_session_id,omitempty"`
	Worktree         string       `json:"worktree,omitempty"`
	LogicalState     LogicalState `json:"logical_state,omitempty"`
	ProcessState     ProcessState `json:"process_state,omitempty"`
}

type Message struct {
	ID              string       `json:"id"`
	Sequence        uint64       `json:"sequence"`
	From            string       `json:"from"`
	Body            string       `json:"body"`
	State           MessageState `json:"state"`
	DeliveryAttempt int          `json:"delivery_attempt,omitempty"`
	CreatedAt       time.Time    `json:"created_at"`
	DeliveredAt     time.Time    `json:"delivered_at,omitempty"`
}

type Session struct {
	Version          int          `json:"version"`
	ID               string       `json:"id"`
	WorkflowID       string       `json:"workflow_id"`
	NodeID           string       `json:"node_id"`
	ParentAgentID    string       `json:"parent_agent_id,omitempty"`
	Name             string       `json:"name"`
	Runtime          string       `json:"runtime"`
	RuntimeSessionID string       `json:"runtime_session_id,omitempty"`
	Worktree         string       `json:"worktree"`
	LogicalState     LogicalState `json:"logical_state"`
	ProcessState     ProcessState `json:"process_state"`
	Inbox            []Message    `json:"inbox,omitempty"`
	CreatedAt        time.Time    `json:"created_at"`
	UpdatedAt        time.Time    `json:"updated_at"`
}

type Event struct {
	Sequence  uint64    `json:"sequence"`
	SessionID string    `json:"session_id"`
	Type      string    `json:"type"`
	At        time.Time `json:"at"`
	Data      any       `json:"data,omitempty"`
}
