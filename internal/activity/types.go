package activity

import "time"

const Version = 1

type State string

const (
	StatePending   State = "pending"
	StateStarting  State = "starting"
	StateRunning   State = "running"
	StateStopping  State = "stopping"
	StateCompleted State = "completed"
	StateFailed    State = "failed"
	StateStopped   State = "stopped"
	StateLost      State = "lost"
)

type Stream string

const (
	StreamStdout Stream = "stdout"
	StreamStderr Stream = "stderr"
)

type WorkSpec struct {
	Kind   string `json:"kind"`
	Cwd    string `json:"cwd"`
	Intent string `json:"intent,omitempty"`
}

type Descriptor struct {
	ID             string
	OwnerSessionID string
	Work           WorkSpec
	// Command is transient execution data. It is intentionally excluded from
	// durable state because argv can contain prompts or credentials.
	Command []string
	Runtime string
	Model   string
}

type OutputRef struct {
	ID       string `json:"id"`
	Stream   Stream `json:"stream"`
	FileName string `json:"file_name"`
	Path     string `json:"path"`
	Closed   bool   `json:"closed"`
}

type Attempt struct {
	ID                   string    `json:"id"`
	Ordinal              int       `json:"ordinal"`
	Runtime              string    `json:"runtime,omitempty"`
	Model                string    `json:"model,omitempty"`
	CommandDigest        string    `json:"command_digest,omitempty"`
	PID                  int       `json:"pid"`
	ProcessStartToken    string    `json:"process_start_token"`
	ProcessTreeID        string    `json:"process_tree_id,omitempty"`
	SupervisorID         string    `json:"supervisor_id"`
	SupervisorGeneration uint64    `json:"supervisor_generation"`
	State                State     `json:"state"`
	Stdout               OutputRef `json:"stdout"`
	Stderr               OutputRef `json:"stderr"`
	StartedAt            time.Time `json:"started_at"`
	FinishedAt           time.Time `json:"finished_at,omitempty"`
	ExitCode             *int      `json:"exit_code,omitempty"`
	Error                string    `json:"error,omitempty"`
}

type AttemptStart struct {
	Runtime       string
	Model         string
	CommandDigest string
}

type ProcessIdentity struct {
	PID                  int
	ProcessStartToken    string
	ProcessTreeID        string
	SupervisorID         string
	SupervisorGeneration uint64
}

type AttemptIdentity struct {
	ID                   string `json:"id"`
	PID                  int    `json:"pid"`
	ProcessStartToken    string `json:"process_start_token"`
	ProcessTreeID        string `json:"process_tree_id,omitempty"`
	SupervisorID         string `json:"supervisor_id"`
	SupervisorGeneration uint64 `json:"supervisor_generation"`
}

type ExitResult struct {
	State    State  `json:"state"`
	ExitCode *int   `json:"exit_code,omitempty"`
	Error    string `json:"error,omitempty"`
}

type ControlOutcome string

const (
	ControlAccepted ControlOutcome = "accepted"
	ControlRejected ControlOutcome = "rejected"
	ControlApplied  ControlOutcome = "applied"
)

type ControlIntent struct {
	ID                 string          `json:"id"`
	Kind               string          `json:"kind"`
	ExpectedGeneration uint64          `json:"expected_generation"`
	ExpectedAttempt    AttemptIdentity `json:"expected_attempt"`
	Outcome            ControlOutcome  `json:"outcome"`
	Reason             string          `json:"reason,omitempty"`
	CreatedAt          time.Time       `json:"created_at"`
	AppliedAt          time.Time       `json:"applied_at,omitempty"`
}

type ControlRequest struct {
	ExpectedGeneration uint64
	ExpectedAttempt    AttemptIdentity
}

type Activity struct {
	Version        int             `json:"version"`
	ID             string          `json:"id"`
	OwnerSessionID string          `json:"owner_session_id,omitempty"`
	Work           WorkSpec        `json:"work"`
	WorkDigest     string          `json:"work_digest"`
	State          State           `json:"state"`
	Generation     uint64          `json:"generation"`
	Revision       uint64          `json:"revision"`
	Attempts       []Attempt       `json:"attempts,omitempty"`
	Controls       []ControlIntent `json:"controls,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

type OutputCursor struct {
	AttemptID string
	Stream    Stream
	OutputID  string
	After     int64
}

type OutputChunk struct {
	ActivityID string `json:"activity_id"`
	AttemptID  string `json:"attempt_id"`
	Stream     Stream `json:"stream"`
	OutputID   string `json:"output_id"`
	Start      int64  `json:"start"`
	End        int64  `json:"end"`
	Size       int64  `json:"size"`
	Data       []byte `json:"data"`
	Closed     bool   `json:"closed"`
	Revision   uint64 `json:"revision"`
}
