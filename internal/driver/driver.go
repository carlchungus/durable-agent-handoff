// Package driver contains deep runtime adapters. A Driver owns both argv
// construction and decoding of its provider protocol into Supervisor facts.
package driver

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/carlchungus/durable-agent-handoff/internal/supervisor"
)

type LaunchRequest struct {
	Runtime    supervisor.RuntimeSpec
	Worktree   string
	Prompt     string
	Session    supervisor.NativeSessionIdentity
	SchemaPath string
	ResultPath string
	TrustMode  TrustMode
	// SessionMode leaves the harness in its native, terminal-like protocol. In
	// this mode handoff records process/output facts but does not append a
	// mandatory structured completion contract.
	SessionMode bool
}

type TrustMode string

const (
	TrustWorkspace TrustMode = "workspace"
	TrustFull      TrustMode = "full"
)

type Launch struct {
	Executable    string
	Args          []string
	PromptOnStdin bool
	// Prompt is transient launch input. Drivers may append a runtime-owned
	// completion contract without putting user content in argv or durable
	// Supervisor state.
	Prompt string
}

type Decoder interface {
	DecodeLine([]byte) ([]supervisor.Milestone, error)
}

type Driver interface {
	Name() string
	Build(LaunchRequest) (Launch, error)
	NewDecoder() Decoder
	Spawned(supervisor.ProcessIdentity) supervisor.Milestone
	StartFailed(error) supervisor.Milestone
	Exited(code int, err error) supervisor.Milestone
}

func Lookup(name string) (Driver, error) {
	switch name {
	case "codex":
		return Codex{}, nil
	case "claude":
		return Claude{}, nil
	case "pi":
		return Pi{}, nil
	default:
		if strings.TrimSpace(name) == "" {
			return nil, errors.New("runtime driver name is required")
		}
		// Unknown names use the deliberately small line-oriented adapter. This
		// keeps the durable process/session layer harness-neutral; a harness can
		// opt into richer native resume semantics by adding a named Driver later.
		return Generic{NameValue: name}, nil
	}
}

type lifecycle struct{}

func (lifecycle) Spawned(process supervisor.ProcessIdentity) supervisor.Milestone {
	return supervisor.Milestone{Kind: supervisor.MilestoneProcessSpawned, Process: &process, SourceType: "process"}
}

func (lifecycle) StartFailed(err error) supervisor.Milestone {
	reason := "adapter failed before start"
	if err != nil {
		reason = err.Error()
	}
	return supervisor.Milestone{Kind: supervisor.MilestoneAdapterStartFailed, Failure: reason, SourceType: "adapter"}
}

func (lifecycle) Exited(code int, err error) supervisor.Milestone {
	exit := &supervisor.Exit{Code: code}
	if err != nil {
		exit.Error = err.Error()
	}
	return supervisor.Milestone{Kind: supervisor.MilestoneExit, Exit: exit, SourceType: "process"}
}

func fallback(value, defaultValue string) string {
	if value != "" {
		return value
	}
	return defaultValue
}

const completionContract = `

Supervisor completion contract: after performing the requested work, emit exactly one JSON object with this shape and no Markdown or surrounding prose:
{"status":"completed|continue|needs_human|blocked","summary":"concise outcome","blocker_kind":"","question":""}
Use continue when this turn or candidate is finished but the objective remains actionable. Assume the human is unavailable while an unattended goal is running. Use needs_human only when indispensable authority or information blocks the entire workflow and no safe partial result can be published; fill blocker_kind plus one concrete question. Missing optional verification, external CI, or production-browser access must downgrade confidence rather than suppress useful output. When GitHub publication is authorized, publish an honest draft PR with verification limits instead of waiting for optional evidence. Once a PR is handed to repository automation, do not idle waiting for it to merge when independent work remains. A plan, progress update, or promise is not a terminal result. Continue using tools until the work is actually complete, should continue in another turn, or has a concrete workflow-wide blocker.`

func promptForRuntime(_ string, prompt string, includeContract bool) string {
	if !includeContract {
		return prompt
	}
	return strings.TrimRight(prompt, "\n") + completionContract
}

// SessionModeDecoder lets the executor restore the native terminal-like
// protocol without changing the public Decoder interface used by extensions.
type SessionModeDecoder interface {
	SetSessionMode(bool)
}

func validateLaunch(request LaunchRequest, runtime string) error {
	if request.Runtime.Name != runtime || request.Session.Runtime != runtime {
		return fmt.Errorf("%s Driver requires matching RuntimeSpec and exact native Session", runtime)
	}
	if strings.TrimSpace(request.Worktree) == "" || strings.TrimSpace(request.Prompt) == "" {
		return errors.New("launch requires worktree and prompt")
	}
	if request.Runtime.Sandbox != supervisor.SandboxReadOnly && request.Runtime.Sandbox != supervisor.SandboxWorkspaceWrite {
		return fmt.Errorf("unsupported sandbox %q", request.Runtime.Sandbox)
	}
	if request.TrustMode == "" {
		request.TrustMode = TrustWorkspace
	}
	if request.TrustMode != TrustWorkspace && request.TrustMode != TrustFull {
		return fmt.Errorf("unsupported trust mode %q", request.TrustMode)
	}
	if request.TrustMode == TrustFull && request.Runtime.Sandbox == supervisor.SandboxReadOnly {
		return errors.New("full trust cannot be combined with read-only authority")
	}
	return nil
}

func disabledProjectMCPArgs(worktree string) []string {
	if _, err := os.Stat(filepath.Join(worktree, ".codex", "config.toml")); err != nil {
		return nil
	}
	return []string{"-c", "mcp_servers={}"}
}

func decodeWorkerResult(raw []byte) (*supervisor.WorkerResult, bool) {
	var result supervisor.WorkerResult
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&result) != nil || strings.TrimSpace(result.Status) == "" || strings.TrimSpace(result.Summary) == "" {
		return nil, false
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return nil, false
	}
	switch result.Status {
	case "completed", "continue", "needs_human", "blocked":
		return &result, true
	default:
		return nil, false
	}
}

func decodeWorkerResultString(value string) (*supervisor.WorkerResult, bool) {
	return decodeWorkerResult([]byte(strings.TrimSpace(value)))
}

func providerUnavailable(code string) bool {
	switch strings.ToLower(code) {
	case "provider_unavailable", "overloaded", "rate_limit", "rate_limited", "usage_limit", "quota_exhausted":
		return true
	default:
		return false
	}
}
