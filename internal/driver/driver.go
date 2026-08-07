// Package driver contains deep runtime adapters. A Driver owns both argv
// construction and decoding of its provider protocol into Supervisor facts.
package driver

import (
	"encoding/json"
	"errors"
	"fmt"
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
		return nil, fmt.Errorf("unsupported runtime driver %q", name)
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
{"status":"completed|needs_human|blocked","summary":" concise outcome "}
The status must be one of completed, needs_human, or blocked, and summary must be a non-empty string. Do not emit this object until the work is finished.`

func promptForRuntime(runtime, prompt string) string {
	if runtime == "codex" {
		// Codex receives the same contract as a native output schema through the
		// schema file, so its user prompt remains unchanged.
		return prompt
	}
	return strings.TrimRight(prompt, "\n") + completionContract
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
	if json.Unmarshal(raw, &result) != nil || strings.TrimSpace(result.Status) == "" || strings.TrimSpace(result.Summary) == "" {
		return nil, false
	}
	switch result.Status {
	case "completed", "needs_human", "blocked":
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
