package driver

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/carlchungus/durable-agent-handoff/internal/supervisor"
)

type Codex struct{ lifecycle }

func (Codex) Name() string { return "codex" }

func (Codex) Build(request LaunchRequest) (Launch, error) {
	if err := validateLaunch(request, "codex"); err != nil {
		return Launch{}, err
	}
	args := []string{"-C", request.Worktree, "-m", fallback(request.Runtime.Model, "gpt-5.6-luna"), "-c", fmt.Sprintf("model_reasoning_effort=%q", fallback(request.Runtime.Effort, "xhigh"))}
	if request.TrustMode == TrustFull {
		args = append(args, "--dangerously-bypass-approvals-and-sandbox")
	} else {
		args = append(args, "-s", string(request.Runtime.Sandbox), "-a", "never")
	}
	args = append(args, disabledProjectMCPArgs(request.Worktree)...)
	args = append(args, "exec", "--ignore-user-config", "--json")
	if request.SchemaPath != "" {
		args = append(args, "--output-schema", request.SchemaPath)
	}
	if request.ResultPath != "" {
		args = append(args, "-o", request.ResultPath)
	}
	if strings.TrimSpace(request.Session.ID) != "" {
		args = append(args, "resume", request.Session.ID, "-")
	} else {
		args = append(args, "-")
	}
	return Launch{Executable: fallback(request.Runtime.Executable, "codex"), Args: args, PromptOnStdin: true, Prompt: promptForRuntime("codex", request.Prompt)}, nil
}

func (Codex) NewDecoder() Decoder { return &codexDecoder{} }

type codexDecoder struct {
	turn          bool
	result        bool
	pendingResult *supervisor.WorkerResult
}

func (d *codexDecoder) DecodeLine(raw []byte) ([]supervisor.Milestone, error) {
	var envelope struct {
		Type     string `json:"type"`
		ThreadID string `json:"thread_id"`
		Message  string `json:"message"`
		Error    struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
		Item struct {
			ID      string `json:"id"`
			Type    string `json:"type"`
			Text    string `json:"text"`
			Command string `json:"command"`
			Server  string `json:"server"`
			Tool    string `json:"tool"`
		} `json:"item"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, err
	}
	if d.result {
		return nil, nil
	}
	switch envelope.Type {
	case "thread.started":
		if strings.TrimSpace(envelope.ThreadID) == "" {
			return nil, errors.New("Codex thread.started omitted thread_id")
		}
		identity := supervisor.NativeSessionIdentity{Runtime: "codex", ID: envelope.ThreadID}
		return []supervisor.Milestone{{Kind: supervisor.MilestoneSessionBound, Session: &identity, SourceType: envelope.Type}}, nil
	case "turn.started":
		if d.turn {
			return nil, errors.New("Codex emitted duplicate turn.started")
		}
		d.turn = true
		return []supervisor.Milestone{{Kind: supervisor.MilestoneTurnStarted, SourceType: envelope.Type}}, nil
	case "item.started":
		effect := ""
		switch envelope.Item.Type {
		case "command_execution":
			effect = "command:" + envelope.Item.ID
		case "mcp_tool_call":
			effect = "mcp:" + envelope.Item.Server + "/" + envelope.Item.Tool
		case "file_change":
			effect = "file_change:" + envelope.Item.ID
		}
		if effect == "" {
			return nil, nil
		}
		return []supervisor.Milestone{{Kind: supervisor.MilestoneEffectStarted, Effect: effect, SourceType: envelope.Type}}, nil
	case "item.completed":
		if envelope.Item.Type != "agent_message" || strings.TrimSpace(envelope.Item.Text) == "" {
			return nil, nil
		}
		if result, ok := decodeWorkerResultString(envelope.Item.Text); ok {
			// Codex applies the output schema to intermediate agent messages as
			// well as the final response. Keep the latest candidate, but only the
			// provider's turn.completed event may make it a durable Result.
			d.pendingResult = result
			return nil, nil
		}
		return []supervisor.Milestone{{Kind: supervisor.MilestoneMeaningfulProgress, Progress: envelope.Item.Text, SourceType: envelope.Type}}, nil
	case "turn.completed":
		if d.pendingResult == nil {
			return nil, errors.New("Codex turn.completed omitted the structured completion result")
		}
		d.result = true
		return []supervisor.Milestone{{Kind: supervisor.MilestoneResult, Result: d.pendingResult, SourceType: envelope.Type}}, nil
	case "error":
		if providerUnavailable(envelope.Error.Code) {
			return []supervisor.Milestone{{Kind: supervisor.MilestoneProviderUnavailable, Failure: envelope.Error.Message, SourceType: envelope.Type}}, nil
		}
		message := strings.TrimSpace(envelope.Error.Message)
		if message == "" {
			message = strings.TrimSpace(envelope.Message)
		}
		if message != "" {
			return nil, fmt.Errorf("Codex runtime error: %s", message)
		}
	case "turn.failed":
		message := strings.TrimSpace(envelope.Error.Message)
		if message == "" {
			message = strings.TrimSpace(envelope.Message)
		}
		if message == "" {
			message = "turn failed without an error message"
		}
		return nil, fmt.Errorf("Codex turn failed: %s", message)
	}
	return nil, nil
}
