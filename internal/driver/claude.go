package driver

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/carlchungus/durable-agent-handoff/internal/supervisor"
)

type Claude struct{ lifecycle }

func (Claude) Name() string { return "claude" }

func (Claude) Build(request LaunchRequest) (Launch, error) {
	if err := validateLaunch(request, "claude"); err != nil {
		return Launch{}, err
	}
	tools := "Bash,Read,Edit,Write,Glob,Grep"
	if request.Runtime.Sandbox == supervisor.SandboxReadOnly {
		tools = "Read,Glob,Grep"
	}
	args := []string{"--print", "--output-format", "stream-json", "--verbose", "--strict-mcp-config", "--mcp-config", `{"mcpServers":{}}`, "--model", fallback(request.Runtime.Model, "sonnet"), "--effort", fallback(request.Runtime.Effort, "high"), "--permission-mode", "dontAsk", "--tools", tools}
	if strings.TrimSpace(request.Session.ID) != "" {
		args = append(args, "--resume", request.Session.ID)
	}
	if request.TrustMode == TrustFull {
		args = append(args, "--dangerously-skip-permissions")
	}
	return Launch{Executable: fallback(request.Runtime.Executable, "claude"), Args: args, PromptOnStdin: true, Prompt: promptForRuntime("claude", request.Prompt)}, nil
}

func (Claude) NewDecoder() Decoder { return &claudeDecoder{} }

type claudeDecoder struct {
	turn   bool
	result bool
}

func (d *claudeDecoder) DecodeLine(raw []byte) ([]supervisor.Milestone, error) {
	var envelope struct {
		Type      string `json:"type"`
		Subtype   string `json:"subtype"`
		SessionID string `json:"session_id"`
		IsError   bool   `json:"is_error"`
		Error     string `json:"error"`
		ErrorCode string `json:"error_code"`
		Code      string `json:"code"`
		Result    string `json:"result"`
		Message   struct {
			Content []struct {
				Type string `json:"type"`
				ID   string `json:"id"`
				Name string `json:"name"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"message"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, err
	}
	if envelope.Type == "system" && envelope.Subtype == "init" {
		if strings.TrimSpace(envelope.SessionID) == "" {
			return nil, errors.New("Claude init omitted session_id")
		}
		identity := supervisor.NativeSessionIdentity{Runtime: "claude", ID: envelope.SessionID}
		return []supervisor.Milestone{{Kind: supervisor.MilestoneSessionBound, Session: &identity, SourceType: "system.init"}}, nil
	}
	if envelope.Type == "system" && envelope.Subtype == "error" && !d.turn {
		if providerUnavailable(envelope.ErrorCode) || providerUnavailable(envelope.Code) {
			return []supervisor.Milestone{{Kind: supervisor.MilestoneProviderUnavailable, Failure: envelope.Error, SourceType: "system.error"}}, nil
		}
		return []supervisor.Milestone{{Kind: supervisor.MilestoneAdapterStartFailed, Failure: envelope.Error, SourceType: "system.error"}}, nil
	}
	if envelope.Type == "assistant" {
		var out []supervisor.Milestone
		if !d.turn {
			d.turn = true
			out = append(out, supervisor.Milestone{Kind: supervisor.MilestoneTurnStarted, SourceType: "assistant"})
		}
		for _, content := range envelope.Message.Content {
			switch content.Type {
			case "tool_use":
				out = append(out, supervisor.Milestone{Kind: supervisor.MilestoneEffectStarted, Effect: content.Name + ":" + content.ID, SourceType: "assistant.tool_use"})
			case "text":
				if strings.TrimSpace(content.Text) == "" {
					continue
				}
				if result, ok := decodeWorkerResultString(content.Text); ok {
					if !d.result {
						d.result = true
						out = append(out, supervisor.Milestone{Kind: supervisor.MilestoneResult, Result: result, SourceType: "assistant.text"})
					}
				} else {
					out = append(out, supervisor.Milestone{Kind: supervisor.MilestoneMeaningfulProgress, Progress: content.Text, SourceType: "assistant.text"})
				}
			}
		}
		return out, nil
	}
	if envelope.Type == "result" {
		if envelope.IsError && providerUnavailable(envelope.Subtype) {
			return []supervisor.Milestone{{Kind: supervisor.MilestoneProviderUnavailable, Failure: envelope.Result, SourceType: "result." + envelope.Subtype}}, nil
		}
		if result, ok := decodeWorkerResultString(envelope.Result); ok {
			if d.result {
				return nil, nil
			}
			d.result = true
			return []supervisor.Milestone{{Kind: supervisor.MilestoneResult, Result: result, SourceType: "result"}}, nil
		}
	}
	return nil, nil
}
