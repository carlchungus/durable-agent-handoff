package driver

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/carlchungus/durable-agent-handoff/internal/supervisor"
)

type Pi struct{ lifecycle }

func (Pi) Name() string { return "pi" }

func (Pi) Build(request LaunchRequest) (Launch, error) {
	if err := validateLaunch(request, "pi"); err != nil {
		return Launch{}, err
	}
	if request.Runtime.Sandbox == supervisor.SandboxReadOnly {
		return Launch{}, errors.New("Pi cannot enforce read-only execution without an external OS sandbox")
	}
	args := []string{"--print", "--mode", "json", "--model", fallback(request.Runtime.Model, "openrouter/deepseek/deepseek-v4-flash"), "--thinking", fallback(request.Runtime.Effort, "xhigh")}
	if strings.TrimSpace(request.Session.ID) != "" {
		args = append(args, "--session", request.Session.ID)
	}
	if request.TrustMode == TrustFull {
		args = append(args, "--approve")
	} else {
		args = append(args, "--no-approve")
	}
	return Launch{Executable: fallback(request.Runtime.Executable, "pi"), Args: args, PromptOnStdin: true, Prompt: promptForRuntime("pi", request.Prompt)}, nil
}

func (Pi) NewDecoder() Decoder { return &piDecoder{} }

type piDecoder struct {
	sessionID string
	turn      bool
	result    bool
}

func (d *piDecoder) DecodeLine(raw []byte) ([]supervisor.Milestone, error) {
	var envelope piJSONEvent
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, err
	}
	switch envelope.Type {
	case "session":
		if strings.TrimSpace(envelope.ID) == "" {
			return nil, errors.New("Pi session event omitted exact id")
		}
		if d.sessionID != "" {
			if d.sessionID != envelope.ID {
				return nil, errors.New("Pi emitted conflicting session identities")
			}
			return nil, nil
		}
		d.sessionID = envelope.ID
		identity := supervisor.NativeSessionIdentity{Runtime: "pi", ID: envelope.ID}
		return []supervisor.Milestone{{Kind: supervisor.MilestoneSessionBound, Session: &identity, SourceType: envelope.Type}}, nil
	case "agent_start":
		return nil, nil
	case "turn_start":
		if d.turn {
			return nil, nil
		}
		d.turn = true
		return []supervisor.Milestone{{Kind: supervisor.MilestoneTurnStarted, SourceType: envelope.Type}}, nil
	case "tool_execution_start":
		if strings.TrimSpace(envelope.ToolName) == "" || strings.TrimSpace(envelope.ToolCallID) == "" {
			return nil, nil
		}
		return []supervisor.Milestone{{Kind: supervisor.MilestoneEffectStarted, Effect: envelope.ToolName + ":" + envelope.ToolCallID, SourceType: envelope.Type}}, nil
	case "message_update":
		if !d.turn {
			return nil, nil
		}
		return piProgressMilestone(envelope.AssistantMessageEvent, envelope.Type), nil
	case "message_end":
		return d.decodePiMessage(envelope.Message, envelope.Type), nil
	case "agent_end":
		var out []supervisor.Milestone
		for _, message := range envelope.Messages {
			if message.Role != "assistant" {
				continue
			}
			out = append(out, d.decodePiMessage(&message, envelope.Type)...)
		}
		return out, nil
	}
	return nil, nil
}

type piJSONEvent struct {
	Type                  string                   `json:"type"`
	ID                    string                   `json:"id"`
	AssistantMessageEvent *piAssistantMessageEvent `json:"assistantMessageEvent"`
	Message               *piMessage               `json:"message"`
	Messages              []piMessage              `json:"messages"`
	ToolName              string                   `json:"toolName"`
	ToolCallID            string                   `json:"toolCallId"`
}

type piAssistantMessageEvent struct {
	Type    string `json:"type"`
	Delta   string `json:"delta"`
	Content string `json:"content"`
}

type piMessage struct {
	Role         string      `json:"role"`
	Content      []piContent `json:"content"`
	StopReason   string      `json:"stopReason"`
	ErrorMessage string      `json:"errorMessage"`
}

type piContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

func piProgressMilestone(event *piAssistantMessageEvent, source string) []supervisor.Milestone {
	if event == nil {
		return nil
	}
	progress := ""
	switch event.Type {
	case "text_delta":
		progress = event.Delta
	case "text_end":
		progress = event.Content
	}
	if strings.TrimSpace(progress) == "" {
		return nil
	}
	return []supervisor.Milestone{{Kind: supervisor.MilestoneMeaningfulProgress, Progress: progress, SourceType: source + ".assistantMessageEvent." + event.Type}}
}

func (d *piDecoder) decodePiMessage(message *piMessage, source string) []supervisor.Milestone {
	if d.result || message == nil || message.Role != "assistant" {
		return nil
	}
	text := ""
	for _, content := range message.Content {
		if content.Type == "text" {
			text += content.Text
		}
	}
	if strings.TrimSpace(text) == "" {
		if !d.turn && message.StopReason == "error" && strings.TrimSpace(message.ErrorMessage) != "" {
			return []supervisor.Milestone{{Kind: supervisor.MilestoneAdapterStartFailed, Failure: message.ErrorMessage, SourceType: source + ".message"}}
		}
		return nil
	}
	if !d.turn {
		return nil
	}
	if result, ok := decodeWorkerResultString(text); ok {
		if d.result {
			return nil
		}
		d.result = true
		return []supervisor.Milestone{{Kind: supervisor.MilestoneResult, Result: result, SourceType: source + ".message.content"}}
	}
	return []supervisor.Milestone{{Kind: supervisor.MilestoneMeaningfulProgress, Progress: text, SourceType: source + ".message.content"}}
}
