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
	return Launch{Executable: fallback(request.Runtime.Executable, "pi"), Args: args, PromptOnStdin: true}, nil
}

func (Pi) NewDecoder() Decoder { return &piDecoder{} }

type piDecoder struct {
	turn   bool
	result bool
}

func (d *piDecoder) DecodeLine(raw []byte) ([]supervisor.Milestone, error) {
	var envelope struct {
		Type        string `json:"type"`
		SessionID   string `json:"session_id"`
		SessionFile string `json:"session_file"`
		Code        string `json:"code"`
		Error       string `json:"error"`
		ToolName    string `json:"tool_name"`
		ToolCallID  string `json:"tool_call_id"`
		Text        string `json:"text"`
		Result      string `json:"result"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, err
	}
	switch envelope.Type {
	case "session", "session_started":
		id := envelope.SessionID
		if id == "" {
			id = envelope.SessionFile
		}
		if strings.TrimSpace(id) == "" {
			return nil, errors.New("Pi session event omitted exact session identity")
		}
		identity := supervisor.NativeSessionIdentity{Runtime: "pi", ID: id}
		return []supervisor.Milestone{{Kind: supervisor.MilestoneSessionBound, Session: &identity, SourceType: envelope.Type}}, nil
	case "agent_start", "turn_start":
		if d.turn {
			return nil, errors.New("Pi emitted duplicate turn start")
		}
		d.turn = true
		return []supervisor.Milestone{{Kind: supervisor.MilestoneTurnStarted, SourceType: envelope.Type}}, nil
	case "tool_execution_start":
		return []supervisor.Milestone{{Kind: supervisor.MilestoneEffectStarted, Effect: envelope.ToolName + ":" + envelope.ToolCallID, SourceType: envelope.Type}}, nil
	case "message_update":
		if strings.TrimSpace(envelope.Text) == "" {
			return nil, nil
		}
		if result, ok := decodeWorkerResultString(envelope.Text); ok {
			if d.result {
				return nil, nil
			}
			d.result = true
			return []supervisor.Milestone{{Kind: supervisor.MilestoneResult, Result: result, SourceType: envelope.Type}}, nil
		}
		return []supervisor.Milestone{{Kind: supervisor.MilestoneMeaningfulProgress, Progress: envelope.Text, SourceType: envelope.Type}}, nil
	case "agent_end":
		if result, ok := decodeWorkerResultString(envelope.Result); ok {
			if d.result {
				return nil, nil
			}
			d.result = true
			return []supervisor.Milestone{{Kind: supervisor.MilestoneResult, Result: result, SourceType: envelope.Type}}, nil
		}
	case "error":
		if providerUnavailable(envelope.Code) {
			return []supervisor.Milestone{{Kind: supervisor.MilestoneProviderUnavailable, Failure: envelope.Error, SourceType: envelope.Type}}, nil
		}
		if !d.turn {
			return []supervisor.Milestone{{Kind: supervisor.MilestoneAdapterStartFailed, Failure: envelope.Error, SourceType: envelope.Type}}, nil
		}
	}
	return nil, nil
}
