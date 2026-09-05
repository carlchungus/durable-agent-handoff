package driver

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/carlchungus/durable-agent-handoff/supervisor"
)

// Generic is the compatibility adapter for harnesses that do not yet have a
// native handoff protocol. It deliberately makes fewer claims: it launches
// the supplied executable with argv and stdin, records readable output, and
// lets the session-mode exit fact settle the Activity. Native session binding,
// exact resume, and provider-specific structured results remain opt-in driver
// capabilities rather than guessed from arbitrary output.
type Generic struct {
	lifecycle
	NameValue string
}

func (g Generic) Name() string { return g.NameValue }

func (g Generic) Build(request LaunchRequest) (Launch, error) {
	if err := validateLaunch(request, g.NameValue); err != nil {
		return Launch{}, err
	}
	if strings.TrimSpace(request.Session.ID) != "" {
		return Launch{}, errors.New("generic harnesses cannot resume an exact native session; configure its native resume argument explicitly")
	}
	if request.Runtime.Sandbox == supervisor.SandboxReadOnly {
		return Launch{}, errors.New("generic harnesses cannot enforce read-only execution without an external OS sandbox")
	}
	executable := request.Runtime.Executable
	if executable == "" {
		executable = g.NameValue
	}
	args := []string{}
	if strings.TrimSpace(request.Runtime.Arguments) != "" {
		if err := json.Unmarshal([]byte(request.Runtime.Arguments), &args); err != nil {
			return Launch{}, errors.New("generic runtime arguments must be a JSON array")
		}
	}
	return Launch{Executable: executable, Args: args, PromptOnStdin: true, Prompt: promptForRuntime(g.NameValue, request.Prompt, !request.SessionMode)}, nil
}

func (g Generic) NewDecoder() Decoder { return &genericDecoder{} }

type genericDecoder struct {
	turn        bool
	sessionMode bool
}

func (d *genericDecoder) SetSessionMode(value bool) { d.sessionMode = value }

func (d *genericDecoder) DecodeLine(raw []byte) ([]supervisor.Milestone, error) {
	text := strings.TrimSpace(string(raw))
	if text == "" {
		return nil, nil
	}
	var out []supervisor.Milestone
	if !d.turn {
		d.turn = true
		out = append(out, supervisor.Milestone{Kind: supervisor.MilestoneTurnStarted, SourceType: "generic.output"})
	}
	if result, ok := decodeWorkerResult(raw); ok && !d.sessionMode {
		out = append(out, supervisor.Milestone{Kind: supervisor.MilestoneResult, Result: result, SourceType: "generic.output"})
		return out, nil
	}
	// JSONL is common among agent harnesses, but a generic adapter must also
	// preserve ordinary terminal text instead of rejecting it.
	var envelope struct {
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &envelope) == nil && strings.TrimSpace(envelope.Text) != "" {
		text = envelope.Text
	}
	out = append(out, supervisor.Milestone{Kind: supervisor.MilestoneMeaningfulProgress, Progress: text, SourceType: "generic.output"})
	return out, nil
}
