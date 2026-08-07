package evaluator

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/supervisor"
)

type transcriptCase struct {
	Name              string                  `json:"name"`
	Source            string                  `json:"source"`
	Goal              string                  `json:"goal"`
	Prompt            string                  `json:"prompt"`
	Turn              supervisor.WorkerResult `json:"turn"`
	SupervisorContext string                  `json:"supervisor_context,omitempty"`
	ExpectedOutcome   string                  `json:"expected_outcome"`
}

func TestRealTranscriptFixturesAreWellFormed(t *testing.T) {
	for _, fixture := range loadTranscriptCases(t) {
		if fixture.Name == "" || fixture.Source == "" || fixture.Goal == "" || fixture.Prompt == "" || fixture.Turn.Status == "" || fixture.Turn.Summary == "" || fixture.ExpectedOutcome == "" {
			t.Fatalf("incomplete transcript fixture: %+v", fixture)
		}
	}
}

func TestLiveDeepSeekExtractionModesAgainstRealTranscripts(t *testing.T) {
	if os.Getenv("HANDOFF_LIVE_EVALUATOR") != "1" {
		t.Skip("set HANDOFF_LIVE_EVALUATOR=1 to run paid OpenRouter compatibility probe")
	}
	apiKey := os.Getenv("OPENROUTER_API_KEY")
	if apiKey == "" {
		t.Fatal("OPENROUTER_API_KEY is required for live evaluator probe")
	}
	for _, mode := range []Mode{ModeStructured, ModeToolCall} {
		t.Run(string(mode), func(t *testing.T) {
			client := OpenRouter{APIKey: apiKey, Mode: mode}
			for _, fixture := range loadTranscriptCases(t) {
				for run := 1; run <= 5; run++ {
					ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
					decision, err := client.Evaluate(ctx, Request{Model: DefaultModel, Goal: fixture.Goal, Prompt: fixture.Prompt, SupervisorContext: fixture.SupervisorContext, Turn: fixture.Turn})
					cancel()
					if err != nil {
						t.Errorf("%s run %d extraction failed: %v", fixture.Name, run, err)
						continue
					}
					if decision.Outcome != fixture.ExpectedOutcome {
						t.Errorf("%s run %d outcome=%s want=%s reason=%q", fixture.Name, run, decision.Outcome, fixture.ExpectedOutcome, decision.Reason)
					}
				}
			}
		})
	}
}

func loadTranscriptCases(t *testing.T) []transcriptCase {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "real_transcript_turns.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []transcriptCase
	if err := json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	if len(fixtures) < 4 {
		t.Fatalf("real transcript corpus unexpectedly small: %d", len(fixtures))
	}
	return fixtures
}
