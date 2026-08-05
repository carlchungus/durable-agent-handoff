package preferences

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/core"
)

func TestLadderSkipsCoolingCandidateAndPersists(t *testing.T) {
	m := Open(t.TempDir())
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	m.now = func() time.Time { return now }
	first := core.RuntimeSpec{Name: "claude", Model: "opus", Effort: "high"}
	second := core.RuntimeSpec{Name: "codex", Model: "gpt-5.6-sol", Effort: "xhigh"}
	if err := m.Set("planner", []core.RuntimeSpec{first, second}); err != nil {
		t.Fatal(err)
	}
	got, index, err := m.Resolve("planner", core.RuntimeSpec{})
	if err != nil || got.Model != "opus" || index != 0 {
		t.Fatalf("got=%#v index=%d err=%v", got, index, err)
	}
	if err = m.Record(first, "usage_limit", "five-hour limit reached"); err != nil {
		t.Fatal(err)
	}
	got, index, err = m.Resolve("planner", core.RuntimeSpec{})
	if err != nil || got.Model != "gpt-5.6-sol" || index != 1 {
		t.Fatalf("got=%#v index=%d err=%v", got, index, err)
	}
	health, err := Open(m.dir).Health()
	if err != nil || len(health) != 1 || health[0].Class != "usage_limit" {
		t.Fatalf("health=%#v err=%v", health, err)
	}
	now = now.Add(61 * time.Minute)
	got, index, err = m.Resolve("planner", core.RuntimeSpec{})
	if err != nil || index != 0 {
		t.Fatalf("cooldown did not expire: %#v %d %v", got, index, err)
	}
}

func TestAllCandidatesCoolingReturnsWakeTime(t *testing.T) {
	m := Open(t.TempDir())
	m.now = func() time.Time { return time.Unix(1000, 0).UTC() }
	a := core.RuntimeSpec{Name: "claude", Model: "opus"}
	b := core.RuntimeSpec{Name: "codex", Model: "sol"}
	_ = m.Set("planner", []core.RuntimeSpec{a, b})
	_ = m.Record(a, "rate_limit", "429")
	_ = m.Record(b, "usage_limit", "quota exceeded")
	_, _, err := m.Resolve("planner", core.RuntimeSpec{})
	var cooldown *CooldownError
	if !errors.As(err, &cooldown) || cooldown.Until.IsZero() {
		t.Fatalf("err=%v", err)
	}
}

func TestFailureClassificationIsNarrow(t *testing.T) {
	cases := map[string]string{"You've hit your usage limit": "usage_limit", "429 Too Many Requests": "rate_limit", "tests failed": "runtime_error", "invalid API key": "runtime_error"}
	for text, want := range cases {
		if got := ClassifyFailure(text); got != want {
			t.Errorf("%q => %s, want %s", text, got, want)
		}
	}
}

func TestSetRejectsIncompleteCandidate(t *testing.T) {
	err := Open(t.TempDir()).Set("planner", []core.RuntimeSpec{{Name: "codex"}})
	if err == nil || !strings.Contains(err.Error(), "requires runtime and model") {
		t.Fatalf("err=%v", err)
	}
}
