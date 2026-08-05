package workflow

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestReplayStopsAtFirstUnfinishedCall(t *testing.T) {
	events := []Event{
		{Type: "agent.started", Call: &AgentCall{ID: "A", Sequence: 1, Fingerprint: "a"}},
		{Type: "agent.started", Call: &AgentCall{ID: "B", Sequence: 2, Fingerprint: "b"}},
		{Type: "agent.started", Call: &AgentCall{ID: "C", Sequence: 3, Fingerprint: "c"}},
		{Type: "agent.started", Call: &AgentCall{ID: "D", Sequence: 4, Fingerprint: "d"}},
		{Type: "agent.result", Result: &Completion{CallID: "A", Fingerprint: "a", Result: json.RawMessage(`"A"`)}},
		{Type: "agent.result", Result: &Completion{CallID: "C", Fingerprint: "c", Result: json.RawMessage(`"C"`)}},
		{Type: "agent.result", Result: &Completion{CallID: "D", Fingerprint: "d", Result: json.RawMessage(`"D"`)}},
	}
	r := NewReplayer(events)
	if _, cached := r.Resolve("a"); !cached {
		t.Fatal("A should replay from cache")
	}
	if _, cached := r.Resolve("b"); cached {
		t.Fatal("unfinished B must run live")
	}
	if _, cached := r.Resolve("c"); cached {
		t.Fatal("C completed after the frontier and must rerun")
	}
	if r.Frontier() != 1 {
		t.Fatalf("frontier=%d", r.Frontier())
	}
}

func TestReplayInvalidatesSuffixWhenCallChanges(t *testing.T) {
	events := []Event{
		{Type: "agent.started", Call: &AgentCall{ID: "A", Sequence: 1, Fingerprint: "old"}},
		{Type: "agent.result", Result: &Completion{CallID: "A", Fingerprint: "old", Null: true}},
	}
	r := NewReplayer(events)
	if _, cached := r.Resolve("new"); cached {
		t.Fatal("changed call fingerprint must not replay")
	}
}

func TestJournalSurvivesTruncatedTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.jsonl")
	j, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = j.Append(Event{Type: "agent.started", Call: &AgentCall{ID: "A", Sequence: 1, Fingerprint: "a"}}); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString(`{"sequence":2,"type":"agent.result"`)
	_ = f.Close()
	events, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Type != "agent.started" {
		t.Fatalf("events=%+v", events)
	}
	j, err = OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = j.Append(Event{Type: "agent.result", Result: &Completion{CallID: "A", Fingerprint: "a", Null: true}}); err != nil {
		t.Fatal(err)
	}
	events, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[1].Type != "agent.result" || events[1].Sequence != 2 {
		t.Fatalf("journal did not recover after truncated tail: %+v", events)
	}
}

func TestFingerprintPreservesStructuredOptions(t *testing.T) {
	a, _ := Fingerprint("prompt", map[string]any{"effort": "xhigh", "schema": map[string]any{"type": "string"}})
	b, _ := Fingerprint("prompt", map[string]any{"effort": "high", "schema": map[string]any{"type": "string"}})
	if a == b {
		t.Fatal("routing and schema options must affect cache identity")
	}
}
