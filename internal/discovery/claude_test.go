package discovery

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseClaudeSanitizesAndSkipsToolPayloads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session-123.jsonl")
	data := strings.Join([]string{
		`{"type":"ai-title","aiTitle":"Fix widget","cwd":"/repo","gitBranch":"feature/widget","timestamp":"2026-01-01T00:00:00Z"}`,
		`{"message":{"role":"user","content":"fix it token=supersecret"}}`,
		`{"message":{"role":"assistant","content":[{"type":"text","text":"I changed the parser"},{"type":"tool_use","input":{"password":"never-copy"}}]}}`,
		`{"type":"last-prompt","lastPrompt":"finish https://private.example/path"}`,
	}, "\n")
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := ParseClaude(path)
	if err != nil {
		t.Fatal(err)
	}
	if r.SessionID != "session-123" || r.CWD != "/repo" || r.Branch != "feature/widget" {
		t.Fatalf("record=%#v", r)
	}
	if strings.Contains(r.Handoff, "supersecret") || strings.Contains(r.Handoff, "never-copy") || strings.Contains(r.LastPrompt, "private.example") {
		t.Fatalf("redaction failed: %#v", r)
	}
	if !strings.Contains(r.Handoff, "changed the parser") {
		t.Fatalf("missing assistant context: %s", r.Handoff)
	}
}

func TestClassifySafetySensitiveWork(t *testing.T) {
	risk, reason := classify("deploy a production migration")
	if risk != "high" || reason == "" {
		t.Fatalf("risk=%s reason=%s", risk, reason)
	}
	risk, _ = classify("one time css fix")
	if risk != "low" {
		t.Fatalf("risk=%s", risk)
	}
}
