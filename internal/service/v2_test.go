package service

import (
	"os"
	"strings"
	"testing"

	"github.com/carlchungus/durable-agent-handoff/internal/driver"
)

func TestInstallV2UnitContainsNoPromptOrEnvironmentValues(t *testing.T) {
	home := t.TempDir()
	path, err := installV2For("linux", home, "/opt/handoff", "/state", "/private/env.json", driver.TrustFull)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if !strings.Contains(text, "Supervisor v2") || !strings.Contains(text, "--trust-mode full") || !strings.Contains(text, "--environment-json /private/env.json") || strings.Contains(text, "prompt") {
		t.Fatalf("unit=%s", text)
	}
}
