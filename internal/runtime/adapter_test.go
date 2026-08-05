package runtime

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/carlchungus/durable-agent-handoff/internal/core"
)

func TestCodexDefaultsAndExactResume(t *testing.T) {
	c, err := Build(core.RuntimeSpec{Name: "codex"}, "/repo", "prompt", "session-123", "/schema", "/out")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"-C", "/repo", "-m", "gpt-5.6-luna", "-c", `model_reasoning_effort="xhigh"`, "-s", "workspace-write", "-a", "never", "exec", "--ignore-user-config", "--json", "--output-schema", "/schema", "-o", "/out", "resume", "session-123", "-"}
	if c.Name != "codex" || !c.PromptOnStdin || !reflect.DeepEqual(c.Args, want) {
		t.Fatalf("command=%#v", c)
	}
}

func TestCodexDisablesProjectMCPServers(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".codex"), 0o700); err != nil {
		t.Fatal(err)
	}
	config := "[mcp_servers.neon]\nurl = \"https://mcp.neon.tech/mcp\"\n\n[mcp_servers.repo_tools]\ncommand = \"tool\"\n"
	if err := os.WriteFile(filepath.Join(root, ".codex", "config.toml"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Build(core.RuntimeSpec{Name: "codex"}, root, "prompt", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(c.Args, " ")
	for _, expected := range []string{"--ignore-user-config", "mcp_servers.neon.enabled=false", "mcp_servers.repo_tools.enabled=false"} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("missing %q in %s", expected, joined)
		}
	}
}

func TestClaudeIsFailClosed(t *testing.T) {
	c, err := Build(core.RuntimeSpec{Name: "claude"}, "/repo", "prompt", "abc", "", "")
	if err != nil {
		t.Fatal(err)
	}
	joined := ""
	for _, a := range c.Args {
		joined += " " + a
	}
	for _, required := range []string{"--safe-mode", "--strict-mcp-config", "--permission-mode dontAsk", "--resume abc"} {
		if !contains(joined, required) {
			t.Errorf("missing %q in %s", required, joined)
		}
	}
}
func TestPiAndOhMyPiResumeExactSession(t *testing.T) {
	for _, tc := range []struct{ name, flag string }{{"pi", "--session"}, {"ohmypi", "--resume"}} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := Build(core.RuntimeSpec{Name: tc.name}, "/repo", "prompt", "/state/exact.jsonl", "", "/state/out")
			if err != nil {
				t.Fatal(err)
			}
			joined := " " + strings.Join(c.Args, " ")
			if !strings.Contains(joined, " "+tc.flag+" /state/exact.jsonl") {
				t.Fatalf("args=%v", c.Args)
			}
		})
	}
}
func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
