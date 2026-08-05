package runtime

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/carlchungus/durable-agent-handoff/internal/core"
)

type Command struct {
	Name          string
	Args          []string
	PromptOnStdin bool
}

func Build(spec core.RuntimeSpec, worktree, prompt, sessionID, schemaPath, outputPath string) (Command, error) {
	name := spec.Name
	sandbox := fallback(spec.Sandbox, "workspace-write")
	if sandbox != "workspace-write" && sandbox != "read-only" {
		return Command{}, fmt.Errorf("unsupported sandbox %q; use read-only or workspace-write", sandbox)
	}
	switch name {
	case "codex":
		exe := fallback(spec.Executable, "codex")
		args := []string{"-C", worktree, "-m", fallback(spec.Model, "gpt-5.6-luna"), "-c", fmt.Sprintf("model_reasoning_effort=%q", fallback(spec.Effort, "xhigh")), "-s", sandbox, "-a", "never"}
		args = append(args, disabledProjectMCPArgs(worktree)...)
		args = append(args, "exec", "--ignore-user-config", "--json")
		if schemaPath != "" {
			args = append(args, "--output-schema", schemaPath)
		}
		if outputPath != "" {
			args = append(args, "-o", outputPath)
		}
		if sessionID != "" {
			args = append(args, "resume", sessionID)
		}
		args = append(args, "-")
		return Command{Name: exe, Args: append(args, spec.Args...), PromptOnStdin: true}, nil
	case "claude":
		exe := fallback(spec.Executable, "claude")
		tools := "Bash,Read,Edit,Write,Glob,Grep"
		if sandbox == "read-only" {
			tools = "Read,Glob,Grep"
		}
		args := []string{"-p", prompt, "--output-format", "stream-json", "--verbose", "--safe-mode", "--strict-mcp-config", "--mcp-config", `{"mcpServers":{}}`, "--model", fallback(spec.Model, "sonnet"), "--effort", fallback(spec.Effort, "high"), "--permission-mode", "dontAsk", "--tools", tools}
		if sessionID != "" {
			args = append(args, "--resume", sessionID)
		}
		return Command{Name: exe, Args: append(args, spec.Args...)}, nil
	case "pi":
		if sandbox == "read-only" {
			return Command{}, errors.New("pi cannot enforce read-only execution; use an external OS sandbox or another runtime")
		}
		exe := fallback(spec.Executable, "pi")
		args := []string{"--mode", "json", "--model", fallback(spec.Model, "openrouter/deepseek/deepseek-v4-flash"), "--thinking", fallback(spec.Effort, "xhigh")}
		if sessionID != "" {
			args = append(args, "--session", sessionID)
		} else if outputPath != "" {
			args = append(args, "--session-dir", filepath.Join(filepath.Dir(outputPath), "session"))
		}
		args = append(args, prompt)
		return Command{Name: exe, Args: append(args, spec.Args...)}, nil
	case "ohmypi", "omp":
		if sandbox == "read-only" {
			return Command{}, errors.New("ohmypi cannot enforce read-only execution; use an external OS sandbox or another runtime")
		}
		exe := fallback(spec.Executable, "omp")
		args := []string{"--mode", "json", "--model", fallback(spec.Model, "vercel-ai-gateway/deepseek/deepseek-v4-flash"), "--thinking", fallback(spec.Effort, "xhigh"), "--no-pty", "--no-title", "--auto-approve"}
		if sessionID != "" {
			args = append(args, "--resume", sessionID)
		} else if outputPath != "" {
			args = append(args, "--session-dir", filepath.Join(filepath.Dir(outputPath), "session"))
		}
		args = append(args, prompt)
		return Command{Name: exe, Args: append(args, spec.Args...)}, nil
	case "exec":
		if sandbox == "read-only" {
			return Command{}, errors.New("exec cannot enforce read-only execution; wrap the executable in an external OS sandbox")
		}
		if spec.Executable == "" {
			return Command{}, errors.New("exec adapter requires executable")
		}
		return Command{Name: spec.Executable, Args: append(spec.Args, prompt)}, nil
	default:
		return Command{}, fmt.Errorf("unsupported runtime %q", name)
	}
}

// --ignore-user-config removes ambient user MCPs, but Codex intentionally still
// loads project config. Replace the whole project MCP table so stale or newer
// transport fields are not parsed before an `enabled=false` leaf can apply.
func disabledProjectMCPArgs(worktree string) []string {
	if _, err := os.Stat(filepath.Join(worktree, ".codex", "config.toml")); err != nil {
		return nil
	}
	return []string{"-c", "mcp_servers={}"}
}

func Available(spec core.RuntimeSpec) error {
	exe := spec.Executable
	if exe == "" {
		exe = spec.Name
		if exe == "ohmypi" {
			exe = "omp"
		}
	}
	if strings.TrimSpace(exe) == "" {
		return errors.New("runtime name is required")
	}
	_, err := exec.LookPath(exe)
	return err
}
func fallback(v, d string) string {
	if v != "" {
		return v
	}
	return d
}
