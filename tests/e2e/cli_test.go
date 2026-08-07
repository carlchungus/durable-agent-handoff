//go:build e2e

package e2e_test

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

var (
	repositoryRoot string
	handoffPath    string
	fakeCodexPath  string
)

func TestMain(m *testing.M) {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		fmt.Fprintln(os.Stderr, "locate e2e test source")
		os.Exit(1)
	}
	repositoryRoot = filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
	buildRoot, err := os.MkdirTemp("", "handoff-e2e-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer os.RemoveAll(buildRoot)
	handoffPath = filepath.Join(buildRoot, executableName("handoff"))
	fakeCodexPath = filepath.Join(buildRoot, executableName("fake-codex"))
	if err = build(repositoryRoot, handoffPath, "./cmd/handoff"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err = build(repositoryRoot, fakeCodexPath, "./tests/e2e/testdata/fake-codex"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func TestPublicCLICompletesOneActivityThroughRuntimeProcess(t *testing.T) {
	state := privateDir(t)
	worktree := t.TempDir()
	fakeBin := t.TempDir()
	if err := os.Symlink(fakeCodexPath, filepath.Join(fakeBin, executableName("codex"))); err != nil {
		t.Fatal(err)
	}
	capturePath := filepath.Join(t.TempDir(), "codex-invocation.json")
	environment := cleanEnvironment(map[string]string{
		"PATH":                      fakeBin + string(os.PathListSeparator) + os.Getenv("PATH"),
		"HANDOFF_E2E_CODEX_CAPTURE": capturePath,
	})
	prompt := "ship the black-box fixture without leaking this prompt"

	var started struct {
		Execution struct {
			ID         string `json:"id"`
			WorkflowID string `json:"workflow_id"`
		} `json:"execution"`
	}
	decodeJSON(t, runCLI(t, environment, prompt, "start", "--state", state, "--root", worktree, "--runtime", "codex", "--file", "-", "--sandbox", "workspace-write", "--authorized-by", "human:e2e", "--idempotency-key", "black-box-start", "--json"), &started)
	if started.Execution.ID == "" || started.Execution.WorkflowID == "" {
		t.Fatalf("start response omitted identities: %+v", started)
	}

	runCLI(t, environment, "", "run", started.Execution.WorkflowID, "--state", state, "--once", "--startup-timeout", "5s")
	statusJSON := runCLI(t, environment, "", "status", started.Execution.ID, "--state", state, "--json")
	if strings.Contains(statusJSON, prompt) {
		t.Fatalf("public status leaked private prompt:\n%s", statusJSON)
	}
	var status struct {
		Activities []struct {
			Status string `json:"status"`
		} `json:"activities"`
		Attempts []struct {
			Health       string `json:"health"`
			ResultStatus string `json:"result_status"`
		} `json:"attempts"`
	}
	decodeJSON(t, statusJSON, &status)
	if len(status.Activities) != 1 || status.Activities[0].Status != "completed" {
		t.Fatalf("completed Activity not visible through status: %+v", status)
	}
	if len(status.Attempts) != 1 || status.Attempts[0].Health != "exited" || status.Attempts[0].ResultStatus != "completed" {
		t.Fatalf("terminal Attempt not visible through status: %+v", status.Attempts)
	}

	var activities []struct {
		Status string `json:"status"`
	}
	decodeJSON(t, runCLI(t, environment, "", "activity", "list", "--state", state, "--json"), &activities)
	if len(activities) != 1 || activities[0].Status != "completed" {
		t.Fatalf("activity list disagrees with status: %+v", activities)
	}

	data, err := os.ReadFile(capturePath)
	if err != nil {
		t.Fatal(err)
	}
	var capture struct {
		Args  []string `json:"args"`
		Stdin string   `json:"stdin"`
	}
	decodeJSON(t, string(data), &capture)
	if capture.Stdin != prompt {
		t.Fatalf("runtime stdin=%q want exact prompt", capture.Stdin)
	}
	for _, argument := range capture.Args {
		if strings.Contains(argument, prompt) {
			t.Fatalf("prompt leaked into runtime argv: %q", capture.Args)
		}
	}
}

func TestPublicCLIRejectsDivergentIdempotencyWithoutMutation(t *testing.T) {
	state := privateDir(t)
	worktree := t.TempDir()
	environment := cleanEnvironment(nil)
	arguments := []string{"start", "--state", state, "--root", worktree, "--runtime", "codex", "--file", "-", "--sandbox", "read-only", "--authorized-by", "human:e2e", "--idempotency-key", "black-box-idempotency", "--json"}
	var first, second struct {
		Execution struct {
			ID string `json:"id"`
		} `json:"execution"`
		Receipt struct {
			Existing bool `json:"existing"`
		} `json:"receipt"`
	}
	decodeJSON(t, runCLI(t, environment, "first private prompt", arguments...), &first)
	decodeJSON(t, runCLI(t, environment, "first private prompt", arguments...), &second)
	if first.Execution.ID == "" || first.Execution.ID != second.Execution.ID || first.Receipt.Existing || !second.Receipt.Existing {
		t.Fatalf("unsafe retry identity: first=%+v second=%+v", first, second)
	}

	_, stderr, err := executeCLI(environment, "different private prompt", arguments...)
	if err == nil {
		t.Fatal("divergent idempotency reuse succeeded")
	}
	if !strings.Contains(stderr, "idempotency conflict") || strings.Contains(stderr, "different private prompt") {
		t.Fatalf("unsafe divergent retry diagnostics:\n%s", stderr)
	}

	var events []json.RawMessage
	for _, line := range strings.Split(strings.TrimSpace(runCLI(t, environment, "", "events", "--state", state)), "\n") {
		if line != "" {
			events = append(events, json.RawMessage(line))
		}
	}
	if len(events) != 1 {
		t.Fatalf("rejected retry mutated journal: entries=%d", len(events))
	}
}

func build(root, output, target string) error {
	command := exec.Command("go", "build", "-o", output, target)
	command.Dir = root
	if combined, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("build %s: %w\n%s", target, err, combined)
	}
	return nil
}

func runCLI(t *testing.T, environment []string, stdin string, args ...string) string {
	t.Helper()
	stdout, stderr, err := executeCLI(environment, stdin, args...)
	if err != nil {
		t.Fatalf("handoff %q failed: %v\nstdout:\n%s\nstderr:\n%s", args, err, stdout, stderr)
	}
	t.Logf("handoff %q\nstdout:\n%s\nstderr:\n%s", args, stdout, stderr)
	return stdout
}

func executeCLI(environment []string, stdin string, args ...string) (string, string, error) {
	command := exec.Command(handoffPath, args...)
	command.Env = environment
	command.Stdin = strings.NewReader(stdin)
	var stdout, stderr strings.Builder
	command.Stdout, command.Stderr = &stdout, &stderr
	err := command.Run()
	return stdout.String(), stderr.String(), err
}

func cleanEnvironment(overrides map[string]string) []string {
	values := map[string]string{
		"HOME": os.Getenv("HOME"),
		"PATH": os.Getenv("PATH"),
	}
	for key, value := range overrides {
		values[key] = value
	}
	environment := make([]string, 0, len(values))
	for key, value := range values {
		environment = append(environment, key+"="+value)
	}
	return environment
}

func privateDir(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	return directory
}

func decodeJSON(t *testing.T, value string, target any) {
	t.Helper()
	decoder := json.NewDecoder(strings.NewReader(value))
	if err := decoder.Decode(target); err != nil {
		t.Fatalf("decode JSON: %v\nvalue:\n%s", err, value)
	}
}

func executableName(name string) string {
	if runtime.GOOS == "windows" {
		return name + ".exe"
	}
	return name
}
