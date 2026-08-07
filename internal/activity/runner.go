// Package activity contains only the non-durable process containment helper
// used by the Supervisor v2 executor. Durable Activity state lives in the
// Supervisor journal; this package has no ledger or reconciliation authority.
package activity

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
)

const (
	runnerEnvironment   = "HANDOFF_INTERNAL_ACTIVITY_RUNNER"
	watchdogEnvironment = "HANDOFF_INTERNAL_ACTIVITY_WATCHDOG"
)

type runnerRequest struct {
	Argv  []string `json:"argv"`
	Stdin []byte   `json:"stdin,omitempty"`
}

type watchdogRequest struct{}

// GatedCommand establishes a dedicated process tree before releasing the
// target. It is a transport boundary only; terminal facts are recorded by the
// Supervisor executor after the target exits.
type GatedCommand struct {
	Command *exec.Cmd
	gate    io.WriteCloser
	request runnerRequest
	tree    *processTreeReservation
	once    sync.Once
}

func PrepareGatedCommand(argv []string, cwd string, env []string, stdin []byte) (*GatedCommand, error) {
	if len(argv) == 0 || strings.TrimSpace(argv[0]) == "" {
		return nil, errors.New("gated activity command is required")
	}
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	command := exec.Command(executable)
	command.Dir = cwd
	command.Env = append(append(os.Environ(), env...), runnerEnvironment+"=1")
	ConfigureBackgroundProcess(command)
	tree, err := prepareProcessTree(command)
	if err != nil {
		return nil, err
	}
	gate, err := command.StdinPipe()
	if err != nil {
		tree.close()
		return nil, err
	}
	return &GatedCommand{Command: command, gate: gate, tree: tree, request: runnerRequest{Argv: append([]string(nil), argv...), Stdin: append([]byte(nil), stdin...)}}, nil
}

func (g *GatedCommand) BindProcessTree(pid int, token string) (string, error) {
	if g == nil || g.tree == nil {
		return "", errors.New("gated activity process tree is not initialized")
	}
	return g.tree.bind(pid, token)
}

func (g *GatedCommand) Release() error {
	if g == nil || g.Command == nil || g.gate == nil {
		return errors.New("gated activity command is not initialized")
	}
	var result error
	g.once.Do(func() {
		if err := json.NewEncoder(g.gate).Encode(g.request); err != nil {
			result = err
		}
		if err := g.gate.Close(); result == nil {
			result = err
		}
		g.tree.close()
	})
	return result
}

func (g *GatedCommand) Abort() {
	if g == nil || g.gate == nil {
		return
	}
	g.once.Do(func() {
		_ = g.gate.Close()
		g.tree.close()
	})
}

// Stop kills the exact contained process tree identified by the command's
// durable process identity. It does not mutate Supervisor state.
func (g *GatedCommand) Stop() error {
	if g == nil || g.Command == nil || g.Command.Process == nil {
		return errors.New("gated activity command is not running")
	}
	err := g.tree.stop(g.Command.Process.Pid)
	if closeErr := g.tree.closeWithError(); err == nil {
		err = closeErr
	}
	return err
}

func init() {
	if os.Getenv(watchdogEnvironment) == "1" {
		runProcessTreeWatchdog(os.Stdin)
		os.Exit(0)
	}
	if os.Getenv(runnerEnvironment) != "1" {
		return
	}
	code, err := runGatedTarget(os.Stdin, os.Stdout, os.Stderr)
	if err != nil {
		_, _ = io.WriteString(os.Stderr, "activity runner: "+err.Error()+"\n")
		os.Exit(127)
	}
	os.Exit(code)
}

func runGatedTarget(input io.Reader, stdout, stderr io.Writer) (int, error) {
	decoder := json.NewDecoder(io.LimitReader(input, 64<<20))
	var request runnerRequest
	if err := decoder.Decode(&request); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, nil
		}
		return 0, err
	}
	if len(request.Argv) == 0 || strings.TrimSpace(request.Argv[0]) == "" {
		return 0, errors.New("runner request has no command")
	}
	watchdog, err := startProcessTreeWatchdog()
	if err != nil {
		return 0, err
	}
	command := exec.Command(request.Argv[0], request.Argv[1:]...)
	command.Env = withoutRunnerEnvironment(os.Environ())
	command.Stdin = strings.NewReader(string(request.Stdin))
	command.Stdout, command.Stderr = stdout, stderr
	err = command.Run()
	code := 0
	if command.ProcessState != nil {
		code = command.ProcessState.ExitCode()
	}
	if completeErr := watchdog.complete(); err == nil {
		err = completeErr
	}
	return code, err
}

func withoutRunnerEnvironment(env []string) []string {
	filtered := make([]string, 0, len(env))
	for _, item := range env {
		if !strings.HasPrefix(item, runnerEnvironment+"=") && !strings.HasPrefix(item, watchdogEnvironment+"=") {
			filtered = append(filtered, item)
		}
	}
	return filtered
}
