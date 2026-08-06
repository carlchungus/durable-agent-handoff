package activity

import (
	"encoding/json"
	"errors"
	"fmt"
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
	Argv       []string          `json:"argv"`
	Stdin      []byte            `json:"stdin,omitempty"`
	Completion *runnerCompletion `json:"completion,omitempty"`
}

type runnerCompletion struct {
	Root       string          `json:"root"`
	ActivityID string          `json:"activity_id"`
	Generation uint64          `json:"generation"`
	Identity   AttemptIdentity `json:"identity"`
}

type watchdogRequest struct {
	Completion *runnerCompletion `json:"completion,omitempty"`
	Result     ExitResult        `json:"result"`
}

func (g *GatedCommand) CompleteActivity(root, activityID string, generation uint64, identity AttemptIdentity) {
	g.request.Completion = &runnerCompletion{Root: root, ActivityID: activityID, Generation: generation, Identity: identity}
}

// GatedCommand is a small per-attempt runner. The runner establishes the
// process-tree identity and waits on an inherited pipe. The target command is
// not executed until Release is called after MarkRunning is durable.
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
		_, _ = fmt.Fprintln(os.Stderr, "activity runner:", err)
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
		return 0, fmt.Errorf("start process-tree watchdog: %w", err)
	}
	command := exec.Command(request.Argv[0], request.Argv[1:]...)
	command.Env = withoutRunnerEnvironment(os.Environ())
	command.Stdin = strings.NewReader(string(request.Stdin))
	command.Stdout = stdout
	command.Stderr = stderr
	err = command.Run()
	code := 0
	if command.ProcessState != nil {
		code = command.ProcessState.ExitCode()
	}
	state := StateCompleted
	errorText := ""
	if err != nil {
		state = StateFailed
		errorText = err.Error()
	}
	if err := watchdog.complete(request.Completion, ExitResult{State: state, ExitCode: &code, Error: errorText}); err != nil {
		return code, fmt.Errorf("complete process-tree watchdog: %w", err)
	}
	return code, nil
}

// finishRunnerAttempt permits a recovered supervisor to advance ownership
// while the same exact runner continues. The runner retries with the current
// owner fence only when attempt ID, PID, and birth token still name itself.
func finishRunnerAttempt(store *Store, completion *runnerCompletion, result ExitResult, runnerPID int) error {
	for range 4 {
		current, err := store.Load(completion.ActivityID)
		if err != nil {
			return err
		}
		if terminal(current.State) {
			return nil
		}
		attempt, ok := currentAttempt(current)
		if !ok || attempt.ID != completion.Identity.ID || attempt.PID != runnerPID || attempt.PID != completion.Identity.PID || attempt.ProcessStartToken != completion.Identity.ProcessStartToken || attempt.ProcessTreeID != completion.Identity.ProcessTreeID || !processMatches(identityOf(attempt)) {
			return ErrFenced
		}
		if err = store.FinishAttempt(current.ID, current.Generation, identityOf(attempt), result); err == nil {
			return nil
		} else if !errors.Is(err, ErrFenced) {
			return err
		}
	}
	return ErrFenced
}

func withoutRunnerEnvironment(env []string) []string {
	runnerPrefix := runnerEnvironment + "="
	watchdogPrefix := watchdogEnvironment + "="
	filtered := make([]string, 0, len(env))
	for _, item := range env {
		if !strings.HasPrefix(item, runnerPrefix) && !strings.HasPrefix(item, watchdogPrefix) {
			filtered = append(filtered, item)
		}
	}
	return filtered
}
