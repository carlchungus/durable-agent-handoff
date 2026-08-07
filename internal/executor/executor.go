// Package executor performs runtime effects while Supervisor remains the only
// durable mutation authority.
package executor

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/activity"
	"github.com/carlchungus/durable-agent-handoff/internal/driver"
	"github.com/carlchungus/durable-agent-handoff/internal/runstate"
	"github.com/carlchungus/durable-agent-handoff/internal/supervisor"
)

type Executor struct {
	Store       *supervisor.Store
	OutputRoot  string
	Drivers     func(string) (driver.Driver, error)
	Environment []string
	TrustMode   driver.TrustMode
}

func (e *Executor) RunActivity(ctx context.Context, activityID supervisor.ActivityID) error {
	if e == nil || e.Store == nil {
		return errors.New("executor requires a Supervisor Store")
	}
	if e.Drivers == nil {
		e.Drivers = driver.Lookup
	}
	if err := ensurePrivateOutputRoot(e.OutputRoot); err != nil {
		return err
	}
	state, err := e.Store.Projection()
	if err != nil {
		return err
	}
	logical := state.Activities[activityID]
	if logical == nil {
		return os.ErrNotExist
	}
	workflow := state.Workflows[logical.WorkflowID]
	node := workflow.Nodes[logical.NodeID]
	session := state.Sessions[logical.SessionID]
	if node == nil || session == nil {
		return errors.New("Activity projection has broken Node or Session identity")
	}
	runtimeDriver, err := e.Drivers(node.Work.Runtime.Name)
	if err != nil {
		return err
	}
	ordinal := 1
	for _, attempt := range state.Attempts {
		if attempt.ActivityID == logical.ID && attempt.Ordinal >= ordinal {
			ordinal = attempt.Ordinal + 1
		}
	}
	keyPrefix := fmt.Sprintf("run/%s/%d/%d", logical.ID, logical.Generation, ordinal)
	fileStem := shortDigest(keyPrefix)
	stdoutPath := filepath.Join(e.OutputRoot, fileStem+".stdout.jsonl")
	stderrPath := filepath.Join(e.OutputRoot, fileStem+".stderr.log")
	resultPath := filepath.Join(e.OutputRoot, fileStem+".result.json")
	schemaPath := filepath.Join(e.OutputRoot, fileStem+".schema.json")
	trustMode := e.TrustMode
	if trustMode == "" {
		trustMode = driver.TrustWorkspace
	}
	launch, err := runtimeDriver.Build(driver.LaunchRequest{Runtime: node.Work.Runtime, Worktree: node.Work.Root, Prompt: logical.Prompt, Session: session.Native, SchemaPath: schemaPath, ResultPath: resultPath, TrustMode: trustMode})
	if err != nil {
		return err
	}
	argv := append([]string{launch.Executable}, launch.Args...)
	attempt, receipt, err := e.Store.PrepareAttempt(ctx, supervisor.PrepareAttemptInput{
		ActivityID: logical.ID, ExpectedGeneration: logical.Generation,
		CommandDigest:  runstate.CommandDigest(launch.Executable, launch.Args),
		Outputs:        supervisor.OutputIdentity{Stdout: "output_" + shortDigest(stdoutPath), Stderr: "output_" + shortDigest(stderrPath), Result: "output_" + shortDigest(resultPath)},
		IdempotencyKey: keyPrefix + "/prepare",
	})
	if err != nil {
		return err
	}
	if receipt.Existing {
		return errors.New("prepared Attempt already exists; explicit recovery is required")
	}
	if err = writeExclusive(schemaPath, []byte(resultSchema)); err != nil {
		return e.failStart(ctx, logical, attempt, keyPrefix, runtimeDriver, err)
	}
	stdout, err := os.OpenFile(stdoutPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return e.failStart(ctx, logical, attempt, keyPrefix, runtimeDriver, err)
	}
	stderr, err := os.OpenFile(stderrPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		_ = stdout.Close()
		return e.failStart(ctx, logical, attempt, keyPrefix, runtimeDriver, err)
	}
	var stdin []byte
	if launch.PromptOnStdin {
		stdin = []byte(logical.Prompt)
	}
	gated, err := activity.PrepareGatedCommand(argv, node.Work.Root, e.Environment, stdin)
	if err != nil {
		_ = stdout.Close()
		_ = stderr.Close()
		return e.failStart(ctx, logical, attempt, keyPrefix, runtimeDriver, err)
	}
	command := gated.Command
	command.Stdout, command.Stderr = stdout, stderr
	if err = command.Start(); err != nil {
		gated.Abort()
		_ = stdout.Close()
		_ = stderr.Close()
		return e.failStart(ctx, logical, attempt, keyPrefix, runtimeDriver, err)
	}
	_ = stdout.Close()
	_ = stderr.Close()
	token := waitForStartToken(command.Process.Pid, 2*time.Second)
	if token == "" {
		gated.Abort()
		_ = command.Process.Kill()
		_ = command.Wait()
		return e.failStart(ctx, logical, attempt, keyPrefix, runtimeDriver, errors.New("could not establish exact process start token"))
	}
	treeID, err := gated.BindProcessTree(command.Process.Pid, token)
	if err != nil {
		gated.Abort()
		_ = command.Process.Kill()
		_ = command.Wait()
		return e.failStart(ctx, logical, attempt, keyPrefix, runtimeDriver, err)
	}
	spawned := runtimeDriver.Spawned(supervisor.ProcessIdentity{PID: command.Process.Pid, StartToken: token, TreeID: treeID})
	if _, err = e.record(ctx, logical, attempt, keyPrefix+"/spawned", spawned); err != nil {
		gated.Abort()
		_ = command.Process.Kill()
		_ = command.Wait()
		return err
	}
	if err = gated.Release(); err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		return e.failAfterSpawn(ctx, logical, attempt, keyPrefix, runtimeDriver, err)
	}
	stopDecode := make(chan struct{})
	decodeErrors := make(chan error, 1)
	go func() {
		decodeErrors <- e.decodeFile(ctx, logical, attempt, keyPrefix, stdoutPath, runtimeDriver.NewDecoder(), stopDecode)
	}()
	waited := make(chan error, 1)
	go func() { waited <- command.Wait() }()
	var processErr error
	select {
	case processErr = <-waited:
		close(stopDecode)
		if decodeErr := <-decodeErrors; decodeErr != nil {
			processErr = errors.Join(processErr, decodeErr)
		}
	case decodeErr := <-decodeErrors:
		if decodeErr != nil {
			_ = command.Process.Kill()
			processErr = errors.Join(<-waited, decodeErr)
		} else {
			processErr = <-waited
		}
	case <-ctx.Done():
		_ = command.Process.Kill()
		processErr = errors.Join(<-waited, ctx.Err())
		close(stopDecode)
		if decodeErr := <-decodeErrors; decodeErr != nil {
			processErr = errors.Join(processErr, decodeErr)
		}
	}
	code := 0
	if command.ProcessState != nil {
		code = command.ProcessState.ExitCode()
	}
	// The gated runner's containment watchdog terminates its own process group
	// after the target finishes, so the runner itself is normally observed as
	// signaled. A committed typed Result proves the target completed its turn;
	// normalize only that exact contained-runner case.
	if code == -1 {
		if current, projectionErr := e.Store.Projection(); projectionErr == nil && activityHasResult(current, logical.ID) {
			code, processErr = 0, nil
		}
	}
	_, exitErr := e.record(context.Background(), logical, attempt, keyPrefix+"/exit", runtimeDriver.Exited(code, processErr))
	if processErr != nil {
		return errors.Join(processErr, exitErr)
	}
	return exitErr
}

func (e *Executor) decodeFile(ctx context.Context, logical *supervisor.Activity, attempt *supervisor.Attempt, keyPrefix, path string, decoder driver.Decoder, stop <-chan struct{}) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	reader := bufio.NewReaderSize(file, 64<<10)
	lineNumber := 0
	stopping := false
	for {
		line, readErr := reader.ReadBytes('\n')
		if len(line) > 0 {
			lineNumber++
			milestones, decodeErr := decoder.DecodeLine(line)
			if decodeErr != nil {
				return fmt.Errorf("decode runtime line %d: %w", lineNumber, decodeErr)
			}
			for index, milestone := range milestones {
				key := fmt.Sprintf("%s/event/%d/%d/%s", keyPrefix, lineNumber, index, milestone.Kind)
				if _, decodeErr = e.record(ctx, logical, attempt, key, milestone); decodeErr != nil {
					return decodeErr
				}
			}
		}
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return readErr
		}
		if len(line) > 0 {
			continue
		}
		if stopping {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-stop:
			stopping = true
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func (e *Executor) record(ctx context.Context, logical *supervisor.Activity, attempt *supervisor.Attempt, key string, milestone supervisor.Milestone) (supervisor.Receipt, error) {
	return e.Store.RecordMilestone(ctx, supervisor.RecordMilestoneInput{ActivityID: logical.ID, ExpectedGeneration: logical.Generation, AttemptID: attempt.ID, LeaseID: attempt.LeaseID, Milestone: milestone, IdempotencyKey: key})
}

func (e *Executor) failStart(ctx context.Context, logical *supervisor.Activity, attempt *supervisor.Attempt, keyPrefix string, runtimeDriver driver.Driver, failure error) error {
	_, recordErr := e.record(ctx, logical, attempt, keyPrefix+"/start-failed", runtimeDriver.StartFailed(failure))
	return errors.Join(failure, recordErr)
}

func (e *Executor) failAfterSpawn(ctx context.Context, logical *supervisor.Activity, attempt *supervisor.Attempt, keyPrefix string, runtimeDriver driver.Driver, failure error) error {
	_, recordErr := e.record(ctx, logical, attempt, keyPrefix+"/exit", runtimeDriver.Exited(127, failure))
	return errors.Join(failure, recordErr)
}

func ensurePrivateOutputRoot(path string) error {
	if strings.TrimSpace(path) == "" {
		return errors.New("executor output root is required")
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return errors.New("executor output root must be a private directory")
	}
	return nil
}

func writeExclusive(path string, value []byte) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err = file.Write(value); err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	return err
}

func waitForStartToken(pid int, timeout time.Duration) string {
	deadline := time.Now().Add(timeout)
	for {
		if token := runstate.ProcessStartToken(pid); token != "" {
			return token
		}
		if time.Now().After(deadline) {
			return ""
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func shortDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:12])
}

func activityHasResult(state *supervisor.State, activityID supervisor.ActivityID) bool {
	for _, result := range state.Results {
		if result.ActivityID == activityID {
			return true
		}
	}
	return false
}

const resultSchema = `{"type":"object","required":["status","summary"],"properties":{"status":{"enum":["completed","needs_human","blocked"]},"summary":{"type":"string"},"attestations":{"type":"array"}},"additionalProperties":true}`
