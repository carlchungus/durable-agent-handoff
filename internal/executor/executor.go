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
	"sync"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/activity"
	"github.com/carlchungus/durable-agent-handoff/internal/commanddigest"
	"github.com/carlchungus/durable-agent-handoff/internal/driver"
	"github.com/carlchungus/durable-agent-handoff/internal/privatepath"
	"github.com/carlchungus/durable-agent-handoff/internal/processidentity"
	"github.com/carlchungus/durable-agent-handoff/internal/supervisor"
)

type Executor struct {
	Store           *supervisor.Store
	OutputRoot      string
	Drivers         func(string) (driver.Driver, error)
	Environment     []string
	TrustMode       driver.TrustMode
	StartupDeadline time.Duration
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
	if session.ImportedUnresolved {
		return errors.New("imported Activity has no recoverable exact native Session")
	}
	runtimeSpec, err := selectRuntime(node.Work, state, logical.ID, session.Native.Runtime)
	if err != nil {
		return err
	}
	if runtimeSpec.Name != session.Native.Runtime {
		fallback := supervisor.FindFallbackActivity(state, logical.ID, runtimeSpec)
		if fallback == nil {
			fallback, _, err = e.Store.StartFallbackActivity(ctx, supervisor.StartFallbackActivityInput{ParentActivityID: logical.ID, Runtime: runtimeSpec, IdempotencyKey: "fallback/" + string(logical.ID) + "/" + shortDigest(runtimeKey(runtimeSpec))})
			if err != nil {
				return err
			}
		}
		logical = fallback
		state, err = e.Store.Projection()
		if err != nil {
			return err
		}
		workflow = state.Workflows[logical.WorkflowID]
		node = workflow.Nodes[logical.NodeID]
		session = state.Sessions[logical.SessionID]
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
	outputs := supervisor.OutputIdentity{Stdout: "output_" + shortDigest(stdoutPath), Stderr: "output_" + shortDigest(stderrPath), Result: "output_" + shortDigest(resultPath)}
	preparePrelaunch := func(failure error) error {
		attempt, receipt, prepareErr := e.Store.PrepareAttempt(ctx, supervisor.PrepareAttemptInput{
			ActivityID: logical.ID, ExpectedGeneration: logical.Generation, Runtime: runtimeSpec,
			CommandDigest: commanddigest.CommandDigest("adapter", []string{runtimeKey(runtimeSpec)}), Outputs: outputs,
			IdempotencyKey: keyPrefix + "/prepare-prelaunch",
		})
		if prepareErr != nil {
			return errors.Join(failure, prepareErr)
		}
		if receipt.Existing {
			return errors.Join(failure, errors.New("prelaunch Attempt already exists; explicit recovery is required"))
		}
		return e.failPrelaunch(ctx, logical, attempt, keyPrefix, failure)
	}
	runtimeDriver, err := e.Drivers(runtimeSpec.Name)
	if err != nil {
		return preparePrelaunch(err)
	}
	launch, err := runtimeDriver.Build(driver.LaunchRequest{Runtime: runtimeSpec, Worktree: node.Work.Root, Prompt: logical.Prompt, Session: session.Native, SchemaPath: schemaPath, ResultPath: resultPath, TrustMode: trustMode})
	if err != nil {
		return preparePrelaunch(err)
	}
	argv := append([]string{launch.Executable}, launch.Args...)
	attempt, receipt, err := e.Store.PrepareAttempt(ctx, supervisor.PrepareAttemptInput{
		ActivityID: logical.ID, ExpectedGeneration: logical.Generation,
		Runtime:        runtimeSpec,
		CommandDigest:  commanddigest.CommandDigest(launch.Executable, launch.Args),
		Outputs:        outputs,
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
		prompt := launch.Prompt
		if prompt == "" {
			// Test and third-party drivers predating the transient launch prompt
			// field retain the safe stdin-only behavior.
			prompt = logical.Prompt
		}
		stdin = []byte(prompt)
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
		return e.failAfterSpawn(ctx, logical, attempt, keyPrefix, runtimeDriver, err)
	}
	if err = gated.Release(); err != nil {
		_ = gated.CloseContainment()
		_ = command.Process.Kill()
		_ = command.Wait()
		return e.failAfterSpawn(ctx, logical, attempt, keyPrefix, runtimeDriver, err)
	}
	defer gated.CloseContainment()
	stopDecode := make(chan struct{})
	decodeErrors := make(chan error, 1)
	go func() {
		decodeErrors <- e.decodeFile(ctx, logical, attempt, keyPrefix, stdoutPath, runtimeDriver.NewDecoder(), stopDecode)
	}()
	waited := make(chan error, 1)
	go func() { waited <- command.Wait() }()
	closeDecoder := sync.Once{}
	stopDecoder := func() { closeDecoder.Do(func() { close(stopDecode) }) }
	startupDeadline := e.StartupDeadline
	if startupDeadline <= 0 {
		startupDeadline = 30 * time.Second
	}
	startupAt := time.Now().Add(startupDeadline)
	poll := time.NewTicker(50 * time.Millisecond)
	defer poll.Stop()
	stopping := false
	startupFailureRecorded := false
	var processErr error

waitLoop:
	for {
		select {
		case processErr = <-waited:
			stopDecoder()
			if decodeErr := <-decodeErrors; decodeErr != nil && !(stopping && errors.Is(decodeErr, supervisor.ErrFenced)) {
				processErr = errors.Join(processErr, decodeErr)
			}
			break waitLoop
		case decodeErr := <-decodeErrors:
			if decodeErr != nil {
				_ = gated.Stop()
				stopping = true
				processErr = errors.Join(<-waited, decodeErr)
				break waitLoop
			}
			processErr = <-waited
			break waitLoop
		case <-ctx.Done():
			_ = gated.Stop()
			stopping = true
			processErr = errors.Join(<-waited, ctx.Err())
			stopDecoder()
			if decodeErr := <-decodeErrors; decodeErr != nil && !errors.Is(decodeErr, supervisor.ErrFenced) {
				processErr = errors.Join(processErr, decodeErr)
			}
			break waitLoop
		case <-poll.C:
			control, turnStarted, observeErr := pendingControl(e.Store, logical.ID, attempt.ID)
			if observeErr != nil {
				_ = gated.Stop()
				stopping = true
				processErr = errors.Join(<-waited, observeErr)
				stopDecoder()
				if decodeErr := <-decodeErrors; decodeErr != nil && !errors.Is(decodeErr, supervisor.ErrFenced) {
					processErr = errors.Join(processErr, decodeErr)
				}
				break waitLoop
			}
			if control != nil && !stopping {
				if err := gated.Stop(); err != nil {
					processErr = errors.Join(<-waited, err)
					stopDecoder()
					if decodeErr := <-decodeErrors; decodeErr != nil && !errors.Is(decodeErr, supervisor.ErrFenced) {
						processErr = errors.Join(processErr, decodeErr)
					}
					break waitLoop
				}
				stopping = true
				if control.AppliedAt.IsZero() {
					if _, _, applyErr := e.Store.ApplyControl(context.Background(), supervisor.ApplyControlInput{ControlID: control.ID, ActivityID: logical.ID, ExpectedGeneration: logical.Generation, AttemptID: attempt.ID, IdempotencyKey: keyPrefix + "/control-applied/" + string(control.ID)}); applyErr != nil && !errors.Is(applyErr, supervisor.ErrFenced) {
						processErr = errors.Join(<-waited, applyErr)
						stopDecoder()
						if decodeErr := <-decodeErrors; decodeErr != nil && !errors.Is(decodeErr, supervisor.ErrFenced) {
							processErr = errors.Join(processErr, decodeErr)
						}
						break waitLoop
					}
				}
			}
			if !stopping && !turnStarted && !startupFailureRecorded && time.Now().After(startupAt) {
				control, _, controlErr := e.Store.RequestControl(context.Background(), supervisor.RequestControlInput{ActivityID: logical.ID, ExpectedGeneration: logical.Generation, ExpectedAttemptID: attempt.ID, Kind: "stop", Actor: "supervisor:startup-deadline", IdempotencyKey: keyPrefix + "/startup-control"})
				if controlErr != nil && !errors.Is(controlErr, supervisor.ErrFenced) {
					processErr = errors.Join(<-waited, controlErr)
					stopDecoder()
					if decodeErr := <-decodeErrors; decodeErr != nil && !errors.Is(decodeErr, supervisor.ErrFenced) {
						processErr = errors.Join(processErr, decodeErr)
					}
					break waitLoop
				}
				if control != nil && control.Accepted {
					if _, recordErr := e.record(context.Background(), logical, attempt, keyPrefix+"/startup-failed", runtimeDriver.StartFailed(errors.New("pre-turn startup deadline exceeded"))); recordErr != nil && !errors.Is(recordErr, supervisor.ErrFenced) {
						_ = gated.Stop()
						processErr = errors.Join(<-waited, recordErr)
						stopDecoder()
						if decodeErr := <-decodeErrors; decodeErr != nil && !errors.Is(decodeErr, supervisor.ErrFenced) {
							processErr = errors.Join(processErr, decodeErr)
						}
						break waitLoop
					}
					if err := gated.Stop(); err != nil {
						processErr = errors.Join(<-waited, err)
						stopDecoder()
						if decodeErr := <-decodeErrors; decodeErr != nil && !errors.Is(decodeErr, supervisor.ErrFenced) {
							processErr = errors.Join(processErr, decodeErr)
						}
						break waitLoop
					}
					stopping = true
					_, _, _ = e.Store.ApplyControl(context.Background(), supervisor.ApplyControlInput{ControlID: control.ID, ActivityID: logical.ID, ExpectedGeneration: logical.Generation, AttemptID: attempt.ID, IdempotencyKey: keyPrefix + "/startup-control-applied"})
				}
				startupFailureRecorded = true
			}
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
		} else if current, projectionErr := e.Store.Projection(); projectionErr == nil && activityHasProviderUnavailable(current, logical.ID) {
			// The containment watchdog exits with a signal after a typed provider
			// failure. The durable provider_unavailable milestone is the authority
			// for routing, so do not turn that expected fallback boundary into an
			// executor crash.
			code, processErr = 0, nil
		}
	}
	_, exitErr := e.record(context.Background(), logical, attempt, keyPrefix+"/exit", runtimeDriver.Exited(code, processErr))
	e.settlePause(logical, keyPrefix)
	if processErr != nil {
		return errors.Join(processErr, exitErr)
	}
	return exitErr
}

func pendingControl(store *supervisor.Store, activityID supervisor.ActivityID, attemptID supervisor.AttemptID) (*supervisor.Control, bool, error) {
	state, err := store.Projection()
	if err != nil {
		return nil, false, err
	}
	attempt := state.Attempts[attemptID]
	activity := state.Activities[activityID]
	if attempt == nil || activity == nil || attempt.ActivityID != activity.ID || attempt.ActivityGeneration != activity.Generation {
		return nil, false, supervisor.ErrFenced
	}
	turnStarted := false
	for _, milestone := range attempt.Milestones {
		if milestone.Kind == supervisor.MilestoneTurnStarted {
			turnStarted = true
		}
	}
	return supervisor.AcceptedControlForAttempt(state, activity, attempt), turnStarted, nil
}

func selectRuntime(work supervisor.WorkSpec, state *supervisor.State, activityID supervisor.ActivityID, sessionRuntime string) (supervisor.RuntimeSpec, error) {
	candidates := append([]supervisor.RuntimeSpec{work.Runtime}, work.Fallbacks...)
	usedUnavailable := make(map[string]bool)
	for _, attempt := range state.Attempts {
		if attempt.ActivityID != activityID {
			continue
		}
		for _, milestone := range attempt.Milestones {
			if milestone.Kind == supervisor.MilestoneProviderUnavailable {
				usedUnavailable[runtimeKey(attempt.Runtime)] = true
				break
			}
		}
	}
	start := 0
	for index, candidate := range candidates {
		if candidate.Name == sessionRuntime {
			start = index
			break
		}
	}
	for _, candidate := range candidates[start:] {
		if !usedUnavailable[runtimeKey(candidate)] {
			return candidate, nil
		}
	}
	return supervisor.RuntimeSpec{}, errors.New("all runtime fallback candidates are provider-unavailable")
}

func runtimeKey(runtime supervisor.RuntimeSpec) string {
	return runtime.Name + "\x00" + runtime.Executable + "\x00" + runtime.Model + "\x00" + runtime.Effort + "\x00" + string(runtime.Sandbox)
}

func (e *Executor) settlePause(logical *supervisor.Activity, keyPrefix string) {
	state, err := e.Store.Projection()
	if err != nil || state.Pauses[logical.WorkflowID] == nil {
		return
	}
	_, _, _ = e.Store.SettlePause(context.Background(), supervisor.SettlePauseInput{WorkflowID: logical.WorkflowID, IdempotencyKey: keyPrefix + "/pause-settle"})
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
	_, exitErr := e.record(context.Background(), logical, attempt, keyPrefix+"/exit", runtimeDriver.Exited(127, failure))
	return errors.Join(failure, recordErr, exitErr)
}

func (e *Executor) failPrelaunch(ctx context.Context, logical *supervisor.Activity, attempt *supervisor.Attempt, keyPrefix string, failure error) error {
	failed := supervisor.Milestone{Kind: supervisor.MilestoneAdapterStartFailed, Failure: failure.Error(), SourceType: "adapter"}
	exited := supervisor.Milestone{Kind: supervisor.MilestoneExit, Exit: &supervisor.Exit{Code: 127, Error: failure.Error()}, SourceType: "adapter"}
	_, recordErr := e.record(ctx, logical, attempt, keyPrefix+"/prelaunch-failed", failed)
	_, exitErr := e.record(context.Background(), logical, attempt, keyPrefix+"/prelaunch-exit", exited)
	return errors.Join(failure, recordErr, exitErr)
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
	if err := privatepath.ValidateDirectory(path); err != nil {
		return fmt.Errorf("executor output root must be an OS-private directory: %w", err)
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
		if token := processidentity.ProcessStartToken(pid); token != "" {
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

func activityHasProviderUnavailable(state *supervisor.State, activityID supervisor.ActivityID) bool {
	for _, attempt := range state.Attempts {
		if attempt.ActivityID != activityID {
			continue
		}
		for _, milestone := range attempt.Milestones {
			if milestone.Kind == supervisor.MilestoneProviderUnavailable {
				return true
			}
		}
	}
	return false
}

const resultSchema = `{"type":"object","required":["status","summary"],"properties":{"status":{"enum":["completed","continue","needs_human","blocked"]},"summary":{"type":"string"},"blocker_kind":{"type":"string"},"question":{"type":"string"}},"additionalProperties":false}`
