package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/driver"
	"github.com/carlchungus/durable-agent-handoff/internal/executor"
	"github.com/carlchungus/durable-agent-handoff/internal/service"
	v2tui "github.com/carlchungus/durable-agent-handoff/internal/tui"
	"github.com/carlchungus/durable-agent-handoff/supervisor"
)

type executionStartRequest struct {
	IdempotencyKey string             `json:"idempotency_key"`
	Goal           string             `json:"goal"`
	Prompt         string             `json:"prompt"`
	RemoteRoot     string             `json:"remote_root"`
	Runtime        string             `json:"runtime"`
	ResumeID       string             `json:"resume_id"`
	Model          string             `json:"model,omitempty"`
	Effort         string             `json:"effort,omitempty"`
	Sandbox        supervisor.Sandbox `json:"sandbox"`
	Role           string             `json:"role"`
}

type executionStartResponse struct {
	WorkflowID supervisor.WorkflowID `json:"workflow_id"`
	NodeID     supervisor.NodeID     `json:"node_id"`
}

type ordinaryStartResponse struct {
	Execution *supervisor.Execution `json:"execution"`
	Receipt   supervisor.Receipt    `json:"receipt"`
}

type pauseResponse struct {
	Pause   *supervisor.Pause  `json:"pause"`
	Receipt supervisor.Receipt `json:"receipt"`
}

type replyResponse struct {
	ActivityID ActivityIDResponse `json:"activity"`
	Receipt    supervisor.Receipt `json:"receipt"`
}

type ActivityIDResponse struct {
	ID         supervisor.ActivityID `json:"id"`
	SessionID  supervisor.SessionID  `json:"session_id"`
	Generation uint64                `json:"generation"`
}

func cmdV2Init(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	state := common(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	store, err := supervisor.Open(stateDir(*state), supervisor.Options{})
	if err != nil {
		return err
	}
	fmt.Fprintln(out, stateDir(*state))
	_ = store
	return nil
}

func cmdV2Start(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("start", flag.ContinueOnError)
	state := common(fs)
	file := fs.String("file", "", "strict JSON request file; use - for stdin")
	root := fs.String("root", ".", "canonical execution root")
	goal := fs.String("goal", "", "desired work title")
	prompt := fs.String("prompt", "", "exact execution prompt")
	runtimeName := fs.String("runtime", "", "codex, claude, or pi")
	nativeSession := fs.String("session", "", "exact native runtime Session identity")
	model := fs.String("model", "", "runtime model")
	effort := fs.String("effort", "", "runtime reasoning effort")
	sandbox := fs.String("sandbox", "workspace-write", "read-only or workspace-write")
	authorizedBy := fs.String("authorized-by", "", "human identity authorizing execution")
	key := fs.String("idempotency-key", "", "stable request identity")
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(reorderFlags(args, map[string]bool{"--state": true, "--file": true, "--root": true, "--goal": true, "--prompt": true, "--runtime": true, "--session": true, "--model": true, "--effort": true, "--sandbox": true, "--authorized-by": true, "--idempotency-key": true, "--json": false})); err != nil {
		return err
	}
	var input supervisor.StartExecutionInput
	if *file != "" {
		if !*jsonOut || fs.NArg() != 0 {
			return errors.New("start --file requires --json and no positional arguments")
		}
		var reader io.Reader = os.Stdin
		if *file != "-" {
			opened, err := os.Open(*file)
			if err != nil {
				return err
			}
			defer opened.Close()
			reader = opened
		}
		var request executionStartRequest
		decoder := json.NewDecoder(bufio.NewReader(reader))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&request); err != nil {
			return fmt.Errorf("decode execution start request: %w", err)
		}
		var extra any
		if err := decoder.Decode(&extra); err != io.EOF {
			if err == nil {
				return errors.New("execution start request must contain exactly one JSON object")
			}
			return fmt.Errorf("decode execution start request trailer: %w", err)
		}
		if strings.TrimSpace(request.ResumeID) == "" || strings.TrimSpace(request.Role) == "" || strings.TrimSpace(request.Goal) == "" {
			return errors.New("promotion request requires goal, resume_id, and role")
		}
		input = supervisor.StartExecutionInput{
			NativeSession: supervisor.NativeSessionIdentity{Runtime: request.Runtime, ID: request.ResumeID},
			Prompt:        request.Prompt, Goal: request.Goal,
			Runtime:   supervisor.RuntimeSpec{Name: request.Runtime, Model: request.Model, Effort: request.Effort, Sandbox: request.Sandbox},
			Root:      request.RemoteRoot,
			Authority: supervisor.AuthoritySpec{RequestedBy: request.Role, HumanAuthorized: true, Sandbox: request.Sandbox},
			Budget:    supervisor.DefaultBudget(), IdempotencyKey: request.IdempotencyKey,
		}
	} else {
		if fs.NArg() != 0 {
			return errors.New("start does not accept positional arguments")
		}
		input = supervisor.StartExecutionInput{
			NativeSession: supervisor.NativeSessionIdentity{Runtime: *runtimeName, ID: *nativeSession},
			Goal:          *goal, Prompt: *prompt,
			Runtime:   supervisor.RuntimeSpec{Name: *runtimeName, Model: *model, Effort: *effort, Sandbox: supervisor.Sandbox(*sandbox)},
			Root:      *root,
			Authority: supervisor.AuthoritySpec{RequestedBy: *authorizedBy, HumanAuthorized: *authorizedBy != "", Sandbox: supervisor.Sandbox(*sandbox)},
			Budget:    supervisor.DefaultBudget(), IdempotencyKey: *key,
		}
	}
	store, err := supervisor.Open(stateDir(*state), supervisor.Options{})
	if err != nil {
		return err
	}
	execution, receipt, err := store.StartExecution(context.Background(), input)
	if err != nil {
		return err
	}
	if *jsonOut {
		if *file != "" {
			return writeJSON(out, executionStartResponse{WorkflowID: execution.WorkflowID, NodeID: execution.RootNodeID})
		}
		return writeJSON(out, ordinaryStartResponse{Execution: execution, Receipt: receipt})
	}
	fmt.Fprintf(out, "execution=%s workflow=%s session=%s sequence=%d existing=%t\n", execution.ID, execution.WorkflowID, execution.SessionID, receipt.Sequence, receipt.Existing)
	return nil
}

func openV2(state string) (*supervisor.Store, error) {
	return supervisor.Open(stateDir(state), supervisor.Options{})
}

func cmdV2Status(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	state := common(fs)
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(reorderFlags(args, map[string]bool{"--state": true, "--json": false})); err != nil {
		return err
	}
	if fs.NArg() == 0 {
		return cmdV2List(args, out)
	}
	if fs.NArg() != 1 {
		return errors.New("status accepts one Execution ID")
	}
	store, err := openV2(*state)
	if err != nil {
		return err
	}
	view, err := store.View(supervisor.ExecutionID(fs.Arg(0)), time.Now().UTC())
	if err != nil {
		return err
	}
	if *jsonOut {
		return writeJSON(out, view)
	}
	_, err = io.WriteString(out, supervisor.RenderText(view))
	return err
}

func v2Views(store *supervisor.Store) ([]*supervisor.ExecutionView, error) {
	state, err := store.Projection()
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(state.Executions))
	for id := range state.Executions {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	views := make([]*supervisor.ExecutionView, 0, len(ids))
	for _, id := range ids {
		view, err := supervisor.ProjectExecution(state, supervisor.ExecutionID(id), time.Now().UTC())
		if err != nil {
			return nil, err
		}
		views = append(views, view)
	}
	return views, nil
}

func cmdV2List(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	state := common(fs)
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(reorderFlags(args, map[string]bool{"--state": true, "--json": false})); err != nil {
		return err
	}
	store, err := openV2(*state)
	if err != nil {
		return err
	}
	views, err := v2Views(store)
	if err != nil {
		return err
	}
	if *jsonOut {
		return writeJSON(out, views)
	}
	for _, view := range views {
		fmt.Fprintf(out, "%s workflow=%s publication=%s queue=%d\n", view.ID, view.WorkflowID, view.Publication, len(view.Queue))
	}
	return nil
}

func cmdV2Pause(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("execution pause", flag.ContinueOnError)
	state := common(fs)
	workflow := fs.String("workflow", "", "Workflow ID")
	requestedBy := fs.String("requested-by", "human:cli", "requesting identity")
	key := fs.String("idempotency-key", "", "stable request identity")
	timeout := fs.Duration("timeout", 30*time.Second, "maximum wait for exact process exits")
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(reorderFlags(args, map[string]bool{"--state": true, "--workflow": true, "--requested-by": true, "--idempotency-key": true, "--timeout": true, "--json": false})); err != nil {
		return err
	}
	if strings.TrimSpace(*workflow) == "" || fs.NArg() != 0 {
		return errors.New("execution pause requires --workflow and no positional arguments")
	}
	if *key == "" {
		*key = "pause/" + *workflow
	}
	store, err := openV2(*state)
	if err != nil {
		return err
	}
	pause, receipt, err := store.PauseWorkflow(context.Background(), supervisor.PauseWorkflowInput{WorkflowID: supervisor.WorkflowID(*workflow), RequestedBy: *requestedBy, IdempotencyKey: *key})
	if err != nil {
		return err
	}
	if *timeout <= 0 {
		return errors.New("pause timeout must be positive")
	}
	if pause.CompletedAt.IsZero() {
		pause, err = waitForPauseCompletion(store, supervisor.WorkflowID(*workflow), *timeout)
		if err != nil {
			return err
		}
	}
	if *jsonOut {
		return writeJSON(out, pauseResponse{Pause: pause, Receipt: receipt})
	}
	fmt.Fprintf(out, "workflow=%s paused fenced=%d released=%d sequence=%d existing=%t\n", pause.WorkflowID, len(pause.FencedAttemptIDs), len(pause.ReleasedLeaseIDs), receipt.Sequence, receipt.Existing)
	return nil
}

func waitForPauseCompletion(store *supervisor.Store, workflowID supervisor.WorkflowID, timeout time.Duration) (*supervisor.Pause, error) {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		state, err := store.Projection()
		if err != nil {
			return nil, err
		}
		pause := state.Pauses[workflowID]
		if pause == nil {
			return nil, os.ErrNotExist
		}
		if !pause.CompletedAt.IsZero() && pause.Phase == supervisor.PauseCompleted {
			copy := *pause
			copy.FencedAttemptIDs = append([]supervisor.AttemptID(nil), pause.FencedAttemptIDs...)
			copy.ReleasedLeaseIDs = append([]supervisor.LeaseID(nil), pause.ReleasedLeaseIDs...)
			return &copy, nil
		}
		if time.Now().After(deadline) {
			return nil, supervisor.ErrPausePending
		}
		<-ticker.C
	}
}

func cmdV2Events(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("events", flag.ContinueOnError)
	state := common(fs)
	after := fs.Uint64("after", 0, "only journal entries after this sequence")
	follow := fs.Bool("follow", false, "follow new journal entries")
	if err := fs.Parse(reorderFlags(args, map[string]bool{"--state": true, "--after": true, "--follow": false})); err != nil {
		return err
	}
	store, err := openV2(*state)
	if err != nil {
		return err
	}
	for {
		entries, err := store.Events(*after)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err := writeJSON(out, entry); err != nil {
				return err
			}
			*after = entry.Sequence
		}
		if !*follow {
			return nil
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func cmdV2Run(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	state := common(fs)
	once := fs.Bool("once", false, "run at most one queued Activity")
	trust := fs.String("trust-mode", string(driver.TrustWorkspace), "workspace or full")
	startupTimeout := fs.Duration("startup-timeout", 30*time.Second, "pre-turn startup deadline")
	if err := fs.Parse(reorderFlags(args, map[string]bool{"--state": true, "--once": false, "--trust-mode": true, "--startup-timeout": true})); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("run requires a workflow ID")
	}
	store, err := openV2(*state)
	if err != nil {
		return err
	}
	views, err := v2Views(store)
	if err != nil {
		return err
	}
	var selected *supervisor.ExecutionView
	for _, view := range views {
		if string(view.WorkflowID) == fs.Arg(0) {
			selected = view
			break
		}
	}
	if selected == nil {
		return os.ErrNotExist
	}
	outputRoot := filepath.Join(stateDir(*state), "supervisor-v2", "outputs")
	if *trust != string(driver.TrustWorkspace) && *trust != string(driver.TrustFull) {
		return fmt.Errorf("invalid trust mode %q", *trust)
	}
	runner := &executor.Executor{Store: store, OutputRoot: outputRoot, Drivers: driver.Lookup, TrustMode: driver.TrustMode(*trust), StartupDeadline: *startupTimeout}
	for _, activityID := range selected.Queue {
		if err := runner.RunActivity(context.Background(), activityID); err != nil {
			return err
		}
		fmt.Fprintf(out, "ran %s\n", activityID)
		if *once {
			break
		}
	}
	return nil
}

func cmdV2Serve(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	state := common(fs)
	interval := fs.Duration("interval", 2*time.Second, "scheduler scan interval")
	workers := fs.Int("workers", 2, "maximum concurrent Activities")
	startupTimeout := fs.Duration("startup-timeout", 30*time.Second, "pre-turn startup deadline")
	environmentJSON := fs.String("environment-json", "", "mode-0600 JSON object of driver environment values")
	trustMode := fs.String("trust-mode", string(driver.TrustWorkspace), "workspace or full")
	if err := fs.Parse(reorderFlags(args, map[string]bool{"--state": true, "--interval": true, "--workers": true, "--startup-timeout": true, "--environment-json": true, "--trust-mode": true})); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("serve does not accept positional arguments")
	}
	if *trustMode != string(driver.TrustWorkspace) && *trustMode != string(driver.TrustFull) {
		return fmt.Errorf("invalid trust mode %q", *trustMode)
	}
	store, err := openV2(*state)
	if err != nil {
		return err
	}
	environment, err := readEnvironmentJSON(*environmentJSON)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "handoff supervisor-v2 · state=%s · workers=%d · trust=%s\n", stateDir(*state), *workers, *trustMode)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return service.ServeV2(ctx, store, service.ServeOptions{Interval: *interval, Workers: *workers, StartupDeadline: *startupTimeout, Environment: environment, TrustMode: driver.TrustMode(*trustMode), OutputRoot: filepath.Join(stateDir(*state), "supervisor-v2", "outputs")}, func(format string, values ...any) { fmt.Fprintf(out, format+"\n", values...) })
}

func cmdV2Service(args []string, out io.Writer) error {
	if len(args) == 0 || args[0] != "install" {
		return errors.New("usage: handoff service install [--state DIR] [--environment-json FILE] [--trust-mode workspace|full]")
	}
	fs := flag.NewFlagSet("service install", flag.ContinueOnError)
	state := common(fs)
	environmentJSON := fs.String("environment-json", "", "private mode-0600 driver environment file")
	trustMode := fs.String("trust-mode", string(driver.TrustWorkspace), "workspace or full")
	if err := fs.Parse(reorderFlags(args[1:], map[string]bool{"--state": true, "--environment-json": true, "--trust-mode": true})); err != nil {
		return err
	}
	path, err := service.InstallV2("", stateDir(*state), *environmentJSON, driver.TrustMode(*trustMode))
	if err != nil {
		return err
	}
	fmt.Fprintln(out, path)
	return nil
}

func readEnvironmentJSON(path string) ([]string, error) {
	if strings.TrimSpace(path) == "" {
		return nil, nil
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return nil, errors.New("--environment-json must be a regular mode-0600 file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var values map[string]string
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&values); err != nil {
		return nil, fmt.Errorf("decode environment JSON: %w", err)
	}
	var extra any
	if err = decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, errors.New("environment JSON must contain exactly one object")
		}
		return nil, fmt.Errorf("decode environment JSON trailer: %w", err)
	}
	if values == nil {
		return nil, errors.New("environment JSON must be an object")
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		if strings.TrimSpace(key) == "" || strings.ContainsAny(key, "=\x00") {
			return nil, fmt.Errorf("invalid environment variable name %q", key)
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	env := make([]string, 0, len(keys))
	for _, key := range keys {
		env = append(env, key+"="+values[key])
	}
	return env, nil
}

func cmdV2TUI(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("tui", flag.ContinueOnError)
	state := common(fs)
	snapshot := fs.Bool("snapshot", false, "render once without interactivity")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("tui does not accept positional arguments")
	}
	store, err := openV2(*state)
	if err != nil {
		return err
	}
	if *snapshot {
		rendered, err := v2tui.Snapshot(store)
		if err != nil {
			return err
		}
		_, err = io.WriteString(out, rendered)
		return err
	}
	views, err := v2Views(store)
	if err != nil {
		return err
	}
	// The machine-readable snapshot and interactive display intentionally share
	// this exact projection. A terminal UI can refresh this rendering without a
	// second lifecycle reducer.
	for _, view := range views {
		_, _ = io.WriteString(out, supervisor.RenderText(view))
	}
	return nil
}

func cmdV2Activity(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: handoff activity list|read")
	}
	fs := flag.NewFlagSet("activity", flag.ContinueOnError)
	state := common(fs)
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(reorderFlags(args[1:], map[string]bool{"--state": true, "--json": false})); err != nil {
		return err
	}
	store, err := openV2(*state)
	if err != nil {
		return err
	}
	views, err := v2Views(store)
	if err != nil {
		return err
	}
	if args[0] == "list" {
		activities := make([]supervisor.ActivityView, 0)
		for _, view := range views {
			activities = append(activities, view.Activities...)
		}
		if *jsonOut {
			return writeJSON(out, activities)
		}
		for _, activity := range activities {
			fmt.Fprintf(out, "%s generation=%d status=%s attempts=%d\n", activity.ID, activity.Generation, activity.Status, len(activity.AttemptIDs))
		}
		return nil
	}
	if args[0] != "read" || fs.NArg() != 1 {
		return errors.New("activity read requires an Activity ID")
	}
	for _, view := range views {
		for _, activity := range view.Activities {
			if string(activity.ID) != fs.Arg(0) {
				continue
			}
			if *jsonOut {
				return writeJSON(out, activity)
			}
			fmt.Fprintf(out, "%s generation=%d status=%s\n", activity.ID, activity.Generation, activity.Status)
			return nil
		}
	}
	return os.ErrNotExist
}

func cmdV2Reply(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("reply", flag.ContinueOnError)
	state := common(fs)
	executionID := fs.String("execution", "", "Execution ID")
	workflowID := fs.String("workflow", "", "Workflow ID")
	sessionID := fs.String("session", "", "exact Session ID")
	activityID := fs.String("activity", "", "predecessor Activity ID")
	message := fs.String("message", "", "reply body")
	from := fs.String("from", "human", "requesting identity")
	key := fs.String("idempotency-key", "", "stable request identity")
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(reorderFlags(args, map[string]bool{"--state": true, "--execution": true, "--session": true, "--activity": true, "--message": true, "--from": true, "--idempotency-key": true, "--json": false})); err != nil {
		return err
	}
	if fs.NArg() != 0 && fs.NArg() != 2 {
		return errors.New("reply accepts either --execution/--activity or WORKFLOW_ID NODE_ID")
	}
	if strings.TrimSpace(*message) == "" {
		return errors.New("reply requires --message")
	}
	store, err := openV2(*state)
	if err != nil {
		return err
	}
	stateProjection, err := store.Projection()
	if err != nil {
		return err
	}
	if fs.NArg() == 2 {
		*workflowID, *activityID = fs.Arg(0), fs.Arg(1)
	}
	if *executionID == "" && *workflowID != "" {
		for id, execution := range stateProjection.Executions {
			if execution.WorkflowID == supervisor.WorkflowID(*workflowID) {
				*executionID = string(id)
				break
			}
		}
	}
	if *executionID == "" || *activityID == "" {
		return errors.New("reply requires an execution/workflow and predecessor activity/node")
	}
	activity := stateProjection.Activities[supervisor.ActivityID(*activityID)]
	if activity == nil && *workflowID != "" {
		for _, candidate := range stateProjection.Activities {
			if candidate.WorkflowID == supervisor.WorkflowID(*workflowID) && candidate.NodeID == supervisor.NodeID(*activityID) {
				if activity == nil || candidate.Generation > activity.Generation {
					activity = candidate
				}
			}
		}
	}
	if activity == nil {
		return os.ErrNotExist
	}
	if *sessionID == "" {
		*sessionID = string(activity.SessionID)
	}
	if *key == "" {
		*key = "reply/" + *activityID
	}
	continuation, receipt, err := store.ContinueSession(context.Background(), supervisor.ContinueSessionInput{ExecutionID: supervisor.ExecutionID(*executionID), SessionID: supervisor.SessionID(*sessionID), PredecessorActivityID: activity.ID, From: *from, Message: *message, IdempotencyKey: *key})
	if err != nil {
		return err
	}
	if *jsonOut {
		return writeJSON(out, replyResponse{ActivityID: ActivityIDResponse{ID: continuation.ID, SessionID: continuation.SessionID, Generation: continuation.Generation}, Receipt: receipt})
	}
	fmt.Fprintf(out, "activity=%s sequence=%d existing=%t\n", continuation.ID, receipt.Sequence, receipt.Existing)
	return nil
}

func cmdV2Agent(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: handoff agent reply|inbox")
	}
	if args[0] == "reply" {
		return cmdV2Reply(args[1:], out)
	}
	if args[0] != "inbox" {
		return fmt.Errorf("unknown agent command %q", args[0])
	}
	fs := flag.NewFlagSet("agent inbox", flag.ContinueOnError)
	state := common(fs)
	sessionID := fs.String("session", "", "Session ID")
	after := fs.Uint64("after", 0, "messages after journal sequence")
	if err := fs.Parse(reorderFlags(args[1:], map[string]bool{"--state": true, "--session": true, "--after": true})); err != nil {
		return err
	}
	if *sessionID == "" {
		return errors.New("agent inbox requires --session")
	}
	store, err := openV2(*state)
	if err != nil {
		return err
	}
	projection, err := store.Projection()
	if err != nil {
		return err
	}
	type messageWithSequence struct {
		Message  supervisor.Message `json:"message"`
		Sequence uint64             `json:"sequence"`
	}
	entries, err := store.Events(*after)
	if err != nil {
		return err
	}
	messages := make([]messageWithSequence, 0)
	for _, entry := range entries {
		for _, message := range projection.Messages {
			if string(message.SessionID) == *sessionID && message.CreatedAt.Equal(entry.At) {
				messages = append(messages, messageWithSequence{Message: *message, Sequence: entry.Sequence})
			}
		}
	}
	return writeJSON(out, messages)
}

func cmdV2Agents(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("agents", flag.ContinueOnError)
	state := common(fs)
	if err := fs.Parse(reorderFlags(args, map[string]bool{"--state": true})); err != nil {
		return err
	}
	store, err := openV2(*state)
	if err != nil {
		return err
	}
	views, err := v2Views(store)
	if err != nil {
		return err
	}
	return writeJSON(out, views)
}
