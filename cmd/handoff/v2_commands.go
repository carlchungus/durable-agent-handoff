package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	"github.com/carlchungus/durable-agent-handoff/internal/evaluator"
	"github.com/carlchungus/durable-agent-handoff/internal/executor"
	"github.com/carlchungus/durable-agent-handoff/internal/githubgate"
	"github.com/carlchungus/durable-agent-handoff/internal/privatepath"
	"github.com/carlchungus/durable-agent-handoff/internal/service"
	v2tui "github.com/carlchungus/durable-agent-handoff/internal/tui"
	"github.com/carlchungus/durable-agent-handoff/supervisor"
)

type runtimeCandidateFlags []string

func (f *runtimeCandidateFlags) String() string { return strings.Join(*f, ",") }
func (f *runtimeCandidateFlags) Set(value string) error {
	*f = append(*f, value)
	return nil
}

type executionStartRequest struct {
	IdempotencyKey          string             `json:"idempotency_key"`
	Goal                    string             `json:"goal"`
	Prompt                  string             `json:"prompt"`
	RemoteRoot              string             `json:"remote_root"`
	Runtime                 string             `json:"runtime"`
	ResumeID                string             `json:"resume_id"`
	Model                   string             `json:"model,omitempty"`
	Effort                  string             `json:"effort,omitempty"`
	Sandbox                 supervisor.Sandbox `json:"sandbox"`
	Role                    string             `json:"role"`
	FinalizerEnabled        bool               `json:"finalizer_enabled,omitempty"`
	FinalizerRequiredChecks []string           `json:"finalizer_required_checks,omitempty"`
	FinalizerRequireHuman   bool               `json:"finalizer_require_human,omitempty"`
	OneShot                 bool               `json:"one_shot,omitempty"`
	EvaluatorModel          string             `json:"evaluator_model,omitempty"`
	MaxTurns                int                `json:"max_turns,omitempty"`
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
	return cmdV2StartMode(args, out, false, false)
}

func cmdV2Goal(args []string, out io.Writer) error {
	if len(args) == 0 || args[0] != "start" {
		return errors.New("usage: handoff goal start --goal GOAL --runtime RUNTIME --file -")
	}
	return cmdV2StartMode(args[1:], out, false, true)
}

func cmdV2StartMode(args []string, out io.Writer, promotion, goalMode bool) error {
	fs := flag.NewFlagSet("start", flag.ContinueOnError)
	state := common(fs)
	file := fs.String("file", "", "read strict request or prompt from stdin; must be -")
	root := fs.String("root", ".", "canonical execution root")
	goal := fs.String("goal", "", "desired work title")
	runtimeName := fs.String("runtime", "", "codex, claude, or pi")
	nativeSession := fs.String("session", "", "exact native runtime Session identity")
	role := fs.String("role", "", "role ladder name, such as planner or release-check")
	model := fs.String("model", "", "runtime model")
	effort := fs.String("effort", "", "runtime reasoning effort")
	sandbox := fs.String("sandbox", "workspace-write", "read-only or workspace-write")
	authorizedBy := fs.String("authorized-by", "", "human identity authorizing execution")
	key := fs.String("idempotency-key", "", "stable request identity")
	finalizerEnabled := fs.Bool("finalizer-enabled", false, "enable the immutable github merge finalizer")
	var requiredChecks runtimeCandidateFlags
	fs.Var(&requiredChecks, "required-check", "required external GitHub check; repeat for each check")
	requireHuman := fs.Bool("require-human", false, "require human approval before finalization")
	evaluatorModel := fs.String("evaluator-model", "", "small OpenRouter model that decides each turn")
	maxTurns := fs.Int("max-turns", 0, "maximum turns before asking a human")
	jsonOut := fs.Bool("json", false, "emit JSON")
	known := map[string]bool{"--state": true, "--file": true, "--root": true, "--goal": true, "--runtime": true, "--session": true, "--role": true, "--model": true, "--effort": true, "--sandbox": true, "--authorized-by": true, "--idempotency-key": true, "--finalizer-enabled": false, "--required-check": true, "--require-human": false, "--evaluator-model": true, "--max-turns": true, "--json": false}
	if err := rejectUnknownFlags(args, known); err != nil {
		return err
	}
	if err := fs.Parse(reorderFlags(args, known)); err != nil {
		return err
	}
	var input supervisor.StartExecutionInput
	if promotion {
		if *file != "-" || !*jsonOut || fs.NArg() != 0 {
			return errors.New("execution start requires --file - --json and no positional arguments")
		}
		var reader io.Reader = os.Stdin
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
		model, turns := goalFields(!request.OneShot, request.EvaluatorModel, request.MaxTurns)
		input = supervisor.StartExecutionInput{
			NativeSession: supervisor.NativeSessionIdentity{Runtime: request.Runtime, ID: request.ResumeID},
			Prompt:        request.Prompt, Goal: request.Goal, Role: request.Role,
			Runtime:        supervisor.RuntimeSpec{Name: request.Runtime, Model: request.Model, Effort: request.Effort, Sandbox: request.Sandbox},
			Root:           request.RemoteRoot,
			Authority:      supervisor.AuthoritySpec{RequestedBy: request.Role, HumanAuthorized: true, Sandbox: request.Sandbox},
			Finalizer:      supervisor.FinalizerSpec{Enabled: request.FinalizerEnabled, RequiredChecks: append([]string(nil), request.FinalizerRequiredChecks...), RequireHuman: request.FinalizerRequireHuman},
			EvaluatorModel: model, MaxTurns: turns,
			Budget: supervisor.DefaultBudget(), IdempotencyKey: request.IdempotencyKey,
		}
	} else {
		if *file != "-" || fs.NArg() != 0 {
			return errors.New("start requires --file - and no positional arguments")
		}
		prompt, err := readPromptStdin()
		if err != nil {
			return err
		}
		var fallbacks []supervisor.RuntimeSpec
		store, err := supervisor.Open(stateDir(*state), supervisor.Options{})
		if err != nil {
			return err
		}
		if strings.TrimSpace(*role) != "" {
			ladder, ladderErr := store.RoleLadder(*role)
			if ladderErr != nil {
				return ladderErr
			}
			if len(ladder) == 0 {
				return fmt.Errorf("role %q has no configured preference ladder", *role)
			}
			primaryIndex := 0
			if strings.TrimSpace(*runtimeName) == "" {
				*runtimeName = ladder[0].Name
				if *model == "" {
					*model = ladder[0].Model
				}
				if *effort == "" {
					*effort = ladder[0].Effort
				}
				if *sandbox == "" {
					*sandbox = string(ladder[0].Sandbox)
				}
			} else {
				for index, candidate := range ladder {
					if candidate.Name == *runtimeName {
						primaryIndex = index
						if *model == "" {
							*model = candidate.Model
						}
						if *effort == "" {
							*effort = candidate.Effort
						}
						break
					}
				}
				if primaryIndex == 0 && ladder[0].Name != *runtimeName {
					return fmt.Errorf("runtime %q is not configured for role %q", *runtimeName, *role)
				}
			}
			if len(ladder) > primaryIndex+1 {
				fallbacks = append([]supervisor.RuntimeSpec(nil), ladder[primaryIndex+1:]...)
			}
		}
		decisionModel, turns := goalFields(goalMode, *evaluatorModel, *maxTurns)
		input = supervisor.StartExecutionInput{
			NativeSession: supervisor.NativeSessionIdentity{Runtime: *runtimeName, ID: *nativeSession},
			Goal:          *goal, Prompt: prompt, Role: *role,
			Fallbacks:      fallbacks,
			Runtime:        supervisor.RuntimeSpec{Name: *runtimeName, Model: *model, Effort: *effort, Sandbox: supervisor.Sandbox(*sandbox)},
			Root:           *root,
			Authority:      supervisor.AuthoritySpec{RequestedBy: *authorizedBy, HumanAuthorized: *authorizedBy != "", Sandbox: supervisor.Sandbox(*sandbox)},
			Finalizer:      supervisor.FinalizerSpec{Enabled: *finalizerEnabled, RequiredChecks: append([]string(nil), requiredChecks...), RequireHuman: *requireHuman},
			EvaluatorModel: decisionModel, MaxTurns: turns,
			Budget: supervisor.DefaultBudget(), IdempotencyKey: *key,
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
		if promotion {
			return writeJSON(out, executionStartResponse{WorkflowID: execution.WorkflowID, NodeID: execution.RootNodeID})
		}
		return writeJSON(out, ordinaryStartResponse{Execution: execution, Receipt: receipt})
	}
	fmt.Fprintf(out, "execution=%s workflow=%s session=%s sequence=%d existing=%t\n", execution.ID, execution.WorkflowID, execution.SessionID, receipt.Sequence, receipt.Existing)
	return nil
}

func goalFields(enabled bool, model string, maxTurns int) (string, int) {
	if !enabled {
		return model, maxTurns
	}
	if strings.TrimSpace(model) == "" {
		model = evaluator.DefaultModel
	}
	if maxTurns == 0 {
		maxTurns = 100
	}
	return model, maxTurns
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
		needsHuman := 0
		for _, activity := range view.Activities {
			if activity.Status == supervisor.ActivityNeedsHuman {
				needsHuman++
			}
		}
		fmt.Fprintf(out, "%s workflow=%s publication=%s queue=%d pending_turns=%d needs_human=%d\n", view.ID, view.WorkflowID, view.Publication, len(view.Queue), len(view.PendingTurns), needsHuman)
	}
	return nil
}

func cmdV2Preference(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: handoff preference set|list|health")
	}
	switch args[0] {
	case "set":
		fs := flag.NewFlagSet("preference set", flag.ContinueOnError)
		state := common(fs)
		var values runtimeCandidateFlags
		fs.Var(&values, "candidate", "runtime:model[:effort], repeat in preference order")
		if err := fs.Parse(reorderFlags(args[1:], map[string]bool{"--state": true, "--candidate": true})); err != nil {
			return err
		}
		if fs.NArg() != 1 || len(values) == 0 {
			return errors.New("preference set requires ROLE and at least one --candidate")
		}
		candidates := make([]supervisor.RuntimeSpec, 0, len(values))
		for _, value := range values {
			parts := strings.SplitN(value, ":", 3)
			if len(parts) < 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
				return fmt.Errorf("invalid candidate %q; expected runtime:model[:effort]", value)
			}
			effort := "xhigh"
			if len(parts) == 3 && strings.TrimSpace(parts[2]) != "" {
				effort = parts[2]
			}
			candidates = append(candidates, supervisor.RuntimeSpec{Name: parts[0], Model: parts[1], Effort: effort, Sandbox: supervisor.SandboxWorkspaceWrite})
		}
		store, err := openV2(*state)
		if err != nil {
			return err
		}
		key := "preference/" + fs.Arg(0) + "/" + preferenceDigest(candidates)
		configured, _, err := store.SetRoleLadder(context.Background(), supervisor.SetRoleLadderInput{Role: fs.Arg(0), Candidates: candidates, IdempotencyKey: key})
		if err != nil {
			return err
		}
		return writeJSON(out, map[string]any{"role": fs.Arg(0), "candidates": configured})
	case "list":
		fs := flag.NewFlagSet("preference list", flag.ContinueOnError)
		stateFlag := common(fs)
		if err := fs.Parse(reorderFlags(args[1:], map[string]bool{"--state": true})); err != nil {
			return err
		}
		if fs.NArg() != 0 {
			return errors.New("preference list accepts only --state")
		}
		store, err := openV2(*stateFlag)
		if err != nil {
			return err
		}
		state, err := store.Projection()
		if err != nil {
			return err
		}
		return writeJSON(out, map[string]any{"ladders": state.RoleLadders})
	case "health":
		fs := flag.NewFlagSet("preference health", flag.ContinueOnError)
		stateDirFlag := common(fs)
		if err := fs.Parse(reorderFlags(args[1:], map[string]bool{"--state": true})); err != nil {
			return err
		}
		if fs.NArg() != 0 {
			return errors.New("preference health accepts only --state")
		}
		store, err := openV2(*stateDirFlag)
		if err != nil {
			return err
		}
		state, err := store.Projection()
		if err != nil {
			return err
		}
		type health struct {
			Runtime             string `json:"runtime"`
			Model               string `json:"model,omitempty"`
			ProviderUnavailable int    `json:"provider_unavailable"`
		}
		counts := map[string]*health{}
		for _, attempt := range state.Attempts {
			if !attemptHasProviderUnavailable(attempt) {
				continue
			}
			key := strings.Join([]string{attempt.Runtime.Name, attempt.Runtime.Executable, attempt.Runtime.Model, attempt.Runtime.Effort, string(attempt.Runtime.Sandbox)}, "\x00")
			if counts[key] == nil {
				counts[key] = &health{Runtime: attempt.Runtime.Name, Model: attempt.Runtime.Model}
			}
			counts[key].ProviderUnavailable++
		}
		items := make([]health, 0, len(counts))
		for _, item := range counts {
			items = append(items, *item)
		}
		sort.Slice(items, func(i, j int) bool { return items[i].Runtime+items[i].Model < items[j].Runtime+items[j].Model })
		return writeJSON(out, items)
	default:
		return fmt.Errorf("unknown preference command %q", args[0])
	}
}

func attemptHasProviderUnavailable(attempt *supervisor.Attempt) bool {
	if attempt == nil {
		return false
	}
	for _, milestone := range attempt.Milestones {
		if milestone.Kind == supervisor.MilestoneProviderUnavailable {
			return true
		}
	}
	return false
}

func preferenceDigest(candidates []supervisor.RuntimeSpec) string {
	raw, _ := json.Marshal(candidates)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:6])
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
		return errors.New("usage: handoff service install [--state DIR] [--environment-json FILE] [--trust-mode workspace|full] [--enable]")
	}
	fs := flag.NewFlagSet("service install", flag.ContinueOnError)
	state := common(fs)
	environmentJSON := fs.String("environment-json", "", "private mode-0600 driver environment file")
	trustMode := fs.String("trust-mode", string(driver.TrustWorkspace), "workspace or full")
	enable := fs.Bool("enable", false, "enable and start the stable handoff.service")
	if err := fs.Parse(reorderFlags(args[1:], map[string]bool{"--state": true, "--environment-json": true, "--trust-mode": true, "--enable": false})); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("service install does not accept positional arguments")
	}
	path, err := service.InstallV2("", stateDir(*state), *environmentJSON, driver.TrustMode(*trustMode))
	if err != nil {
		return err
	}
	if *enable {
		if err := service.EnableV2(path); err != nil {
			return fmt.Errorf("service installed but enable failed: %w", err)
		}
	}
	fmt.Fprintln(out, path)
	return nil
}

func cmdV2GitHub(args []string, out io.Writer) error {
	if len(args) == 0 || args[0] != "merge" {
		return errors.New("usage: handoff github merge --execution ID --repo OWNER/REPO --pr NUMBER --gate NAME --idempotency-key KEY [--approved] --json")
	}
	fs := flag.NewFlagSet("github merge", flag.ContinueOnError)
	state := common(fs)
	executionID := fs.String("execution", "", "Execution ID")
	repository := fs.String("repo", "", "OWNER/REPO")
	pullRequest := fs.String("pr", "", "pull request number")
	method := fs.String("method", "squash", "merge method")
	approved := fs.Bool("approved", false, "explicit human publication approval")
	key := fs.String("idempotency-key", "", "stable publication effect identity")
	jsonOut := fs.Bool("json", false, "emit JSON")
	var gates runtimeCandidateFlags
	fs.Var(&gates, "gate", "exact required external GitHub check, repeat for each check")
	if err := fs.Parse(reorderFlags(args[1:], map[string]bool{"--state": true, "--execution": true, "--repo": true, "--pr": true, "--method": true, "--approved": false, "--idempotency-key": true, "--json": false, "--gate": true})); err != nil {
		return err
	}
	if fs.NArg() != 0 || strings.TrimSpace(*executionID) == "" || strings.TrimSpace(*repository) == "" || strings.TrimSpace(*pullRequest) == "" || strings.TrimSpace(*key) == "" {
		return errors.New("github merge requires execution, repo, pr, and idempotency-key")
	}
	store, err := openV2(*state)
	if err != nil {
		return err
	}
	result, err := store.Finalize(context.Background(), supervisor.FinalizationRequest{ExecutionID: supervisor.ExecutionID(*executionID), Repository: *repository, PullRequest: *pullRequest, Gates: append([]string(nil), gates...), Method: *method, HumanApproved: *approved, IdempotencyKey: *key}, githubgate.ExecRunner{})
	if err != nil {
		return err
	}
	if *jsonOut {
		return writeJSON(out, result)
	}
	fmt.Fprintf(out, "finalization=%s state=%s merged=%t head=%s\n", result.FinalizationID, result.State, result.Merged, result.HeadSHA)
	return nil
}

func readEnvironmentJSON(path string) ([]string, error) {
	if strings.TrimSpace(path) == "" {
		return nil, nil
	}
	file, err := privatepath.OpenFile(path)
	if err != nil {
		return nil, fmt.Errorf("--environment-json must be an OS-private regular file: %w", err)
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

func readPromptStdin() (string, error) {
	return readRequiredStdin("prompt")
}

func readRequiredStdin(label string) (string, error) {
	raw, err := io.ReadAll(os.Stdin)
	if err != nil {
		return "", fmt.Errorf("read %s input: %w", label, err)
	}
	if strings.TrimSpace(string(raw)) == "" {
		return "", fmt.Errorf("%s input is empty", label)
	}
	return string(raw), nil
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
			attempts := 0
			for _, view := range views {
				for _, attempt := range view.Attempts {
					if attempt.ActivityID == activity.ID {
						attempts++
					}
				}
			}
			fmt.Fprintf(out, "%s generation=%d status=%s attempts=%d", activity.ID, activity.Generation, activity.Status, attempts)
			if activity.BlockerKind != "" {
				fmt.Fprintf(out, " blocker=%s question=%q", activity.BlockerKind, activity.Question)
			}
			fmt.Fprintln(out)
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
			fmt.Fprintf(out, "%s generation=%d status=%s", activity.ID, activity.Generation, activity.Status)
			if activity.BlockerKind != "" {
				fmt.Fprintf(out, " blocker=%s question=%q", activity.BlockerKind, activity.Question)
			}
			fmt.Fprintln(out)
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
	file := fs.String("file", "", "read reply body from stdin; must be -")
	from := fs.String("from", "human", "requesting identity")
	key := fs.String("idempotency-key", "", "stable request identity")
	jsonOut := fs.Bool("json", false, "emit JSON")
	known := map[string]bool{"--state": true, "--execution": true, "--workflow": true, "--session": true, "--activity": true, "--file": true, "--from": true, "--idempotency-key": true, "--json": false}
	if err := rejectUnknownFlags(args, known); err != nil {
		return err
	}
	if err := fs.Parse(reorderFlags(args, known)); err != nil {
		return err
	}
	for _, arg := range args {
		if arg == "--message" || strings.HasPrefix(arg, "--message=") {
			return errors.New("reply requires --file -; --message is not supported")
		}
	}
	if *file != "-" {
		return errors.New("reply requires --file -")
	}
	if fs.NArg() != 0 && fs.NArg() != 2 {
		return errors.New("reply accepts either --execution/--activity or WORKFLOW_ID NODE_ID")
	}
	message, err := readPromptStdin()
	if err != nil {
		return err
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
	continuation, receipt, err := store.ContinueSession(context.Background(), supervisor.ContinueSessionInput{ExecutionID: supervisor.ExecutionID(*executionID), SessionID: supervisor.SessionID(*sessionID), PredecessorActivityID: activity.ID, From: *from, Message: message, IdempotencyKey: *key})
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
