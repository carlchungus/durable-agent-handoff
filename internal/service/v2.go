package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/driver"
	"github.com/carlchungus/durable-agent-handoff/internal/evaluator"
	"github.com/carlchungus/durable-agent-handoff/internal/executor"
	"github.com/carlchungus/durable-agent-handoff/supervisor"
)

const serviceWatchdogInterval = 10 * time.Minute

// ServeOptions is the Supervisor v2 service configuration. Environment values
// are read by the CLI from a private mode-0600 file and are passed to Drivers
// only at launch; they are never persisted in the journal or service unit.
type ServeOptions struct {
	Interval        time.Duration
	Workers         int
	Environment     []string
	TrustMode       driver.TrustMode
	OutputRoot      string
	StartupDeadline time.Duration
	// DecisionRetryDelay bounds transient model failures. Pending turns remain
	// durable and are retried after restart.
	DecisionRetryDelay time.Duration
	// ActivityRetryDelay prevents a persistent runtime failure from consuming
	// the complete launch or turn budget in a tight service loop.
	ActivityRetryDelay time.Duration
	// RunActivity is a test seam for the service drain contract. Production
	// callers leave it nil so the durable Executor is used.
	RunActivity func(context.Context, supervisor.ActivityID) error
	// Evaluator is the fresh, tool-less model that decides a completed turn. Production
	// callers leave it nil to use OpenRouter from the transient environment.
	Evaluator evaluator.Evaluator
	// DetachActive keeps exact live harness processes running when this service
	// instance is stopped or restarted. The next instance adopts them by their
	// durable process identity. It is false for embedders that require the old
	// graceful-drain behavior.
	DetachActive bool
}

// ServeV2 reconciles inherited Attempts once before scheduling queued
// Activities from the Supervisor projection. It never reconciles or mutates a
// legacy Workflow/Session/Activity store.
func ServeV2(ctx context.Context, store *supervisor.Store, options ServeOptions, logf func(string, ...any)) error {
	if store == nil {
		return errors.New("Supervisor v2 store is required")
	}
	if options.Interval < 100*time.Millisecond {
		return fmt.Errorf("interval must be at least 100ms")
	}
	if options.Workers < 1 {
		return errors.New("workers must be positive")
	}
	if options.TrustMode == "" {
		options.TrustMode = driver.TrustWorkspace
	}
	if options.TrustMode != driver.TrustWorkspace && options.TrustMode != driver.TrustFull {
		return fmt.Errorf("unsupported trust mode %q", options.TrustMode)
	}
	if options.OutputRoot == "" {
		return errors.New("output root is required")
	}
	if options.StartupDeadline <= 0 {
		options.StartupDeadline = 30 * time.Second
	}
	if options.DecisionRetryDelay <= 0 {
		options.DecisionRetryDelay = 30 * time.Second
	}
	if options.ActivityRetryDelay <= 0 {
		options.ActivityRetryDelay = 30 * time.Second
	}
	if err := ctx.Err(); err != nil {
		return nil
	}
	if err := store.ReconcileStartup(ctx); err != nil {
		return err
	}
	sem := make(chan struct{}, options.Workers)
	var mu sync.Mutex
	var activeWorkers sync.WaitGroup
	active := map[supervisor.ActivityID]bool{}
	activityRetryAt := map[supervisor.ActivityID]time.Time{}
	activeDecisions := map[supervisor.ActivityID]bool{}
	decisionRetryAt := map[supervisor.ActivityID]time.Time{}
	lastQuietCheck := map[supervisor.WorkflowID]time.Time{}
	runner := &executor.Executor{Store: store, OutputRoot: options.OutputRoot, Drivers: driver.Lookup, Environment: options.Environment, TrustMode: options.TrustMode, StartupDeadline: options.StartupDeadline}
	if liveAttempts, liveErr := store.LiveAttemptIDs(); liveErr != nil {
		return liveErr
	} else {
		for _, attemptID := range liveAttempts {
			id := attemptID
			adopt := func() {
				if err := runner.AdoptAttempt(context.Background(), id); err != nil && logf != nil && !errors.Is(err, supervisor.ErrFenced) {
					logf("attempt=%s adoption_error=%v", id, err)
				}
			}
			if options.DetachActive {
				go adopt()
			} else {
				activeWorkers.Add(1)
				go func() {
					defer activeWorkers.Done()
					adopt()
				}()
			}
		}
	}
	// A runner can exit in the small interval between the first reconciliation
	// and the live-attempt read. Recheck before scheduling so that race becomes
	// durable recovery rather than a permanently orphaned queue item.
	if err := store.ReconcileStartup(ctx); err != nil {
		return err
	}
	run := func() {
		if ctx.Err() != nil {
			return
		}
		if err := quietSessionCheck(store, lastQuietCheck, time.Now().UTC()); err != nil && logf != nil {
			logf("quiet_supervision_error=%v", err)
		}
		views, err := supervisorViews(store)
		if err != nil {
			if logf != nil {
				logf("projection_error=%v", err)
			}
			return
		}
		for _, view := range views {
			for _, activityID := range view.PendingTurns {
				if ctx.Err() != nil {
					return
				}
				mu.Lock()
				if activeDecisions[activityID] || time.Now().Before(decisionRetryAt[activityID]) {
					mu.Unlock()
					continue
				}
				select {
				case sem <- struct{}{}:
					activeDecisions[activityID] = true
				default:
					mu.Unlock()
					return
				}
				activeWorkers.Add(1)
				mu.Unlock()
				go func(id supervisor.ActivityID) {
					failed := false
					defer func() {
						activeWorkers.Done()
						<-sem
						mu.Lock()
						delete(activeDecisions, id)
						if failed {
							decisionRetryAt[id] = time.Now().Add(options.DecisionRetryDelay)
						} else {
							delete(decisionRetryAt, id)
						}
						mu.Unlock()
					}()
					state, projectionErr := store.Projection()
					if projectionErr != nil {
						failed = true
						if logf != nil {
							logf("activity=%s decision_projection_error=%v", id, projectionErr)
						}
						return
					}
					request, attemptID, generation, requestErr := turnDecisionRequest(state, id)
					if requestErr != nil {
						failed = true
						if logf != nil {
							logf("activity=%s decision_request_error=%v", id, requestErr)
						}
						return
					}
					turnEvaluator := options.Evaluator
					if turnEvaluator == nil {
						turnEvaluator = evaluator.OpenRouter{APIKey: environmentValue(options.Environment, "OPENROUTER_API_KEY"), Endpoint: environmentValue(options.Environment, "HANDOFF_EVALUATOR_ENDPOINT"), Mode: evaluator.ModeToolCall}
					}
					decision, evaluationErr := turnEvaluator.Evaluate(ctx, request)
					if evaluationErr != nil {
						failed = true
						if logf != nil {
							logf("activity=%s decision_error=%v", id, evaluationErr)
						}
						return
					}
					_, _, resolveErr := store.DecideTurn(context.Background(), supervisor.DecideTurnInput{ActivityID: id, ExpectedGeneration: generation, AttemptID: attemptID, Decision: decision, IdempotencyKey: "decision/" + string(id)})
					if resolveErr != nil {
						failed = true
						if logf != nil {
							logf("activity=%s decision_commit_error=%v", id, resolveErr)
						}
					}
				}(activityID)
			}
			for _, activityID := range view.Queue {
				if ctx.Err() != nil {
					return
				}
				mu.Lock()
				if active[activityID] || time.Now().Before(activityRetryAt[activityID]) {
					mu.Unlock()
					continue
				}
				select {
				case sem <- struct{}{}:
					active[activityID] = true
				default:
					mu.Unlock()
					return
				}
				activeWorkers.Add(1)
				mu.Unlock()
				go func(id supervisor.ActivityID) {
					failed := false
					defer func() {
						activeWorkers.Done()
						<-sem
						mu.Lock()
						delete(active, id)
						if failed {
							activityRetryAt[id] = time.Now().Add(options.ActivityRetryDelay)
						} else {
							delete(activityRetryAt, id)
						}
						mu.Unlock()
					}()
					var runErr error
					runContext := ctx
					if options.DetachActive {
						runContext = context.Background()
					}
					if options.RunActivity != nil {
						runErr = options.RunActivity(runContext, id)
					} else {
						runErr = runner.RunActivity(runContext, id)
					}
					if runErr != nil && logf != nil {
						failed = true
						logf("activity=%s error=%v", id, runErr)
					} else if runErr != nil {
						failed = true
					}
				}(activityID)
			}
		}
	}
	run()
	ticker := time.NewTicker(options.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			if !options.DetachActive {
				activeWorkers.Wait()
			}
			return nil
		case <-ticker.C:
			run()
		}
	}
}

// quietSessionCheck is intentionally a read/reconcile boundary, not another
// agent turn. Once per configured session cadence it rechecks exact process
// identities and terminalizes only dead attempts, which lets the normal queue
// make progress. A live attempt is left entirely alone: no prompt, signal, or
// evaluator call is emitted.
func quietSessionCheck(store *supervisor.Store, last map[supervisor.WorkflowID]time.Time, now time.Time) error {
	state, err := store.Projection()
	if err != nil {
		return err
	}
	due := false
	for workflowID, workflow := range state.Workflows {
		if workflow.Mode != supervisor.ExecutionModeSession || workflow.SupervisionIntervalSeconds <= 0 {
			continue
		}
		previous := last[workflowID]
		if previous.IsZero() || !now.Before(previous.Add(time.Duration(workflow.SupervisionIntervalSeconds)*time.Second)) {
			last[workflowID] = now
			due = true
		}
	}
	if !due {
		return nil
	}
	return store.ReconcileStartup(context.Background())
}

func turnDecisionRequest(state *supervisor.State, activityID supervisor.ActivityID) (evaluator.Request, supervisor.AttemptID, uint64, error) {
	activity := state.Activities[activityID]
	if activity == nil {
		return evaluator.Request{}, "", 0, errors.New("pending turn is absent")
	}
	workflow := state.Workflows[activity.WorkflowID]
	if workflow == nil || activity == nil {
		return evaluator.Request{}, "", 0, errors.New("pending turn has broken workflow identity")
	}
	node := workflow.Nodes[activity.NodeID]
	if node == nil {
		return evaluator.Request{}, "", 0, errors.New("pending turn has no goal")
	}
	var attempt *supervisor.Attempt
	var turn *supervisor.WorkerResult
	for _, candidate := range state.Attempts {
		if candidate.ActivityID != activity.ID {
			continue
		}
		for _, milestone := range candidate.Milestones {
			if milestone.Kind == supervisor.MilestoneResult {
				if turn != nil {
					return evaluator.Request{}, "", 0, errors.New("pending turn has more than one worker result")
				}
				attempt, turn = candidate, milestone.Result
			}
		}
	}
	if attempt == nil || turn == nil {
		return evaluator.Request{}, "", 0, errors.New("pending turn has no worker result")
	}
	request := evaluator.Request{Model: workflow.EvaluatorModel, Goal: node.Title, Prompt: activity.Prompt, Turn: *turn, Publication: string(supervisor.ProjectPublication(workflow, state))}
	if workflow.Finalizer.Enabled {
		request.SupervisorContext = "The configured follow-up step can merge only an explicitly supplied existing pull request after an accepted completed Result and exact unchanged-head checks. It does not push branches, create or discover pull requests, or start itself. Do not treat those unfinished steps as already handled."
	} else {
		request.SupervisorContext = "No follow-up publication step is enabled for this goal."
	}
	if attempt != nil {
		for _, milestone := range attempt.Milestones {
			switch milestone.Kind {
			case supervisor.MilestoneEffectStarted:
				request.Evidence = appendBounded(request.Evidence, "effect: "+milestone.Effect)
			case supervisor.MilestoneMeaningfulProgress:
				request.Evidence = appendBounded(request.Evidence, "progress: "+milestone.Progress)
			}
		}
	}
	return request, attempt.ID, activity.Generation, nil
}

func appendBounded(values []string, value string) []string {
	const maxItems = 20
	const maxBytes = 1000
	if len(value) > maxBytes {
		value = value[:maxBytes]
	}
	values = append(values, value)
	if len(values) > maxItems {
		values = values[len(values)-maxItems:]
	}
	return values
}

func environmentValue(environment []string, name string) string {
	prefix := name + "="
	for index := len(environment) - 1; index >= 0; index-- {
		if strings.HasPrefix(environment[index], prefix) {
			return strings.TrimPrefix(environment[index], prefix)
		}
	}
	return ""
}

func supervisorViews(store *supervisor.Store) ([]*supervisor.ExecutionView, error) {
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

// InstallV2 writes a unit that starts the Supervisor v2 service directly.
// Prompt text and environment values are intentionally absent; the service
// receives only the private environment-file path and trust policy.
func InstallV2(binary, state, environmentJSON string, trustMode driver.TrustMode) (string, error) {
	if binary == "" {
		var err error
		binary, err = os.Executable()
		if err != nil {
			return "", err
		}
	}
	binary, err := filepath.Abs(binary)
	if err != nil {
		return "", err
	}
	state, err = filepath.Abs(state)
	if err != nil {
		return "", err
	}
	if trustMode == "" {
		trustMode = driver.TrustWorkspace
	}
	if trustMode != driver.TrustWorkspace && trustMode != driver.TrustFull {
		return "", fmt.Errorf("unsupported trust mode %q", trustMode)
	}
	if environmentJSON != "" {
		environmentJSON, err = filepath.Abs(environmentJSON)
		if err != nil {
			return "", err
		}
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return installV2For(runtime.GOOS, home, binary, state, environmentJSON, trustMode)
}

func installV2For(goos, home, binary, state, environmentJSON string, trustMode driver.TrustMode) (string, error) {
	args := "serve --state " + systemd(state) + " --trust-mode " + systemd(string(trustMode))
	workerPath := filepath.Join(home, ".local", "bin") + string(os.PathListSeparator) + "/usr/local/bin" + string(os.PathListSeparator) + "/usr/bin" + string(os.PathListSeparator) + "/bin" + string(os.PathListSeparator) + "/usr/sbin" + string(os.PathListSeparator) + "/sbin"
	if environmentJSON != "" {
		args += " --environment-json " + systemd(environmentJSON)
	}
	switch goos {
	case "darwin":
		dir := filepath.Join(home, "Library", "LaunchAgents")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return "", err
		}
		path := filepath.Join(dir, "io.github.carlchungus.handoff.plist")
		body := fmt.Sprintf("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict><key>Label</key><string>io.github.carlchungus.handoff</string><key>ProgramArguments</key><array><string>%s</string><string>serve</string><string>--state</string><string>%s</string><string>--trust-mode</string><string>%s</string>", xml(binary), xml(state), xml(string(trustMode)))
		if environmentJSON != "" {
			body += fmt.Sprintf("<string>--environment-json</string><string>%s</string>", xml(environmentJSON))
		}
		body += fmt.Sprintf("</array><key>EnvironmentVariables</key><dict><key>PATH</key><string>%s</string></dict><key>RunAtLoad</key><true/><key>KeepAlive</key><true/><key>StartInterval</key><integer>%d</integer></dict></plist>\n", xml(workerPath), int(serviceWatchdogInterval/time.Second))
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			return "", err
		}
		return path, nil
	case "linux":
		dir := filepath.Join(home, ".config", "systemd", "user")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return "", err
		}
		path := filepath.Join(dir, "handoff.service")
		body := fmt.Sprintf("[Unit]\nDescription=Durable agent handoff Supervisor v2\n\n[Service]\nEnvironment=PATH=%s\nExecStart=%s %s\nRestart=always\nRestartSec=3\n\n[Install]\nWantedBy=default.target\n", systemd(workerPath), systemd(binary), args)
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			return "", err
		}
		watchdogService := "[Unit]\nDescription=Start durable agent handoff if inactive\n\n[Service]\nType=oneshot\nExecStart=/usr/bin/systemctl --user start handoff.service\n"
		if err := os.WriteFile(filepath.Join(dir, "handoff-watchdog.service"), []byte(watchdogService), 0o600); err != nil {
			return "", err
		}
		watchdogTimer := fmt.Sprintf("[Unit]\nDescription=Periodic durable agent handoff wake\n\n[Timer]\nOnBootSec=2min\nOnUnitActiveSec=%ds\nPersistent=true\nUnit=handoff-watchdog.service\n\n[Install]\nWantedBy=timers.target\n", int(serviceWatchdogInterval/time.Second))
		if err := os.WriteFile(filepath.Join(dir, "handoff-watchdog.timer"), []byte(watchdogTimer), 0o600); err != nil {
			return "", err
		}
		return path, nil
	default:
		return "", fmt.Errorf("service installation is not yet supported on %s", goos)
	}
}
