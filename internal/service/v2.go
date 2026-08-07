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
	// EvaluationRetryDelay bounds transient evaluator failures. It is process
	// state only: unresolved Claims remain durable and are retried after restart.
	EvaluationRetryDelay time.Duration
	// RunActivity is a test seam for the service drain contract. Production
	// callers leave it nil so the durable Executor is used.
	RunActivity func(context.Context, supervisor.ActivityID) error
	// Evaluator is a fresh, tool-less terminal-claim evaluator. Production
	// callers leave it nil to use OpenRouter from the transient environment.
	Evaluator evaluator.Evaluator
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
	if options.EvaluationRetryDelay <= 0 {
		options.EvaluationRetryDelay = 30 * time.Second
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
	activeEvaluations := map[supervisor.ClaimID]bool{}
	evaluationRetryAt := map[supervisor.ClaimID]time.Time{}
	run := func() {
		if ctx.Err() != nil {
			return
		}
		views, err := supervisorViews(store)
		if err != nil {
			if logf != nil {
				logf("projection_error=%v", err)
			}
			return
		}
		for _, view := range views {
			for _, claimID := range view.EvaluationQueue {
				if ctx.Err() != nil {
					return
				}
				mu.Lock()
				if activeEvaluations[claimID] || time.Now().Before(evaluationRetryAt[claimID]) {
					mu.Unlock()
					continue
				}
				select {
				case sem <- struct{}{}:
					activeEvaluations[claimID] = true
				default:
					mu.Unlock()
					return
				}
				activeWorkers.Add(1)
				mu.Unlock()
				go func(id supervisor.ClaimID) {
					failed := false
					defer func() {
						activeWorkers.Done()
						<-sem
						mu.Lock()
						delete(activeEvaluations, id)
						if failed {
							evaluationRetryAt[id] = time.Now().Add(options.EvaluationRetryDelay)
						} else {
							delete(evaluationRetryAt, id)
						}
						mu.Unlock()
					}()
					state, projectionErr := store.Projection()
					if projectionErr != nil {
						failed = true
						if logf != nil {
							logf("claim=%s evaluation_projection_error=%v", id, projectionErr)
						}
						return
					}
					request, requestErr := claimEvaluationRequest(state, id)
					if requestErr != nil {
						failed = true
						if logf != nil {
							logf("claim=%s evaluation_request_error=%v", id, requestErr)
						}
						return
					}
					claimEvaluator := options.Evaluator
					if claimEvaluator == nil {
						claimEvaluator = evaluator.OpenRouter{APIKey: environmentValue(options.Environment, "OPENROUTER_API_KEY"), Endpoint: environmentValue(options.Environment, "HANDOFF_EVALUATOR_ENDPOINT"), Mode: evaluator.ModeToolCall}
					}
					decision, evaluationErr := claimEvaluator.Evaluate(ctx, request)
					if evaluationErr != nil {
						failed = true
						if logf != nil {
							logf("claim=%s evaluation_error=%v", id, evaluationErr)
						}
						return
					}
					_, _, resolveErr := store.ResolveClaim(context.Background(), supervisor.ResolveClaimInput{ClaimID: id, Decision: decision, IdempotencyKey: "evaluation/" + string(id)})
					if resolveErr != nil {
						failed = true
						if logf != nil {
							logf("claim=%s evaluation_commit_error=%v", id, resolveErr)
						}
					}
				}(claimID)
			}
			for _, activityID := range view.Queue {
				if ctx.Err() != nil {
					return
				}
				mu.Lock()
				if active[activityID] {
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
					defer func() {
						activeWorkers.Done()
						<-sem
						mu.Lock()
						delete(active, id)
						mu.Unlock()
					}()
					var runErr error
					if options.RunActivity != nil {
						runErr = options.RunActivity(ctx, id)
					} else {
						runner := &executor.Executor{Store: store, OutputRoot: options.OutputRoot, Drivers: driver.Lookup, Environment: options.Environment, TrustMode: options.TrustMode, StartupDeadline: options.StartupDeadline}
						runErr = runner.RunActivity(ctx, id)
					}
					if runErr != nil && logf != nil {
						logf("activity=%s error=%v", id, runErr)
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
			activeWorkers.Wait()
			return nil
		case <-ticker.C:
			run()
		}
	}
}

func claimEvaluationRequest(state *supervisor.State, claimID supervisor.ClaimID) (evaluator.Request, error) {
	claim := state.Claims[claimID]
	if claim == nil {
		return evaluator.Request{}, errors.New("evaluation claim is absent")
	}
	workflow := state.Workflows[claim.WorkflowID]
	activity := state.Activities[claim.ActivityID]
	if workflow == nil || activity == nil {
		return evaluator.Request{}, errors.New("evaluation claim has broken workflow or activity identity")
	}
	node := workflow.Nodes[claim.NodeID]
	if node == nil {
		return evaluator.Request{}, errors.New("evaluation claim has no desired work")
	}
	request := evaluator.Request{Model: workflow.Autonomy.EvaluatorModel, Goal: node.Title, Prompt: activity.Prompt, Claim: claim.Result}
	if workflow.Finalizer.Enabled {
		request.SupervisorContext = "An enabled deterministic finalizer is already human-authorized to publish only after the worker result is accepted and the exact configured external checks pass on an unchanged head. Publication is intentionally not worker authority."
	} else {
		request.SupervisorContext = "No Supervisor-owned downstream finalizer is enabled for this workflow."
	}
	if attempt := state.Attempts[claim.AttemptID]; attempt != nil {
		for _, milestone := range attempt.Milestones {
			switch milestone.Kind {
			case supervisor.MilestoneEffectStarted:
				request.Evidence = appendBounded(request.Evidence, "effect: "+milestone.Effect)
			case supervisor.MilestoneMeaningfulProgress:
				request.Evidence = appendBounded(request.Evidence, "progress: "+milestone.Progress)
			}
		}
	}
	return request, nil
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
		body += "</array><key>RunAtLoad</key><true/><key>KeepAlive</key><true/></dict></plist>\n"
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
		body := fmt.Sprintf("[Unit]\nDescription=Durable agent handoff Supervisor v2\n\n[Service]\nExecStart=%s %s\nRestart=always\nRestartSec=3\n\n[Install]\nWantedBy=default.target\n", systemd(binary), args)
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			return "", err
		}
		return path, nil
	default:
		return "", fmt.Errorf("service installation is not yet supported on %s", goos)
	}
}
