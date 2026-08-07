package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/driver"
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
}

// ServeV2 schedules only queued Activities from the Supervisor projection.
// It never reconciles or mutates a legacy Workflow/Session/Activity store.
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
	sem := make(chan struct{}, options.Workers)
	var mu sync.Mutex
	active := map[supervisor.ActivityID]bool{}
	run := func() {
		views, err := supervisorViews(store)
		if err != nil {
			if logf != nil {
				logf("projection_error=%v", err)
			}
			return
		}
		for _, view := range views {
			for _, activityID := range view.Queue {
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
				mu.Unlock()
				go func(id supervisor.ActivityID) {
					defer func() {
						<-sem
						mu.Lock()
						delete(active, id)
						mu.Unlock()
					}()
					runner := &executor.Executor{Store: store, OutputRoot: options.OutputRoot, Drivers: driver.Lookup, Environment: options.Environment, TrustMode: options.TrustMode, StartupDeadline: options.StartupDeadline}
					if err := runner.RunActivity(ctx, id); err != nil && logf != nil {
						logf("activity=%s error=%v", id, err)
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
			return nil
		case <-ticker.C:
			run()
		}
	}
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
