package service

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/driver"
	"github.com/carlchungus/durable-agent-handoff/supervisor"
)

func TestInstallV2UnitContainsNoPromptOrEnvironmentValues(t *testing.T) {
	home := t.TempDir()
	path, err := installV2For("linux", home, "/opt/handoff", "/state", "/private/env.json", driver.TrustFull)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if filepath.Base(path) != "handoff.service" || !strings.Contains(text, "Supervisor v2") || !strings.Contains(text, "--trust-mode full") || !strings.Contains(text, "--environment-json /private/env.json") || strings.Contains(text, "prompt") {
		t.Fatalf("unit=%s", text)
	}
}

func TestServeV2GracefulCancellationDrainsActiveExecutor(t *testing.T) {
	stateRoot, worktree, outputRoot := t.TempDir(), t.TempDir(), t.TempDir()
	for _, path := range []string{stateRoot, worktree, outputRoot} {
		if err := os.Chmod(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	store, err := supervisor.Open(stateRoot, supervisor.Options{})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = store.StartExecution(context.Background(), supervisor.StartExecutionInput{
		NativeSession:  supervisor.NativeSessionIdentity{Runtime: "test", ID: "serve-session"},
		Prompt:         "serve drain",
		Runtime:        supervisor.RuntimeSpec{Name: "test", Sandbox: supervisor.SandboxWorkspaceWrite},
		Root:           worktree,
		Authority:      supervisor.AuthoritySpec{RequestedBy: "human", HumanAuthorized: true, Sandbox: supervisor.SandboxWorkspaceWrite},
		Budget:         supervisor.DefaultBudget(),
		IdempotencyKey: "serve-drain-start",
	})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- ServeV2(ctx, store, ServeOptions{
			Interval:   100 * time.Millisecond,
			Workers:    1,
			OutputRoot: outputRoot,
			RunActivity: func(context.Context, supervisor.ActivityID) error {
				close(started)
				<-release
				return nil
			},
		}, nil)
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("ServeV2 did not start the queued executor")
	}
	cancel()
	select {
	case err := <-done:
		t.Fatalf("ServeV2 returned before active executor drained: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ServeV2 did not wait for active executor completion")
	}
}
