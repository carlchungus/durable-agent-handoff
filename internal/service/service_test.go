package service

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/core"
)

func TestInstallTemplatesArePortableAndExplicit(t *testing.T) {
	for _, tc := range []struct{ goos, want string }{{"darwin", "LaunchAgents"}, {"linux", "systemd"}} {
		t.Run(tc.goos, func(t *testing.T) {
			home := t.TempDir()
			path, err := installFor(tc.goos, home, "/opt/handoff bin", "/tmp/handoff state")
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(path, tc.want) {
				t.Fatalf("path=%s", path)
			}
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			text := string(b)
			if !strings.Contains(text, "serve") || !strings.Contains(text, "handoff") {
				t.Fatalf("service=%s", text)
			}
		})
	}
}

func TestServeStopsOnContext(t *testing.T) {
	st, err := core.OpenStore(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err = Serve(ctx, st, nil, 10*time.Millisecond, 1, func(string, ...any) {}); err == nil || !strings.Contains(err.Error(), "at least 100ms") {
		t.Fatalf("expected interval validation, got %v", err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel2()
	if err = Serve(ctx2, st, nil, 100*time.Millisecond, 1, func(string, ...any) {}); err != nil {
		t.Fatal(err)
	}
}
