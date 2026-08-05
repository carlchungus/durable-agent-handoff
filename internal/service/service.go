package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/core"
	"github.com/carlchungus/durable-agent-handoff/internal/engine"
	"github.com/carlchungus/durable-agent-handoff/internal/preferences"
)

func Serve(ctx context.Context, store *core.Store, prefs *preferences.Manager, interval time.Duration, workers int, logf func(string, ...any)) error {
	if interval < 100*time.Millisecond {
		return fmt.Errorf("interval must be at least 100ms")
	}
	if workers < 1 {
		return fmt.Errorf("workers must be positive")
	}
	sem := make(chan struct{}, workers)
	active := map[string]bool{}
	var mu sync.Mutex
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		ws, err := store.List()
		if err != nil {
			return err
		}
		for _, w := range ws {
			if w.Paused {
				continue
			}
			eng := &engine.Engine{Store: store, Preferences: prefs}
			if err = eng.Reconcile(ctx, w.ID); err != nil {
				logf("workflow=%s reconcile_error=%v", w.ID, err)
				continue
			}
			w, err = store.Load(w.ID)
			if err != nil {
				logf("workflow=%s reload_error=%v", w.ID, err)
				continue
			}
			if w.Status != core.WorkflowActive {
				continue
			}
			mu.Lock()
			busy := active[w.ID]
			if !busy {
				active[w.ID] = true
			}
			mu.Unlock()
			if busy {
				continue
			}
			select {
			case sem <- struct{}{}:
				go func(id string) {
					defer func() { <-sem; mu.Lock(); delete(active, id); mu.Unlock() }()
					n, err := (&engine.Engine{Store: store, Preferences: prefs}).RunOne(ctx, id)
					var cooldown *preferences.CooldownError
					if err != nil && !strings.Contains(err.Error(), "no runnable node") && !errors.As(err, &cooldown) {
						logf("workflow=%s error=%v", id, err)
					} else if n != nil {
						logf("workflow=%s node=%s runtime=%s", id, n.ID, n.Runtime.Name)
					}
				}(w.ID)
			default:
				mu.Lock()
				delete(active, w.ID)
				mu.Unlock()
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func Install(binary, state string) (string, error) {
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
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return installFor(runtime.GOOS, home, binary, state)
}

func installFor(goos, home, binary, state string) (string, error) {
	var err error
	switch goos {
	case "darwin":
		dir := filepath.Join(home, "Library", "LaunchAgents")
		if err = os.MkdirAll(dir, 0o755); err != nil {
			return "", err
		}
		path := filepath.Join(dir, "io.github.carlchungus.handoff.plist")
		body := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>Label</key><string>io.github.carlchungus.handoff</string>
<key>ProgramArguments</key><array><string>%s</string><string>serve</string><string>--state</string><string>%s</string></array>
<key>RunAtLoad</key><true/><key>KeepAlive</key><true/>
<key>StandardOutPath</key><string>%s</string><key>StandardErrorPath</key><string>%s</string>
</dict></plist>
`, xml(binary), xml(state), xml(filepath.Join(state, "daemon.log")), xml(filepath.Join(state, "daemon.err.log")))
		if err = os.WriteFile(path, []byte(body), 0o600); err != nil {
			return "", err
		}
		return path, nil
	case "linux":
		dir := filepath.Join(home, ".config", "systemd", "user")
		if err = os.MkdirAll(dir, 0o755); err != nil {
			return "", err
		}
		path := filepath.Join(dir, "handoff.service")
		body := fmt.Sprintf("[Unit]\nDescription=Durable agent handoff scheduler\n\n[Service]\nExecStart=%s serve --state %s\nRestart=always\nRestartSec=3\n\n[Install]\nWantedBy=default.target\n", systemd(binary), systemd(state))
		if err = os.WriteFile(path, []byte(body), 0o600); err != nil {
			return "", err
		}
		return path, nil
	default:
		return "", fmt.Errorf("service installation is not yet supported on %s; run `handoff serve` under your process manager", goos)
	}
}

func Enable(path string) error {
	switch runtime.GOOS {
	case "darwin":
		return exec.Command("launchctl", "bootstrap", "gui/"+fmt.Sprint(os.Getuid()), path).Run()
	case "linux":
		if err := exec.Command("systemctl", "--user", "daemon-reload").Run(); err != nil {
			return err
		}
		return exec.Command("systemctl", "--user", "enable", "--now", "handoff.service").Run()
	default:
		return nil
	}
}
func xml(s string) string {
	r := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", "\"", "&quot;", "'", "&apos;")
	return r.Replace(s)
}
func systemd(s string) string { return strings.ReplaceAll(s, " ", "\\x20") }
