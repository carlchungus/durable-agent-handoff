package service

import (
	"fmt"
	"os/exec"
	"runtime"
	"strings"
)

func Enable(path string) error {
	switch runtime.GOOS {
	case "darwin":
		return exec.Command("launchctl", "bootstrap", "gui/"+fmt.Sprint(0), path).Run()
	case "linux":
		if err := exec.Command("systemctl", "--user", "daemon-reload").Run(); err != nil {
			return err
		}
		return exec.Command("systemctl", "--user", "enable", "--now", "handoff.service").Run()
	default:
		return nil
	}
}

func EnableV2(path string) error { return Enable(path) }

func xml(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", "\"", "&quot;", "'", "&apos;").Replace(s)
}

func systemd(s string) string { return strings.ReplaceAll(s, " ", "\\x20") }
