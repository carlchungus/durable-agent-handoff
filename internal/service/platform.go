package service

import (
	"errors"
	"os/exec"
	"os/user"
	"runtime"
	"strconv"
	"strings"
)

func Enable(path string) error {
	uid := -1
	if runtime.GOOS == "darwin" {
		current, err := currentUserUID()
		if err != nil {
			return err
		}
		uid = current
	}
	name, args, err := serviceEnableCommand(runtime.GOOS, uid, path)
	if err != nil {
		return err
	}
	if name == "" {
		return nil
	}
	if err := exec.Command(name, args...).Run(); err != nil {
		return err
	}
	if runtime.GOOS == "linux" {
		return exec.Command("systemctl", "--user", "enable", "--now", "handoff.service").Run()
	}
	return nil
}

func EnableV2(path string) error { return Enable(path) }

func xml(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", "\"", "&quot;", "'", "&apos;").Replace(s)
}

func systemd(s string) string { return strings.ReplaceAll(s, " ", "\\x20") }

func currentUserUID() (int, error) {
	current, err := user.Current()
	if err != nil {
		return 0, err
	}
	uid, err := strconv.Atoi(current.Uid)
	if err != nil || uid < 0 {
		return 0, errors.New("current user has no numeric UID")
	}
	return uid, nil
}

// serviceEnableCommand is deliberately pure so platform-specific command
// construction can be tested without invoking launchctl/systemctl. Enable
// passes the resulting argv directly to exec.Command and never uses a shell.
func serviceEnableCommand(goos string, uid int, path string) (string, []string, error) {
	switch goos {
	case "darwin":
		if uid < 0 {
			return "", nil, errors.New("Darwin service enable requires the current user UID")
		}
		return "launchctl", []string{"bootstrap", "gui/" + strconv.Itoa(uid), path}, nil
	case "linux":
		return "systemctl", []string{"--user", "daemon-reload"}, nil
	default:
		return "", nil, nil
	}
}
