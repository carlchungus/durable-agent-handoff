//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package activity

import (
	"errors"
	"fmt"
	"os/exec"
	"syscall"
)

func BindProcessTree(pid int, _ string) (string, error) {
	pgid, err := syscall.Getpgid(pid)
	if err != nil {
		return "", err
	}
	if pgid != pid {
		return "", fmt.Errorf("activity runner %d did not establish its own process group", pid)
	}
	return fmt.Sprint(pgid), nil
}

func ConfigureBackgroundProcess(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func killProcessGroup(identity AttemptIdentity) error {
	if !processMatches(identity) {
		return ErrFenced
	}
	if err := syscall.Kill(-identity.PID, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}
