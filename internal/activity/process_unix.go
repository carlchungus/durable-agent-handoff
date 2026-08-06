//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package activity

import (
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"syscall"
)

type processTreeReservation struct{}

func prepareProcessTree(_ *exec.Cmd) (*processTreeReservation, error) {
	return &processTreeReservation{}, nil
}

func (r *processTreeReservation) bind(pid int, _ string) (string, error) {
	pgid, err := syscall.Getpgid(pid)
	if err != nil {
		return "", err
	}
	if pgid != pid {
		return "", fmt.Errorf("activity runner %d did not establish its own process group", pid)
	}
	return fmt.Sprint(pgid), nil
}

func (r *processTreeReservation) close() {}

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

func cleanupOrphanedProcessTree(identity AttemptIdentity) error {
	if identity.ProcessTreeID == "" {
		return nil
	}
	pgid, err := strconv.Atoi(identity.ProcessTreeID)
	if err != nil || pgid != identity.PID {
		return ErrFenced
	}
	// While any descendant remains, this dedicated PGID still names the exact
	// original group and cannot be reused. ESRCH means the tree is already gone.
	if err = syscall.Kill(-pgid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}
