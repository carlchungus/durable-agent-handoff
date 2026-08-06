//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package activity

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
)

type processTreeReservation struct{}

type processTreeWatchdog struct {
	command *exec.Cmd
	gate    io.WriteCloser
}

func startProcessTreeWatchdog() (*processTreeWatchdog, error) {
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	command := exec.Command(executable)
	command.Env = append(os.Environ(), watchdogEnvironment+"=1")
	gate, err := command.StdinPipe()
	if err != nil {
		return nil, err
	}
	if err = command.Start(); err != nil {
		_ = gate.Close()
		return nil, err
	}
	return &processTreeWatchdog{command: command, gate: gate}, nil
}

func (w *processTreeWatchdog) complete() error {
	_, writeErr := w.gate.Write([]byte{1})
	closeErr := w.gate.Close()
	waitErr := w.command.Wait()
	return errors.Join(writeErr, closeErr, waitErr)
}

func runProcessTreeWatchdog(input io.Reader) {
	var signal [1]byte
	if n, _ := input.Read(signal[:]); n == 1 && signal[0] == 1 {
		return
	}
	// The watchdog remains a member of the original dedicated process group,
	// so that group cannot be recycled while this kill is issued. EOF means
	// the runner died before durably recording and acknowledging completion.
	_ = syscall.Kill(-syscall.Getpgrp(), syscall.SIGKILL)
}

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

func cleanupOrphanedProcessTree(_ AttemptIdentity) error {
	// The in-group watchdog contains descendants before the runner's process
	// group can be recycled. Recovery must never signal a dead runner's numeric
	// PGID because it may now identify unrelated work.
	return nil
}
