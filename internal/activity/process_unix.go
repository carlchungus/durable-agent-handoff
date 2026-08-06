//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package activity

import (
	"encoding/json"
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

func (w *processTreeWatchdog) complete(completion *runnerCompletion, result ExitResult) error {
	writeErr := json.NewEncoder(w.gate).Encode(watchdogRequest{Completion: completion, Result: result})
	closeErr := w.gate.Close()
	waitErr := w.command.Wait()
	return errors.Join(writeErr, closeErr, waitErr)
}

func runProcessTreeWatchdog(input io.Reader) {
	var request watchdogRequest
	if err := json.NewDecoder(io.LimitReader(input, 1<<20)).Decode(&request); err == nil && request.Completion != nil {
		if store, openErr := OpenStore(request.Completion.Root); openErr == nil {
			_ = finishRunnerAttempt(store, request.Completion, request.Result, os.Getppid())
		}
	}
	// Whether the runner died or requested terminal completion, the watchdog
	// remains in the exact dedicated group while terminating every member. On
	// normal completion it writes the fenced terminal record first, then kills
	// the runner and any background descendants as one contained tree.
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
