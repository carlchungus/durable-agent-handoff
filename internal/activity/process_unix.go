//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package activity

import (
	"encoding/json"
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
	command.Env = append(withoutRunnerEnvironment(os.Environ()), watchdogEnvironment+"=1")
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
	writeErr := json.NewEncoder(w.gate).Encode(watchdogRequest{})
	closeErr := w.gate.Close()
	waitErr := w.command.Wait()
	return errorsJoin(writeErr, closeErr, waitErr)
}

func runProcessTreeWatchdog(input io.Reader) {
	var request watchdogRequest
	if err := json.NewDecoder(input).Decode(&request); err == nil {
		_ = request
	}
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
		return "", syscall.EINVAL
	}
	return stringPID(pgid), nil
}

func (r *processTreeReservation) close() {}

func (r *processTreeReservation) closeWithError() error { return nil }

func (r *processTreeReservation) stop(pid int) error {
	err := syscall.Kill(-pid, syscall.SIGKILL)
	if err == syscall.ESRCH {
		return nil
	}
	return err
}

func ConfigureBackgroundProcess(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func stringPID(pid int) string { return fmtInt(pid) }

func fmtInt(value int) string {
	if value == 0 {
		return "0"
	}
	negative := value < 0
	if negative {
		value = -value
	}
	var digits [24]byte
	i := len(digits)
	for value > 0 {
		i--
		digits[i] = byte('0' + value%10)
		value /= 10
	}
	if negative {
		i--
		digits[i] = '-'
	}
	return string(digits[i:])
}

func errorsJoin(values ...error) error {
	var first error
	for _, value := range values {
		if value != nil && first == nil {
			first = value
		}
	}
	return first
}
