//go:build windows

package activity

import (
	"fmt"
	"os/exec"
	"strconv"
	"syscall"

	"golang.org/x/sys/windows"
)

func ConfigureBackgroundProcess(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
}

func killProcessGroup(identity AttemptIdentity) error {
	handle, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION|windows.SYNCHRONIZE, false, uint32(identity.PID))
	if err != nil {
		return ErrFenced
	}
	defer windows.CloseHandle(handle)
	if !processMatches(identity) {
		return ErrFenced
	}
	out, err := exec.Command("taskkill", "/PID", strconv.Itoa(identity.PID), "/T", "/F").CombinedOutput()
	if err != nil && processMatches(identity) {
		return fmt.Errorf("terminate exact Windows process tree: %w: %s", err, out)
	}
	return nil
}
