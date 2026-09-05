//go:build windows

package processidentity

import (
	"errors"
	"fmt"

	"golang.org/x/sys/windows"
)

// StopExact terminates a previously claimed runner after rechecking its exact
// PID/start-token identity. Windows Job Object containment owns descendants.
func StopExact(pid int, startToken, _ string) error {
	status, err := InspectMatch(pid, startToken)
	if err != nil {
		return err
	}
	switch status {
	case MatchAbsent:
		return nil
	case MatchDifferent:
		return fmt.Errorf("refusing to stop reused PID %d", pid)
	case MatchUnknown:
		return errors.New("refusing to stop process with unknown identity")
	}
	handle, err := windows.OpenProcess(windows.PROCESS_TERMINATE, false, uint32(pid))
	if err != nil {
		return err
	}
	defer windows.CloseHandle(handle)
	return windows.TerminateProcess(handle, 1)
}
