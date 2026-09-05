//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package processidentity

import (
	"errors"
	"fmt"
	"strconv"
	"syscall"
)

// StopExact terminates one previously claimed process tree only after the
// durable PID/start-token fence still matches. The tree ID is the process
// group established by the activity runner on POSIX systems.
func StopExact(pid int, startToken, treeID string) error {
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
	if treeID == "" {
		if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && err != syscall.ESRCH {
			return err
		}
		return nil
	}
	target := pid
	parsed, parseErr := strconv.Atoi(treeID)
	if parseErr != nil || parsed <= 0 {
		return fmt.Errorf("invalid process tree identity %q", treeID)
	}
	target = parsed
	if err := syscall.Kill(-target, syscall.SIGKILL); err != nil && err != syscall.ESRCH {
		return err
	}
	return nil
}
