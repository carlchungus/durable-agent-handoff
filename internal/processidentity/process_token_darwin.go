//go:build darwin

package processidentity

import (
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

func platformProcessStartToken(pid int) (string, bool, error) {
	process, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		if errors.Is(err, unix.ESRCH) || errors.Is(err, unix.ENOENT) {
			return "", false, nil
		}
		// A process can disappear between the sysctl lookup and its final
		// reap. Confirm that the PID is gone before classifying this transient
		// Darwin EIO as absence; otherwise retain the fail-closed unknown state.
		if errors.Is(err, unix.EIO) && errors.Is(unix.Kill(pid, 0), unix.ESRCH) {
			return "", false, nil
		}
		return "", false, err
	}
	if process == nil || process.Proc.P_pid != int32(pid) {
		return "", false, nil
	}
	started := process.Proc.P_starttime
	return fmt.Sprintf("%d.%06d", started.Sec, started.Usec), true, nil
}
