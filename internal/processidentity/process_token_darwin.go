//go:build darwin

package processidentity

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func platformProcessStartToken(pid int) string {
	process, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil || process == nil || process.Proc.P_pid != int32(pid) {
		return ""
	}
	started := process.Proc.P_starttime
	return fmt.Sprintf("%d.%06d", started.Sec, started.Usec)
}
