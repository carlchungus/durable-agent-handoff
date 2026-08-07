//go:build dragonfly || freebsd || netbsd || openbsd || solaris

package processidentity

import (
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

func platformProcessStartToken(pid int) (string, bool, error) {
	if err := syscall.Kill(pid, 0); err != nil {
		if err == syscall.ESRCH {
			return "", false, nil
		}
		if err != syscall.EPERM {
			return "", false, err
		}
	}
	out, err := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "lstart=").Output()
	if err != nil {
		return "", false, err
	}
	token := strings.TrimSpace(string(out))
	if token == "" {
		return "", false, syscall.EINVAL
	}
	return token, true, nil
}
