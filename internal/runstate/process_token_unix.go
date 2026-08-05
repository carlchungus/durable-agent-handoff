//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package runstate

import (
	"os/exec"
	"strconv"
	"strings"
)

func platformProcessStartToken(pid int) string {
	out, err := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "lstart=").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}
