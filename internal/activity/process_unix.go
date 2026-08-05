//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package activity

import (
	"os/exec"
	"syscall"
)

func ConfigureBackgroundProcess(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}
