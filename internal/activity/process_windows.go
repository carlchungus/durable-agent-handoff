//go:build windows

package activity

import (
	"os/exec"
	"syscall"
)

func ConfigureBackgroundProcess(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
}
