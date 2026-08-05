//go:build windows

package activity

import (
	"errors"
	"fmt"
	"os/exec"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

var openJobObjectW = windows.NewLazySystemDLL("kernel32.dll").NewProc("OpenJobObjectW")

const jobObjectTerminate = 0x0008

func BindProcessTree(pid int, token string) (string, error) {
	name := fmt.Sprintf("Local\\handoff-activity-%d-%s", pid, token)
	namePtr, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return "", err
	}
	job, err := windows.CreateJobObject(nil, namePtr)
	if err != nil {
		return "", err
	}
	defer windows.CloseHandle(job)
	process, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE|windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		return "", err
	}
	defer windows.CloseHandle(process)
	if err = windows.AssignProcessToJobObject(job, process); err != nil {
		return "", fmt.Errorf("assign activity runner to Windows Job Object: %w", err)
	}
	return name, nil
}

func ConfigureBackgroundProcess(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
}

func killProcessGroup(identity AttemptIdentity) error {
	if identity.ProcessTreeID == "" {
		return errors.New("exact Windows process tree identity is missing")
	}
	name, err := windows.UTF16PtrFromString(identity.ProcessTreeID)
	if err != nil {
		return err
	}
	value, _, _ := openJobObjectW.Call(jobObjectTerminate, 0, uintptr(unsafe.Pointer(name)))
	if value == 0 {
		return ErrFenced
	}
	job := windows.Handle(value)
	defer windows.CloseHandle(job)
	if !processMatches(identity) {
		return ErrFenced
	}
	if err = windows.TerminateJobObject(job, 1); err != nil && processMatches(identity) {
		return fmt.Errorf("terminate exact Windows Job Object: %w", err)
	}
	return nil
}
