//go:build windows

package activity

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os/exec"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

type processTreeReservation struct {
	name   string
	handle windows.Handle
}

type processTreeWatchdog struct{}

func startProcessTreeWatchdog() (*processTreeWatchdog, error) { return &processTreeWatchdog{}, nil }
func (w *processTreeWatchdog) complete() error                { return nil }
func runProcessTreeWatchdog(_ io.Reader)                      {}

func prepareProcessTree(command *exec.Cmd) (*processTreeReservation, error) {
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return nil, err
	}
	name := "Local\\handoff-activity-" + hex.EncodeToString(random)
	namePtr, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return nil, err
	}
	job, err := windows.CreateJobObject(nil, namePtr)
	if err != nil {
		return nil, err
	}
	limits := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	limits.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err = windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&limits)), uint32(unsafe.Sizeof(limits))); err != nil {
		windows.CloseHandle(job)
		return nil, fmt.Errorf("configure Windows Job Object: %w", err)
	}
	if err = windows.SetHandleInformation(job, windows.HANDLE_FLAG_INHERIT, windows.HANDLE_FLAG_INHERIT); err != nil {
		windows.CloseHandle(job)
		return nil, err
	}
	command.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP, AdditionalInheritedHandles: []syscall.Handle{syscall.Handle(job)}}
	return &processTreeReservation{name: name, handle: job}, nil
}

func (r *processTreeReservation) bind(_ int, _ string) (string, error) { return r.name, nil }
func (r *processTreeReservation) close() {
	if r != nil && r.handle != 0 {
		_ = windows.CloseHandle(r.handle)
		r.handle = 0
	}
}

func ConfigureBackgroundProcess(command *exec.Cmd) {
	if command.SysProcAttr == nil {
		command.SysProcAttr = &syscall.SysProcAttr{}
	}
	command.SysProcAttr.CreationFlags |= syscall.CREATE_NEW_PROCESS_GROUP
}
