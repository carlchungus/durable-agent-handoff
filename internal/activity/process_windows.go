//go:build windows

package activity

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os/exec"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

var openJobObjectW = windows.NewLazySystemDLL("kernel32.dll").NewProc("OpenJobObjectW")

const jobObjectTerminate = 0x0008

type processTreeReservation struct {
	name   string
	handle windows.Handle
}

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
		return nil, fmt.Errorf("configure Windows Job Object kill-on-close: %w", err)
	}
	if err = windows.SetHandleInformation(job, windows.HANDLE_FLAG_INHERIT, windows.HANDLE_FLAG_INHERIT); err != nil {
		windows.CloseHandle(job)
		return nil, err
	}
	command.SysProcAttr.AdditionalInheritedHandles = append(command.SysProcAttr.AdditionalInheritedHandles, syscall.Handle(job))
	return &processTreeReservation{name: name, handle: job}, nil
}

func (r *processTreeReservation) bind(pid int, _ string) (string, error) {
	process, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE|windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		return "", err
	}
	defer windows.CloseHandle(process)
	if err = windows.AssignProcessToJobObject(r.handle, process); err != nil {
		return "", fmt.Errorf("assign activity runner to Windows Job Object: %w", err)
	}
	return r.name, nil
}

func (r *processTreeReservation) close() {
	if r != nil && r.handle != 0 {
		_ = windows.CloseHandle(r.handle)
		r.handle = 0
	}
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

// KILL_ON_JOB_CLOSE guarantees that an unexpectedly dead runner closes the
// last inherited job handle and the kernel terminates all remaining members.
func cleanupOrphanedProcessTree(_ AttemptIdentity) error { return nil }
