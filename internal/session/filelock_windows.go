//go:build windows

package session

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

type windowsSessionFileLock struct {
	file       *os.File
	overlapped windows.Overlapped
}

func newPlatformFileLock(file *os.File) sessionFileLock {
	return &windowsSessionFileLock{file: file}
}

func (l *windowsSessionFileLock) TryLock() (bool, error) {
	err := windows.LockFileEx(
		windows.Handle(l.file.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0,
		1,
		0,
		&l.overlapped,
	)
	if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
		return false, nil
	}
	return err == nil, err
}

func (l *windowsSessionFileLock) Unlock() error {
	return windows.UnlockFileEx(windows.Handle(l.file.Fd()), 0, 1, 0, &l.overlapped)
}

func validateRegularFile(file *os.File) error {
	handle := windows.Handle(file.Fd())
	fileType, err := windows.GetFileType(handle)
	if err != nil {
		return err
	}
	var info windows.ByHandleFileInformation
	if err = windows.GetFileInformationByHandle(handle, &info); err != nil {
		return err
	}
	if fileType != windows.FILE_TYPE_DISK ||
		info.FileAttributes&(windows.FILE_ATTRIBUTE_DIRECTORY|windows.FILE_ATTRIBUTE_REPARSE_POINT) != 0 ||
		info.NumberOfLinks != 1 {
		return errors.New("file is not a single-link regular disk file")
	}
	return nil
}

func validateTrustedDirectory(info os.FileInfo) error {
	// Windows user-profile ACLs, rather than Unix mode bits, define the
	// supervisor-private state boundary. os.Root still rejects reparse traversal.
	if info == nil {
		return errors.New("directory identity is unavailable")
	}
	return nil
}
