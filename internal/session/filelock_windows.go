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
