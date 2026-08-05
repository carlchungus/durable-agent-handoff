//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package session

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

type unixSessionFileLock struct {
	file *os.File
}

func newPlatformFileLock(file *os.File) sessionFileLock {
	return &unixSessionFileLock{file: file}
}

func (l *unixSessionFileLock) TryLock() (bool, error) {
	err := unix.Flock(int(l.file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
		return false, nil
	}
	return err == nil, err
}

func (l *unixSessionFileLock) Unlock() error {
	return unix.Flock(int(l.file.Fd()), unix.LOCK_UN)
}
