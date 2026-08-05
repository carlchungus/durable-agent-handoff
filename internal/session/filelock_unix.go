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

func validateRegularFile(file *os.File) error {
	var info unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &info); err != nil {
		return err
	}
	if info.Mode&unix.S_IFMT != unix.S_IFREG || info.Nlink != 1 {
		return errors.New("file is not a single-link regular file")
	}
	return nil
}
