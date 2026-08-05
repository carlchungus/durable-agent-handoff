//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package session

import (
	"errors"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

type unixSessionFileLock struct {
	file *os.File
}

type storageIdentity struct {
	device uint64
	inode  uint64
}

func identifyStorageFile(file *os.File) (storageIdentity, error) {
	var info unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &info); err != nil {
		return storageIdentity{}, err
	}
	return storageIdentity{device: uint64(info.Dev), inode: uint64(info.Ino)}, nil
}

func sameStorageIdentity(left, right storageIdentity) bool { return left == right }

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

func validateTrustedDirectory(info os.FileInfo) error {
	if info == nil {
		return errors.New("directory identity is unavailable")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errors.New("directory ownership is unavailable")
	}
	if int(stat.Uid) != os.Geteuid() {
		return errors.New("directory is not owned by the supervisor user")
	}
	if info.Mode().Perm()&0o022 != 0 {
		return errors.New("directory is group- or world-writable")
	}
	return nil
}
