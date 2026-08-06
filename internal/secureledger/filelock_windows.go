//go:build windows

package secureledger

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

type windowsFileLock struct {
	file       *os.File
	overlapped windows.Overlapped
}

type storageIdentity struct {
	volume uint32
	high   uint32
	low    uint32
}

func identifyStorageFile(file *os.File) (storageIdentity, error) {
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(file.Fd()), &info); err != nil {
		return storageIdentity{}, err
	}
	return storageIdentity{volume: info.VolumeSerialNumber, high: info.FileIndexHigh, low: info.FileIndexLow}, nil
}

func sameStorageIdentity(left, right storageIdentity) bool { return left == right }

func newPlatformFileLock(file *os.File) fileLock { return &windowsFileLock{file: file} }

func (l *windowsFileLock) TryLock() (bool, error) {
	err := windows.LockFileEx(windows.Handle(l.file.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &l.overlapped)
	if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
		return false, nil
	}
	return err == nil, err
}

func (l *windowsFileLock) Unlock() error {
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
	if fileType != windows.FILE_TYPE_DISK || info.FileAttributes&(windows.FILE_ATTRIBUTE_DIRECTORY|windows.FILE_ATTRIBUTE_REPARSE_POINT) != 0 || info.NumberOfLinks != 1 {
		return errors.New("file is not a single-link regular disk file")
	}
	return nil
}

func validateTrustedDirectory(info os.FileInfo) error {
	if info == nil {
		return errors.New("directory identity is unavailable")
	}
	return nil
}
