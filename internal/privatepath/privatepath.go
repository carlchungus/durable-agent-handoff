// Package privatepath validates secret-bearing files and agent-owned
// directories against the host operating system's access-control model.
package privatepath

import (
	"errors"
	"os"
)

func ValidateFile(path string) error {
	file, err := OpenFile(path)
	if err == nil {
		err = file.Close()
	}
	return err
}

func ValidateDirectory(path string) error {
	file, err := openValidated(path, true)
	if err == nil {
		err = file.Close()
	}
	return err
}

// OpenFile opens and validates one stable, private regular-file handle. The
// caller reads from the returned handle instead of resolving the path again.
func OpenFile(path string) (*os.File, error) {
	return openValidated(path, false)
}

// ValidateOpenedFile validates the exact open handle, avoiding a second path
// lookup at security-sensitive call sites.
func ValidateOpenedFile(file *os.File) error {
	return validateHandle(file, false)
}

// ValidateOpenedDirectory validates the exact open directory handle.
func ValidateOpenedDirectory(file *os.File) error {
	return validateHandle(file, true)
}

func openValidated(path string, directory bool) (*os.File, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if before.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("path must not be a symbolic link")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	after, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if !os.SameFile(before, after) {
		_ = file.Close()
		return nil, errors.New("path changed while it was being validated")
	}
	if directory {
		if !after.IsDir() {
			_ = file.Close()
			return nil, errors.New("path is not a directory")
		}
	} else if !after.Mode().IsRegular() {
		_ = file.Close()
		return nil, errors.New("path is not a regular file")
	}
	if err = validatePlatform(file, after, directory); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func validateHandle(file *os.File, directory bool) error {
	if file == nil {
		return errors.New("open path handle is required")
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if directory {
		if !info.IsDir() {
			return errors.New("open handle is not a directory")
		}
	} else if !info.Mode().IsRegular() {
		return errors.New("open handle is not a regular file")
	}
	return validatePlatform(file, info, directory)
}
