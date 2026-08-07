//go:build !windows

package privatepath

import (
	"errors"
	"os"
)

func validatePlatform(_ *os.File, info os.FileInfo, directory bool) error {
	if directory {
		if info.Mode().Perm()&0o077 != 0 {
			return errors.New("directory is accessible outside its owner")
		}
		return nil
	}
	if info.Mode().Perm() != 0o600 {
		return errors.New("file mode must be 0600")
	}
	return nil
}
