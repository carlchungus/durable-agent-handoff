//go:build !windows

package privatepath

import (
	"os"
	"path/filepath"
	"testing"
)

func TestUnixPathsRejectAccessOutsideOwner(t *testing.T) {
	directory := t.TempDir()
	file := filepath.Join(directory, "secret.json")
	if err := os.WriteFile(file, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := ValidateFile(file); err == nil {
		t.Fatal("non-private file was accepted")
	}
	if err := os.Chmod(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := ValidateDirectory(directory); err == nil {
		t.Fatal("non-private directory was accepted")
	}
}
