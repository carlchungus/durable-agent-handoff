package privatepath

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCurrentUserPrivatePathsValidate(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(directory, "secret.json")
	if err := os.WriteFile(file, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(file, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ValidateDirectory(directory); err != nil {
		t.Fatalf("private directory rejected: %v", err)
	}
	if err := ValidateFile(file); err != nil {
		t.Fatalf("private file rejected: %v", err)
	}
}
