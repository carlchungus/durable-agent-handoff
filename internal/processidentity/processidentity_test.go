package processidentity

import (
	"os"
	"testing"
)

func TestProcessMatchesUsesExactStartToken(t *testing.T) {
	pid := os.Getpid()
	token := ProcessStartToken(pid)
	if token == "" {
		t.Fatal("current process has no start token")
	}
	if !ProcessMatches(pid, token) {
		t.Fatal("current process did not match its exact identity")
	}
	if ProcessMatches(pid, token+" stale") {
		t.Fatal("stale process token matched")
	}
}
