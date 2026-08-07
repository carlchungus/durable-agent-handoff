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
	if status, err := InspectMatch(pid, token); err != nil || status != MatchExact {
		t.Fatalf("exact inspection status=%d err=%v", status, err)
	}
	if status, err := InspectMatch(pid, token+" stale"); err != nil || status != MatchDifferent {
		t.Fatalf("reused-PID inspection status=%d err=%v", status, err)
	}
	if status, err := InspectMatch(0, token); err == nil || status != MatchUnknown {
		t.Fatalf("invalid identity did not fail closed: status=%d err=%v", status, err)
	}
}
