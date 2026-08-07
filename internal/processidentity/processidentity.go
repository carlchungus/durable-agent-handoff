// Package processidentity contains only exact OS process identity helpers.
// It is deliberately not a workflow state store or supervisor lease owner.
package processidentity

import (
	"errors"
	"fmt"
	"strings"
)

type MatchStatus uint8

const (
	MatchUnknown MatchStatus = iota
	MatchAbsent
	MatchDifferent
	MatchExact
)

// InspectMatch distinguishes an exact live process from a dead process, a
// reused PID, and an identity that the OS would not let us inspect. Callers
// that own durable leases must fail closed on MatchUnknown.
func InspectMatch(pid int, startToken string) (MatchStatus, error) {
	if pid <= 0 || strings.TrimSpace(startToken) == "" {
		return MatchUnknown, errors.New("exact process identity requires a positive PID and start token")
	}
	token, live, err := platformProcessStartToken(pid)
	if err != nil {
		return MatchUnknown, fmt.Errorf("inspect process %d: %w", pid, err)
	}
	if !live {
		return MatchAbsent, nil
	}
	if token != startToken {
		return MatchDifferent, nil
	}
	return MatchExact, nil
}

// ProcessStartToken returns the OS incarnation token for pid, or an empty
// string when the process is not live or its identity cannot be inspected.
func ProcessStartToken(pid int) string {
	if pid <= 0 {
		return ""
	}
	token, live, err := platformProcessStartToken(pid)
	if err != nil || !live {
		return ""
	}
	return token
}

// ProcessMatches checks both PID and its exact start token. PID alone is not a
// safe identity because the operating system may reuse it.
func ProcessMatches(pid int, startToken string) bool {
	status, _ := InspectMatch(pid, startToken)
	return status == MatchExact
}
