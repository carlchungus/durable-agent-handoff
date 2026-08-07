// Package processidentity contains only exact OS process identity helpers.
// It is deliberately not a workflow state store or supervisor lease owner.
package processidentity

import (
	"strings"
)

// ProcessStartToken returns the OS incarnation token for pid, or an empty
// string when the process is not live or its identity cannot be inspected.
func ProcessStartToken(pid int) string {
	if pid <= 0 {
		return ""
	}
	return platformProcessStartToken(pid)
}

// ProcessMatches checks both PID and its exact start token. PID alone is not a
// safe identity because the operating system may reuse it.
func ProcessMatches(pid int, startToken string) bool {
	return pid > 0 && strings.TrimSpace(startToken) != "" && ProcessStartToken(pid) == startToken
}
