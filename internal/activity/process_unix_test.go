//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package activity

import (
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/runstate"
)

func TestStopKillsExactProcessTreeAndLeavesUnrelatedProcess(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	unrelated := exec.Command("sleep", "60")
	if err = unrelated.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unrelated.Process.Kill(); _, _ = unrelated.Process.Wait() })
	unrelatedToken := runstate.ProcessStartToken(unrelated.Process.Pid)

	supervisor := &Supervisor{Store: store}
	tracked, attempt, err := supervisor.Start(Descriptor{
		ID:      "activity_eeeeeeeeeeeeeeeeeeeeeeee",
		Work:    WorkSpec{Kind: "command", Cwd: t.TempDir(), Intent: "tree stop test"},
		Command: []string{"sh", "-c", "sleep 60 & echo $!; wait"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var descendantPID int
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		chunk, _ := store.ReadOutput(tracked.ID, OutputCursor{AttemptID: attempt.ID, Stream: StreamStdout, OutputID: attempt.Stdout.ID}, 4096)
		descendantPID, _ = strconv.Atoi(strings.TrimSpace(string(chunk.Data)))
		if descendantPID > 0 && runstate.ProcessStartToken(descendantPID) != "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if descendantPID == 0 {
		t.Fatal("descendant process was not observed")
	}
	if _, err = supervisor.Stop(tracked.ID); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(3 * time.Second)
	for runstate.ProcessStartToken(descendantPID) != "" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if runstate.ProcessStartToken(descendantPID) != "" {
		t.Fatalf("descendant %d survived exact process-group stop", descendantPID)
	}
	if got := runstate.ProcessStartToken(unrelated.Process.Pid); got == "" || got != unrelatedToken {
		t.Fatalf("unrelated process was affected: before=%q after=%q", unrelatedToken, got)
	}
}
