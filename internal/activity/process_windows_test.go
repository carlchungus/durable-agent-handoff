//go:build windows

package activity

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestWindowsJobHelper(t *testing.T) {
	if os.Getenv("HANDOFF_TEST_WINDOWS_JOB_HELPER") != "1" {
		return
	}
	if err := os.WriteFile(os.Getenv("HANDOFF_TEST_WINDOWS_JOB_READY"), []byte("ready"), 0o600); err != nil {
		t.Fatal(err)
	}
	for {
		time.Sleep(time.Hour)
	}
}

func TestWindowsJobObjectContainsAndStopsRunner(t *testing.T) {
	ready := filepath.Join(t.TempDir(), "ready")
	command := exec.Command(os.Args[0], "-test.run=^TestWindowsJobHelper$")
	command.Env = append(os.Environ(), "HANDOFF_TEST_WINDOWS_JOB_HELPER=1", "HANDOFF_TEST_WINDOWS_JOB_READY="+ready)
	reservation, err := prepareProcessTree(command)
	if err != nil {
		t.Fatal(err)
	}
	defer reservation.close()
	if err = command.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err = reservation.bind(command.Process.Pid, "test-token"); err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err = os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = command.Process.Kill()
			_ = command.Wait()
			t.Fatal("contained runner did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err = reservation.stop(command.Process.Pid); err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatal(err)
	}
	if err = command.Wait(); err == nil {
		t.Fatal("stopped Job Object runner exited successfully instead of being terminated")
	}
}
