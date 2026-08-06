package activity

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGatedRunnerDoesNotExecuteBeforeDurableRelease(t *testing.T) {
	target := filepath.Join(t.TempDir(), "executed")
	gated, err := PrepareGatedCommand([]string{os.Args[0], "-test.run=^TestGatedRunnerTargetHelper$", "--", target}, t.TempDir(), []string{"HANDOFF_GATED_TARGET=1"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = gated.Command.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, err = os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("target ran before durable release: %v", err)
	}
	gated.Abort()
	if err = gated.Command.Wait(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("target ran after gate EOF: %v", err)
	}
}

func TestGatedRunnerTargetHelper(t *testing.T) {
	if os.Getenv("HANDOFF_GATED_TARGET") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" && i+1 < len(os.Args) {
			if err := os.WriteFile(os.Args[i+1], []byte("executed"), 0o600); err != nil {
				os.Exit(2)
			}
			os.Exit(0)
		}
	}
	os.Exit(3)
}
