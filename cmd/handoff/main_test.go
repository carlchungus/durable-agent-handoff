package main

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/carlchungus/durable-agent-handoff/supervisor"
)

func TestExecutionStartFileStdinUsesStrictV2Response(t *testing.T) {
	state, root := t.TempDir(), t.TempDir()
	if err := os.Chmod(state, 0o700); err != nil {
		t.Fatal(err)
	}
	request := `{"native_session":{"runtime":"codex","id":"exact-thread"},"prompt":"secret stdin prompt","runtime":{"name":"codex","sandbox":"read-only"},"root":"` + root + `","authority":{"requested_by":"human:arca","human_authorized":true,"sandbox":"read-only"},"finalizer":{"enabled":false},"budget":{"max_task_attempts":3,"max_launches":12},"idempotency_key":"arca-file-start-01"}`
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = write.WriteString(request); err != nil {
		t.Fatal(err)
	}
	_ = write.Close()
	previous := os.Stdin
	os.Stdin = read
	t.Cleanup(func() {
		os.Stdin = previous
		_ = read.Close()
	})
	var out, errOut bytes.Buffer
	if err = run([]string{"execution", "start", "--state", state, "--file", "-", "--json"}, &out, &errOut); err != nil {
		t.Fatalf("start: %v stderr=%s", err, errOut.String())
	}
	var response struct {
		Execution supervisor.Execution `json:"execution"`
		Receipt   supervisor.Receipt   `json:"receipt"`
	}
	if err = json.Unmarshal(out.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Execution.ID == "" || response.Receipt.Existing || strings.Contains(out.String(), "secret stdin prompt") {
		t.Fatalf("unexpected response=%s", out.String())
	}
	if err = run([]string{"execution", "start", "--state", state, "--file", "-", "--json"}, &out, &errOut); err == nil {
		t.Fatal("closed stdin unexpectedly accepted a second request")
	}
}

func TestStatusListReplyAndPauseUseSupervisorProjection(t *testing.T) {
	state, root := t.TempDir(), t.TempDir()
	if err := os.Chmod(state, 0o700); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	if err := run([]string{"start", "--state", state, "--root", root, "--runtime", "codex", "--session", "exact-thread", "--prompt", "work", "--sandbox", "read-only", "--authorized-by", "human:test", "--idempotency-key", "cli-v2-start-01", "--json"}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	var response struct {
		Execution supervisor.Execution `json:"execution"`
	}
	if err := json.Unmarshal(out.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Execution.ID == "" {
		t.Fatal("missing execution")
	}
	out.Reset()
	if err := run([]string{"list", "--state", state, "--json"}, &out, &errOut); err != nil || !strings.Contains(out.String(), string(response.Execution.ID)) {
		t.Fatalf("list=%s err=%v", out.String(), err)
	}
	out.Reset()
	if err := run([]string{"status", string(response.Execution.ID), "--state", state}, &out, &errOut); err != nil || !strings.Contains(out.String(), "status=queued") {
		t.Fatalf("status=%s err=%v", out.String(), err)
	}
	out.Reset()
	if err := run([]string{"execution", "pause", "--state", state, "--workflow", string(response.Execution.WorkflowID), "--json"}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"workflow_id"`) || !strings.Contains(out.String(), `"existing":false`) {
		t.Fatalf("pause=%s", out.String())
	}
}
