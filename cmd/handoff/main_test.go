package main

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/carlchungus/durable-agent-handoff/internal/core"
	"github.com/carlchungus/durable-agent-handoff/internal/team"
)

func TestStartAndStatusJSONContract(t *testing.T) {
	state := t.TempDir()
	root := t.TempDir()
	var out, errOut bytes.Buffer
	if err := run([]string{"start", "--state", state, "--goal", "test the CLI", "--root", root, "--runtime", "codex", "--json"}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	var created core.Workflow
	if err := json.Unmarshal(out.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.ID == "" || created.Nodes["lead"] == nil {
		t.Fatalf("created=%#v", created)
	}
	out.Reset()
	if err := run([]string{"status", "--state", state, created.ID, "--json"}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	var loaded core.Workflow
	if err := json.Unmarshal(out.Bytes(), &loaded); err != nil {
		t.Fatal(err)
	}
	if loaded.ID != created.ID || loaded.Goal != "test the CLI" {
		t.Fatalf("loaded=%#v", loaded)
	}
}

func TestFinalizationRequiresExplicitGate(t *testing.T) {
	var out, errOut bytes.Buffer
	err := run([]string{"start", "--state", t.TempDir(), "--goal", "ship", "--root", t.TempDir(), "--finalize-repo", "owner/repo"}, &out, &errOut)
	if err == nil {
		t.Fatal("expected missing gate error")
	}
}

func TestPreferenceCLIStoresOrderedLadder(t *testing.T) {
	state := t.TempDir()
	var out, errOut bytes.Buffer
	err := run([]string{"preference", "set", "--state", state, "planner", "--candidate", "claude:opus:xhigh", "--candidate", "codex:gpt-5.6-sol:xhigh"}, &out, &errOut)
	if err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err = run([]string{"preference", "list", "--state", state}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(out.Bytes(), []byte(`"opus"`)) || !bytes.Contains(out.Bytes(), []byte(`"gpt-5.6-sol"`)) {
		t.Fatalf("output=%s", out.String())
	}
}

func TestTeamCLIProducesMachineReadableState(t *testing.T) {
	state := t.TempDir()
	var out, errOut bytes.Buffer
	if err := run([]string{"team", "create", "--state", state, "--name", "review", "--workflow", "wf_1", "--lead", "captain"}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	var created team.Team
	if err := json.Unmarshal(out.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.ID == "" || created.LeadID != "captain" || created.WorkflowID != "wf_1" {
		t.Fatalf("created=%+v", created)
	}
	out.Reset()
	if err := run([]string{"team", "status", "--state", state, created.ID}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	var loaded team.Team
	if err := json.Unmarshal(out.Bytes(), &loaded); err != nil {
		t.Fatal(err)
	}
	if loaded.ID != created.ID || loaded.Members["captain"] == nil {
		t.Fatalf("loaded=%+v", loaded)
	}
}
