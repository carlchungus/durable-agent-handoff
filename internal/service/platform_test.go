package service

import (
	"reflect"
	"testing"
)

func TestDarwinServiceEnableUsesCurrentUIDAndDirectArgv(t *testing.T) {
	name, args, err := serviceEnableCommand("darwin", 501, "/private/handoff.plist")
	if err != nil {
		t.Fatal(err)
	}
	if name != "launchctl" {
		t.Fatalf("command=%q", name)
	}
	want := []string{"bootstrap", "gui/501", "/private/handoff.plist"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("argv=%q want=%q", args, want)
	}
	if _, _, err = serviceEnableCommand("darwin", -1, "/private/handoff.plist"); err == nil {
		t.Fatal("missing UID was accepted")
	}
}

func TestServiceEnableCommandDoesNotBuildShellInput(t *testing.T) {
	name, args, err := serviceEnableCommand("darwin", 42, "/tmp/unit; touch /tmp/escaped")
	if err != nil {
		t.Fatal(err)
	}
	if name != "launchctl" || len(args) != 3 || args[2] != "/tmp/unit; touch /tmp/escaped" {
		t.Fatalf("path was not preserved as one argv element: %q %q", name, args)
	}
}
