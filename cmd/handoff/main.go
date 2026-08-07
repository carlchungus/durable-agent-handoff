package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

var version = "dev"

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string, out, _ io.Writer) error {
	if len(args) == 0 {
		return cmdV2TUI(nil, out)
	}
	switch args[0] {
	case "version", "--version", "-v":
		_, err := fmt.Fprintln(out, version)
		return err
	case "init":
		return cmdV2Init(args[1:], out)
	case "start", "create":
		return cmdV2Start(args[1:], out)
	case "execution":
		return cmdExecution(args[1:], out)
	case "status":
		return cmdV2Status(args[1:], out)
	case "list":
		return cmdV2List(args[1:], out)
	case "events":
		return cmdV2Events(args[1:], out)
	case "run":
		return cmdV2Run(args[1:], out)
	case "serve":
		return cmdV2Serve(args[1:], out)
	case "service":
		return cmdV2Service(args[1:], out)
	case "github":
		return cmdV2GitHub(args[1:], out)
	case "preference":
		return cmdV2Preference(args[1:], out)
	case "tui":
		return cmdV2TUI(args[1:], out)
	case "activity":
		return cmdV2Activity(args[1:], out)
	case "reply":
		return cmdV2Reply(args[1:], out)
	case "pause":
		return cmdV2Pause(args[1:], out)
	case "agent":
		return cmdV2Agent(args[1:], out)
	case "agents":
		return cmdV2Agents(args[1:], out)
	case "help", "-h", "--help":
		_, err := fmt.Fprint(out, usage)
		return err
	default:
		return errors.New("unknown command; use handoff help")
	}
}

func stateDir(value string) string {
	if value != "" {
		return value
	}
	if value = os.Getenv("HANDOFF_HOME"); value != "" {
		return value
	}
	directory, err := os.UserConfigDir()
	if err != nil {
		return ".handoff"
	}
	return directory + string(os.PathSeparator) + "handoff"
}

func common(fs *flag.FlagSet) *string {
	return fs.String("state", "", "state directory (or HANDOFF_HOME)")
}

func reorderFlags(args []string, known map[string]bool) []string {
	flags, positional := []string{}, []string{}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		name := arg
		for index, char := range arg {
			if char == '=' {
				name = arg[:index]
				break
			}
		}
		takesValue, ok := known[name]
		if !ok {
			positional = append(positional, arg)
			continue
		}
		flags = append(flags, arg)
		if takesValue && len(arg) == len(name) && i+1 < len(args) {
			i++
			flags = append(flags, args[i])
		}
	}
	return append(flags, positional...)
}

func rejectUnknownFlags(args []string, known map[string]bool) error {
	for index := 0; index < len(args); index++ {
		arg := args[index]
		if arg == "-" || !strings.HasPrefix(arg, "-") {
			continue
		}
		name := arg
		for position, char := range arg {
			if char == '=' {
				name = arg[:position]
				break
			}
		}
		takesValue, ok := known[name]
		if !ok {
			return fmt.Errorf("unknown option %q", name)
		}
		if takesValue && name == arg && index+1 < len(args) {
			index++
		}
	}
	return nil
}

func writeJSON(out io.Writer, value any) error {
	encoder := jsonEncoder(out)
	return encoder.Encode(value)
}

type encoder interface {
	Encode(any) error
}

func jsonEncoder(out io.Writer) *json.Encoder {
	encoder := json.NewEncoder(out)
	encoder.SetEscapeHTML(false)
	return encoder
}

const usage = `handoff — Supervisor v2 durable execution

Usage:
  handoff start [--session EXACT_ID] --runtime codex --file - --idempotency-key KEY --authorized-by HUMAN
  handoff execution start --file - --json
  handoff execution pause --workflow ID --timeout 30s --json
  handoff status [EXECUTION_ID] [--json]
  handoff list [--json]
  handoff run WORKFLOW_ID [--once]
  handoff serve [--environment-json FILE] [--trust-mode workspace|full]
  handoff preference set ROLE --candidate runtime:model[:effort]
  handoff github merge --execution ID --repo OWNER/REPO --pr NUMBER --gate NAME --idempotency-key KEY --approved --json
  handoff reply --execution ID --activity ID --file -
  handoff activity list|read [--json]
  handoff tui [--snapshot]
  handoff execution import-v1 --source LEGACY_HOME --idempotency-key KEY
`
