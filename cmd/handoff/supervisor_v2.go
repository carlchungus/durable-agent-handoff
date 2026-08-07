package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/carlchungus/durable-agent-handoff/supervisor"
)

func cmdExecution(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: handoff execution start|status|list|pause|import-v1")
	}
	switch args[0] {
	case "start":
		return cmdExecutionStart(args[1:], out)
	case "status":
		return cmdV2Status(args[1:], out)
	case "list":
		return cmdV2List(args[1:], out)
	case "pause":
		return cmdV2Pause(args[1:], out)
	case "import-v1":
		return cmdExecutionImport(args[1:], out)
	default:
		return fmt.Errorf("unknown execution command %q", args[0])
	}
}

func cmdExecutionStart(args []string, out io.Writer) error {
	file := ""
	for index, arg := range args {
		if arg == "--file" && index+1 < len(args) {
			file = args[index+1]
		}
		if strings.HasPrefix(arg, "--file=") {
			file = strings.TrimPrefix(arg, "--file=")
		}
	}
	if file != "-" {
		return errors.New("execution start requires --file - --json; prompt input is stdin-only")
	}
	return cmdV2Start(args, out)
}

func cmdExecutionImport(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("execution import-v1", flag.ContinueOnError)
	state := common(fs)
	source := fs.String("source", "", "legacy HANDOFF_HOME")
	key := fs.String("idempotency-key", "", "stable import request identity")
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(reorderFlags(args, map[string]bool{"--state": true, "--source": true, "--idempotency-key": true, "--json": false})); err != nil {
		return err
	}
	store, err := supervisor.Open(stateDir(*state), supervisor.Options{})
	if err != nil {
		return err
	}
	receipt, err := store.ImportV1(context.Background(), supervisor.ImportV1Input{SourceRoot: *source, IdempotencyKey: *key})
	if err != nil {
		return err
	}
	if *jsonOut {
		return writeJSON(out, receipt)
	}
	fmt.Fprintf(out, "legacy_digest=%s sequence=%d existing=%t\n", receipt.ResourceID, receipt.Sequence, receipt.Existing)
	return nil
}
