package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

type capture struct {
	Args  []string `json:"args"`
	Stdin string   `json:"stdin"`
}

func main() {
	prompt, err := io.ReadAll(os.Stdin)
	if err != nil {
		fatalf("read prompt: %v", err)
	}
	encoded, err := json.Marshal(capture{Args: os.Args[1:], Stdin: string(prompt)})
	if err != nil {
		fatalf("encode capture: %v", err)
	}
	if err = os.WriteFile(os.Getenv("HANDOFF_E2E_CODEX_CAPTURE"), append(encoded, '\n'), 0o600); err != nil {
		fatalf("write capture: %v", err)
	}
	for _, event := range []any{
		map[string]any{"type": "thread.started", "thread_id": "e2e-native-session"},
		map[string]any{"type": "turn.started"},
		map[string]any{"type": "item.started", "item": map[string]any{"id": "fixture-command", "type": "command_execution", "command": "fixture"}},
		map[string]any{"type": "item.completed", "item": map[string]any{"id": "fixture-result", "type": "agent_message", "text": `{"status":"completed","summary":"black-box fixture completed"}`}},
	} {
		if err = json.NewEncoder(os.Stdout).Encode(event); err != nil {
			fatalf("encode runtime event: %v", err)
		}
	}
}

func fatalf(format string, values ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", values...)
	os.Exit(1)
}
