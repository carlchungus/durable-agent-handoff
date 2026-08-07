package evaluator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/carlchungus/durable-agent-handoff/internal/supervisor"
)

func TestOpenRouterEvaluatorUsesFreshToollessStructuredRequest(t *testing.T) {
	var captured map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer private-key" {
			t.Fatalf("authorization header was not isolated to evaluator request")
		}
		if err := json.NewDecoder(request.Body).Decode(&captured); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"{\"outcome\":\"continue\",\"reason\":\"The rejected candidate is local to an open-ended campaign.\",\"blocker_kind\":\"\",\"question\":\"\"}"}}]}`))
	}))
	defer server.Close()

	client := OpenRouter{APIKey: "private-key", Endpoint: server.URL, HTTPClient: server.Client(), Mode: ModeStructured}
	decision, err := client.Evaluate(context.Background(), Request{
		Model:  "deepseek/deepseek-v4-flash-0731",
		Goal:   "Ship 100 safe type-hardening pull requests; skip unsuitable candidates",
		Prompt: "Find and ship the next safe type-hardening change",
		Turn: supervisor.WorkerResult{
			Status:  "needs_human",
			Summary: "This candidate cannot be changed safely",
		},
		Evidence: []string{"turn started", "no files changed"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Outcome != "continue" || decision.Model != "deepseek/deepseek-v4-flash-0731" {
		t.Fatalf("decision=%+v", decision)
	}
	if captured["model"] != "deepseek/deepseek-v4-flash-0731" {
		t.Fatalf("model=%v", captured["model"])
	}
	messages, err := json.Marshal(captured["messages"])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(messages), "does not grant new authority") {
		t.Fatalf("evaluator prompt omitted immutable authority semantics: %s", messages)
	}
	for _, want := range []string{"human is unavailable", "draft PR", "external CI", "another independent candidate"} {
		if !strings.Contains(string(messages), want) {
			t.Fatalf("evaluator prompt omitted unattended behavior %q: %s", want, messages)
		}
	}
	tools, ok := captured["tools"].([]any)
	if !ok || len(tools) != 0 {
		t.Fatalf("evaluator unexpectedly received tools: %#v", captured["tools"])
	}
	format, ok := captured["response_format"].(map[string]any)
	if !ok || format["type"] != "json_schema" {
		t.Fatalf("strict response format missing: %#v", captured["response_format"])
	}
	provider, ok := captured["provider"].(map[string]any)
	if !ok || provider["require_parameters"] != true {
		t.Fatalf("structured-output provider routing was not required: %#v", captured["provider"])
	}
}

func TestOpenRouterEvaluatorAcceptsOnlyForcedDecisionTool(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		var captured map[string]any
		if err := json.NewDecoder(request.Body).Decode(&captured); err != nil {
			t.Fatal(err)
		}
		if _, ok := captured["response_format"]; ok {
			t.Fatal("tool-call mode also sent response_format")
		}
		choice, ok := captured["tool_choice"].(map[string]any)
		if !ok || choice["type"] != "function" {
			t.Fatalf("forced tool choice missing: %#v", captured["tool_choice"])
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"tool_calls":[{"function":{"name":"submit_turn_decision","arguments":"{\"outcome\":\"continue\",\"reason\":\"Work is only starting.\",\"blocker_kind\":\"none\",\"question\":\"none\"}"}}]}}]}`))
	}))
	defer server.Close()
	client := OpenRouter{APIKey: "private-key", Endpoint: server.URL, HTTPClient: server.Client(), Mode: ModeToolCall}
	decision, err := client.Evaluate(context.Background(), Request{Model: DefaultModel, Goal: "finish the work", Prompt: "do the work", Turn: supervisor.WorkerResult{Status: "completed", Summary: "I will begin"}})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Outcome != "continue" || decision.BlockerKind != "" || decision.Question != "" {
		t.Fatalf("decision=%+v", decision)
	}
}
