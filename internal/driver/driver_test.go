package driver

import (
	"bufio"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/carlchungus/durable-agent-handoff/internal/supervisor"
)

func launchRequest(runtime string) LaunchRequest {
	return LaunchRequest{Runtime: supervisor.RuntimeSpec{Name: runtime, Sandbox: supervisor.SandboxWorkspaceWrite}, Worktree: "/workspace", Prompt: "prompt", Session: supervisor.NativeSessionIdentity{Runtime: runtime, ID: "exact-native-session"}, SchemaPath: "/tmp/schema", ResultPath: "/tmp/result"}
}

func TestDriversOwnExactResumeLaunches(t *testing.T) {
	tests := []struct {
		driver Driver
		want   string
	}{
		{driver: Codex{}, want: "resume exact-native-session"},
		{driver: Claude{}, want: "--resume exact-native-session"},
		{driver: Pi{}, want: "--session exact-native-session"},
	}
	for _, test := range tests {
		t.Run(test.driver.Name(), func(t *testing.T) {
			launch, err := test.driver.Build(launchRequest(test.driver.Name()))
			if err != nil {
				t.Fatal(err)
			}
			joined := strings.Join(launch.Args, " ")
			if !strings.Contains(joined, test.want) {
				t.Fatalf("launch does not resume exact identity: %s", joined)
			}
		})
	}
}

func TestDriversLaunchNewSessionsWithoutResumeSelectors(t *testing.T) {
	for _, runtimeName := range []string{"codex", "claude", "pi"} {
		t.Run(runtimeName, func(t *testing.T) {
			request := launchRequest(runtimeName)
			request.Session.ID = ""
			built, err := Lookup(runtimeName)
			if err != nil {
				t.Fatal(err)
			}
			launch, err := built.Build(request)
			if err != nil {
				t.Fatal(err)
			}
			joined := strings.Join(launch.Args, " ")
			if strings.Contains(joined, "resume") || strings.Contains(joined, "--session") {
				t.Fatalf("new session launch selected exact resume: %s", joined)
			}
			if runtimeName == "pi" && !strings.Contains(joined, "--print") {
				t.Fatalf("Pi new session launch is not noninteractive: %s", joined)
			}
		})
	}
}

func TestDriversNeverPlacePromptInArgvOrServiceData(t *testing.T) {
	secret := "prompt-secret-that-must-stay-on-stdin"
	for _, runtimeName := range []string{"codex", "claude", "pi"} {
		t.Run(runtimeName, func(t *testing.T) {
			request := launchRequest(runtimeName)
			request.Prompt = secret
			launch, err := Lookup(runtimeName)
			if err != nil {
				t.Fatal(err)
			}
			built, err := launch.Build(request)
			if err != nil {
				t.Fatal(err)
			}
			if !built.PromptOnStdin || strings.Contains(strings.Join(built.Args, "\x00"), secret) {
				t.Fatalf("prompt was not stdin-only: %+v", built)
			}
		})
	}
}

func TestDriversUseRuntimeOwnedCompletionContract(t *testing.T) {
	for _, runtimeName := range []string{"codex", "claude", "pi"} {
		t.Run(runtimeName, func(t *testing.T) {
			request := launchRequest(runtimeName)
			request.Prompt = "ordinary work that must produce a durable result"
			built, err := Lookup(runtimeName)
			if err != nil {
				t.Fatal(err)
			}
			launch, err := built.Build(request)
			if err != nil {
				t.Fatal(err)
			}
			if !launch.PromptOnStdin || !strings.Contains(launch.Prompt, completionContract) || !strings.Contains(launch.Prompt, request.Prompt) {
				t.Fatalf("runtime-owned completion contract was not attached to stdin prompt: %+v", launch)
			}
			if strings.Contains(strings.Join(launch.Args, "\x00"), completionContract) || strings.Contains(strings.Join(launch.Args, "\x00"), request.Prompt) {
				t.Fatalf("completion contract or prompt leaked into argv: %+v", launch.Args)
			}
		})
	}
}

func TestClaudeAndPiContractResultCompletesOrdinaryWork(t *testing.T) {
	resultJSON := `{"status":"completed","summary":"ordinary work finished"}`
	encodedResult := strconv.Quote(resultJSON)
	tests := []struct {
		name    string
		decoder Decoder
		lines   []string
	}{
		{name: "claude", decoder: Claude{}.NewDecoder(), lines: []string{
			`{"type":"system","subtype":"init","session_id":"claude-contract"}`,
			`{"type":"assistant","message":{"content":[{"type":"text","text":` + encodedResult + `}]}}`,
		}},
		{name: "pi", decoder: Pi{}.NewDecoder(), lines: []string{
			`{"type":"session","version":3,"id":"pi-contract-session"}`,
			`{"type":"agent_start"}`,
			`{"type":"turn_start"}`,
			`{"type":"message_update","assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":"working"}}`,
			`{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":` + encodedResult + `}]}}`,
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var resultCount int
			for _, line := range test.lines {
				milestones, err := test.decoder.DecodeLine([]byte(line))
				if err != nil {
					t.Fatal(err)
				}
				for _, milestone := range milestones {
					if milestone.Kind == supervisor.MilestoneResult {
						resultCount++
						if milestone.Result == nil || milestone.Result.Status != "completed" || milestone.Result.Summary != "ordinary work finished" {
							t.Fatalf("wrong contract result: %+v", milestone)
						}
					}
				}
			}
			if resultCount != 1 {
				t.Fatalf("ordinary work did not complete through the exact contract: %d results", resultCount)
			}
		})
	}
}

func TestWorkerResultDecoderRejectsUnknownFields(t *testing.T) {
	if result, ok := decodeWorkerResult([]byte(`{"status":"completed","summary":"done","unexpected":"authority"}`)); ok || result != nil {
		t.Fatalf("worker payload with an unknown authority field was accepted as a Result: result=%+v ok=%v", result, ok)
	}
	if result, ok := decodeWorkerResult([]byte(`{"status":"completed","summary":"done"}`)); !ok || result == nil || result.Status != "completed" {
		t.Fatalf("strict ordinary Result was rejected: result=%+v ok=%v", result, ok)
	}
	if result, ok := decodeWorkerResult([]byte(`{"status":"continue","summary":"candidate was unsuitable"}`)); !ok || result == nil || result.Status != "continue" {
		t.Fatalf("first-class continuation was rejected: result=%+v ok=%v", result, ok)
	}
}

func TestTrustModeIsAppliedByNativeDrivers(t *testing.T) {
	for _, runtimeName := range []string{"codex", "claude", "pi"} {
		t.Run(runtimeName, func(t *testing.T) {
			request := launchRequest(runtimeName)
			request.TrustMode = TrustFull
			built, err := Lookup(runtimeName)
			if err != nil {
				t.Fatal(err)
			}
			launch, err := built.Build(request)
			if err != nil {
				t.Fatal(err)
			}
			joined := strings.Join(launch.Args, " ")
			if runtimeName == "codex" && !strings.Contains(joined, "--dangerously-bypass-approvals-and-sandbox") {
				t.Fatalf("Codex full trust flag missing: %s", joined)
			}
			if runtimeName == "claude" && !strings.Contains(joined, "--dangerously-skip-permissions") {
				t.Fatalf("Claude full trust flag missing: %s", joined)
			}
			if runtimeName == "pi" && !strings.Contains(joined, "--approve") {
				t.Fatalf("Pi full trust flag missing: %s", joined)
			}
		})
	}
}

func TestPiWorkspaceTrustDisablesApproval(t *testing.T) {
	request := launchRequest("pi")
	launch, err := (Pi{}).Build(request)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(launch.Args, " ")
	if !strings.Contains(joined, "--print") || !strings.Contains(joined, "--no-approve") || strings.Contains(joined, "--approve") {
		t.Fatalf("Pi workspace launch has wrong noninteractive trust flags: %s", joined)
	}
}

func TestCodexThreadStartedBindsSessionButDoesNotStartTurn(t *testing.T) {
	decoder := Codex{}.NewDecoder()
	milestones, err := decoder.DecodeLine([]byte(`{"type":"thread.started","thread_id":"thread-123"}`))
	if err != nil || len(milestones) != 1 || milestones[0].Kind != supervisor.MilestoneSessionBound {
		t.Fatalf("thread event=%+v err=%v", milestones, err)
	}
	if milestones[0].Kind == supervisor.MilestoneTurnStarted {
		t.Fatal("thread.started was decoded as a healthy turn")
	}
	milestones, err = decoder.DecodeLine([]byte(`{"type":"turn.started"}`))
	if err != nil || len(milestones) != 1 || milestones[0].Kind != supervisor.MilestoneTurnStarted {
		t.Fatalf("turn event=%+v err=%v", milestones, err)
	}
}

func TestCodexDecoderPreservesRuntimeFailure(t *testing.T) {
	decoder := Codex{}.NewDecoder()
	if _, err := decoder.DecodeLine([]byte(`{"type":"turn.started"}`)); err != nil {
		t.Fatal(err)
	}
	_, err := decoder.DecodeLine([]byte(`{"type":"error","message":"invalid response schema"}`))
	if err == nil || !strings.Contains(err.Error(), "invalid response schema") {
		t.Fatalf("runtime failure was hidden: %v", err)
	}
}

func TestDecodersIgnoreNestedFakeSessionIDs(t *testing.T) {
	tests := []struct {
		name    string
		decoder Decoder
		raw     string
	}{
		{name: "codex", decoder: Codex{}.NewDecoder(), raw: `{"type":"item.completed","item":{"type":"agent_message","text":"progress","metadata":{"thread_id":"fake"}}}`},
		{name: "claude", decoder: Claude{}.NewDecoder(), raw: `{"type":"user","message":{"content":[{"type":"tool_result","content":{"session_id":"fake"}}]}}`},
		{name: "pi", decoder: Pi{}.NewDecoder(), raw: `{"type":"message_update","text":"progress","metadata":{"session_file":"fake"}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			milestones, err := test.decoder.DecodeLine([]byte(test.raw))
			if err != nil {
				t.Fatal(err)
			}
			for _, milestone := range milestones {
				if milestone.Kind == supervisor.MilestoneSessionBound {
					t.Fatalf("nested fake identity was scraped: %+v", milestone)
				}
			}
		})
	}
}

func TestClaudeDecoderEmitsTypedTurnEffectProgressAndOneResult(t *testing.T) {
	decoder := Claude{}.NewDecoder()
	init, err := decoder.DecodeLine([]byte(`{"type":"system","subtype":"init","session_id":"claude-exact"}`))
	if err != nil || len(init) != 1 || init[0].Kind != supervisor.MilestoneSessionBound {
		t.Fatalf("init=%+v err=%v", init, err)
	}
	assistant, err := decoder.DecodeLine([]byte(`{"type":"assistant","message":{"content":[{"type":"tool_use","id":"tool-1","name":"Read"},{"type":"text","text":"meaningful semantic progress"}]}}`))
	if err != nil || len(assistant) != 3 || assistant[0].Kind != supervisor.MilestoneTurnStarted || assistant[1].Kind != supervisor.MilestoneEffectStarted || assistant[2].Kind != supervisor.MilestoneMeaningfulProgress {
		t.Fatalf("assistant=%+v err=%v", assistant, err)
	}
	resultLine := `{"type":"result","result":"{\"status\":\"completed\",\"summary\":\"done\"}"}`
	result, err := decoder.DecodeLine([]byte(resultLine))
	if err != nil || len(result) != 1 || result[0].Kind != supervisor.MilestoneResult {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	duplicate, err := decoder.DecodeLine([]byte(resultLine))
	if err != nil || len(duplicate) != 0 {
		t.Fatalf("duplicate terminal result was emitted: %+v err=%v", duplicate, err)
	}
}

func TestPiDecoderClassifiesPreTurnStartupFailure(t *testing.T) {
	decoder := Pi{}.NewDecoder()
	milestones, err := decoder.DecodeLine([]byte(`{"type":"message_end","message":{"role":"assistant","content":[],"stopReason":"error","errorMessage":"adapter config died"}}`))
	if err != nil || len(milestones) != 1 || milestones[0].Kind != supervisor.MilestoneAdapterStartFailed {
		t.Fatalf("milestones=%+v err=%v", milestones, err)
	}
}

func TestPiProviderFixturesCompleteNewAndExactResumeThroughStdinContract(t *testing.T) {
	tests := []struct {
		name       string
		fixture    string
		sessionID  string
		wantNative string
		wantResult string
	}{
		{name: "new", fixture: "pi-new-session.jsonl", wantNative: "pi-new-session", wantResult: "new Pi work finished"},
		{name: "exact resume", fixture: "pi-exact-resume.jsonl", sessionID: "pi-exact-resume", wantNative: "pi-exact-resume", wantResult: "exact Pi resume finished"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := launchRequest("pi")
			request.Prompt = "provider-realistic prompt stays on stdin"
			request.Session.ID = test.sessionID
			launch, err := (Pi{}).Build(request)
			if err != nil {
				t.Fatal(err)
			}
			if !launch.PromptOnStdin || strings.Contains(strings.Join(launch.Args, "\x00"), request.Prompt) || !strings.Contains(launch.Prompt, completionContract) {
				t.Fatalf("Pi prompt contract escaped stdin: launch=%+v", launch)
			}
			if test.sessionID == "" && strings.Contains(strings.Join(launch.Args, " "), "--session") {
				t.Fatalf("new Pi launch selected a resume identity: %v", launch.Args)
			}
			if test.sessionID != "" && !strings.Contains(strings.Join(launch.Args, " "), "--session "+test.sessionID) {
				t.Fatalf("Pi launch did not resume exact identity: %v", launch.Args)
			}

			file, err := os.Open(filepath.Join("testdata", test.fixture))
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			decoder := Pi{}.NewDecoder()
			var bound string
			var progress, effects, results int
			var summary string
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				milestones, decodeErr := decoder.DecodeLine(scanner.Bytes())
				if decodeErr != nil {
					t.Fatalf("decode %q: %v", scanner.Text(), decodeErr)
				}
				for _, milestone := range milestones {
					switch milestone.Kind {
					case supervisor.MilestoneSessionBound:
						bound = milestone.Session.ID
					case supervisor.MilestoneMeaningfulProgress:
						progress++
					case supervisor.MilestoneEffectStarted:
						effects++
					case supervisor.MilestoneResult:
						results++
						summary = milestone.Result.Summary
					}
				}
			}
			if err := scanner.Err(); err != nil {
				t.Fatal(err)
			}
			if bound != test.wantNative || progress == 0 || (test.sessionID == "" && effects != 1) || results != 1 || summary != test.wantResult {
				t.Fatalf("Pi fixture milestones bound=%q progress=%d effects=%d results=%d summary=%q", bound, progress, effects, results, summary)
			}
		})
	}
}

func TestPiDecoderUsesOnlyDocumentedNestedResultAndSessionFields(t *testing.T) {
	decoder := Pi{}.NewDecoder()
	if milestones, err := decoder.DecodeLine([]byte(`{"type":"session","session_id":"invented"}`)); err == nil || len(milestones) != 0 {
		t.Fatalf("invented Pi session field was accepted: milestones=%+v err=%v", milestones, err)
	}
	if milestones, err := decoder.DecodeLine([]byte(`{"type":"message_update","text":"{\"status\":\"completed\",\"summary\":\"fake\"}"}`)); err != nil || len(milestones) != 0 {
		t.Fatalf("invented top-level Pi result field was decoded: milestones=%+v err=%v", milestones, err)
	}
	if milestones, err := decoder.DecodeLine([]byte(`{"type":"message_update","assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":"nested progress"}}`)); err != nil || len(milestones) != 0 {
		t.Fatalf("progress before a turn was accepted: milestones=%+v err=%v", milestones, err)
	}
	if milestones, err := decoder.DecodeLine([]byte(`{"type":"turn_start"}`)); err != nil || len(milestones) != 1 || milestones[0].Kind != supervisor.MilestoneTurnStarted {
		t.Fatalf("turn=%+v err=%v", milestones, err)
	}
	if milestones, err := decoder.DecodeLine([]byte(`{"type":"message_update","assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":"nested progress"}}`)); err != nil || len(milestones) != 1 || milestones[0].Kind != supervisor.MilestoneMeaningfulProgress {
		t.Fatalf("nested progress=%+v err=%v", milestones, err)
	}
	if milestones, err := decoder.DecodeLine([]byte(`{"type":"message_end","message":{"role":"user","content":[{"type":"text","text":"{\"status\":\"completed\",\"summary\":\"fake\"}"}]}}`)); err != nil || len(milestones) != 0 {
		t.Fatalf("non-assistant authoritative message was decoded: milestones=%+v err=%v", milestones, err)
	}
}

func TestPiDecoderUsesAgentEndMessagesAsAuthoritativeResult(t *testing.T) {
	decoder := Pi{}.NewDecoder()
	for _, line := range []string{
		`{"type":"session","version":3,"id":"pi-agent-end"}`,
		`{"type":"turn_start"}`,
	} {
		if _, err := decoder.DecodeLine([]byte(line)); err != nil {
			t.Fatal(err)
		}
	}
	milestones, err := decoder.DecodeLine([]byte(`{"type":"agent_end","messages":[{"role":"user","content":[{"type":"text","text":"prompt"}]},{"role":"assistant","content":[{"type":"text","text":"{\"status\":\"completed\",\"summary\":\"agent end finished\"}"}],"stopReason":"stop"}],"willRetry":false}`))
	if err != nil || len(milestones) != 1 || milestones[0].Kind != supervisor.MilestoneResult || milestones[0].Result.Summary != "agent end finished" {
		t.Fatalf("agent_end milestones=%+v err=%v", milestones, err)
	}
}

func TestClaudeProviderUnavailableUsesTypedCodeNotMessageGuess(t *testing.T) {
	typed := Claude{}.NewDecoder()
	milestones, err := typed.DecodeLine([]byte(`{"type":"system","subtype":"error","error_code":"overloaded","error":"try later"}`))
	if err != nil || len(milestones) != 1 || milestones[0].Kind != supervisor.MilestoneProviderUnavailable {
		t.Fatalf("typed provider failure=%+v err=%v", milestones, err)
	}
	untyped := Claude{}.NewDecoder()
	milestones, err = untyped.DecodeLine([]byte(`{"type":"system","subtype":"error","error":"message happens to say rate limit"}`))
	if err != nil || len(milestones) != 1 || milestones[0].Kind != supervisor.MilestoneAdapterStartFailed {
		t.Fatalf("untyped message was heuristically classified: %+v err=%v", milestones, err)
	}
}

func TestLifecycleEmitsExplicitAdapterFailureAndExit(t *testing.T) {
	driver := Claude{}
	spawned := driver.Spawned(supervisor.ProcessIdentity{PID: 42, StartToken: "birth"})
	failed := driver.StartFailed(errors.New("exec missing"))
	exited := driver.Exited(127, errors.New("exit status 127"))
	if spawned.Kind != supervisor.MilestoneProcessSpawned || spawned.Process == nil || failed.Kind != supervisor.MilestoneAdapterStartFailed || exited.Kind != supervisor.MilestoneExit || exited.Exit == nil || exited.Exit.Code != 127 {
		t.Fatalf("spawned=%+v failed=%+v exited=%+v", spawned, failed, exited)
	}
}

func TestReadOnlyIsEnforcedByEachDriver(t *testing.T) {
	request := launchRequest("pi")
	request.Runtime.Sandbox = supervisor.SandboxReadOnly
	if _, err := (Pi{}).Build(request); err == nil {
		t.Fatal("Pi silently widened read-only authority")
	}
	request = launchRequest("claude")
	request.Runtime.Sandbox = supervisor.SandboxReadOnly
	launch, err := (Claude{}).Build(request)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(launch.Args, " ")
	if strings.Contains(joined, "Edit") || strings.Contains(joined, "Write") || strings.Contains(joined, "Bash") {
		t.Fatalf("Claude read-only launch contains write tools: %s", joined)
	}
}
