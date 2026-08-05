package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/carlchungus/durable-agent-handoff/internal/core"
)

const scriptPolicyVersion = "workflow-script-policy-v1"

type Script struct {
	Filename string `json:"filename"`
	Source   string `json:"-"`
	Hash     string `json:"hash"`
}

type VMLimits struct {
	MaxSourceBytes  int64         `json:"max_source_bytes"`
	MaxInputBytes   int           `json:"max_input_bytes"`
	MaxOutputBytes  int           `json:"max_output_bytes"`
	MemoryBytes     uint64        `json:"memory_bytes"`
	MaxStackBytes   uint64        `json:"max_stack_bytes"`
	InstructionFuel uint64        `json:"instruction_fuel"`
	Timeout         time.Duration `json:"timeout"`
	MaxMutations    int           `json:"max_mutations"`
}

func DefaultVMLimits() VMLimits {
	return VMLimits{
		MaxSourceBytes:  256 << 10,
		MaxInputBytes:   4 << 20,
		MaxOutputBytes:  256 << 10,
		MemoryBytes:     64 << 20,
		MaxStackBytes:   1 << 20,
		InstructionFuel: 250_000,
		Timeout:         5 * time.Second,
		MaxMutations:    128,
	}
}

func (l VMLimits) validate() error {
	if l.MaxSourceBytes < 1 || l.MaxInputBytes < 1 || l.MaxOutputBytes < 1 {
		return errors.New("source, input, and output limits must be positive")
	}
	if l.MemoryBytes < 8<<20 {
		return errors.New("memory limit must be at least 8 MiB for QuickJS")
	}
	if l.MaxStackBytes < 64<<10 || l.MaxStackBytes > l.MemoryBytes {
		return errors.New("stack limit must be between 64 KiB and the memory limit")
	}
	if l.InstructionFuel < 1 || l.Timeout <= 0 || l.MaxMutations < 1 {
		return errors.New("instruction, time, and mutation limits must be positive")
	}
	return nil
}

// LoadScript resolves symlinks before enforcing the caller's explicit roots.
// The returned bytes are immutable from the VM's perspective; the sandbox is
// never given the path or any filesystem capability.
func LoadScript(path string, allowedRoots []string, maxBytes int64) (Script, error) {
	if strings.TrimSpace(path) == "" {
		return Script{}, errors.New("workflow script path is required")
	}
	if len(allowedRoots) == 0 {
		return Script{}, errors.New("at least one allowed script root is required")
	}
	if maxBytes < 1 {
		return Script{}, errors.New("workflow script size limit must be positive")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return Script{}, fmt.Errorf("resolve workflow script: %w", err)
	}
	resolved, err = filepath.Abs(resolved)
	if err != nil {
		return Script{}, fmt.Errorf("resolve workflow script: %w", err)
	}
	allowed := false
	for _, root := range allowedRoots {
		r, rootErr := filepath.EvalSymlinks(root)
		if rootErr != nil {
			return Script{}, fmt.Errorf("resolve allowed script root %q: %w", root, rootErr)
		}
		r, rootErr = filepath.Abs(r)
		if rootErr != nil {
			return Script{}, fmt.Errorf("resolve allowed script root %q: %w", root, rootErr)
		}
		rel, relErr := filepath.Rel(r, resolved)
		if relErr == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			allowed = true
			break
		}
	}
	if !allowed {
		return Script{}, fmt.Errorf("workflow script %q is outside allowed roots", resolved)
	}
	f, err := os.Open(resolved)
	if err != nil {
		return Script{}, fmt.Errorf("open workflow script: %w", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return Script{}, fmt.Errorf("stat workflow script: %w", err)
	}
	if !info.Mode().IsRegular() {
		return Script{}, fmt.Errorf("workflow script %q is not a regular file", resolved)
	}
	b, err := io.ReadAll(io.LimitReader(f, maxBytes+1))
	if err != nil {
		return Script{}, fmt.Errorf("read workflow script: %w", err)
	}
	if int64(len(b)) > maxBytes {
		return Script{}, fmt.Errorf("workflow script exceeds %d-byte limit", maxBytes)
	}
	if !utf8.Valid(b) || strings.IndexByte(string(b), 0) >= 0 {
		return Script{}, errors.New("workflow script must be NUL-free UTF-8")
	}
	sum := sha256.Sum256(b)
	return Script{Filename: resolved, Source: string(b), Hash: hex.EncodeToString(sum[:])}, nil
}

type ScriptRequest struct {
	RunID    string
	Actor    string
	Workflow *core.Workflow
	Script   Script
	Args     json.RawMessage
	Limits   VMLimits
	Journal  *Journal
}

type ScriptEvaluation struct {
	Proposal    core.Proposal `json:"proposal"`
	Fingerprint string        `json:"fingerprint"`
	Replayed    bool          `json:"replayed"`
	FuelUsed    uint64        `json:"fuel_used"`
}

type ScriptRuntime interface {
	Identity() string
	Evaluate(context.Context, VMInput) (VMOutput, error)
}

type ScriptEvaluator struct {
	Runtime ScriptRuntime
}

func (e ScriptEvaluator) Evaluate(ctx context.Context, req ScriptRequest) (ScriptEvaluation, error) {
	if strings.TrimSpace(req.RunID) == "" || strings.TrimSpace(req.Actor) == "" {
		return ScriptEvaluation{}, errors.New("script run ID and actor are required")
	}
	if req.Workflow == nil {
		return ScriptEvaluation{}, errors.New("workflow is required")
	}
	if req.Journal == nil {
		return ScriptEvaluation{}, errors.New("workflow script journal is required")
	}
	if req.Limits == (VMLimits{}) {
		req.Limits = DefaultVMLimits()
	}
	if err := req.Limits.validate(); err != nil {
		return ScriptEvaluation{}, fmt.Errorf("invalid workflow VM limits: %w", err)
	}
	if err := validateLoadedScript(req.Script, req.Limits); err != nil {
		return ScriptEvaluation{}, err
	}
	args, err := canonicalJSON(req.Args, json.RawMessage(`null`))
	if err != nil {
		return ScriptEvaluation{}, fmt.Errorf("workflow script args must be valid JSON: %w", err)
	}
	snapshot, err := json.Marshal(core.CloneWorkflow(req.Workflow))
	if err != nil {
		return ScriptEvaluation{}, fmt.Errorf("encode workflow VM snapshot: %w", err)
	}
	if len(snapshot)+len(args) > req.Limits.MaxInputBytes {
		return ScriptEvaluation{}, fmt.Errorf("workflow VM input exceeds %d-byte limit", req.Limits.MaxInputBytes)
	}
	runtime := e.Runtime
	if runtime == nil {
		runtime = QuickJSRuntime{}
	}
	runtimeIdentity := runtime.Identity()
	if strings.TrimSpace(runtimeIdentity) == "" {
		return ScriptEvaluation{}, errors.New("workflow script runtime identity is required")
	}
	fingerprint, err := scriptFingerprint(req, snapshot, args, runtimeIdentity)
	if err != nil {
		return ScriptEvaluation{}, err
	}
	events, err := Load(req.Journal.path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return ScriptEvaluation{}, fmt.Errorf("load workflow script journal: %w", err)
	}
	if cached, ok := ReplayScript(events, req.RunID, fingerprint); ok {
		proposal := cached.Proposal
		if proposal.WorkflowID != req.Workflow.ID || proposal.Actor != req.Actor {
			return ScriptEvaluation{}, errors.New("cached workflow script proposal identity does not match the requested workflow and actor")
		}
		if err := (core.Policy{}).ValidateProposal(req.Workflow, proposal); err != nil {
			return ScriptEvaluation{}, fmt.Errorf("cached workflow script proposal is no longer valid: %w", err)
		}
		return ScriptEvaluation{Proposal: proposal, Fingerprint: fingerprint, Replayed: true, FuelUsed: cached.FuelUsed}, nil
	}
	call := &ScriptCall{RunID: req.RunID, Fingerprint: fingerprint, SourceHash: req.Script.Hash, Filename: req.Script.Filename, Engine: runtimeIdentity}
	if err := req.Journal.Append(Event{Type: "script.started", Script: call}); err != nil {
		return ScriptEvaluation{}, fmt.Errorf("journal workflow script start: %w", err)
	}
	output, runErr := runtime.Evaluate(ctx, VMInput{
		Filename:     req.Script.Filename,
		Source:       req.Script.Source,
		WorkflowJSON: snapshot,
		ArgsJSON:     args,
		MaxMutations: req.Limits.MaxMutations,
		Limits:       req.Limits,
	})
	if runErr != nil {
		_ = req.Journal.Append(Event{Type: "script.failed", ScriptFailure: &ScriptFailure{
			RunID: req.RunID, Fingerprint: fingerprint, Kind: errorKind(runErr), Message: runErr.Error(),
		}})
		return ScriptEvaluation{}, runErr
	}
	proposal := core.Proposal{
		WorkflowID: req.Workflow.ID,
		Actor:      req.Actor,
		Mutations:  output.Mutations,
		Rationale:  output.Rationale,
	}
	if len(proposal.Mutations) > req.Limits.MaxMutations {
		runErr = fmt.Errorf("workflow script proposed %d mutations, limit is %d", len(proposal.Mutations), req.Limits.MaxMutations)
		_ = req.Journal.Append(Event{Type: "script.failed", ScriptFailure: &ScriptFailure{
			RunID: req.RunID, Fingerprint: fingerprint, Kind: "policy", Message: runErr.Error(),
		}})
		return ScriptEvaluation{}, runErr
	}
	if err := (core.Policy{}).ValidateProposal(req.Workflow, proposal); err != nil {
		runErr = fmt.Errorf("workflow script proposal rejected atomically: %w", err)
		_ = req.Journal.Append(Event{Type: "script.failed", ScriptFailure: &ScriptFailure{
			RunID: req.RunID, Fingerprint: fingerprint, Kind: "policy", Message: runErr.Error(),
		}})
		return ScriptEvaluation{}, runErr
	}
	if err := req.Journal.Append(Event{Type: "script.proposed", ScriptResult: &ScriptResult{
		RunID: req.RunID, Fingerprint: fingerprint, Proposal: proposal, FuelUsed: output.FuelUsed,
	}}); err != nil {
		return ScriptEvaluation{}, fmt.Errorf("journal workflow script proposal: %w", err)
	}
	return ScriptEvaluation{Proposal: proposal, Fingerprint: fingerprint, FuelUsed: output.FuelUsed}, nil
}

func validateLoadedScript(script Script, limits VMLimits) error {
	if strings.TrimSpace(script.Filename) == "" {
		return errors.New("workflow script filename is required")
	}
	if int64(len(script.Source)) > limits.MaxSourceBytes {
		return fmt.Errorf("workflow script exceeds %d-byte limit", limits.MaxSourceBytes)
	}
	if !utf8.ValidString(script.Source) || strings.IndexByte(script.Source, 0) >= 0 {
		return errors.New("workflow script must be NUL-free UTF-8")
	}
	sum := sha256.Sum256([]byte(script.Source))
	actual := hex.EncodeToString(sum[:])
	if script.Hash == "" {
		return errors.New("workflow script content hash is required")
	}
	if script.Hash != actual {
		return fmt.Errorf("workflow script hash mismatch: loaded %s, actual %s", script.Hash, actual)
	}
	return nil
}

func canonicalJSON(raw json.RawMessage, fallback json.RawMessage) ([]byte, error) {
	if len(raw) == 0 {
		raw = fallback
	}
	var value any
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	if err := dec.Decode(&value); err != nil {
		return nil, err
	}
	if dec.Decode(&struct{}{}) != io.EOF {
		return nil, errors.New("multiple JSON values")
	}
	return json.Marshal(value)
}

func scriptFingerprint(req ScriptRequest, snapshot, args []byte, runtimeIdentity string) (string, error) {
	limits, err := json.Marshal(req.Limits)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	for _, part := range [][]byte{
		[]byte(scriptPolicyVersion), []byte(runtimeIdentity), []byte(req.Script.Hash), []byte(req.Actor), snapshot, args, limits,
	} {
		_, _ = h.Write(part)
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

type kindedError interface{ Kind() string }

func errorKind(err error) string {
	var kinded kindedError
	if errors.As(err, &kinded) {
		return kinded.Kind()
	}
	return "runtime"
}
