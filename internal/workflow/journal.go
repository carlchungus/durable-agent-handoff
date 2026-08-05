package workflow

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/core"
)

const (
	DefaultMaxConcurrentAgents = 16
	DefaultMaxAgents           = 1000
)

type RunSpec struct {
	ID            string          `json:"id"`
	SessionID     string          `json:"session_id"`
	SourceHash    string          `json:"source_hash"`
	Resolution    string          `json:"resolution"`
	Args          json.RawMessage `json:"args,omitempty"`
	PolicyHash    string          `json:"policy_hash"`
	MaxConcurrent int             `json:"max_concurrent"`
	MaxAgents     int             `json:"max_agents"`
	CreatedAt     time.Time       `json:"created_at"`
}

type AgentCall struct {
	ID          string `json:"id"`
	Sequence    int    `json:"sequence"`
	Fingerprint string `json:"fingerprint"`
	Label       string `json:"label,omitempty"`
	Phase       string `json:"phase,omitempty"`
}

type Completion struct {
	CallID      string          `json:"call_id"`
	Fingerprint string          `json:"fingerprint"`
	Result      json.RawMessage `json:"result,omitempty"`
	Null        bool            `json:"null,omitempty"`
}

// ScriptCall identifies one deterministic evaluation of an orchestration
// script. RunID is supplied by the caller and remains stable across supervisor
// restarts; Fingerprint binds the run to the exact script, input, policy, and
// resource limits.
type ScriptCall struct {
	RunID       string `json:"run_id"`
	Fingerprint string `json:"fingerprint"`
	SourceHash  string `json:"source_hash"`
	Filename    string `json:"filename"`
	Engine      string `json:"engine"`
}

type ScriptResult struct {
	RunID       string        `json:"run_id"`
	Fingerprint string        `json:"fingerprint"`
	Proposal    core.Proposal `json:"proposal"`
	FuelUsed    uint64        `json:"fuel_used,omitempty"`
}

type ScriptFailure struct {
	RunID       string `json:"run_id"`
	Fingerprint string `json:"fingerprint"`
	Kind        string `json:"kind"`
	Message     string `json:"message"`
}

type Event struct {
	Sequence      int             `json:"sequence"`
	Type          string          `json:"type"`
	At            time.Time       `json:"at"`
	Call          *AgentCall      `json:"call,omitempty"`
	Result        *Completion     `json:"result,omitempty"`
	Script        *ScriptCall     `json:"script,omitempty"`
	ScriptResult  *ScriptResult   `json:"script_result,omitempty"`
	ScriptFailure *ScriptFailure  `json:"script_failure,omitempty"`
	Data          json.RawMessage `json:"data,omitempty"`
}

type Journal struct {
	mu   sync.Mutex
	path string
	seq  int
}

func OpenJournal(path string) (*Journal, error) {
	events, err := Load(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err == nil {
		if err = truncateIncompleteTail(path); err != nil {
			return nil, fmt.Errorf("repair truncated workflow journal tail: %w", err)
		}
	}
	seq := 0
	if len(events) > 0 {
		seq = events[len(events)-1].Sequence
	}
	return &Journal{path: path, seq: seq}, nil
}

// truncateIncompleteTail removes only the final non-newline-terminated record.
// Load intentionally ignores that record after a crash; repairing it here is
// necessary so the next append cannot concatenate valid JSON onto the corrupt
// fragment and make the recovery result unreadable too.
func truncateIncompleteTail(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.Size() == 0 {
		return err
	}
	var last [1]byte
	if _, err = f.ReadAt(last[:], info.Size()-1); err != nil {
		return err
	}
	if last[0] == '\n' {
		return nil
	}
	const chunkSize int64 = 4096
	end := info.Size()
	truncateAt := int64(0)
	for end > 0 {
		start := end - chunkSize
		if start < 0 {
			start = 0
		}
		buf := make([]byte, end-start)
		if _, err = f.ReadAt(buf, start); err != nil {
			return err
		}
		if index := bytes.LastIndexByte(buf, '\n'); index >= 0 {
			truncateAt = start + int64(index) + 1
			break
		}
		end = start
	}
	if err = f.Truncate(truncateAt); err != nil {
		return err
	}
	return f.Sync()
}

func (j *Journal) Append(event Event) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(j.path), 0o700); err != nil {
		return err
	}
	j.seq++
	event.Sequence = j.seq
	if event.At.IsZero() {
		event.At = time.Now().UTC()
	}
	b, err := json.Marshal(event)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(j.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err = f.Write(append(b, '\n')); err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	return err
}

// Load accepts a truncated final line because a supervisor can die between a
// write and fsync. Corruption before the final line is not silently skipped.
func Load(path string) ([]Event, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	var events []Event
	for {
		line, readErr := r.ReadBytes('\n')
		if len(line) > 0 {
			var event Event
			if err = json.Unmarshal(line, &event); err != nil {
				if errors.Is(readErr, io.EOF) {
					break
				}
				return nil, fmt.Errorf("decode workflow journal event %d: %w", len(events)+1, err)
			}
			events = append(events, event)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, readErr
		}
	}
	return events, nil
}

func Fingerprint(prompt string, options any) (string, error) {
	b, err := json.Marshal(options)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	_, _ = h.Write([]byte(prompt))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(b)
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Replayer implements Claude's ordered-prefix replay contract. Once a call is
// missing, unfinished, or changed, that call and the entire suffix run live,
// even if later calls happened to finish before the interruption.
type Replayer struct {
	calls       []AgentCall
	completions map[string]Completion
	cursor      int
	live        bool
}

func NewReplayer(events []Event) *Replayer {
	r := &Replayer{completions: map[string]Completion{}}
	for _, event := range events {
		if event.Type == "agent.started" && event.Call != nil {
			r.calls = append(r.calls, *event.Call)
		}
		if event.Type == "agent.result" && event.Result != nil {
			r.completions[event.Result.CallID] = *event.Result
		}
	}
	sort.SliceStable(r.calls, func(i, j int) bool { return r.calls[i].Sequence < r.calls[j].Sequence })
	return r
}

func (r *Replayer) Resolve(fingerprint string) (Completion, bool) {
	if r.live || r.cursor >= len(r.calls) {
		r.live = true
		return Completion{}, false
	}
	call := r.calls[r.cursor]
	result, ok := r.completions[call.ID]
	if !ok || call.Fingerprint != fingerprint || result.Fingerprint != fingerprint {
		r.live = true
		return Completion{}, false
	}
	r.cursor++
	return result, true
}

func (r *Replayer) Frontier() int { return r.cursor }

// ReplayScript returns a completed script proposal only when both the durable
// run identity and the complete evaluation fingerprint match. A started run
// without a result is deliberately not cached: after a crash it executes from
// the beginning against the same immutable input.
func ReplayScript(events []Event, runID, fingerprint string) (ScriptResult, bool) {
	started := false
	for _, event := range events {
		if event.Type == "script.started" && event.Script != nil &&
			event.Script.RunID == runID && event.Script.Fingerprint == fingerprint {
			started = true
		}
		if started && event.Type == "script.proposed" && event.ScriptResult != nil &&
			event.ScriptResult.RunID == runID && event.ScriptResult.Fingerprint == fingerprint {
			return *event.ScriptResult, true
		}
	}
	return ScriptResult{}, false
}
