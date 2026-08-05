package workflow

import (
	"bufio"
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

type Event struct {
	Sequence int             `json:"sequence"`
	Type     string          `json:"type"`
	At       time.Time       `json:"at"`
	Call     *AgentCall      `json:"call,omitempty"`
	Result   *Completion     `json:"result,omitempty"`
	Data     json.RawMessage `json:"data,omitempty"`
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
	seq := 0
	if len(events) > 0 {
		seq = events[len(events)-1].Sequence
	}
	return &Journal{path: path, seq: seq}, nil
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
