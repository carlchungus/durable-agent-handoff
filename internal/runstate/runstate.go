package runstate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const Version = 1

type Manifest struct {
	Version           int       `json:"version"`
	ID                string    `json:"id"`
	WorkflowID        string    `json:"workflow_id"`
	NodeID            string    `json:"node_id"`
	Attempt           int       `json:"attempt"`
	Status            string    `json:"status"`
	Runtime           string    `json:"runtime"`
	Model             string    `json:"model,omitempty"`
	Effort            string    `json:"effort,omitempty"`
	SessionID         string    `json:"session_id,omitempty"`
	PID               int       `json:"pid,omitempty"`
	ProcessStartToken string    `json:"process_start_token,omitempty"`
	CommandDigest     string    `json:"command_digest"`
	Worktree          string    `json:"worktree"`
	RestartSafe       bool      `json:"restart_safe"`
	EventOffset       int64     `json:"event_offset,omitempty"`
	StartedAt         time.Time `json:"started_at"`
	HeartbeatAt       time.Time `json:"heartbeat_at"`
	FinishedAt        time.Time `json:"finished_at,omitempty"`
	ExitCode          *int      `json:"exit_code,omitempty"`
	Error             string    `json:"error,omitempty"`
}

type Recorder struct {
	mu       sync.Mutex
	path     string
	manifest Manifest
}

func Create(path string, manifest Manifest) (*Recorder, error) {
	now := time.Now().UTC()
	manifest.Version = Version
	if manifest.StartedAt.IsZero() {
		manifest.StartedAt = now
	}
	manifest.HeartbeatAt = now
	if manifest.Status == "" {
		manifest.Status = "starting"
	}
	r := &Recorder{path: path, manifest: manifest}
	if err := r.writeLocked(); err != nil {
		return nil, err
	}
	return r, nil
}

func Load(path string) (Manifest, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Manifest{}, err
	}
	var manifest Manifest
	if err = json.Unmarshal(b, &manifest); err != nil {
		return Manifest{}, err
	}
	if manifest.Version != Version {
		return Manifest{}, fmt.Errorf("unsupported run manifest version %d", manifest.Version)
	}
	return manifest, nil
}

func (r *Recorder) Update(change func(*Manifest)) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	change(&r.manifest)
	r.manifest.HeartbeatAt = time.Now().UTC()
	return r.writeLocked()
}

func (r *Recorder) Snapshot() Manifest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.manifest
}

func (r *Recorder) writeLocked() error {
	if err := os.MkdirAll(filepath.Dir(r.path), 0o700); err != nil {
		return err
	}
	b, err := json.MarshalIndent(r.manifest, "", "  ")
	if err != nil {
		return err
	}
	tmp := r.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err = f.Write(append(b, '\n')); err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(tmp, r.path); err != nil {
		return err
	}
	if dir, openErr := os.Open(filepath.Dir(r.path)); openErr == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}

func CommandDigest(name string, args []string) string {
	h := sha256.New()
	_, _ = h.Write([]byte(name))
	for _, arg := range args {
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(arg))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func ProcessStartToken(pid int) string {
	if pid <= 0 || runtime.GOOS == "windows" {
		return ""
	}
	out, err := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "lstart=").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func ProcessMatches(manifest Manifest) bool {
	if manifest.PID <= 0 {
		return false
	}
	process, err := os.FindProcess(manifest.PID)
	if err != nil {
		return false
	}
	if runtime.GOOS != "windows" {
		if err = process.Signal(syscall.Signal(0)); err != nil {
			return false
		}
	}
	if manifest.ProcessStartToken == "" {
		return true
	}
	return ProcessStartToken(manifest.PID) == manifest.ProcessStartToken
}
