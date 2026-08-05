package runstate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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

const SupervisorLeaseDuration = 10 * time.Second

var ErrFenced = fmt.Errorf("supervisor lease was fenced by a newer owner")

var (
	supervisorIdentityOnce sync.Once
	supervisorIdentity     string
)

type Manifest struct {
	Version                  int       `json:"version"`
	ID                       string    `json:"id"`
	WorkflowID               string    `json:"workflow_id"`
	NodeID                   string    `json:"node_id"`
	Attempt                  int       `json:"attempt"`
	Status                   string    `json:"status"`
	Runtime                  string    `json:"runtime"`
	Model                    string    `json:"model,omitempty"`
	Effort                   string    `json:"effort,omitempty"`
	SessionID                string    `json:"session_id,omitempty"`
	SupervisorID             string    `json:"supervisor_id,omitempty"`
	SupervisorGeneration     uint64    `json:"supervisor_generation,omitempty"`
	SupervisorLeaseExpiresAt time.Time `json:"supervisor_lease_expires_at,omitempty"`
	PID                      int       `json:"pid,omitempty"`
	ProcessStartToken        string    `json:"process_start_token,omitempty"`
	CommandDigest            string    `json:"command_digest"`
	Worktree                 string    `json:"worktree"`
	RestartSafe              bool      `json:"restart_safe"`
	EventOffset              int64     `json:"event_offset,omitempty"`
	StartedAt                time.Time `json:"started_at"`
	HeartbeatAt              time.Time `json:"heartbeat_at"`
	FinishedAt               time.Time `json:"finished_at,omitempty"`
	ExitCode                 *int      `json:"exit_code,omitempty"`
	Error                    string    `json:"error,omitempty"`
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
	if manifest.SupervisorID == "" {
		manifest.SupervisorID = SupervisorIdentity()
	}
	if manifest.SupervisorGeneration == 0 {
		manifest.SupervisorGeneration = 1
	}
	manifest.SupervisorLeaseExpiresAt = now.Add(SupervisorLeaseDuration)
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
	release, err := acquireFileLock(r.path + ".write.lock")
	if err != nil {
		return err
	}
	defer release()
	if current, loadErr := Load(r.path); loadErr == nil {
		if current.SupervisorGeneration > r.manifest.SupervisorGeneration ||
			(current.SupervisorGeneration == r.manifest.SupervisorGeneration && current.SupervisorID != "" && current.SupervisorID != r.manifest.SupervisorID) {
			return ErrFenced
		}
	}
	change(&r.manifest)
	r.manifest.HeartbeatAt = time.Now().UTC()
	r.manifest.SupervisorLeaseExpiresAt = r.manifest.HeartbeatAt.Add(SupervisorLeaseDuration)
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

// SupervisorIdentity identifies one process incarnation rather than a recycled
// PID. It is safe to persist and compare across service restarts.
func SupervisorIdentity() string {
	supervisorIdentityOnce.Do(func() {
		supervisorIdentity = fmt.Sprintf("%d:%s", os.Getpid(), ProcessStartToken(os.Getpid()))
	})
	return supervisorIdentity
}

// ClaimSupervisor adopts an attempt after the prior lease expires. The
// generation is a fencing token: recorders from older owners can no longer
// overwrite the manifest after this call succeeds.
func ClaimSupervisor(path, owner string, lease time.Duration) (Manifest, bool, error) {
	if owner == "" {
		return Manifest{}, false, fmt.Errorf("supervisor owner is required")
	}
	if lease <= 0 {
		lease = SupervisorLeaseDuration
	}
	release, err := acquireFileLock(path + ".write.lock")
	if err != nil {
		return Manifest{}, false, err
	}
	defer release()
	m, err := Load(path)
	if err != nil {
		return Manifest{}, false, err
	}
	now := time.Now().UTC()
	if m.SupervisorID != "" && m.SupervisorID != owner && m.SupervisorLeaseExpiresAt.After(now) {
		return m, false, nil
	}
	if m.SupervisorID != owner {
		m.SupervisorGeneration++
		if m.SupervisorGeneration == 0 {
			m.SupervisorGeneration = 1
		}
	}
	m.SupervisorID = owner
	m.SupervisorLeaseExpiresAt = now.Add(lease)
	m.HeartbeatAt = now
	r := &Recorder{path: path, manifest: m}
	if err = r.writeLocked(); err != nil {
		return Manifest{}, false, err
	}
	return m, true, nil
}

func acquireFileLock(path string) (func(), error) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		candidate := fmt.Sprintf("%s.%d.%d", path, os.Getpid(), time.Now().UnixNano())
		owner := Manifest{PID: os.Getpid(), ProcessStartToken: ProcessStartToken(os.Getpid())}
		b, _ := json.Marshal(owner)
		f, err := os.OpenFile(candidate, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return nil, err
		}
		if _, err = f.Write(append(b, '\n')); err == nil {
			err = f.Sync()
		}
		if closeErr := f.Close(); err == nil {
			err = closeErr
		}
		if err == nil {
			err = os.Link(candidate, path)
		}
		_ = os.Remove(candidate)
		if err == nil {
			return func() { _ = os.Remove(path) }, nil
		}
		if !errors.Is(err, os.ErrExist) {
			if _, statErr := os.Stat(path); statErr != nil {
				return nil, err
			}
		}
		if staleFileLock(path) {
			_ = os.Remove(path)
			continue
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("timed out waiting for run manifest lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func staleFileLock(path string) bool {
	b, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	var owner Manifest
	if json.Unmarshal(b, &owner) == nil && owner.PID > 0 {
		return !ProcessMatches(owner)
	}
	info, err := os.Stat(path)
	return err == nil && time.Since(info.ModTime()) > time.Minute
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
	if runtime.GOOS == "windows" {
		out, err := exec.Command("tasklist", "/FI", fmt.Sprintf("PID eq %d", manifest.PID), "/FO", "CSV", "/NH").Output()
		return err == nil && strings.Contains(string(out), fmt.Sprintf("\",\"%d\",", manifest.PID))
	}
	process, err := os.FindProcess(manifest.PID)
	if err != nil {
		return false
	}
	if err = process.Signal(syscall.Signal(0)); err != nil {
		return false
	}
	if manifest.ProcessStartToken == "" {
		return true
	}
	return ProcessStartToken(manifest.PID) == manifest.ProcessStartToken
}
