package preferences

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/core"
)

type Config struct {
	Ladders            map[string][]core.RuntimeSpec `json:"ladders"`
	UsageLimitCooldown time.Duration                 `json:"usage_limit_cooldown"`
	RateLimitCooldown  time.Duration                 `json:"rate_limit_cooldown"`
}
type Health struct {
	Key              string    `json:"key"`
	Class            string    `json:"class"`
	Reason           string    `json:"reason"`
	ObservedAt       time.Time `json:"observed_at"`
	UnavailableUntil time.Time `json:"unavailable_until"`
}
type healthFile struct {
	Providers map[string]Health `json:"providers"`
}
type Manager struct {
	dir string
	mu  sync.Mutex
	now func() time.Time
}
type CooldownError struct {
	Role  string
	Until time.Time
}

func (e *CooldownError) Error() string {
	return fmt.Sprintf("all candidates for role %q are cooling down until %s", e.Role, e.Until.Format(time.RFC3339))
}

func Open(dir string) *Manager {
	return &Manager{dir: dir, now: func() time.Time { return time.Now().UTC() }}
}
func DefaultConfig() Config {
	return Config{Ladders: map[string][]core.RuntimeSpec{}, UsageLimitCooldown: time.Hour, RateLimitCooldown: 5 * time.Minute}
}
func (m *Manager) Config() (Config, error) { m.mu.Lock(); defer m.mu.Unlock(); return m.loadConfig() }
func (m *Manager) Set(role string, candidates []core.RuntimeSpec) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	role = strings.TrimSpace(role)
	if role == "" {
		return errors.New("role is required")
	}
	if len(candidates) == 0 {
		return errors.New("at least one candidate is required")
	}
	for i, c := range candidates {
		if c.Name == "" || c.Model == "" {
			return fmt.Errorf("candidate %d requires runtime and model", i)
		}
	}
	cfg, err := m.loadConfig()
	if err != nil {
		return err
	}
	cfg.Ladders[role] = candidates
	return atomicJSON(filepath.Join(m.dir, "config.json"), cfg)
}
func (m *Manager) Resolve(role string, explicit core.RuntimeSpec) (core.RuntimeSpec, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cfg, err := m.loadConfig()
	if err != nil {
		return core.RuntimeSpec{}, 0, err
	}
	candidates := cfg.Ladders[role]
	if len(candidates) == 0 {
		if explicit.Name == "" {
			return core.RuntimeSpec{}, 0, fmt.Errorf("no runtime or preference ladder configured for role %q", role)
		}
		return explicit, 0, nil
	}
	health, err := m.loadHealth()
	if err != nil {
		return core.RuntimeSpec{}, 0, err
	}
	now := m.now()
	var earliest time.Time
	for i, c := range candidates {
		h, blocked := health.Providers[Key(c)]
		if !blocked || !h.UnavailableUntil.After(now) {
			// The ladder chooses execution capacity, not authority. Preserve the
			// job's sandbox across providers so fallback can never widen access.
			c.Sandbox = narrowerSandbox(explicit.Sandbox, c.Sandbox)
			return c, i, nil
		}
		if earliest.IsZero() || h.UnavailableUntil.Before(earliest) {
			earliest = h.UnavailableUntil
		}
	}
	return core.RuntimeSpec{}, 0, &CooldownError{Role: role, Until: earliest}
}

func narrowerSandbox(job, candidate string) string {
	if job == "read-only" || candidate == "read-only" {
		return "read-only"
	}
	if candidate != "" {
		return candidate
	}
	return job
}
func (m *Manager) Record(spec core.RuntimeSpec, class, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cfg, err := m.loadConfig()
	if err != nil {
		return err
	}
	duration := cfg.UsageLimitCooldown
	if class == "rate_limit" {
		duration = cfg.RateLimitCooldown
	}
	h, err := m.loadHealth()
	if err != nil {
		return err
	}
	now := m.now()
	h.Providers[Key(spec)] = Health{Key: Key(spec), Class: class, Reason: truncate(reason, 500), ObservedAt: now, UnavailableUntil: now.Add(duration)}
	return atomicJSON(filepath.Join(m.dir, "providers.json"), h)
}
func (m *Manager) Health() ([]Health, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, err := m.loadHealth()
	if err != nil {
		return nil, err
	}
	out := make([]Health, 0, len(h.Providers))
	for _, v := range h.Providers {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UnavailableUntil.Before(out[j].UnavailableUntil) })
	return out, nil
}
func (m *Manager) Reset(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, err := m.loadHealth()
	if err != nil {
		return err
	}
	if key == "" {
		h.Providers = map[string]Health{}
	} else {
		delete(h.Providers, key)
	}
	return atomicJSON(filepath.Join(m.dir, "providers.json"), h)
}
func Key(s core.RuntimeSpec) string { return s.Name + "/" + s.Model }
func ClassifyFailure(text string) string {
	v := strings.ToLower(text)
	for _, p := range []string{"usage limit", "session limit", "hit your limit", "quota exceeded", "insufficient_quota", "credit balance", "out of extra usage", "five-hour limit", "weekly limit", "monthly limit"} {
		if strings.Contains(v, p) {
			return "usage_limit"
		}
	}
	for _, p := range []string{"rate_limit", "rate limit", "too many requests", "status 429", "error 429"} {
		if strings.Contains(v, p) {
			return "rate_limit"
		}
	}
	return "runtime_error"
}
func (m *Manager) loadConfig() (Config, error) {
	cfg := DefaultConfig()
	b, err := os.ReadFile(filepath.Join(m.dir, "config.json"))
	if errors.Is(err, os.ErrNotExist) {
		return cfg, nil
	}
	if err != nil {
		return cfg, err
	}
	if err = json.Unmarshal(b, &cfg); err != nil {
		return cfg, err
	}
	if cfg.Ladders == nil {
		cfg.Ladders = map[string][]core.RuntimeSpec{}
	}
	if cfg.UsageLimitCooldown == 0 {
		cfg.UsageLimitCooldown = time.Hour
	}
	if cfg.RateLimitCooldown == 0 {
		cfg.RateLimitCooldown = 5 * time.Minute
	}
	return cfg, nil
}
func (m *Manager) loadHealth() (healthFile, error) {
	h := healthFile{Providers: map[string]Health{}}
	b, err := os.ReadFile(filepath.Join(m.dir, "providers.json"))
	if errors.Is(err, os.ErrNotExist) {
		return h, nil
	}
	if err != nil {
		return h, err
	}
	if err = json.Unmarshal(b, &h); err != nil {
		return h, err
	}
	if h.Providers == nil {
		h.Providers = map[string]Health{}
	}
	return h, nil
}
func atomicJSON(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	tmp := path + ".tmp"
	if err = os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
