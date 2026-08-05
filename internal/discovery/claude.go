package discovery

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

type ClaudeSession struct {
	SessionID   string    `json:"session_id"`
	Transcript  string    `json:"transcript"`
	ModifiedAt  time.Time `json:"modified_at"`
	LastEventAt string    `json:"last_event_at,omitempty"`
	CWD         string    `json:"cwd,omitempty"`
	Branch      string    `json:"branch,omitempty"`
	Title       string    `json:"title,omitempty"`
	LastPrompt  string    `json:"last_prompt,omitempty"`
	PRURLs      []string  `json:"pr_urls,omitempty"`
	Risk        string    `json:"risk"`
	RiskReason  string    `json:"risk_reason"`
	Handoff     string    `json:"handoff,omitempty"`
}

var secretPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)(postgres(?:ql)?|mysql|redis)://\S+`),
	regexp.MustCompile(`(?i)\b(bearer|api[_-]?key|token|password|secret)\b\s*[:=]\s*\S+`),
	regexp.MustCompile(`\b(?:sk|rk|pk)-[A-Za-z0-9_-]{12,}\b`),
	regexp.MustCompile(`https?://\S+`),
}

func DiscoverClaude(root string, since time.Duration) ([]ClaudeSession, error) {
	if root == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, err
		}
		root = filepath.Join(home, ".claude", "projects")
	}
	cutoff := time.Now().Add(-since)
	var records []ClaudeSession
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		if d.IsDir() || filepath.Ext(path) != ".jsonl" {
			return nil
		}
		info, err := d.Info()
		if err != nil || info.ModTime().Before(cutoff) {
			return nil
		}
		record, err := ParseClaude(path)
		if err == nil {
			records = append(records, record)
		}
		return nil
	})
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	sort.Slice(records, func(i, j int) bool { return records[i].ModifiedAt.After(records[j].ModifiedAt) })
	return records, nil
}

func ParseClaude(path string) (ClaudeSession, error) {
	info, err := os.Stat(path)
	if err != nil {
		return ClaudeSession{}, err
	}
	r := ClaudeSession{SessionID: strings.TrimSuffix(filepath.Base(path), filepath.Ext(path)), Transcript: path, ModifiedAt: info.ModTime().UTC()}
	prs := map[string]bool{}
	var turns []string
	f, err := os.Open(path)
	if err != nil {
		return r, err
	}
	defer f.Close()
	scan := bufio.NewScanner(f)
	scan.Buffer(make([]byte, 64<<10), 8<<20)
	for scan.Scan() {
		var item map[string]any
		if json.Unmarshal(scan.Bytes(), &item) != nil {
			continue
		}
		if v, ok := item["timestamp"].(string); ok {
			r.LastEventAt = v
		}
		if v, ok := item["cwd"].(string); ok {
			r.CWD = v
		}
		if v, ok := item["gitBranch"].(string); ok {
			r.Branch = v
		}
		switch item["type"] {
		case "ai-title":
			if v, ok := item["aiTitle"].(string); ok {
				r.Title = sanitize(v, 300)
			}
		case "last-prompt":
			if v, ok := item["lastPrompt"].(string); ok {
				r.LastPrompt = sanitize(v, 600)
			}
		case "pr-link":
			if v, ok := item["prUrl"].(string); ok {
				prs[v] = true
			}
		}
		if msg, ok := item["message"].(map[string]any); ok {
			role, _ := msg["role"].(string)
			if role == "user" || role == "assistant" {
				if text := messageText(msg["content"]); text != "" {
					turns = append(turns, role+": "+sanitize(text, 1200))
					if len(turns) > 16 {
						turns = turns[len(turns)-16:]
					}
				}
			}
		}
	}
	if err = scan.Err(); err != nil {
		return r, err
	}
	for url := range prs {
		r.PRURLs = append(r.PRURLs, url)
	}
	sort.Strings(r.PRURLs)
	r.Handoff = strings.Join(turns, "\n\n")
	r.Risk, r.RiskReason = classify(strings.Join([]string{r.Title, r.LastPrompt, r.Handoff}, " "))
	return r, nil
}

func messageText(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []any:
		var parts []string
		for _, part := range x {
			if m, ok := part.(map[string]any); ok && m["type"] == "text" {
				if s, ok := m["text"].(string); ok {
					parts = append(parts, s)
				}
			}
		}
		return strings.Join(parts, " ")
	}
	return ""
}
func sanitize(s string, limit int) string {
	s = strings.Join(strings.Fields(s), " ")
	for _, p := range secretPatterns {
		s = p.ReplaceAllString(s, "[REDACTED]")
	}
	r := []rune(s)
	if len(r) > limit {
		return string(r[:limit]) + "…"
	}
	return s
}
func classify(s string) (string, string) {
	lower := strings.ToLower(s)
	high := []string{"production", "prod db", "migration", "credential", "secret", "authentication", "authorization", "billing", "tenant isolation", "deploy", "infrastructure"}
	for _, term := range high {
		if strings.Contains(lower, term) {
			return "high", fmt.Sprintf("matched safety-sensitive term %q", term)
		}
	}
	medium := []string{"dependency", "refactor", "schema", "delete", "release"}
	for _, term := range medium {
		if strings.Contains(lower, term) {
			return "medium", fmt.Sprintf("matched broad-change term %q", term)
		}
	}
	return "low", "no safety-sensitive terms detected; live checkout still requires revalidation"
}
