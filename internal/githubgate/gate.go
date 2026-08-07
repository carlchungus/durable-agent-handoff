package githubgate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
)

type Check struct {
	Name       string `json:"name"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
}
type PR struct {
	Number     int     `json:"number"`
	URL        string  `json:"url"`
	HeadOID    string  `json:"headRefOid"`
	MergeState string  `json:"mergeStateStatus"`
	State      string  `json:"state"`
	MergedAt   string  `json:"mergedAt"`
	Checks     []Check `json:"statusCheckRollup"`
}

type Runner interface {
	Run(context.Context, string, ...string) ([]byte, error)
}
type ExecRunner struct{}

func (ExecRunner) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}

func Inspect(ctx context.Context, r Runner, repo, pr string) (PR, error) {
	b, err := r.Run(ctx, "gh", "pr", "view", pr, "--repo", repo, "--json", "number,url,headRefOid,mergeStateStatus,statusCheckRollup,state,mergedAt")
	if err != nil {
		return PR{}, fmt.Errorf("gh pr view: %w: %s", err, b)
	}
	var out PR
	if err = json.Unmarshal(b, &out); err != nil {
		return PR{}, err
	}
	return out, nil
}

func Verify(p PR, gates []string) error {
	if p.HeadOID == "" {
		return errors.New("pull request head SHA is empty")
	}
	if len(gates) == 0 {
		return errors.New("at least one named gate is required")
	}
	for _, gate := range gates {
		found := false
		for _, c := range p.Checks {
			if c.Name == gate {
				found = true
				if strings.ToUpper(c.Status) != "COMPLETED" || strings.ToUpper(c.Conclusion) != "SUCCESS" {
					return fmt.Errorf("gate %q is %s/%s", gate, c.Status, c.Conclusion)
				}
			}
		}
		if !found {
			return fmt.Errorf("gate %q was not found", gate)
		}
	}
	if strings.EqualFold(p.MergeState, "DIRTY") || strings.EqualFold(p.MergeState, "BLOCKED") {
		return fmt.Errorf("pull request merge state is %s", p.MergeState)
	}
	return nil
}

func Merge(ctx context.Context, r Runner, repo, pr string, gates []string, method string) (PR, error) {
	before, err := Inspect(ctx, r, repo, pr)
	if err != nil {
		return PR{}, err
	}
	if err = Verify(before, gates); err != nil {
		return PR{}, err
	}
	if method == "" {
		method = "squash"
	}
	if _, err = MergeAtHead(ctx, r, repo, pr, before.HeadOID, method); err != nil {
		return PR{}, err
	}
	return before, nil
}

// MergeAtHead performs only the authority-owned argv effect after a prepared
// gate decision. GitHub itself rejects the command if the head changed.
func MergeAtHead(ctx context.Context, r Runner, repo, pr, head, method string) (PR, error) {
	if strings.TrimSpace(head) == "" {
		return PR{}, errors.New("pull request head SHA is empty")
	}
	if method == "" {
		method = "squash"
	}
	if method != "merge" && method != "squash" && method != "rebase" {
		return PR{}, fmt.Errorf("unsupported merge method %q", method)
	}
	args := []string{"pr", "merge", pr, "--repo", repo, "--match-head-commit", head, "--" + method}
	b, err := r.Run(ctx, "gh", args...)
	if err != nil {
		return PR{}, fmt.Errorf("gh pr merge: %w: %s", err, b)
	}
	return PR{URL: "", HeadOID: head}, nil
}
