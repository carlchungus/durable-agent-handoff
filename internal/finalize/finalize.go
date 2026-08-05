package finalize

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/carlchungus/durable-agent-handoff/internal/core"
	"github.com/carlchungus/durable-agent-handoff/internal/githubgate"
)

var ErrGatesPending = errors.New("required checks are not successful yet")

type Runner interface {
	Run(context.Context, string, ...string) ([]byte, error)
}
type ExecRunner struct{ Dir string }

func (r ExecRunner) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = r.Dir
	return cmd.CombinedOutput()
}

type DiffStats struct {
	Files int
	Lines int
	Paths []string
}
type Result struct {
	PRURL   string
	HeadSHA string
	Merged  bool
	Summary string
}

func Execute(ctx context.Context, r Runner, w *core.Workflow, n *core.Node) (Result, error) {
	if !hasPass(w) {
		return Result{}, errors.New("finalization requires a passing independent attestation")
	}
	root := n.Worktree
	if root == "" {
		root = w.Root
	}
	stats, err := InspectDiff(ctx, r, root)
	if err != nil {
		return Result{}, err
	}
	if stats.Files > w.Budget.MaxChangedFiles {
		return Result{}, fmt.Errorf("changed-file budget exceeded: %d > %d", stats.Files, w.Budget.MaxChangedFiles)
	}
	if stats.Lines > w.Budget.MaxDiffLines {
		return Result{}, fmt.Errorf("diff-line budget exceeded: %d > %d", stats.Lines, w.Budget.MaxDiffLines)
	}
	branch, err := runText(ctx, r, "git", "branch", "--show-current")
	if err != nil {
		return Result{}, err
	}
	if branch == "" || branch == "main" || branch == "master" {
		return Result{}, fmt.Errorf("refusing to finalize protected or detached branch %q", branch)
	}
	repo := n.Metadata["repo"]
	if repo == "" {
		return Result{}, errors.New("finalize node metadata.repo is required")
	}
	gates := splitCSV(n.Metadata["gates"])
	if len(gates) == 0 {
		return Result{}, errors.New("finalize node requires exact named gates")
	}
	if stats.Files > 0 {
		if _, err = run(ctx, r, "git", "add", "--all"); err != nil {
			return Result{}, err
		}
		message := fallback(n.Metadata["commit_message"], n.Title)
		if _, err = run(ctx, r, "git", "commit", "-m", message); err != nil {
			return Result{}, err
		}
		if _, err = run(ctx, r, "git", "push", "--set-upstream", "origin", branch); err != nil {
			return Result{}, err
		}
	}
	prURL, err := runText(ctx, r, "gh", "pr", "view", branch, "--repo", repo, "--json", "url", "--jq", ".url")
	if err != nil {
		title := fallback(n.Metadata["pr_title"], n.Title)
		body := fallback(n.Metadata["pr_body"], "Automated by handoff with independent attestation and deterministic merge gates.")
		base := fallback(n.Metadata["base"], "main")
		prURL, err = runText(ctx, r, "gh", "pr", "create", "--repo", repo, "--head", branch, "--base", base, "--title", title, "--body", body)
		if err != nil {
			return Result{}, err
		}
	}
	p, err := githubgate.Inspect(ctx, runnerAdapter{r}, repo, prURL)
	if err != nil {
		return Result{}, err
	}
	if err = githubgate.Verify(p, gates); err != nil {
		return Result{PRURL: prURL, HeadSHA: p.HeadOID, Summary: err.Error()}, fmt.Errorf("%w: %v", ErrGatesPending, err)
	}
	method := fallback(n.Metadata["merge_method"], "squash")
	p, err = githubgate.Merge(ctx, runnerAdapter{r}, repo, prURL, gates, method)
	if err != nil {
		return Result{}, err
	}
	return Result{PRURL: p.URL, HeadSHA: p.HeadOID, Merged: true, Summary: "merged after exact named gates passed on unchanged head"}, nil
}

func InspectDiff(ctx context.Context, r Runner, root string) (DiffStats, error) {
	b, err := run(ctx, r, "git", "status", "--porcelain=v1", "-z")
	if err != nil {
		return DiffStats{}, err
	}
	records := strings.Split(string(b), "\x00")
	seen := map[string]bool{}
	var paths []string
	for _, rec := range records {
		if len(rec) < 4 || rec[2] != ' ' {
			continue
		}
		p := rec[3:]
		if !seen[p] {
			seen[p] = true
			paths = append(paths, p)
		}
	}
	num, err := run(ctx, r, "git", "diff", "--numstat", "HEAD")
	if err != nil {
		return DiffStats{}, err
	}
	lines := 0
	tracked := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(string(num)), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		a, _ := strconv.Atoi(fields[0])
		d, _ := strconv.Atoi(fields[1])
		lines += a + d
		tracked[fields[2]] = true
	}
	for _, p := range paths {
		if tracked[p] {
			continue
		}
		full := p
		if root != "" {
			full = filepath.Join(root, p)
		}
		f, openErr := os.Open(full)
		if openErr != nil {
			continue
		}
		scan := bufio.NewScanner(f)
		for scan.Scan() {
			lines++
		}
		_ = f.Close()
	}
	return DiffStats{Files: len(paths), Lines: lines, Paths: paths}, nil
}
func hasPass(w *core.Workflow) bool {
	for _, a := range w.Attestations {
		if a.Verdict == "pass" {
			return true
		}
	}
	return false
}
func splitCSV(s string) []string {
	var out []string
	for _, v := range strings.Split(s, ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}
func fallback(v, d string) string {
	if v != "" {
		return v
	}
	return d
}
func run(ctx context.Context, r Runner, name string, args ...string) ([]byte, error) {
	b, err := r.Run(ctx, name, args...)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(b)))
	}
	return b, nil
}
func runText(ctx context.Context, r Runner, name string, args ...string) (string, error) {
	b, err := run(ctx, r, name, args...)
	return strings.TrimSpace(string(b)), err
}

type runnerAdapter struct{ Runner }

func (r runnerAdapter) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	return r.Runner.Run(ctx, name, args...)
}
