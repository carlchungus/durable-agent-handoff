package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/carlchungus/durable-agent-handoff/internal/core"
	"github.com/carlchungus/durable-agent-handoff/internal/discovery"
	"github.com/carlchungus/durable-agent-handoff/internal/engine"
	"github.com/carlchungus/durable-agent-handoff/internal/githubgate"
	"github.com/carlchungus/durable-agent-handoff/internal/preferences"
	"github.com/carlchungus/durable-agent-handoff/internal/runtime"
	"github.com/carlchungus/durable-agent-handoff/internal/service"
	agentsession "github.com/carlchungus/durable-agent-handoff/internal/session"
	"github.com/carlchungus/durable-agent-handoff/internal/team"
	"github.com/carlchungus/durable-agent-handoff/internal/tui"
)

var version = "dev"

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string, out, errOut io.Writer) error {
	if len(args) == 0 {
		return runTUI(nil, out)
	}
	switch args[0] {
	case "version", "--version", "-v":
		fmt.Fprintln(out, version)
		return nil
	case "init":
		return cmdInit(args[1:], out)
	case "create":
		return cmdCreate(args[1:], out, false)
	case "start":
		return cmdCreate(args[1:], out, true)
	case "propose":
		return cmdPropose(args[1:], out)
	case "status":
		return cmdStatus(args[1:], out)
	case "events":
		return cmdEvents(args[1:], out)
	case "run":
		return cmdRun(args[1:], out)
	case "recover":
		return cmdRecover(args[1:], out)
	case "serve":
		return cmdServe(args[1:], out)
	case "service":
		return cmdService(args[1:], out)
	case "tui":
		return runTUI(args[1:], out)
	case "doctor":
		return cmdDoctor(args[1:], out)
	case "preference":
		return cmdPreference(args[1:], out)
	case "team":
		return cmdTeam(args[1:], out)
	case "agent":
		return cmdAgent(args[1:], out)
	case "agents":
		return cmdAgents(args[1:], out)
	case "discover":
		return cmdDiscover(args[1:], out)
	case "import":
		return cmdImport(args[1:], out)
	case "github":
		return cmdGitHub(args[1:], out)
	case "help", "-h", "--help":
		fmt.Fprint(out, usage)
		return nil
	default:
		return fmt.Errorf("unknown command %q\n\n%s", args[0], usage)
	}
}

func cmdAgent(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: handoff agent reply|inbox WORKFLOW_ID NODE_ID")
	}
	switch args[0] {
	case "reply":
		args = reorderFlags(args[1:], map[string]bool{"--state": true, "--message": true})
		fs := flag.NewFlagSet("agent reply", flag.ContinueOnError)
		state := common(fs)
		message := fs.String("message", "", "message to deliver to the exact durable agent session")
		if err := fs.Parse(args); err != nil {
			return err
		}
		if fs.NArg() != 2 || strings.TrimSpace(*message) == "" {
			return errors.New("agent reply requires workflow id, node id, and --message")
		}
		st, err := store(*state)
		if err != nil {
			return err
		}
		w, err := st.Load(fs.Arg(0))
		if err != nil {
			return err
		}
		n := w.Nodes[fs.Arg(1)]
		if n == nil || n.Kind != "agent" || strings.TrimSpace(n.SessionID) == "" {
			return errors.New("reply requires an agent node with an exact persisted runtime session id")
		}
		sessions, err := agentsession.OpenStore(stateDir(*state))
		if err != nil {
			return err
		}
		agent, err := sessions.Ensure(agentsession.Descriptor{WorkflowID: w.ID, NodeID: n.ID, ParentAgentID: n.Metadata["parent_id"], Name: n.Title, Runtime: n.Runtime.Name, RuntimeSessionID: n.SessionID, Worktree: agentWorktree(w, n), LogicalState: agentsession.LogicalNeedsInput, ProcessState: agentsession.ProcessExited})
		if err != nil {
			return err
		}
		if err = sessions.Observe(agent.ID, agentsession.Observation{Runtime: n.Runtime.Name, RuntimeSessionID: n.SessionID, Worktree: agentWorktree(w, n)}); err != nil {
			return err
		}
		queued, err := sessions.Queue(agent.ID, "human", *message)
		if err != nil {
			return err
		}
		if n.State == core.NodeCompleted || n.State == core.NodeWaiting || n.State == core.NodeFailed {
			if _, err = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []core.Mutation{{Op: "reopen_agent", NodeID: n.ID}}, Rationale: "wake exact durable agent session for queued reply"}); err != nil {
				return err
			}
		}
		return writeJSON(out, queued)
	case "inbox":
		args = reorderFlags(args[1:], map[string]bool{"--state": true, "--after": true})
		fs := flag.NewFlagSet("agent inbox", flag.ContinueOnError)
		state := common(fs)
		after := fs.Uint64("after", 0, "only messages after sequence")
		if err := fs.Parse(args); err != nil {
			return err
		}
		if fs.NArg() != 2 {
			return errors.New("agent inbox requires workflow id and node id")
		}
		sessions, err := agentsession.OpenStore(stateDir(*state))
		if err != nil {
			return err
		}
		agent, err := sessions.LoadByNode(fs.Arg(0), fs.Arg(1))
		if err != nil {
			return err
		}
		messages := make([]agentsession.Message, 0, len(agent.Inbox))
		for _, message := range agent.Inbox {
			if message.Sequence > *after {
				messages = append(messages, message)
			}
		}
		return writeJSON(out, messages)
	default:
		return fmt.Errorf("unknown agent command %q", args[0])
	}
}

func cmdAgents(args []string, out io.Writer) error {
	args = reorderFlags(args, map[string]bool{"--state": true, "--workflow": true, "--json": false})
	fs := flag.NewFlagSet("agents", flag.ContinueOnError)
	state := common(fs)
	workflow := fs.String("workflow", "", "filter by workflow id")
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	sessions, err := agentsession.OpenStore(stateDir(*state))
	if err != nil {
		return err
	}
	agents, err := sessions.List()
	if err != nil {
		return err
	}
	if *workflow != "" {
		filtered := agents[:0]
		for _, agent := range agents {
			if agent.WorkflowID == *workflow {
				filtered = append(filtered, agent)
			}
		}
		agents = filtered
	}
	if *jsonOut {
		return writeJSON(out, agents)
	}
	table := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "AGENT\tLOGICAL\tPROCESS\tRUNTIME\tRUNTIME SESSION\tWORKFLOW:NODE")
	for _, agent := range agents {
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s:%s\n", agent.ID, agent.LogicalState, agent.ProcessState, agent.Runtime, agent.RuntimeSessionID, agent.WorkflowID, agent.NodeID)
	}
	return table.Flush()
}

func agentWorktree(w *core.Workflow, n *core.Node) string {
	if n.Worktree != "" {
		return n.Worktree
	}
	return w.Root
}

func stateDir(v string) string {
	if v != "" {
		return v
	}
	if v = os.Getenv("HANDOFF_HOME"); v != "" {
		return v
	}
	d, err := os.UserConfigDir()
	if err != nil {
		return ".handoff"
	}
	return filepath.Join(d, "handoff")
}
func common(fs *flag.FlagSet) *string {
	return fs.String("state", "", "state directory (or HANDOFF_HOME)")
}
func store(path string) (*core.Store, error) { return core.OpenStore(stateDir(path)) }

func cmdInit(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	s := common(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store(*s)
	if err == nil {
		fmt.Fprintln(out, st.Dir())
	}
	return err
}
func cmdCreate(args []string, out io.Writer, seed bool) error {
	fs := flag.NewFlagSet("create", flag.ContinueOnError)
	s := common(fs)
	goal := fs.String("goal", "", "workflow goal")
	root := fs.String("root", ".", "allowed repository root")
	runtimeName := fs.String("runtime", "codex", "codex, claude, pi, ohmypi, or exec")
	model := fs.String("model", "", "runtime model")
	effort := fs.String("effort", "xhigh", "reasoning effort")
	sandbox := fs.String("sandbox", "workspace-write", "read-only or workspace-write")
	role := fs.String("role", "", "preference-ladder role, for example planner or verifier")
	finalizeRepo := fs.String("finalize-repo", "", "authorize deterministic PR creation and merge in OWNER/REPO")
	var mergeGates stringList
	fs.Var(&mergeGates, "merge-gate", "exact required check name; repeatable")
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store(*s)
	if err != nil {
		return err
	}
	w, err := st.Create(*goal, *root, core.DefaultBudget())
	if err != nil {
		return err
	}
	if seed {
		n := &core.Node{ID: "lead", Title: "Own the goal and dynamically adapt the workflow", Kind: "agent", Role: *role, Prompt: "Discover the live state, decide the smallest safe next action, implement it, and add independent verification or follow-up nodes when evidence warrants it.", Runtime: core.RuntimeSpec{Name: *runtimeName, Model: *model, Effort: *effort, Sandbox: *sandbox}}
		mutations := []core.Mutation{{Op: "add_node", Node: n}}
		if *finalizeRepo != "" {
			if len(mergeGates) == 0 {
				return errors.New("--finalize-repo requires at least one --merge-gate")
			}
			verify := &core.Node{ID: "verify", Title: "Independently evaluate the implementation", Kind: "agent", DependsOn: []string{"lead"}, Prompt: "Review the live diff and goal independently. Run the verification that best tests the actual risk. If sound, emit a pass attestation with concrete evidence; otherwise propose repair work.", Runtime: core.RuntimeSpec{Name: *runtimeName, Model: *model, Effort: *effort, Sandbox: "read-only"}}
			finish := &core.Node{ID: "finalize", Title: "Create and safely merge the pull request", Kind: "finalize", DependsOn: []string{"verify"}, MaxAttempts: 100, Metadata: map[string]string{"repo": *finalizeRepo, "gates": strings.Join(mergeGates, ","), "pr_title": *goal, "commit_message": *goal}}
			mutations = append(mutations, core.Mutation{Op: "add_node", Node: verify}, core.Mutation{Op: "add_node", Node: finish})
		}
		w, err = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "human", Mutations: mutations, Rationale: "initial autonomous lead with explicit finalization authority"})
		if err != nil {
			return err
		}
	}
	return outputJSONOrSummary(out, w, *jsonOut)
}
func cmdPropose(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("propose", flag.ContinueOnError)
	s := common(fs)
	file := fs.String("file", "-", "proposal JSON file or - for stdin")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var r io.Reader = os.Stdin
	if *file != "-" {
		f, err := os.Open(*file)
		if err != nil {
			return err
		}
		defer f.Close()
		r = f
	}
	var p core.Proposal
	if err := json.NewDecoder(r).Decode(&p); err != nil {
		return err
	}
	st, err := store(*s)
	if err != nil {
		return err
	}
	w, err := st.Apply(p)
	if err != nil {
		return err
	}
	return writeJSON(out, w)
}
func cmdStatus(args []string, out io.Writer) error {
	args = reorderFlags(args, map[string]bool{"--state": true, "--json": false})
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	s := common(fs)
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store(*s)
	if err != nil {
		return err
	}
	if fs.NArg() > 0 {
		w, err := st.Load(fs.Arg(0))
		if err != nil {
			return err
		}
		return outputJSONOrSummary(out, w, *jsonOut)
	}
	ws, err := st.List()
	if err != nil {
		return err
	}
	if *jsonOut {
		return writeJSON(out, ws)
	}
	for _, w := range ws {
		fmt.Fprintf(out, "%-24s %-12s %3d  %s\n", w.ID, w.Status, len(w.Nodes), w.Goal)
	}
	return nil
}
func cmdEvents(args []string, out io.Writer) error {
	args = reorderFlags(args, map[string]bool{"--state": true, "--follow": false, "--after": true})
	fs := flag.NewFlagSet("events", flag.ContinueOnError)
	s := common(fs)
	follow := fs.Bool("follow", false, "follow new events")
	after := fs.Uint64("after", 0, "only events after sequence")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("events requires workflow id")
	}
	st, err := store(*s)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cursor := *after
	for {
		events, err := st.Events(fs.Arg(0), cursor)
		if err != nil {
			return err
		}
		for _, e := range events {
			if err := writeJSON(out, e); err != nil {
				return err
			}
			cursor = e.Sequence
		}
		if !*follow {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(500 * time.Millisecond):
		}
	}
}
func cmdRun(args []string, out io.Writer) error {
	args = reorderFlags(args, map[string]bool{"--state": true, "--once": false})
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	s := common(fs)
	once := fs.Bool("once", false, "run at most one ready node")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("run requires workflow id")
	}
	st, err := store(*s)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	eng := engine.Engine{Store: st, Preferences: preferences.Open(st.Dir())}
	for {
		n, err := eng.RunOne(ctx, fs.Arg(0))
		if err != nil {
			if strings.Contains(err.Error(), "no runnable node") {
				return nil
			}
			return err
		}
		fmt.Fprintf(out, "ran %s (%s)\n", n.ID, n.Runtime.Name)
		if *once {
			return nil
		}
		w, err := st.Load(fs.Arg(0))
		if err != nil {
			return err
		}
		if w.Status != core.WorkflowActive {
			return nil
		}
	}
}

func cmdRecover(args []string, out io.Writer) error {
	args = reorderFlags(args, map[string]bool{"--state": true, "--node": true, "--attempt": true})
	fs := flag.NewFlagSet("recover", flag.ContinueOnError)
	s := common(fs)
	node := fs.String("node", "lead", "node id")
	attempt := fs.Int("attempt", 0, "exact recorded attempt number")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 || *attempt < 1 {
		return errors.New("recover requires workflow id and --attempt N")
	}
	st, err := store(*s)
	if err != nil {
		return err
	}
	if err = (&engine.Engine{Store: st, Preferences: preferences.Open(st.Dir())}).RecoverAttempt(fs.Arg(0), *node, *attempt); err != nil {
		return err
	}
	w, err := st.Load(fs.Arg(0))
	if err != nil {
		return err
	}
	return writeJSON(out, w)
}
func cmdServe(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	s := common(fs)
	interval := fs.Duration("interval", 2*time.Second, "scheduler scan interval")
	workers := fs.Int("workers", 2, "maximum concurrent workflows")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store(*s)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Fprintf(out, "handoff scheduler · state=%s · workers=%d\n", st.Dir(), *workers)
	return service.Serve(ctx, st, preferences.Open(st.Dir()), *interval, *workers, func(format string, v ...any) { fmt.Fprintf(out, time.Now().Format("15:04:05 ")+format+"\n", v...) })
}
func cmdService(args []string, out io.Writer) error {
	if len(args) == 0 || args[0] != "install" {
		return errors.New("usage: handoff service install [--enable] [--state DIR]")
	}
	fs := flag.NewFlagSet("service install", flag.ContinueOnError)
	s := common(fs)
	enable := fs.Bool("enable", false, "load and start the user service now")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	path, err := service.Install("", stateDir(*s))
	if err != nil {
		return err
	}
	fmt.Fprintln(out, path)
	if *enable {
		if err = service.Enable(path); err != nil {
			return fmt.Errorf("service file created but enable failed: %w", err)
		}
		fmt.Fprintln(out, "service enabled")
	}
	return nil
}
func runTUI(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("tui", flag.ContinueOnError)
	s := common(fs)
	snapshot := fs.Bool("snapshot", false, "render once without interactivity")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store(*s)
	if err != nil {
		return err
	}
	m := tui.New(st)
	if *snapshot {
		view, err := tui.Snapshot(st)
		if err != nil {
			return err
		}
		fmt.Fprintln(out, view)
		return nil
	}
	_, err = tea.NewProgram(m).Run()
	return err
}

func cmdDoctor(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	type check struct {
		Name      string `json:"name"`
		Available bool   `json:"available"`
		Path      string `json:"path,omitempty"`
	}
	names := []string{"git", "gh", "codex", "claude", "pi", "omp"}
	checks := make([]check, 0, len(names))
	for _, n := range names {
		p, err := exec.LookPath(n)
		checks = append(checks, check{n, err == nil, p})
	}
	if *jsonOut {
		return writeJSON(out, checks)
	}
	for _, c := range checks {
		mark := "✗"
		if c.Available {
			mark = "✓"
		}
		fmt.Fprintf(out, "%s %-8s %s\n", mark, c.Name, c.Path)
	}
	_ = runtime.Available
	return nil
}
func cmdPreference(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: handoff preference set|list|health|reset")
	}
	switch args[0] {
	case "set":
		fs := flag.NewFlagSet("preference set", flag.ContinueOnError)
		s := common(fs)
		var values stringList
		fs.Var(&values, "candidate", "runtime:model[:effort], repeat in preference order")
		if err := fs.Parse(reorderFlags(args[1:], map[string]bool{"--state": true, "--candidate": true})); err != nil {
			return err
		}
		if fs.NArg() != 1 {
			return errors.New("preference set requires a role")
		}
		candidates := make([]core.RuntimeSpec, 0, len(values))
		for _, value := range values {
			parts := strings.SplitN(value, ":", 3)
			if len(parts) < 2 {
				return fmt.Errorf("invalid candidate %q; expected runtime:model[:effort]", value)
			}
			effort := "xhigh"
			if len(parts) == 3 {
				effort = parts[2]
			}
			candidates = append(candidates, core.RuntimeSpec{Name: parts[0], Model: parts[1], Effort: effort})
		}
		mgr := preferences.Open(stateDir(*s))
		if err := mgr.Set(fs.Arg(0), candidates); err != nil {
			return err
		}
		cfg, err := mgr.Config()
		if err != nil {
			return err
		}
		return writeJSON(out, map[string]any{"role": fs.Arg(0), "candidates": cfg.Ladders[fs.Arg(0)]})
	case "list":
		fs := flag.NewFlagSet("preference list", flag.ContinueOnError)
		s := common(fs)
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		cfg, err := preferences.Open(stateDir(*s)).Config()
		if err != nil {
			return err
		}
		return writeJSON(out, cfg)
	case "health":
		fs := flag.NewFlagSet("preference health", flag.ContinueOnError)
		s := common(fs)
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		health, err := preferences.Open(stateDir(*s)).Health()
		if err != nil {
			return err
		}
		return writeJSON(out, health)
	case "reset":
		fs := flag.NewFlagSet("preference reset", flag.ContinueOnError)
		s := common(fs)
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		key := ""
		if fs.NArg() > 0 {
			key = fs.Arg(0)
		}
		return preferences.Open(stateDir(*s)).Reset(key)
	default:
		return fmt.Errorf("unknown preference command %q", args[0])
	}
}

func cmdTeam(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: handoff team create|status|apply|inbox")
	}
	switch args[0] {
	case "create":
		fs := flag.NewFlagSet("team create", flag.ContinueOnError)
		s := common(fs)
		name := fs.String("name", "", "team name")
		workflow := fs.String("workflow", "", "associated workflow id")
		lead := fs.String("lead", "lead", "lead member id")
		leadName := fs.String("lead-name", "Lead", "lead display name")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		st, err := team.OpenStore(stateDir(*s))
		if err != nil {
			return err
		}
		tm, err := st.Create(*name, *workflow, team.Member{ID: *lead, Name: *leadName})
		if err != nil {
			return err
		}
		return writeJSON(out, tm)
	case "status":
		fs := flag.NewFlagSet("team status", flag.ContinueOnError)
		s := common(fs)
		if err := fs.Parse(reorderFlags(args[1:], map[string]bool{"--state": true})); err != nil {
			return err
		}
		st, err := team.OpenStore(stateDir(*s))
		if err != nil {
			return err
		}
		if fs.NArg() == 1 {
			tm, loadErr := st.Load(fs.Arg(0))
			if loadErr != nil {
				return loadErr
			}
			return writeJSON(out, tm)
		}
		teams, err := st.List()
		if err != nil {
			return err
		}
		return writeJSON(out, teams)
	case "apply":
		fs := flag.NewFlagSet("team apply", flag.ContinueOnError)
		s := common(fs)
		file := fs.String("file", "-", "team command JSON file or - for stdin")
		if err := fs.Parse(reorderFlags(args[1:], map[string]bool{"--state": true, "--file": true})); err != nil {
			return err
		}
		if fs.NArg() != 1 {
			return errors.New("team apply requires team id")
		}
		var reader io.Reader = os.Stdin
		if *file != "-" {
			f, err := os.Open(*file)
			if err != nil {
				return err
			}
			defer f.Close()
			reader = f
		}
		var command team.Command
		if err := json.NewDecoder(reader).Decode(&command); err != nil {
			return err
		}
		st, err := team.OpenStore(stateDir(*s))
		if err != nil {
			return err
		}
		tm, err := st.Apply(fs.Arg(0), command)
		if err != nil {
			return err
		}
		return writeJSON(out, tm)
	case "inbox":
		fs := flag.NewFlagSet("team inbox", flag.ContinueOnError)
		s := common(fs)
		member := fs.String("member", "", "member id")
		after := fs.Uint64("after", 0, "messages after sequence")
		if err := fs.Parse(reorderFlags(args[1:], map[string]bool{"--state": true, "--member": true, "--after": true})); err != nil {
			return err
		}
		if fs.NArg() != 1 || *member == "" {
			return errors.New("team inbox requires team id and --member")
		}
		st, err := team.OpenStore(stateDir(*s))
		if err != nil {
			return err
		}
		messages, err := st.Inbox(fs.Arg(0), *member, *after)
		if err != nil {
			return err
		}
		return writeJSON(out, messages)
	default:
		return fmt.Errorf("unknown team command %q", args[0])
	}
}
func cmdDiscover(args []string, out io.Writer) error {
	if len(args) == 0 || args[0] != "claude" {
		return errors.New("usage: handoff discover claude [--since 8h] [--root PATH] [--json]")
	}
	fs := flag.NewFlagSet("discover claude", flag.ContinueOnError)
	since := fs.Duration("since", 8*time.Hour, "recent transcript window")
	root := fs.String("root", "", "Claude projects directory")
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	records, err := discovery.DiscoverClaude(*root, *since)
	if err != nil {
		return err
	}
	if *jsonOut {
		return writeJSON(out, records)
	}
	for _, r := range records {
		fmt.Fprintf(out, "%-8s %-22s %-18s %s\n", r.Risk, r.SessionID, truncateCLI(r.Branch, 18), firstNonempty(r.Title, r.LastPrompt))
	}
	return nil
}
func cmdImport(args []string, out io.Writer) error {
	if len(args) == 0 || args[0] != "claude" {
		return errors.New("usage: handoff import claude --session ID [--runtime codex]")
	}
	fs := flag.NewFlagSet("import claude", flag.ContinueOnError)
	s := common(fs)
	session := fs.String("session", "", "exact Claude session ID")
	root := fs.String("transcript-root", "", "Claude projects directory")
	runtimeName := fs.String("runtime", "codex", "target runtime")
	model := fs.String("model", "", "target model")
	effort := fs.String("effort", "xhigh", "reasoning effort")
	sandbox := fs.String("sandbox", "workspace-write", "read-only or workspace-write")
	allowRisk := fs.Bool("allow-risk", false, "explicitly allow medium/high risk import")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *session == "" {
		return errors.New("--session is required")
	}
	records, err := discovery.DiscoverClaude(*root, 365*24*time.Hour)
	if err != nil {
		return err
	}
	var found *discovery.ClaudeSession
	for i := range records {
		if records[i].SessionID == *session {
			found = &records[i]
			break
		}
	}
	if found == nil {
		return fmt.Errorf("Claude session %s not found", *session)
	}
	if found.Risk != "low" && !*allowRisk {
		return fmt.Errorf("session classified %s risk (%s); re-run with --allow-risk only after human review", found.Risk, found.RiskReason)
	}
	if found.CWD == "" {
		return errors.New("transcript has no working directory")
	}
	goal := firstNonempty(found.Title, found.LastPrompt, "Continue interrupted Claude Code work")
	st, err := store(*s)
	if err != nil {
		return err
	}
	w, err := st.Create(goal, found.CWD, core.DefaultBudget())
	if err != nil {
		return err
	}
	prompt := fmt.Sprintf("Recovered from Claude Code session %s on branch %s. Revalidate every claim against the live checkout before editing. Sanitized text-only transcript context follows:\n\n%s", found.SessionID, found.Branch, found.Handoff)
	n := &core.Node{ID: "lead", Title: "Continue the interrupted work", Kind: "agent", Prompt: prompt, Worktree: found.CWD, Runtime: core.RuntimeSpec{Name: *runtimeName, Model: *model, Effort: *effort, Sandbox: *sandbox}, Metadata: map[string]string{"source": "claude", "source_session_id": found.SessionID, "source_transcript": found.Transcript, "risk": found.Risk}}
	if *runtimeName == "claude" {
		n.SessionID = found.SessionID
	}
	w, err = st.Apply(core.Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []core.Mutation{{Op: "add_node", Node: n}}, Rationale: "explicit transcript handoff"})
	if err != nil {
		return err
	}
	return writeJSON(out, w)
}
func truncateCLI(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n-1]) + "…"
}
func firstNonempty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
func reorderFlags(args []string, known map[string]bool) []string {
	flags, positional := []string{}, []string{}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		name := arg
		if at := strings.IndexByte(arg, '='); at >= 0 {
			name = arg[:at]
		}
		takesValue, ok := known[name]
		if !ok {
			positional = append(positional, arg)
			continue
		}
		flags = append(flags, arg)
		if takesValue && !strings.Contains(arg, "=") && i+1 < len(args) {
			i++
			flags = append(flags, args[i])
		}
	}
	return append(flags, positional...)
}
func cmdGitHub(args []string, out io.Writer) error {
	if len(args) == 0 || args[0] != "merge" {
		return errors.New("usage: handoff github merge --repo OWNER/REPO --pr NUMBER --gate NAME [--gate NAME]")
	}
	fs := flag.NewFlagSet("github merge", flag.ContinueOnError)
	repo := fs.String("repo", "", "OWNER/REPO")
	pr := fs.String("pr", "", "pull request number or URL")
	method := fs.String("method", "squash", "merge, squash, or rebase")
	var gates stringList
	fs.Var(&gates, "gate", "exact required check name; repeatable")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *repo == "" || *pr == "" {
		return errors.New("--repo and --pr are required")
	}
	p, err := githubgate.Merge(context.Background(), githubgate.ExecRunner{}, *repo, *pr, gates, *method)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "merge requested for %s at verified head %s\n", p.URL, p.HeadOID)
	return nil
}

type stringList []string

func (s *stringList) String() string     { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error { *s = append(*s, v); return nil }
func outputJSONOrSummary(out io.Writer, w *core.Workflow, jsonOut bool) error {
	if jsonOut {
		return writeJSON(out, w)
	}
	fmt.Fprintf(out, "%s  %s  %s\n", w.ID, w.Status, w.Goal)
	return nil
}
func writeJSON(out io.Writer, v any) error {
	enc := json.NewEncoder(out)
	enc.SetEscapeHTML(false)
	return enc.Encode(v)
}

const usage = `handoff — durable, mutable workflows for coding agents

Usage:
  handoff start --goal TEXT [--root PATH] [--runtime codex]
  handoff create --goal TEXT [--root PATH]
  handoff propose --file proposal.json
  handoff run WORKFLOW_ID [--once]
  handoff recover WORKFLOW_ID --node NODE --attempt N
  handoff serve [--workers 2]
  handoff service install [--enable]
  handoff status [WORKFLOW_ID] [--json]
  handoff events WORKFLOW_ID [--follow]
  handoff tui
  handoff doctor [--json]
  handoff preference set ROLE --candidate runtime:model[:effort] [...]
  handoff preference list | health | reset
  handoff team create --name NAME [--workflow ID]
  handoff team status [TEAM_ID]
  handoff team apply TEAM_ID [--file command.json]
  handoff team inbox TEAM_ID --member MEMBER [--after N]
  handoff agent reply WORKFLOW_ID NODE_ID --message TEXT
  handoff agent inbox WORKFLOW_ID NODE_ID [--after N]
  handoff agents [--workflow WORKFLOW_ID] --json
  handoff discover claude [--since 8h] [--json]
  handoff import claude --session ID [--runtime codex]
  handoff github merge --repo OWNER/REPO --pr N --gate EXACT_NAME

Set HANDOFF_HOME to move durable state. Run "handoff" with no arguments for the TUI.
`
