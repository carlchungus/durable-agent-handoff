package tui

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/carlchungus/durable-agent-handoff/internal/core"
	"github.com/carlchungus/durable-agent-handoff/internal/engine"
	"github.com/carlchungus/durable-agent-handoff/internal/preferences"
	"github.com/carlchungus/durable-agent-handoff/internal/runstate"
	coord "github.com/carlchungus/durable-agent-handoff/internal/team"
)

type tickMsg time.Time
type loadMsg struct {
	workflows []*core.Workflow
	teams     []*coord.Team
	events    []core.Event
	attempts  map[string]runstate.Manifest
	err       error
}
type runMsg struct{ err error }

type Model struct {
	store      *core.Store
	teamStore  *coord.Store
	workflows  []*core.Workflow
	teams      []*coord.Team
	events     []core.Event
	attempts   map[string]runstate.Manifest
	cursor     int
	teamCursor int
	mode       string
	width      int
	height     int
	frame      int
	err        error
}

var (
	cyan   = lipgloss.Color("#5DE4C7")
	purple = lipgloss.Color("#A78BFA")
	muted  = lipgloss.Color("#697386")
	green  = lipgloss.Color("#4ADE80")
	yellow = lipgloss.Color("#FACC15")
	red    = lipgloss.Color("#FB7185")
	panel  = lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color("#334155")).Padding(0, 1)
	title  = lipgloss.NewStyle().Bold(true).Foreground(cyan)
)

func New(store *core.Store) Model {
	teamStore, _ := coord.OpenStore(store.Dir())
	return Model{store: store, teamStore: teamStore, mode: "workflows", width: 100, height: 30}
}
func Snapshot(store *core.Store) (string, error) {
	m := New(store)
	ws, err := store.List()
	if err != nil {
		return "", err
	}
	m.workflows = ws
	if m.teamStore != nil {
		m.teams, _ = m.teamStore.List()
	}
	if len(ws) > 0 {
		m.events, err = store.Events(ws[0].ID, 0)
		if err != nil {
			return "", err
		}
		m.attempts = loadAttempts(store, ws[0])
	}
	return m.RenderPlain(), nil
}
func (m Model) Init() tea.Cmd { return tea.Batch(m.load(), tick()) }
func tick() tea.Cmd {
	return tea.Tick(180*time.Millisecond, func(t time.Time) tea.Msg { return tickMsg(t) })
}
func (m Model) load() tea.Cmd {
	return func() tea.Msg {
		ws, err := m.store.List()
		var teams []*coord.Team
		if err == nil && m.teamStore != nil {
			teams, err = m.teamStore.List()
		}
		var events []core.Event
		attempts := map[string]runstate.Manifest{}
		if err == nil && len(ws) > 0 {
			selected := ws[min(m.cursor, len(ws)-1)]
			events, err = m.store.Events(selected.ID, 0)
			if len(events) > 12 {
				events = events[len(events)-12:]
			}
			attempts = loadAttempts(m.store, selected)
		}
		return loadMsg{workflows: ws, teams: teams, events: events, attempts: attempts, err: err}
	}
}

func loadAttempts(store *core.Store, workflow *core.Workflow) map[string]runstate.Manifest {
	attempts := map[string]runstate.Manifest{}
	for _, n := range workflow.Nodes {
		if n.State != core.NodeRunning || n.Attempt < 1 {
			continue
		}
		path := filepath.Join(store.Dir(), "workflows", workflow.ID, "runs", n.ID, fmt.Sprint(n.Attempt), "attempt.json")
		if attempt, err := runstate.Load(path); err == nil {
			attempts[n.ID] = attempt
		}
	}
	return attempts
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch v := msg.(type) {
	case tea.KeyPressMsg:
		switch v.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		case "up", "k":
			if m.mode == "teams" && m.teamCursor > 0 {
				m.teamCursor--
				return m, m.load()
			}
			if m.mode == "workflows" && m.cursor > 0 {
				m.cursor--
				return m, m.load()
			}
		case "down", "j":
			if m.mode == "teams" && m.teamCursor+1 < len(m.teams) {
				m.teamCursor++
				return m, m.load()
			}
			if m.mode == "workflows" && m.cursor+1 < len(m.workflows) {
				m.cursor++
				return m, m.load()
			}
		case "tab":
			if m.mode == "workflows" {
				m.mode = "teams"
			} else {
				m.mode = "workflows"
			}
			return m, m.load()
		case "r":
			return m, m.load()
		case "space":
			if len(m.workflows) > 0 {
				return m, m.runSelected()
			}
		case "p":
			if len(m.workflows) > 0 {
				return m, m.togglePause()
			}
		}
	case tea.WindowSizeMsg:
		m.width = v.Width
		m.height = v.Height
	case tickMsg:
		m.frame = (m.frame + 1) % 8
		return m, tea.Batch(tick(), m.load())
	case loadMsg:
		m.workflows = v.workflows
		m.teams = v.teams
		m.events = v.events
		m.attempts = v.attempts
		m.err = v.err
		if m.cursor >= len(m.workflows) {
			m.cursor = max(0, len(m.workflows)-1)
		}
		if m.teamCursor >= len(m.teams) {
			m.teamCursor = max(0, len(m.teams)-1)
		}
	case runMsg:
		if v.err != nil && !strings.Contains(v.err.Error(), "no runnable node") {
			m.err = v.err
		}
		return m, m.load()
	}
	return m, nil
}

func (m Model) View() tea.View {
	content := m.render()
	v := tea.NewView(content)
	v.AltScreen = true
	v.WindowTitle = "handoff · durable agent workflows"
	return v
}

func (m Model) RenderPlain() string { return stripANSI(m.render()) }

func (m Model) render() string {
	header := title.Render("handoff") + "  " + lipgloss.NewStyle().Foreground(muted).Render("durable, mutable agent workflows") + "  " + lipgloss.NewStyle().Foreground(cyan).Render([]string{"◐", "◓", "◑", "◒", "◐", "◓", "◑", "◒"}[m.frame])
	if m.err != nil {
		return header + "\n\n" + lipgloss.NewStyle().Foreground(red).Render(m.err.Error())
	}
	leftWidth := clamp(m.width/3, 28, 48)
	rightWidth := max(36, m.width-leftWidth-3)
	bodyHeight := max(10, m.height-5)
	leftContent, rightContent := m.workflowList(leftWidth-6), m.detail(rightWidth-6)
	if m.mode == "teams" {
		leftContent, rightContent = m.teamList(leftWidth-6), m.teamDetail(rightWidth-6)
	}
	left := panel.Width(leftWidth - 2).Height(bodyHeight).Render(leftContent)
	right := panel.Width(rightWidth - 2).Height(bodyHeight).Render(rightContent)
	body := lipgloss.JoinHorizontal(lipgloss.Top, left, " ", right)
	footer := lipgloss.NewStyle().Foreground(muted).Render("tab workflows/teams  •  ↑/↓ select  •  space run next  •  p pause/resume  •  r refresh  •  q quit  •  agent API: status --json / events --follow")
	return lipgloss.NewStyle().Padding(1, 1).Render(header + "\n\n" + body + "\n" + footer)
}

func (m Model) teamList(width int) string {
	if len(m.teams) == 0 {
		return title.Render("TEAMS") + "\n\nNo teams yet.\n\n  handoff team create --name \"…\""
	}
	lines := []string{title.Render("TEAMS"), ""}
	for i, tm := range m.teams {
		cursor := "  "
		if i == m.teamCursor {
			cursor = "› "
		}
		working, needsInput := 0, 0
		for _, member := range tm.Members {
			if member.State == coord.MemberWorking {
				working++
			}
			if member.State == coord.MemberNeedsInput {
				needsInput++
			}
		}
		line := fmt.Sprintf("%s%s %s", cursor, statusDot(core.WorkflowActive), truncate(tm.Name, max(8, width-7)))
		if i == m.teamCursor {
			line = lipgloss.NewStyle().Bold(true).Foreground(purple).Render(line)
		}
		lines = append(lines, line, lipgloss.NewStyle().Foreground(muted).Render(fmt.Sprintf("    %d working · %d need input", working, needsInput)))
	}
	return strings.Join(lines, "\n")
}

func (m Model) teamDetail(width int) string {
	if len(m.teams) == 0 {
		return title.Render("AGENT TEAM") + "\n\nWaiting for peers."
	}
	tm := m.teams[m.teamCursor]
	lines := []string{title.Render("AGENT TEAM"), lipgloss.NewStyle().Bold(true).Render(truncate(tm.Name, width)), lipgloss.NewStyle().Foreground(muted).Render(tm.ID + "  •  workflow " + fallback(tm.WorkflowID, "unlinked")), "", title.Render("MEMBERS")}
	memberIDs := make([]string, 0, len(tm.Members))
	for id := range tm.Members {
		memberIDs = append(memberIDs, id)
	}
	sort.Strings(memberIDs)
	for _, id := range memberIDs {
		member := tm.Members[id]
		marker := " "
		if id == tm.LeadID {
			marker = "★"
		}
		lines = append(lines, fmt.Sprintf(" %s %-14s %-12s process=%-7s plan=%s", marker, truncate(member.Name, 14), member.State, member.Process, member.Plan))
		if member.NeedsInputReason != "" {
			lines = append(lines, lipgloss.NewStyle().Foreground(yellow).Render("    ↳ "+truncate(member.NeedsInputReason, width-6)))
		}
	}
	lines = append(lines, "", title.Render("TASKS"))
	for _, id := range tm.TaskOrder {
		task := tm.Tasks[id]
		if task == nil {
			continue
		}
		claim := ""
		if task.Claim != nil {
			claim = fmt.Sprintf(" · %s/g%d", task.Claim.MemberID, task.Claim.Generation)
		}
		blocked := ""
		if len(task.BlockedBy) > 0 {
			blocked = " ← " + strings.Join(task.BlockedBy, ",")
		}
		lines = append(lines, fmt.Sprintf(" %s %-18s %s%s%s", teamTaskGlyph(task.State), truncate(task.Title, 18), task.State, claim, blocked))
	}
	lines = append(lines, "", title.Render("MAILBOX"))
	start := max(0, len(tm.Messages)-5)
	for _, message := range tm.Messages[start:] {
		target := "all"
		if message.To != "" {
			target = message.To
		}
		lines = append(lines, lipgloss.NewStyle().Foreground(muted).Render(message.CreatedAt.Local().Format("15:04:05"))+fmt.Sprintf("  %s → %s  %s", message.From, target, truncate(message.Body, max(8, width-24))))
	}
	return strings.Join(lines, "\n")
}

func teamTaskGlyph(state coord.TaskState) string {
	switch state {
	case coord.TaskCompleted:
		return lipgloss.NewStyle().Foreground(green).Render("✓")
	case coord.TaskInProgress:
		return lipgloss.NewStyle().Foreground(cyan).Render("◆")
	case coord.TaskFailed:
		return lipgloss.NewStyle().Foreground(red).Render("×")
	default:
		return lipgloss.NewStyle().Foreground(muted).Render("○")
	}
}

func (m Model) runSelected() tea.Cmd {
	id := m.workflows[m.cursor].ID
	return func() tea.Msg {
		_, err := (&engine.Engine{Store: m.store, Preferences: preferences.Open(m.store.Dir())}).RunOne(context.Background(), id)
		return runMsg{err}
	}
}
func (m Model) togglePause() tea.Cmd {
	w := m.workflows[m.cursor]
	op := "pause"
	if w.Paused {
		op = "resume"
	}
	return func() tea.Msg {
		_, err := m.store.Apply(core.Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []core.Mutation{{Op: op}}})
		return runMsg{err}
	}
}

func (m Model) workflowList(width int) string {
	if len(m.workflows) == 0 {
		return title.Render("WORKFLOWS") + "\n\nNo workflows yet.\n\n  handoff start --goal \"…\""
	}
	lines := []string{title.Render("WORKFLOWS"), ""}
	for i, w := range m.workflows {
		cursor := "  "
		if i == m.cursor {
			cursor = "› "
		}
		dot := statusDot(w.Status)
		name := truncate(w.Goal, max(8, width-7))
		line := fmt.Sprintf("%s%s %s", cursor, dot, name)
		if i == m.cursor {
			line = lipgloss.NewStyle().Bold(true).Foreground(purple).Render(line)
		}
		lines = append(lines, line, lipgloss.NewStyle().Foreground(muted).Render(fmt.Sprintf("    %s · %d nodes", w.Status, len(w.Nodes))))
	}
	return strings.Join(lines, "\n")
}

func (m Model) detail(width int) string {
	if len(m.workflows) == 0 {
		return title.Render("LIVE STATE") + "\n\nWaiting for work."
	}
	w := m.workflows[m.cursor]
	lines := []string{title.Render(strings.ToUpper(string(w.Status))), lipgloss.NewStyle().Bold(true).Render(truncate(w.Goal, width)), lipgloss.NewStyle().Foreground(muted).Render(w.ID + "  •  " + w.Root), "", title.Render("GRAPH")}
	for _, id := range w.Order {
		n := w.Nodes[id]
		if n == nil {
			continue
		}
		deps := ""
		if len(n.DependsOn) > 0 {
			deps = " ← " + strings.Join(n.DependsOn, ",")
		}
		runtime := ""
		if n.Runtime.Name != "" {
			runtime = "  " + n.Runtime.Name + "/" + fallback(n.Runtime.Model, "default")
			if n.Role != "" {
				runtime += fmt.Sprintf(" [%s #%d]", n.Role, n.CandidateIndex+1)
			}
		}
		lines = append(lines, fmt.Sprintf(" %s %-18s %s%s", m.nodeGlyph(n.State), truncate(n.Title, 18), lipgloss.NewStyle().Foreground(muted).Render(string(n.State)+runtime), lipgloss.NewStyle().Foreground(muted).Render(deps)))
		if attempt, ok := m.attempts[n.ID]; ok {
			heartbeat := time.Since(attempt.HeartbeatAt).Round(time.Second)
			if heartbeat < 0 {
				heartbeat = 0
			}
			session := attempt.SessionID
			if len(session) > 12 {
				session = session[:12] + "…"
			}
			lines = append(lines, lipgloss.NewStyle().Foreground(muted).Render(fmt.Sprintf("    ↳ pid %d · supervisor g%d · heartbeat %s · %s · %.1f KB", attempt.PID, attempt.SupervisorGeneration, heartbeat, session, float64(attempt.EventOffset)/1024)))
		}
	}
	lines = append(lines, "", title.Render("EVENTS"))
	for _, e := range m.events {
		lines = append(lines, lipgloss.NewStyle().Foreground(muted).Render(e.At.Local().Format("15:04:05"))+"  "+truncate(e.Type+" · "+e.Actor, width-12))
	}
	pass := 0
	for _, a := range w.Attestations {
		if a.Verdict == "pass" {
			pass++
		}
	}
	lines = append(lines, "", fmt.Sprintf("evidence %d  •  attestations %d pass / %d total", len(w.Evidence), pass, len(w.Attestations)))
	return strings.Join(lines, "\n")
}

func statusDot(s core.WorkflowStatus) string {
	switch s {
	case core.WorkflowCompleted:
		return lipgloss.NewStyle().Foreground(green).Render("●")
	case core.WorkflowFailed, core.WorkflowNeedsHuman:
		return lipgloss.NewStyle().Foreground(red).Render("●")
	case core.WorkflowWaiting:
		return lipgloss.NewStyle().Foreground(yellow).Render("●")
	default:
		return lipgloss.NewStyle().Foreground(cyan).Render("●")
	}
}
func (m Model) nodeGlyph(s core.NodeState) string {
	switch s {
	case core.NodeCompleted:
		return lipgloss.NewStyle().Foreground(green).Render("✓")
	case core.NodeRunning:
		return lipgloss.NewStyle().Foreground(cyan).Render([]string{"◆", "◇", "◆", "◇", "◆", "◇", "◆", "◇"}[m.frame])
	case core.NodeFailed:
		return lipgloss.NewStyle().Foreground(red).Render("×")
	case core.NodeWaiting:
		return lipgloss.NewStyle().Foreground(yellow).Render("?")
	case core.NodeSuperseded:
		return lipgloss.NewStyle().Foreground(muted).Render("–")
	default:
		return lipgloss.NewStyle().Foreground(purple).Render("○")
	}
}
func truncate(s string, n int) string {
	r := []rune(strings.ReplaceAll(s, "\n", " "))
	if len(r) <= n {
		return string(r)
	}
	if n < 2 {
		return string(r[:n])
	}
	return string(r[:n-1]) + "…"
}
func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func fallback(v, d string) string {
	if v != "" {
		return v
	}
	return d
}
func stripANSI(s string) string {
	var b strings.Builder
	in := false
	for _, r := range s {
		if r == '\x1b' {
			in = true
			continue
		}
		if in {
			if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') {
				in = false
			}
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

func SortedNodes(w *core.Workflow) []*core.Node {
	out := make([]*core.Node, 0, len(w.Nodes))
	for _, n := range w.Nodes {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
