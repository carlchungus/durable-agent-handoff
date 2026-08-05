package tui

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/carlchungus/durable-agent-handoff/internal/core"
	"github.com/carlchungus/durable-agent-handoff/internal/engine"
)

type tickMsg time.Time
type loadMsg struct {
	workflows []*core.Workflow
	events    []core.Event
	err       error
}
type runMsg struct{ err error }

type Model struct {
	store     *core.Store
	workflows []*core.Workflow
	events    []core.Event
	cursor    int
	width     int
	height    int
	frame     int
	err       error
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

func New(store *core.Store) Model { return Model{store: store, width: 100, height: 30} }
func Snapshot(store *core.Store) (string, error) {
	m := New(store)
	ws, err := store.List()
	if err != nil {
		return "", err
	}
	m.workflows = ws
	if len(ws) > 0 {
		m.events, err = store.Events(ws[0].ID, 0)
		if err != nil {
			return "", err
		}
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
		var events []core.Event
		if err == nil && len(ws) > 0 {
			events, err = m.store.Events(ws[min(m.cursor, len(ws)-1)].ID, 0)
			if len(events) > 12 {
				events = events[len(events)-12:]
			}
		}
		return loadMsg{ws, events, err}
	}
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch v := msg.(type) {
	case tea.KeyPressMsg:
		switch v.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
				return m, m.load()
			}
		case "down", "j":
			if m.cursor+1 < len(m.workflows) {
				m.cursor++
				return m, m.load()
			}
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
		m.events = v.events
		m.err = v.err
		if m.cursor >= len(m.workflows) {
			m.cursor = max(0, len(m.workflows)-1)
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
	left := panel.Width(leftWidth - 2).Height(bodyHeight).Render(m.workflowList(leftWidth - 6))
	right := panel.Width(rightWidth - 2).Height(bodyHeight).Render(m.detail(rightWidth - 6))
	body := lipgloss.JoinHorizontal(lipgloss.Top, left, " ", right)
	footer := lipgloss.NewStyle().Foreground(muted).Render("↑/↓ select  •  space run next  •  p pause/resume  •  r refresh  •  q quit  •  agent API: status --json / events --follow")
	return lipgloss.NewStyle().Padding(1, 1).Render(header + "\n\n" + body + "\n" + footer)
}

func (m Model) runSelected() tea.Cmd {
	id := m.workflows[m.cursor].ID
	return func() tea.Msg {
		_, err := (&engine.Engine{Store: m.store}).RunOne(context.Background(), id)
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
		}
		lines = append(lines, fmt.Sprintf(" %s %-18s %s%s", nodeGlyph(n.State), truncate(n.Title, 18), lipgloss.NewStyle().Foreground(muted).Render(string(n.State)+runtime), lipgloss.NewStyle().Foreground(muted).Render(deps)))
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
func nodeGlyph(s core.NodeState) string {
	switch s {
	case core.NodeCompleted:
		return lipgloss.NewStyle().Foreground(green).Render("✓")
	case core.NodeRunning:
		return lipgloss.NewStyle().Foreground(cyan).Render("◆")
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
