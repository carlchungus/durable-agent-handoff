// Package tui renders only Supervisor v2 projections. It never reads process
// tables, output sizes, or legacy ledgers to invent lifecycle state.
package tui

import (
	"sort"
	"strings"
	"time"

	"github.com/carlchungus/durable-agent-handoff/supervisor"
)

func Snapshot(store *supervisor.Store) (string, error) {
	state, err := store.Projection()
	if err != nil {
		return "", err
	}
	ids := make([]string, 0, len(state.Executions))
	for id := range state.Executions {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	var output strings.Builder
	for _, id := range ids {
		view, err := supervisor.ProjectExecution(state, supervisor.ExecutionID(id), time.Now().UTC())
		if err != nil {
			return "", err
		}
		output.WriteString(supervisor.RenderText(view))
	}
	return output.String(), nil
}
