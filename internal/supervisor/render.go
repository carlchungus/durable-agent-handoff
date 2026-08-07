package supervisor

import (
	"fmt"
	"strings"
)

// RenderText is the linear, screen-reader-friendly Supervisor view. Interactive
// TUI code may add navigation, but lifecycle labels and progress must come from
// this ExecutionView rather than process inspection or output byte counts.
func RenderText(view *ExecutionView) string {
	if view == nil {
		return ""
	}
	var output strings.Builder
	fmt.Fprintf(&output, "Execution %s\nWorkflow %s\nPublication %s\n", view.ID, view.WorkflowID, view.Publication)
	if len(view.Queue) > 0 {
		output.WriteString("Queue")
		for index, activityID := range view.Queue {
			fmt.Fprintf(&output, " %d:%s", index+1, activityID)
		}
		output.WriteByte('\n')
	}
	if len(view.PendingTurns) > 0 {
		output.WriteString("PendingTurns")
		for index, activityID := range view.PendingTurns {
			fmt.Fprintf(&output, " %d:%s", index+1, activityID)
		}
		output.WriteByte('\n')
	}
	for _, node := range view.Nodes {
		fmt.Fprintf(&output, "Node %s %s\n", node.ID, node.Status)
	}
	for _, activity := range view.Activities {
		fmt.Fprintf(&output, "Activity %s generation=%d status=%s", activity.ID, activity.Generation, activity.Status)
		if activity.ParentActivityID != "" {
			fmt.Fprintf(&output, " continuation_of=%s", activity.ParentActivityID)
		}
		if activity.BlockerKind != "" {
			fmt.Fprintf(&output, " blocker=%s question=%s", singleLine(activity.BlockerKind), singleLine(activity.Question))
		}
		output.WriteByte('\n')
	}
	for _, attempt := range view.Attempts {
		fmt.Fprintf(&output, "Attempt %s health=%s", attempt.ID, attempt.Health)
		if attempt.TaskAttempt > 0 {
			fmt.Fprintf(&output, " task_attempt=%d", attempt.TaskAttempt)
		}
		if attempt.ResultStatus != "" {
			fmt.Fprintf(&output, " result=%s", attempt.ResultStatus)
		}
		if attempt.TerminalReason != "" {
			fmt.Fprintf(&output, " terminal_reason=%s", singleLine(attempt.TerminalReason))
		}
		if attempt.MeaningfulProgress != "" {
			fmt.Fprintf(&output, " progress=%s", singleLine(attempt.MeaningfulProgress))
		}
		if attempt.ProviderUnavailable {
			output.WriteString(" provider_unavailable=true")
		}
		output.WriteByte('\n')
	}
	return output.String()
}

func singleLine(value string) string {
	return strings.Join(strings.Fields(value), " ")
}
