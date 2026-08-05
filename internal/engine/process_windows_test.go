//go:build windows

package engine

func workerIsDetached(_, _ int) bool { return true }
