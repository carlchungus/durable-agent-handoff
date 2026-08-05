//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package engine

import "syscall"

func workerIsDetached(supervisorPID, workerPID int) bool {
	supervisorGroup, supervisorErr := syscall.Getpgid(supervisorPID)
	workerGroup, workerErr := syscall.Getpgid(workerPID)
	return supervisorErr == nil && workerErr == nil && supervisorGroup != workerGroup
}
