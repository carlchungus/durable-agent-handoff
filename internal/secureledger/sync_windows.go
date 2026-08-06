//go:build windows

package secureledger

import "os"

// Windows does not provide the POSIX directory-fsync contract through an
// os.Root directory handle (FlushFileBuffers returns access denied). Files are
// still fsynced before publication, and snapshots remain reconstructible from
// the authoritative event log. Process-crash recovery therefore does not rely
// on a directory flush; power-loss metadata durability is filesystem-defined.
func syncRoot(_ *os.Root) error { return nil }
