//go:build darwin

package wal

import (
	"os"
	"syscall"
)

// F_BARRIERFSYNC provides write-barrier semantics without flushing the
// drive write cache to physical media (unlike F_FULLFSYNC which Go's
// file.Sync uses). Sufficient for WAL ordering guarantees.
const fBarrierFsync = 0x54

func platformSync(f *os.File) error {
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), fBarrierFsync, 0)
	if errno != 0 {
		return f.Sync()
	}
	return nil
}
