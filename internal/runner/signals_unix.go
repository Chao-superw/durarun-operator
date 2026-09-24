//go:build unix

package runner

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// ForwardSignals relays SIGTERM and SIGINT to the child process group
// identified by pid. It runs in a goroutine and stops when ctx is cancelled.
func ForwardSignals(ctx context.Context, pid int) {
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		defer signal.Stop(sigCh)
		for {
			select {
			case <-ctx.Done():
				return
			case sig := <-sigCh:
				if sysSig, ok := sig.(syscall.Signal); ok {
					// Send to the entire process group (negative pid).
					_ = syscall.Kill(-pid, sysSig)
				}
			}
		}
	}()
}
