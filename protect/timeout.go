package protect

import (
	"context"
	"time"
)

// ApplyStepTimeout wraps a context with step-level timeout.
// Returns the derived context and cancel function.
// If timeout <= 0, returns the original context and a no-op cancel.
func ApplyStepTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

// ApplyJobTimeout wraps a context with job-level timeout.
// If timeout <= 0, returns the original context and a no-op cancel.
func ApplyJobTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

// IsTimeout checks if an error is a context deadline exceeded error.
func IsTimeout(err error) bool {
	return err == context.DeadlineExceeded
}
