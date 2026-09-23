package v3

import (
	"context"
	"crypto/sha256"
	"time"
)

// SHA256Work performs real SHA-256 CPU hashing for the given duration.
// It respects context cancellation without using sleep.
func SHA256Work(ctx context.Context, duration time.Duration) error {
	deadline := time.Now().Add(duration)
	data := []byte("agent-fabric-v3-benchmark-payload")
	for time.Now().Before(deadline) {
		for i := 0; i < 1000; i++ {
			sha256.Sum256(data)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}

// NativeRun7Steps runs 7 sequential steps of SHA-256 work without any
// AF Runtime overhead. Used as the baseline for overhead measurement.
func NativeRun7Steps(stepDuration time.Duration) time.Duration {
	start := time.Now()
	data := []byte("agent-fabric-v3-benchmark-payload")
	for step := 0; step < 7; step++ {
		deadline := time.Now().Add(stepDuration)
		for time.Now().Before(deadline) {
			for i := 0; i < 1000; i++ {
				sha256.Sum256(data)
			}
		}
	}
	return time.Since(start)
}
