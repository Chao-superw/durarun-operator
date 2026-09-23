package protect

import (
	"context"
	"time"
)

// BackoffType defines the backoff strategy for retries.
type BackoffType int

const (
	// NoBackoff uses a fixed delay between retries.
	NoBackoff BackoffType = iota
	// ExponentialBackoff doubles the delay each retry, capped at MaxDelay.
	ExponentialBackoff
)

// RetryConfig controls retry behaviour.
type RetryConfig struct {
	MaxAttempts  int               // max number of attempts (including first); 0 or 1 means no retry
	InitialDelay time.Duration    // delay before the first retry
	MaxDelay     time.Duration    // upper bound on any single delay
	Backoff      BackoffType      // backoff strategy
	RetryOn      func(error) bool // predicate; nil means retry on any error
}

// DefaultRetryConfig returns a no-retry config.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{MaxAttempts: 1}
}

// Execute runs fn with retry logic according to cfg.
// onRetry is called before each retry with the 1-based attempt number about to start.
// Returns the total number of attempts executed and the final error (nil on success).
func Execute(ctx context.Context, cfg RetryConfig, fn func(ctx context.Context) error, onRetry func(attempt int)) (int, error) {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 1
	}

	var err error
	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		err = fn(ctx)
		if err == nil {
			return attempt, nil
		}

		// Last attempt — no retry.
		if attempt == cfg.MaxAttempts {
			break
		}

		// Check RetryOn predicate.
		if cfg.RetryOn != nil && !cfg.RetryOn(err) {
			return attempt, err
		}

		// Compute delay.
		delay := cfg.InitialDelay
		if cfg.Backoff == ExponentialBackoff {
			shift := uint(attempt - 1) // attempt 1 -> shift 0 -> delay * 1
			delay = cfg.InitialDelay << shift
			if cfg.MaxDelay > 0 && delay > cfg.MaxDelay {
				delay = cfg.MaxDelay
			}
		}

		if onRetry != nil {
			onRetry(attempt + 1)
		}

		// Wait with context awareness.
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return attempt, ctx.Err()
		}
	}

	return cfg.MaxAttempts, err
}
