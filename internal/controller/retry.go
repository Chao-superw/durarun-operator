package controller

import (
	"fmt"
	"math"
	"time"

	v1alpha1 "durarun-operator/api/v1alpha1"
)

const (
	backoffBaseDelay = 5 * time.Second
	backoffMaxDelay  = 5 * time.Minute
)

// RetryDecision describes whether to retry and when.
type RetryDecision struct {
	ShouldRetry bool
	Delay       time.Duration
	Reason      string
}

// ShouldRetry determines if a failed attempt should be retried.
//
// It takes into account:
//   - maxAttempts from job spec (defaults to 3 via DefaultAgentJobSpec)
//   - current failedAttempts count
//   - error category (infra/OOM/timeout/user errors all count against limit)
//
// When failedAttempts >= maxAttempts the job is not retried regardless of
// error category. Otherwise a retry is issued with deterministic
// exponential backoff based on the current failedAttempts ordinal.
func ShouldRetry(job *v1alpha1.AgentJob, failedAttempts int32, errorCategory v1alpha1.ErrorCategory) RetryDecision {
	maxAttempts := job.Spec.Execution.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = v1alpha1.DefaultMaxAttempts
	}

	// Already exhausted all attempts.
	if failedAttempts >= maxAttempts {
		return RetryDecision{
			ShouldRetry: false,
			Reason:      fmt.Sprintf("failedAttempts (%d) >= maxAttempts (%d)", failedAttempts, maxAttempts),
		}
	}

	delay := ComputeBackoff(failedAttempts)

	switch errorCategory {
	case v1alpha1.ErrorCategoryInfra:
		return RetryDecision{
			ShouldRetry: true,
			Delay:       delay,
			Reason:      "infra error, retrying",
		}
	case v1alpha1.ErrorCategoryOOM:
		return RetryDecision{
			ShouldRetry: true,
			Delay:       delay,
			Reason:      "OOM error, retrying",
		}
	case v1alpha1.ErrorCategoryTimeout:
		return RetryDecision{
			ShouldRetry: true,
			Delay:       delay,
			Reason:      "timeout error, retrying",
		}
	case v1alpha1.ErrorCategoryUser:
		return RetryDecision{
			ShouldRetry: true,
			Delay:       delay,
			Reason:      "user error, retrying",
		}
	case v1alpha1.ErrorCategoryUnknown:
		return RetryDecision{
			ShouldRetry: true,
			Delay:       delay,
			Reason:      "unknown error, retrying",
		}
	default:
		// Empty category (success) or unrecognised — do not retry.
		return RetryDecision{
			ShouldRetry: false,
			Reason:      "no error or unrecognised category",
		}
	}
}

// ComputeBackoff calculates deterministic exponential backoff.
//
// Formula: min(baseDelay * 2^attemptOrdinal, maxDelay)
// baseDelay = 5s, maxDelay = 5m.
//
// The computation is deterministic: the same attemptOrdinal always
// produces the same delay, so the controller survives restarts without
// drift.
func ComputeBackoff(attemptOrdinal int32) time.Duration {
	multiplier := math.Pow(2, float64(attemptOrdinal))
	delay := time.Duration(float64(backoffBaseDelay) * multiplier)
	if delay > backoffMaxDelay {
		delay = backoffMaxDelay
	}
	return delay
}
