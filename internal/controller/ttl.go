package controller

import (
	"time"

	"durarun-operator/api/v1alpha1"
	"durarun-operator/internal/state"
)

// CheckTTL checks if a terminal job has exceeded its TTL and should be garbage collected.
// Returns (expired bool, deleteAfter time.Duration).
// - If TTL is not set, returns (false, 0).
// - If the job is not terminal, returns (false, 0).
// - If TTL has expired, returns (true, 0).
// - If TTL has not yet expired, returns (false, remaining).
func CheckTTL(job *v1alpha1.AgentJob, now time.Time) (bool, time.Duration) {
	// TTL only applies to terminal jobs.
	if !state.IsJobTerminal(job.Status.Conditions) {
		return false, 0
	}

	// If TTL is not configured, skip.
	if job.Spec.Execution.TTLSecondsAfterFinished == nil {
		return false, 0
	}

	// Need a completion time to measure from.
	if job.Status.CompletionTime == nil {
		return false, 0
	}

	ttl := time.Duration(*job.Spec.Execution.TTLSecondsAfterFinished) * time.Second
	expiry := job.Status.CompletionTime.Time.Add(ttl)

	if now.After(expiry) || now.Equal(expiry) {
		return true, 0
	}

	return false, expiry.Sub(now)
}
