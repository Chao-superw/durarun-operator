package controller

import (
	"time"

	"durarun-operator/api/v1alpha1"
)

// CheckDeadline checks if the job or attempt has exceeded its deadline.
// Returns true if timed out, along with which level triggered it ("job" or "attempt").
// When multiple deadlines are exceeded, job-level takes precedence.
func CheckDeadline(job *v1alpha1.AgentJob, attempt *v1alpha1.AgentAttempt, now time.Time) (timedOut bool, level string) {
	// 1. Check job-level activeDeadlineSeconds against status.startTime.
	if job.Spec.Execution.ActiveDeadlineSeconds != nil && job.Status.StartTime != nil {
		deadline := job.Status.StartTime.Time.Add(time.Duration(*job.Spec.Execution.ActiveDeadlineSeconds) * time.Second)
		if now.After(deadline) {
			return true, "job"
		}
	}

	// 2. Check job-level timeout (Duration) against status.startTime.
	if job.Spec.Execution.Timeout != nil && job.Status.StartTime != nil {
		deadline := job.Status.StartTime.Time.Add(job.Spec.Execution.Timeout.Duration)
		if now.After(deadline) {
			return true, "job"
		}
	}

	// 3. Check attempt-level deadline against current time.
	if attempt != nil && attempt.Spec.Deadline != nil {
		if now.After(attempt.Spec.Deadline.Time) {
			return true, "attempt"
		}
	}

	return false, ""
}
