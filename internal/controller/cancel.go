package controller

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"durarun-operator/api/v1alpha1"
	"durarun-operator/internal/state"
)

const cancelAnnotation = "durarun.io/cancel"

// IsCancelled checks if a job has the cancel annotation set to "true".
func IsCancelled(job *v1alpha1.AgentJob) bool {
	if job.Annotations == nil {
		return false
	}
	return job.Annotations[cancelAnnotation] == "true"
}

// HandleCancel processes a cancellation request on a job.
// A job is considered cancelled if it has the annotation "durarun.io/cancel=true".
// Returns true if cancellation was triggered.
// If the job is already terminal (succeeded or failed), cancel is a no-op and returns false.
func HandleCancel(job *v1alpha1.AgentJob) bool {
	if !IsCancelled(job) {
		return false
	}

	// Use TrySetJobTerminal to ensure the cancel races correctly with success.
	// If the job already reached a terminal state, this is a no-op.
	set := state.TrySetJobTerminal(
		&job.Status.Conditions,
		v1alpha1.JobConditionFailed,
		metav1.ConditionTrue,
		"Cancelled",
		"Job was cancelled via annotation",
	)
	if set {
		now := metav1.Now()
		job.Status.CompletionTime = &now
	}
	return set
}
