package controller

import (
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"durarun-operator/api/v1alpha1"
)

// PatchJobStatus applies a status subresource patch with conflict detection.
// Uses MergeFrom to compute a minimal diff. On conflict (resource version
// changed), returns a retriable error that triggers a re-enqueue.
func PatchJobStatus(ctx context.Context, c client.Client, job *v1alpha1.AgentJob) error {
	base := job.DeepCopy()
	patch := client.MergeFrom(base)
	if err := c.Status().Patch(ctx, job, patch); err != nil {
		return fmt.Errorf("patch job status %s/%s: %w", job.Namespace, job.Name, err)
	}
	return nil
}

// PatchAttemptStatus applies a status subresource patch for an attempt.
// Uses MergeFrom to compute a minimal diff.
func PatchAttemptStatus(ctx context.Context, c client.Client, attempt *v1alpha1.AgentAttempt) error {
	base := attempt.DeepCopy()
	patch := client.MergeFrom(base)
	if err := c.Status().Patch(ctx, attempt, patch); err != nil {
		return fmt.Errorf("patch attempt status %s/%s: %w", attempt.Namespace, attempt.Name, err)
	}
	return nil
}
