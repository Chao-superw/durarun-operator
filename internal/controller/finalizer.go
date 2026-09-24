package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"durarun-operator/api/v1alpha1"
)

// FinalizerName is the finalizer added to AgentJob objects for cleanup.
const FinalizerName = "durarun.io/cleanup"

// EnsureFinalizer adds the cleanup finalizer to a job if not present.
// Returns true if the finalizer was added (i.e., the object was modified).
func EnsureFinalizer(job *v1alpha1.AgentJob) bool {
	return controllerutil.AddFinalizer(job, FinalizerName)
}

// RemoveFinalizer removes the cleanup finalizer from a job.
// Returns true if the finalizer was removed (i.e., the object was modified).
func RemoveFinalizer(job *v1alpha1.AgentJob) bool {
	return controllerutil.RemoveFinalizer(job, FinalizerName)
}

// HandleFinalization performs cleanup when a job is being deleted:
//  1. Delete owned pods
//  2. Clean up artifacts (bounded timeout)
//  3. Remove finalizer
//
// Returns (done bool, err error). If not done, caller should requeue.
// Key: if cleanup takes longer than maxWait, the finalizer is removed anyway
// to prevent blocking deletion indefinitely.
func HandleFinalization(ctx context.Context, c client.Client, job *v1alpha1.AgentJob, maxWait time.Duration) (bool, error) {
	// Create a bounded context to ensure we don't block forever.
	ctx, cancel := context.WithTimeout(ctx, maxWait)
	defer cancel()

	// Step 1: Delete all owned pods.
	selector := labels.SelectorFromSet(labels.Set{
		"durarun.io/job": job.Name,
	})

	var podList corev1.PodList
	if err := c.List(ctx, &podList, &client.ListOptions{
		Namespace:     job.Namespace,
		LabelSelector: selector,
	}); err != nil {
		// If the context timed out, we still remove the finalizer.
		if ctx.Err() != nil {
			RemoveFinalizer(job)
			return true, nil
		}
		return false, fmt.Errorf("list pods for finalization: %w", err)
	}

	for i := range podList.Items {
		if err := c.Delete(ctx, &podList.Items[i]); err != nil {
			if ctx.Err() != nil {
				// Timeout hit — remove finalizer anyway.
				RemoveFinalizer(job)
				return true, nil
			}
			// Ignore not-found errors (pod already deleted).
			if client.IgnoreNotFound(err) != nil {
				return false, fmt.Errorf("delete pod %s: %w", podList.Items[i].Name, err)
			}
		}
	}

	// Step 2: Clean up attempts (optional — owned by controller reference,
	// Kubernetes GC handles them, but we delete explicitly for faster cleanup).
	var attemptList v1alpha1.AgentAttemptList
	if err := c.List(ctx, &attemptList, &client.ListOptions{
		Namespace:     job.Namespace,
		LabelSelector: selector,
	}); err != nil {
		if ctx.Err() != nil {
			RemoveFinalizer(job)
			return true, nil
		}
		return false, fmt.Errorf("list attempts for finalization: %w", err)
	}

	for i := range attemptList.Items {
		if err := c.Delete(ctx, &attemptList.Items[i]); err != nil {
			if ctx.Err() != nil {
				RemoveFinalizer(job)
				return true, nil
			}
			if client.IgnoreNotFound(err) != nil {
				return false, fmt.Errorf("delete attempt %s: %w", attemptList.Items[i].Name, err)
			}
		}
	}

	// Step 3: Remove finalizer.
	RemoveFinalizer(job)
	return true, nil
}
