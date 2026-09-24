package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"durarun-operator/api/v1alpha1"
	"durarun-operator/internal/executor"
	"durarun-operator/internal/observability"
	"durarun-operator/internal/state"
)

// ReconcileAttempt handles the lifecycle of a single attempt within the job reconcile.
// It ensures a Pod exists for the attempt, observes pod status, and updates attempt
// conditions accordingly.
// Returns (terminal bool, result ctrl.Result, err error):
//   - terminal=true means the attempt reached a terminal condition
//   - result contains requeue instructions
//   - err indicates an unrecoverable error during reconciliation
func (r *JobReconciler) ReconcileAttempt(ctx context.Context, snap *JobSnapshot, attempt *v1alpha1.AgentAttempt) (bool, ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("attempt", attempt.Name)

	// If the attempt is already terminal, nothing to do.
	if state.IsAttemptTerminal(attempt.Status.Conditions) {
		return true, ctrl.Result{}, nil
	}

	// Step 1: Find or create the Pod for this attempt.
	pod := snap.PodForAttempt(attempt.Name)

	if pod == nil && attempt.Status.PodName != "" {
		// Pod referenced by attempt but not in snapshot; try direct fetch.
		var fetchedPod corev1.Pod
		podKey := types.NamespacedName{Namespace: snap.Job.Namespace, Name: attempt.Status.PodName}
		if err := r.Get(ctx, podKey, &fetchedPod); err != nil {
			if apierrors.IsNotFound(err) {
				// Pod disappeared. Treat as failure.
				logger.Info("pod not found, treating as failure", "pod", attempt.Status.PodName)
				r.markAttemptFailed(attempt, nil)
				if err := r.Status().Update(ctx, attempt); err != nil {
					return false, ctrl.Result{}, fmt.Errorf("update attempt to Failed (pod gone): %w", err)
				}
				return true, ctrl.Result{}, nil
			}
			return false, ctrl.Result{}, fmt.Errorf("get pod %s: %w", attempt.Status.PodName, err)
		}
		pod = &fetchedPod
	}

	if pod == nil {
		// No pod exists at all. Create one.
		newPod := executor.BuildPod(snap.Job, attempt)
		if err := controllerutil.SetControllerReference(attempt, newPod, r.Scheme); err != nil {
			return false, ctrl.Result{}, fmt.Errorf("set pod owner ref: %w", err)
		}

		// Idempotent: try Get first, then Create.
		var existingPod corev1.Pod
		podKey := types.NamespacedName{Namespace: newPod.Namespace, Name: newPod.Name}
		if err := r.Get(ctx, podKey, &existingPod); err == nil {
			pod = &existingPod
		} else if apierrors.IsNotFound(err) {
			if err := r.Create(ctx, newPod); err != nil {
				if apierrors.IsAlreadyExists(err) {
					if err := r.Get(ctx, podKey, &existingPod); err != nil {
						return false, ctrl.Result{}, fmt.Errorf("get pod after race: %w", err)
					}
					pod = &existingPod
				} else {
					return false, ctrl.Result{}, fmt.Errorf("create pod: %w", err)
				}
			} else {
				pod = newPod
			}
		} else {
			return false, ctrl.Result{}, fmt.Errorf("check existing pod: %w", err)
		}
	}

	// Step 2: Ensure attempt status records the Pod reference.
	if attempt.Status.PodName == "" {
		attempt.Status.PodName = pod.Name
		attempt.Status.PodUID = pod.UID
		state.SetAttemptCondition(&attempt.Status.Conditions,
			v1alpha1.AttemptConditionPodCreated, metav1.ConditionTrue,
			"PodCreated", fmt.Sprintf("Pod %s created", pod.Name))
		if err := r.Status().Update(ctx, attempt); err != nil {
			return false, ctrl.Result{}, fmt.Errorf("update attempt pod ref: %w", err)
		}
		logger.Info("pod associated with attempt", "pod", pod.Name)
		return false, ctrl.Result{RequeueAfter: requeueDelay}, nil
	}

	// Step 3: UID fencing - reject stale/replaced Pod.
	if attempt.Status.PodUID != "" && pod.UID != attempt.Status.PodUID {
		logger.Info("pod UID mismatch, treating as failure",
			"expected", attempt.Status.PodUID, "actual", pod.UID)
		observability.FencingRejectTotal.Inc()
		r.markAttemptFailed(attempt, nil)
		if err := r.Status().Update(ctx, attempt); err != nil {
			return false, ctrl.Result{}, fmt.Errorf("update attempt to Failed (UID mismatch): %w", err)
		}
		return true, ctrl.Result{}, nil
	}

	// Step 4: Observe Pod status and update attempt conditions.
	now := metav1.Now()

	switch pod.Status.Phase {
	case corev1.PodRunning:
		// Mark attempt as running if not already.
		if attempt.Status.StartTime == nil {
			attempt.Status.StartTime = &now
			state.SetAttemptCondition(&attempt.Status.Conditions,
				v1alpha1.AttemptConditionRunning, metav1.ConditionTrue,
				"PodRunning", "Pod is running")
			if err := r.Status().Update(ctx, attempt); err != nil {
				return false, ctrl.Result{}, fmt.Errorf("update attempt to Running: %w", err)
			}
		}
		return false, ctrl.Result{RequeueAfter: requeueDelay}, nil

	case corev1.PodSucceeded:
		exitCode := int32(0)
		attempt.Status.ExitCode = &exitCode
		attempt.Status.CompletionTime = &now
		if attempt.Status.StartTime == nil {
			attempt.Status.StartTime = &now
		}
		state.SetAttemptCondition(&attempt.Status.Conditions,
			v1alpha1.AttemptConditionRunning, metav1.ConditionTrue,
			"PodRunning", "Pod ran (fast path)")
		state.SetAttemptCondition(&attempt.Status.Conditions,
			v1alpha1.AttemptConditionComplete, metav1.ConditionTrue,
			"PodSucceeded", "Pod completed successfully")
		if err := r.Status().Update(ctx, attempt); err != nil {
			return false, ctrl.Result{}, fmt.Errorf("update attempt to Completed: %w", err)
		}
		return true, ctrl.Result{}, nil

	case corev1.PodFailed:
		exitCode := extractExitCode(pod)
		if attempt.Status.StartTime == nil {
			attempt.Status.StartTime = &now
		}
		r.markAttemptFailed(attempt, exitCode)
		if err := r.Status().Update(ctx, attempt); err != nil {
			return false, ctrl.Result{}, fmt.Errorf("update attempt to Failed: %w", err)
		}
		return true, ctrl.Result{}, nil

	default:
		// Pod still pending or in init; requeue.
		return false, ctrl.Result{RequeueAfter: requeueDelay}, nil
	}
}

// markAttemptFailed sets the Failed condition and CompletionTime on an attempt.
func (r *JobReconciler) markAttemptFailed(attempt *v1alpha1.AgentAttempt, exitCode *int32) {
	now := metav1.Now()
	attempt.Status.CompletionTime = &now
	if exitCode != nil {
		attempt.Status.ExitCode = exitCode
	}
	state.SetAttemptCondition(&attempt.Status.Conditions,
		v1alpha1.AttemptConditionFailed, metav1.ConditionTrue,
		"Failed", "Attempt failed")
}

// retryDelay returns the delay before creating a retry attempt.
// Uses the BackoffLimit from the spec, or a default of 5 seconds.
func retryDelay(job *v1alpha1.AgentJob) time.Duration {
	if job.Spec.Execution.BackoffLimit != nil {
		return job.Spec.Execution.BackoffLimit.Duration
	}
	return requeueDelay
}
