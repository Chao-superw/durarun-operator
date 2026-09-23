package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"durarun-operator/api/v1alpha1"
	"durarun-operator/internal/executor"
	"durarun-operator/internal/observability"
)

const requeueDelay = 5 * time.Second

// JobReconciler reconciles AgentJob objects, driving them through the
// Pending -> Scheduling -> Running -> Succeeded/Failed lifecycle.
type JobReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=durarun.io,resources=agentjobs,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=durarun.io,resources=agentjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=durarun.io,resources=agentattempts,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=durarun.io,resources=agentattempts/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile handles a single reconciliation loop for an AgentJob.
func (r *JobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	start := time.Now()
	defer func() {
		observability.ReconcileDuration.WithLabelValues("job").Observe(time.Since(start).Seconds())
	}()

	logger := log.FromContext(ctx).WithName("job-controller")

	// 1. Fetch the AgentJob.
	var job v1alpha1.AgentJob
	if err := r.Get(ctx, req.NamespacedName, &job); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("AgentJob deleted, nothing to do")
			return ctrl.Result{}, nil
		}
		observability.ReconcileTotal.WithLabelValues("job", "error").Inc()
		return ctrl.Result{}, err
	}

	ctx, span := observability.StartReconcileSpan(ctx, "job", job.Name, string(job.Status.Phase))
	defer span.End()

	// 2. Dispatch based on phase.
	var result ctrl.Result
	var err error

	switch job.Status.Phase {
	case "", v1alpha1.JobPhasePending:
		result, err = r.reconcilePending(ctx, &job)
	case v1alpha1.JobPhaseScheduling:
		result, err = r.reconcileScheduling(ctx, &job)
	case v1alpha1.JobPhaseRunning:
		result, err = r.reconcileRunning(ctx, &job)
	case v1alpha1.JobPhaseSucceeded, v1alpha1.JobPhaseFailed, v1alpha1.JobPhaseTerminated:
		// Terminal states: nothing to do.
		observability.ReconcileTotal.WithLabelValues("job", "success").Inc()
		return ctrl.Result{}, nil
	default:
		logger.Info("unhandled phase", "phase", job.Status.Phase)
		observability.ReconcileTotal.WithLabelValues("job", "success").Inc()
		return ctrl.Result{}, nil
	}

	if err != nil {
		observability.ReconcileTotal.WithLabelValues("job", "error").Inc()
	} else {
		observability.ReconcileTotal.WithLabelValues("job", "success").Inc()
	}
	return result, err
}

// reconcilePending initialises the Job: creates the first AgentAttempt and
// transitions the Job to Scheduling.
func (r *JobReconciler) reconcilePending(ctx context.Context, job *v1alpha1.AgentJob) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Ensure phase is set to Pending if empty.
	if job.Status.Phase == "" {
		job.Status.Phase = v1alpha1.JobPhasePending
	}

	attempt, err := r.createAttempt(ctx, job)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("create attempt: %w", err)
	}
	observability.AttemptTotal.WithLabelValues("created").Inc()

	job.Status.ActiveAttempt = &v1alpha1.AttemptReference{
		Name:   attempt.Name,
		Number: attempt.Spec.Number,
		UID:    attempt.UID,
	}
	job.Status.Phase = v1alpha1.JobPhaseScheduling

	if err := r.Status().Update(ctx, job); err != nil {
		return ctrl.Result{}, fmt.Errorf("update job status to Scheduling: %w", err)
	}

	r.record(job, "Normal", "AttemptCreated",
		fmt.Sprintf("Created attempt %s (#%d)", attempt.Name, attempt.Spec.Number))
	logger.Info("transitioned to Scheduling", "attempt", attempt.Name)

	return ctrl.Result{Requeue: true}, nil
}

// reconcileScheduling ensures a Pod exists for the active Attempt and
// transitions to Running when the Pod starts.
func (r *JobReconciler) reconcileScheduling(ctx context.Context, job *v1alpha1.AgentJob) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	attempt, err := r.getActiveAttempt(ctx, job)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("get active attempt: %w", err)
	}

	// Create Pod if the Attempt does not yet reference one.
	if attempt.Status.PodName == "" {
		pod := executor.BuildPod(job, attempt)
		if err := controllerutil.SetControllerReference(attempt, pod, r.Scheme); err != nil {
			return ctrl.Result{}, fmt.Errorf("set pod owner ref: %w", err)
		}

		// Idempotent: try Get first, then Create.
		var existingPod corev1.Pod
		podKey := types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}
		if err := r.Get(ctx, podKey, &existingPod); err == nil {
			logger.Info("pod already exists, reusing", "pod", pod.Name)
			pod = &existingPod
		} else if apierrors.IsNotFound(err) {
			if err := r.Create(ctx, pod); err != nil {
				if apierrors.IsAlreadyExists(err) {
					// Race: created between Get and Create.
					if err := r.Get(ctx, podKey, pod); err != nil {
						return ctrl.Result{}, fmt.Errorf("get existing pod after race: %w", err)
					}
				} else {
					return ctrl.Result{}, fmt.Errorf("create pod: %w", err)
				}
			}
		} else {
			return ctrl.Result{}, fmt.Errorf("check existing pod: %w", err)
		}

		attempt.Status.PodName = pod.Name
		attempt.Status.PodUID = pod.UID
		attempt.Status.Phase = v1alpha1.AttemptPhaseNew
		if err := r.Status().Update(ctx, attempt); err != nil {
			return ctrl.Result{}, fmt.Errorf("update attempt pod ref: %w", err)
		}

		return ctrl.Result{RequeueAfter: requeueDelay}, nil
	}

	// Pod reference exists; check its status.
	var pod corev1.Pod
	podKey := types.NamespacedName{Namespace: job.Namespace, Name: attempt.Status.PodName}
	if err := r.Get(ctx, podKey, &pod); err != nil {
		if apierrors.IsNotFound(err) {
			// Pod disappeared before starting. Treat as failure.
			logger.Info("pod not found during scheduling, treating as failure", "pod", podKey.Name)
			return r.handleAttemptFailure(ctx, job, attempt, nil)
		}
		return ctrl.Result{}, err
	}

	// UID fencing: reject stale/replaced Pod.
	if attempt.Status.PodUID != "" && pod.UID != attempt.Status.PodUID {
		logger.Info("pod UID mismatch during scheduling (stale pod), treating as failure",
			"expected", attempt.Status.PodUID, "actual", pod.UID)
		observability.FencingRejectTotal.Inc()
		return r.handleAttemptFailure(ctx, job, attempt, nil)
	}

	if pod.Status.Phase == corev1.PodRunning {
		now := metav1.Now()

		attempt.Status.Phase = v1alpha1.AttemptPhaseExecuting
		attempt.Status.StartTime = &now
		if err := r.Status().Update(ctx, attempt); err != nil {
			return ctrl.Result{}, fmt.Errorf("update attempt to Executing: %w", err)
		}

		if job.Status.StartTime == nil {
			job.Status.StartTime = &now
		}
		job.Status.Phase = v1alpha1.JobPhaseRunning
		if err := r.Status().Update(ctx, job); err != nil {
			return ctrl.Result{}, fmt.Errorf("update job to Running: %w", err)
		}

		r.record(job, "Normal", "Running", "Job is now running")
		logger.Info("transitioned to Running", "pod", pod.Name)
		return ctrl.Result{RequeueAfter: requeueDelay}, nil
	}

	// Pod completed before we saw it Running (fast jobs). Transition
	// through Running -> terminal in one step.
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		now := metav1.Now()

		// Ensure StartTime is set.
		if job.Status.StartTime == nil {
			job.Status.StartTime = &now
		}
		job.Status.Phase = v1alpha1.JobPhaseRunning
		if err := r.Status().Update(ctx, job); err != nil {
			return ctrl.Result{}, fmt.Errorf("update job to Running (fast path): %w", err)
		}

		attempt.Status.Phase = v1alpha1.AttemptPhaseExecuting
		attempt.Status.StartTime = &now
		if err := r.Status().Update(ctx, attempt); err != nil {
			return ctrl.Result{}, fmt.Errorf("update attempt to Executing (fast path): %w", err)
		}

		logger.Info("pod completed during scheduling, fast-path to Running", "pod", pod.Name, "podPhase", pod.Status.Phase)
		return ctrl.Result{Requeue: true}, nil
	}

	// Pod is still pending or in an init state; requeue.
	return ctrl.Result{RequeueAfter: requeueDelay}, nil
}

// reconcileRunning watches the active Pod and transitions the Job to a
// terminal state or triggers a retry.
func (r *JobReconciler) reconcileRunning(ctx context.Context, job *v1alpha1.AgentJob) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	attempt, err := r.getActiveAttempt(ctx, job)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("get active attempt: %w", err)
	}

	var pod corev1.Pod
	podKey := types.NamespacedName{Namespace: job.Namespace, Name: attempt.Status.PodName}
	if err := r.Get(ctx, podKey, &pod); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("pod not found, treating as failure", "pod", podKey.Name)
			return r.handleAttemptFailure(ctx, job, attempt, nil)
		}
		return ctrl.Result{}, err
	}

	// UID fencing: reject stale/replaced Pod.
	if attempt.Status.PodUID != "" && pod.UID != attempt.Status.PodUID {
		logger.Info("pod UID mismatch (stale pod), treating as failure",
			"expected", attempt.Status.PodUID, "actual", pod.UID)
		observability.FencingRejectTotal.Inc()
		return r.handleAttemptFailure(ctx, job, attempt, nil)
	}

	switch pod.Status.Phase {
	case corev1.PodSucceeded:
		now := metav1.Now()
		exitCode := int32(0)

		attempt.Status.Phase = v1alpha1.AttemptPhaseCompleted
		attempt.Status.ExitCode = &exitCode
		attempt.Status.CompletionTime = &now
		if err := r.Status().Update(ctx, attempt); err != nil {
			return ctrl.Result{}, fmt.Errorf("update attempt to Completed: %w", err)
		}

		job.Status.Phase = v1alpha1.JobPhaseSucceeded
		job.Status.CompletionTime = &now
		job.Status.CompletedAttempts++
		if err := r.Status().Update(ctx, job); err != nil {
			return ctrl.Result{}, fmt.Errorf("update job to Succeeded: %w", err)
		}

		r.record(job, "Normal", "Succeeded", "Job completed successfully")
		logger.Info("job succeeded")
		observability.AttemptTotal.WithLabelValues("succeeded").Inc()
		return ctrl.Result{}, nil

	case corev1.PodFailed:
		logger.Info("pod failed", "pod", pod.Name)
		exitCode := extractExitCode(&pod)
		observability.AttemptTotal.WithLabelValues("failed").Inc()
		return r.handleAttemptFailure(ctx, job, attempt, exitCode)

	default:
		// Still running or unknown; requeue.
		return ctrl.Result{RequeueAfter: requeueDelay}, nil
	}
}

// handleAttemptFailure marks the current attempt as failed and either
// retries (creating a new attempt) or marks the whole job as failed.
func (r *JobReconciler) handleAttemptFailure(ctx context.Context, job *v1alpha1.AgentJob, attempt *v1alpha1.AgentAttempt, exitCode *int32) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	now := metav1.Now()

	// Mark attempt as failed.
	attempt.Status.Phase = v1alpha1.AttemptPhaseFailed
	attempt.Status.CompletionTime = &now
	if exitCode != nil {
		attempt.Status.ExitCode = exitCode
	}
	if err := r.Status().Update(ctx, attempt); err != nil {
		return ctrl.Result{}, fmt.Errorf("update attempt to Failed: %w", err)
	}

	job.Status.FailedAttempts++

	// Check retry budget: total attempts allowed is MaxRetries + 1 (the
	// initial attempt plus MaxRetries retries).
	if job.Status.FailedAttempts < job.Spec.MaxRetries+1 {
		logger.Info("retrying job",
			"failedAttempts", job.Status.FailedAttempts,
			"maxRetries", job.Spec.MaxRetries)

		newAttempt, err := r.createAttempt(ctx, job)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("create retry attempt: %w", err)
		}
		observability.AttemptTotal.WithLabelValues("created").Inc()

		job.Status.ActiveAttempt = &v1alpha1.AttemptReference{
			Name:   newAttempt.Name,
			Number: newAttempt.Spec.Number,
			UID:    newAttempt.UID,
		}
		job.Status.Phase = v1alpha1.JobPhaseScheduling
		if err := r.Status().Update(ctx, job); err != nil {
			return ctrl.Result{}, fmt.Errorf("update job for retry: %w", err)
		}

		r.record(job, "Warning", "Retrying",
			fmt.Sprintf("Attempt #%d failed, retrying (#%d)", attempt.Spec.Number, newAttempt.Spec.Number))
		return ctrl.Result{Requeue: true}, nil
	}

	// Retry budget exhausted.
	job.Status.Phase = v1alpha1.JobPhaseFailed
	job.Status.CompletionTime = &now
	if err := r.Status().Update(ctx, job); err != nil {
		return ctrl.Result{}, fmt.Errorf("update job to Failed: %w", err)
	}

	r.record(job, "Warning", "Failed",
		fmt.Sprintf("Job failed after %d attempt(s)", job.Status.FailedAttempts))
	logger.Info("job failed permanently", "failedAttempts", job.Status.FailedAttempts)
	return ctrl.Result{}, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// createAttempt builds and creates an AgentAttempt CR owned by the given job.
// It is idempotent: if an Attempt with the deterministic name already exists
// (e.g. due to a re-entrant reconcile after a status-update failure), it
// returns the existing resource instead of creating a duplicate.
func (r *JobReconciler) createAttempt(ctx context.Context, job *v1alpha1.AgentJob) (*v1alpha1.AgentAttempt, error) {
	logger := log.FromContext(ctx)
	number := job.Status.FailedAttempts + job.Status.CompletedAttempts + 1
	attemptName := fmt.Sprintf("%s-%d", job.Name, number)

	// Idempotent: check if the Attempt already exists.
	var existing v1alpha1.AgentAttempt
	key := types.NamespacedName{Namespace: job.Namespace, Name: attemptName}
	if err := r.Get(ctx, key, &existing); err == nil {
		logger.Info("attempt already exists, reusing", "attempt", attemptName)
		return &existing, nil
	}

	attempt := &v1alpha1.AgentAttempt{
		ObjectMeta: metav1.ObjectMeta{
			Name:      attemptName,
			Namespace: job.Namespace,
			Labels: map[string]string{
				"durarun.io/job": job.Name,
			},
		},
		Spec: v1alpha1.AgentAttemptSpec{
			JobRef: job.Name,
			Number: number,
		},
	}

	if err := controllerutil.SetControllerReference(job, attempt, r.Scheme); err != nil {
		return nil, fmt.Errorf("set attempt owner ref: %w", err)
	}

	if err := r.Create(ctx, attempt); err != nil {
		if apierrors.IsAlreadyExists(err) {
			// Race: created between our Get and Create.
			if err := r.Get(ctx, key, &existing); err != nil {
				return nil, fmt.Errorf("get existing attempt after race: %w", err)
			}
			return &existing, nil
		}
		return nil, err
	}

	return attempt, nil
}

// getActiveAttempt fetches the AgentAttempt referenced by job.Status.ActiveAttempt.
// It performs UID fencing: if the stored UID does not match the fetched object,
// the reference is stale and an error is returned.
func (r *JobReconciler) getActiveAttempt(ctx context.Context, job *v1alpha1.AgentJob) (*v1alpha1.AgentAttempt, error) {
	if job.Status.ActiveAttempt == nil {
		return nil, fmt.Errorf("job %s has no active attempt", job.Name)
	}

	var attempt v1alpha1.AgentAttempt
	key := types.NamespacedName{
		Namespace: job.Namespace,
		Name:      job.Status.ActiveAttempt.Name,
	}
	if err := r.Get(ctx, key, &attempt); err != nil {
		return nil, err
	}

	// UID fencing: reject stale attempt references.
	if job.Status.ActiveAttempt.UID != "" && attempt.UID != job.Status.ActiveAttempt.UID {
		return nil, fmt.Errorf("attempt UID mismatch: expected %s, got %s (stale reference)",
			job.Status.ActiveAttempt.UID, attempt.UID)
	}

	return &attempt, nil
}

// extractExitCode returns the exit code from the first terminated container,
// or nil if not available.
func extractExitCode(pod *corev1.Pod) *int32 {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Terminated != nil {
			code := cs.State.Terminated.ExitCode
			return &code
		}
	}
	return nil
}

// record emits a Kubernetes event if the Recorder is configured.
func (r *JobReconciler) record(job *v1alpha1.AgentJob, eventType, reason, msg string) {
	if r.Recorder != nil {
		r.Recorder.Event(job, eventType, reason, msg)
	}
}

// SetupWithManager registers the controller with the manager. It watches
// AgentJob as the primary resource and AgentAttempt as an owned resource.
// Pod status is checked actively during reconciliation (poll model) rather
// than via a watch, keeping the watch topology simple.
func (r *JobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.AgentJob{}).
		Owns(&v1alpha1.AgentAttempt{}).
		Complete(r)
}
