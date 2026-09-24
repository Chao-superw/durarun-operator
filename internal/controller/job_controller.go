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
	"durarun-operator/internal/observability"
	"durarun-operator/internal/state"
)

const requeueDelay = 5 * time.Second

// JobReconciler reconciles AgentJob objects using a snapshot-driven,
// idempotent reconcile loop based on Conditions as the sole state truth.
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
// The loop is snapshot-driven: it loads the full state once and makes all
// decisions from that snapshot without mid-reconcile re-fetches.
func (r *JobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	start := time.Now()
	defer func() {
		observability.ReconcileDuration.WithLabelValues("job").Observe(time.Since(start).Seconds())
	}()

	logger := log.FromContext(ctx).WithName("job-controller")

	// 1. Load snapshot (Job + all owned Attempts + Pods).
	snap, err := LoadJobSnapshot(ctx, r.Client, req.NamespacedName)
	if err != nil {
		observability.ReconcileTotal.WithLabelValues("job", "error").Inc()
		return ctrl.Result{}, err
	}
	if snap == nil {
		// Job was deleted.
		logger.Info("AgentJob deleted, nothing to do")
		return ctrl.Result{}, nil
	}

	job := snap.Job
	ctx, span := observability.StartReconcileSpan(ctx, "job", job.Name, v1alpha1.JobPhaseFromConditions(job.Status.Conditions))
	defer span.End()

	// 2. If already terminal, nothing to do.
	if state.IsJobTerminal(job.Status.Conditions) {
		observability.ReconcileTotal.WithLabelValues("job", "success").Inc()
		return ctrl.Result{}, nil
	}

	// 3. Compute and store spec hash for drift detection.
	specHash := ComputeSpecHash(&job.Spec)
	if job.Status.SpecHash == "" || job.Status.SpecHash != specHash {
		job.Status.SpecHash = specHash
	}

	// 4. Verify ownership of all children (Attempts and Pods).
	for _, attempt := range snap.Attempts {
		if err := VerifyAttemptOwnership(job, attempt); err != nil {
			logger.Info("ownership verification failed for attempt", "attempt", attempt.Name, "error", err)
			observability.FencingRejectTotal.Inc()
			// Mark the job as failed due to ownership violation.
			if state.TrySetJobTerminal(&job.Status.Conditions,
				v1alpha1.JobConditionFailed, metav1.ConditionTrue,
				"OwnershipViolation", fmt.Sprintf("stale attempt detected: %s", err)) {
				now := metav1.Now()
				job.Status.CompletionTime = &now
				if err := r.Status().Update(ctx, job); err != nil {
					return ctrl.Result{}, fmt.Errorf("update job status after ownership violation: %w", err)
				}
				r.record(job, "Warning", "OwnershipViolation", err.Error())
			}
			observability.ReconcileTotal.WithLabelValues("job", "error").Inc()
			return ctrl.Result{}, nil
		}
	}

	// 5. Find or create active attempt.
	attempt := snap.ActiveAttempt()

	if attempt == nil {
		// No active attempt exists. Create one.
		attempt, err = r.createAttempt(ctx, snap)
		if err != nil {
			observability.ReconcileTotal.WithLabelValues("job", "error").Inc()
			return ctrl.Result{}, fmt.Errorf("create attempt: %w", err)
		}
		observability.AttemptTotal.WithLabelValues("created").Inc()

		job.Status.ActiveAttempt = &v1alpha1.AttemptReference{
			Name:    attempt.Name,
			Ordinal: attempt.Spec.Ordinal,
			UID:     attempt.UID,
		}
		state.SetJobCondition(&job.Status.Conditions,
			v1alpha1.JobConditionScheduled, metav1.ConditionTrue,
			"AttemptCreated", fmt.Sprintf("Created attempt %s (#%d)", attempt.Name, attempt.Spec.Ordinal))

		if err := r.Status().Update(ctx, job); err != nil {
			observability.ReconcileTotal.WithLabelValues("job", "error").Inc()
			return ctrl.Result{}, fmt.Errorf("update job status to Scheduling: %w", err)
		}

		r.record(job, "Normal", "AttemptCreated",
			fmt.Sprintf("Created attempt %s (#%d)", attempt.Name, attempt.Spec.Ordinal))
		logger.Info("created attempt, transitioning to Scheduling", "attempt", attempt.Name)

		observability.ReconcileTotal.WithLabelValues("job", "success").Inc()
		return ctrl.Result{Requeue: true}, nil
	}

	// 6. Reconcile the active attempt (ensure pod, observe status).
	terminal, result, err := r.ReconcileAttempt(ctx, snap, attempt)
	if err != nil {
		observability.ReconcileTotal.WithLabelValues("job", "error").Inc()
		return result, err
	}

	if !terminal {
		// Attempt is still in progress. Ensure job Running condition is set
		// if the attempt has started.
		if attempt.Status.StartTime != nil {
			now := metav1.Now()
			if job.Status.StartTime == nil {
				job.Status.StartTime = &now
			}
			state.SetJobCondition(&job.Status.Conditions,
				v1alpha1.JobConditionRunning, metav1.ConditionTrue,
				"PodRunning", "Job is now running")
			if err := r.Status().Update(ctx, job); err != nil {
				observability.ReconcileTotal.WithLabelValues("job", "error").Inc()
				return ctrl.Result{}, fmt.Errorf("update job to Running: %w", err)
			}
			r.record(job, "Normal", "Running", "Job is now running")
		}
		observability.ReconcileTotal.WithLabelValues("job", "success").Inc()
		return result, nil
	}

	// 7. Attempt is terminal. Decide retry or job terminal.
	if state.IsAttemptSucceeded(attempt.Status.Conditions) {
		// Attempt succeeded -> Job succeeded.
		now := metav1.Now()
		job.Status.CompletedAttempts++
		job.Status.CompletionTime = &now
		state.TrySetJobTerminal(&job.Status.Conditions,
			v1alpha1.JobConditionComplete, metav1.ConditionTrue,
			"Succeeded", "Job completed successfully")

		if err := r.Status().Update(ctx, job); err != nil {
			observability.ReconcileTotal.WithLabelValues("job", "error").Inc()
			return ctrl.Result{}, fmt.Errorf("update job to Succeeded: %w", err)
		}

		r.record(job, "Normal", "Succeeded", "Job completed successfully")
		logger.Info("job succeeded")
		observability.AttemptTotal.WithLabelValues("succeeded").Inc()
		observability.ReconcileTotal.WithLabelValues("job", "success").Inc()
		return ctrl.Result{}, nil
	}

	// Attempt failed. Check retry budget.
	job.Status.FailedAttempts++
	observability.AttemptTotal.WithLabelValues("failed").Inc()

	if job.Status.FailedAttempts < job.Spec.Execution.MaxAttempts {
		logger.Info("retrying job",
			"failedAttempts", job.Status.FailedAttempts,
			"maxAttempts", job.Spec.Execution.MaxAttempts)

		// Create a new attempt for retry. We need to reload snap ordinals.
		newAttempt, err := r.createAttempt(ctx, snap)
		if err != nil {
			observability.ReconcileTotal.WithLabelValues("job", "error").Inc()
			return ctrl.Result{}, fmt.Errorf("create retry attempt: %w", err)
		}
		observability.AttemptTotal.WithLabelValues("created").Inc()

		job.Status.ActiveAttempt = &v1alpha1.AttemptReference{
			Name:    newAttempt.Name,
			Ordinal: newAttempt.Spec.Ordinal,
			UID:     newAttempt.UID,
		}

		// Reset to Scheduled (clear Running condition).
		now := metav1.Now()
		state.SetJobCondition(&job.Status.Conditions,
			v1alpha1.JobConditionRunning, metav1.ConditionFalse,
			"Retrying", "Attempt failed, retrying")
		state.SetJobCondition(&job.Status.Conditions,
			v1alpha1.JobConditionScheduled, metav1.ConditionTrue,
			"Retrying", fmt.Sprintf("Attempt #%d failed, retrying (#%d)", attempt.Spec.Ordinal, newAttempt.Spec.Ordinal))
		_ = now

		if err := r.Status().Update(ctx, job); err != nil {
			observability.ReconcileTotal.WithLabelValues("job", "error").Inc()
			return ctrl.Result{}, fmt.Errorf("update job for retry: %w", err)
		}

		r.record(job, "Warning", "Retrying",
			fmt.Sprintf("Attempt #%d failed, retrying (#%d)", attempt.Spec.Ordinal, newAttempt.Spec.Ordinal))
		observability.ReconcileTotal.WithLabelValues("job", "success").Inc()
		return ctrl.Result{Requeue: true}, nil
	}

	// Retry budget exhausted.
	now := metav1.Now()
	job.Status.CompletionTime = &now
	state.TrySetJobTerminal(&job.Status.Conditions,
		v1alpha1.JobConditionFailed, metav1.ConditionTrue,
		"RetriesExhausted", fmt.Sprintf("Job failed after %d attempt(s)", job.Status.FailedAttempts))

	if err := r.Status().Update(ctx, job); err != nil {
		observability.ReconcileTotal.WithLabelValues("job", "error").Inc()
		return ctrl.Result{}, fmt.Errorf("update job to Failed: %w", err)
	}

	r.record(job, "Warning", "Failed",
		fmt.Sprintf("Job failed after %d attempt(s)", job.Status.FailedAttempts))
	logger.Info("job failed permanently", "failedAttempts", job.Status.FailedAttempts)
	observability.ReconcileTotal.WithLabelValues("job", "success").Inc()
	return ctrl.Result{}, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// createAttempt builds and creates an AgentAttempt CR owned by the given job.
// Uses counter-based ordinals (FailedAttempts + CompletedAttempts + 1) so that
// repeated calls for the same state produce the same name, making it idempotent.
func (r *JobReconciler) createAttempt(ctx context.Context, snap *JobSnapshot) (*v1alpha1.AgentAttempt, error) {
	logger := log.FromContext(ctx)
	job := snap.Job

	ordinal := job.Status.FailedAttempts + job.Status.CompletedAttempts + 1
	attemptName := fmt.Sprintf("%s-%d", job.Name, ordinal)

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
			JobRef: v1alpha1.ObjectRef{
				Name:      job.Name,
				Namespace: job.Namespace,
				UID:       job.UID,
			},
			Ordinal: ordinal,
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

// SetupWithManager registers the controller with the manager.
func (r *JobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.AgentJob{}).
		Owns(&v1alpha1.AgentAttempt{}).
		Complete(r)
}
