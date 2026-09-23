package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"durarun-operator/api/v1alpha1"
	"durarun-operator/internal/observability"
)

// PoolReconciler reconciles SandboxPool objects, maintaining a warm pool of
// pre-created Pods that can be quickly claimed by the JobController.
type PoolReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=durarun.io,resources=sandboxpools,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=durarun.io,resources=sandboxpools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;delete;patch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile handles a single reconciliation loop for a SandboxPool.
func (r *PoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	start := time.Now()
	defer func() {
		observability.ReconcileDuration.WithLabelValues("pool").Observe(time.Since(start).Seconds())
	}()

	logger := log.FromContext(ctx).WithName("pool-controller")

	// 1. Fetch the SandboxPool.
	var pool v1alpha1.SandboxPool
	if err := r.Get(ctx, req.NamespacedName, &pool); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("SandboxPool deleted, nothing to do")
			return ctrl.Result{}, nil
		}
		observability.ReconcileTotal.WithLabelValues("pool", "error").Inc()
		return ctrl.Result{}, err
	}

	ctx, span := observability.StartReconcileSpan(ctx, "pool", pool.Name, "reconcile")
	defer span.End()

	// 2. List all Pods owned by this pool.
	var podList corev1.PodList
	if err := r.List(ctx, &podList,
		client.InNamespace(pool.Namespace),
		client.MatchingLabels{"durarun.io/pool": pool.Name},
	); err != nil {
		return ctrl.Result{}, fmt.Errorf("list pool pods: %w", err)
	}

	// 3. Classify pods.
	var idle, claimed, pending, terminating int
	var idlePods []*corev1.Pod
	for i := range podList.Items {
		pod := &podList.Items[i]

		if pod.DeletionTimestamp != nil {
			terminating++
			continue
		}

		state := pod.Labels["durarun.io/pool-state"]
		switch {
		case pod.Status.Phase == corev1.PodRunning && state == "idle":
			idle++
			idlePods = append(idlePods, pod)
		case pod.Status.Phase == corev1.PodRunning && state == "claimed":
			claimed++
		case pod.Status.Phase == corev1.PodPending:
			pending++
		default:
			// Pods in other states (Succeeded, Failed, Unknown) are counted
			// separately and not included in scaling decisions.
		}
	}

	// Total counts non-terminating pods that are in idle, claimed, or pending states.
	total := idle + claimed + pending

	// 4. Scale logic ported from old pool.go Reconcile().
	cooldownOK := pool.Status.LastScaleTime == nil ||
		time.Since(pool.Status.LastScaleTime.Time) >= time.Duration(pool.Spec.CooldownSeconds)*time.Second

	scaled := false

	if total > 0 {
		ratio := float64(idle) / float64(total)

		// Scale up: too few idle pods relative to total.
		if ratio < pool.Spec.ScaleUpThreshold && cooldownOK {
			added := 0
			for i := 0; i < pool.Spec.ScaleUpStep && total+added < pool.Spec.MaxSize; i++ {
				pod := r.buildPoolPod(&pool)
				if err := controllerutil.SetControllerReference(&pool, pod, r.Scheme); err != nil {
					return ctrl.Result{}, fmt.Errorf("set pool pod owner ref: %w", err)
				}
				if err := r.Create(ctx, pod); err != nil {
					return ctrl.Result{}, fmt.Errorf("create pool pod: %w", err)
				}
				added++
			}
			if added > 0 {
				scaled = true
				r.record(&pool, "Normal", "ScaledUp", fmt.Sprintf("Created %d pod(s), total=%d", added, total+added))
				logger.Info("scaled up", "added", added, "total", total+added)
			}
		}

		// Scale down: too many idle pods relative to total.
		if ratio > pool.Spec.ScaleDownThreshold && total > pool.Spec.MinSize && cooldownOK {
			removed := 0
			for _, pod := range idlePods {
				if total-removed <= pool.Spec.MinSize {
					break
				}
				if err := r.Delete(ctx, pod); err != nil {
					if !apierrors.IsNotFound(err) {
						return ctrl.Result{}, fmt.Errorf("delete idle pod: %w", err)
					}
				}
				removed++
			}
			if removed > 0 {
				scaled = true
				r.record(&pool, "Normal", "ScaledDown", fmt.Sprintf("Removed %d idle pod(s), total=%d", removed, total-removed))
				logger.Info("scaled down", "removed", removed, "total", total-removed)
			}
		}
	} else {
		// No pods at all; scale up to minSize.
		if pool.Spec.MinSize > 0 {
			added := 0
			for i := 0; i < pool.Spec.MinSize; i++ {
				pod := r.buildPoolPod(&pool)
				if err := controllerutil.SetControllerReference(&pool, pod, r.Scheme); err != nil {
					return ctrl.Result{}, fmt.Errorf("set pool pod owner ref: %w", err)
				}
				if err := r.Create(ctx, pod); err != nil {
					return ctrl.Result{}, fmt.Errorf("create pool pod: %w", err)
				}
				added++
			}
			if added > 0 {
				scaled = true
				r.record(&pool, "Normal", "ScaledUp", fmt.Sprintf("Initialized pool with %d pod(s)", added))
				logger.Info("initialized pool", "pods", added)
			}
		}
	}

	// 5. Update SandboxPool status.
	now := metav1.Now()
	pool.Status.TotalPods = total
	pool.Status.IdlePods = idle
	pool.Status.ClaimedPods = claimed
	pool.Status.Ready = idle > 0
	if scaled {
		pool.Status.LastScaleTime = &now
	}

	if err := r.Status().Update(ctx, &pool); err != nil {
		observability.ReconcileTotal.WithLabelValues("pool", "error").Inc()
		return ctrl.Result{}, fmt.Errorf("update pool status: %w", err)
	}

	observability.ReconcileTotal.WithLabelValues("pool", "success").Inc()

	// Requeue periodically to re-evaluate scaling.
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// buildPoolPod creates a Pod spec from the pool's template.
func (r *PoolReconciler) buildPoolPod(pool *v1alpha1.SandboxPool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: fmt.Sprintf("%s-", pool.Name),
			Namespace:    pool.Namespace,
			Labels: map[string]string{
				"durarun.io/pool":       pool.Name,
				"durarun.io/pool-state": "idle",
				"durarun.io/managed-by": "pool-controller",
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:      "agent",
				Image:     pool.Spec.Template.Image,
				Command:   pool.Spec.Template.Command,
				Env:       buildPoolEnvVars(pool.Spec.Template.Env),
				Resources: buildPoolResources(pool.Spec.Template.Resources),
			}},
			AutomountServiceAccountToken: poolBoolPtr(false),
		},
	}

	// If no command specified, keep the pod alive with sleep.
	if len(pod.Spec.Containers[0].Command) == 0 {
		pod.Spec.Containers[0].Command = []string{"sleep", "infinity"}
	}

	// Set RuntimeClassName if specified.
	if pool.Spec.RuntimeClassName != "" {
		rc := pool.Spec.RuntimeClassName
		pod.Spec.RuntimeClassName = &rc
	}

	return pod
}

// buildPoolEnvVars converts a map of environment variables into the K8s format.
func buildPoolEnvVars(env map[string]string) []corev1.EnvVar {
	if len(env) == 0 {
		return nil
	}
	var envs []corev1.EnvVar
	for k, v := range env {
		envs = append(envs, corev1.EnvVar{Name: k, Value: v})
	}
	return envs
}

// buildPoolResources converts a v1alpha1.ResourceSpec into Kubernetes
// ResourceRequirements for pool pods.
func buildPoolResources(spec v1alpha1.ResourceSpec) corev1.ResourceRequirements {
	reqs := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{},
		Limits:   corev1.ResourceList{},
	}

	if spec.CPULimit != "" {
		q := resource.MustParse(spec.CPULimit)
		reqs.Requests[corev1.ResourceCPU] = q
		reqs.Limits[corev1.ResourceCPU] = q
	}

	if spec.MemoryLimit != "" {
		q := resource.MustParse(spec.MemoryLimit)
		reqs.Requests[corev1.ResourceMemory] = q
		reqs.Limits[corev1.ResourceMemory] = q
	}

	return reqs
}

// poolBoolPtr returns a pointer to a bool value.
func poolBoolPtr(b bool) *bool {
	return &b
}

// record emits a Kubernetes event if the Recorder is configured.
func (r *PoolReconciler) record(pool *v1alpha1.SandboxPool, eventType, reason, msg string) {
	if r.Recorder != nil {
		r.Recorder.Event(pool, eventType, reason, msg)
	}
}

// SetupWithManager registers the PoolReconciler with the manager.
// It watches SandboxPool as the primary resource and Pods as owned resources.
func (r *PoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.SandboxPool{}).
		Owns(&corev1.Pod{}).
		Complete(r)
}

// ---------------------------------------------------------------------------
// ClaimPoolPod - callable by JobController
// ---------------------------------------------------------------------------

// ClaimPoolPod finds an idle pod in the named pool and patches its labels to
// "claimed". Returns the claimed Pod, or nil if no idle pods are available.
func ClaimPoolPod(ctx context.Context, c client.Client, poolName, namespace, jobName string) (*corev1.Pod, error) {
	var podList corev1.PodList
	if err := c.List(ctx, &podList,
		client.InNamespace(namespace),
		client.MatchingLabels{
			"durarun.io/pool":       poolName,
			"durarun.io/pool-state": "idle",
		},
	); err != nil {
		return nil, err
	}

	if len(podList.Items) == 0 {
		return nil, nil // no idle pods
	}

	// Try to claim the first idle pod via a label patch.
	pod := &podList.Items[0]
	patch := client.MergeFrom(pod.DeepCopy())
	pod.Labels["durarun.io/pool-state"] = "claimed"
	pod.Labels["durarun.io/job"] = jobName
	if err := c.Patch(ctx, pod, patch); err != nil {
		return nil, err // might be claimed by another controller
	}

	return pod, nil
}
