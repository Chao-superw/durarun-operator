// Package e2e provides a fault-injection E2E test framework for the
// durarun-operator. It is designed to run against a fake client today and
// can later be pointed at a real kind cluster with minimal changes.
package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"durarun-operator/api/v1alpha1"
	"durarun-operator/internal/controller"
)

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

// Scenario defines a fault injection test scenario.
type Scenario struct {
	Name        string
	Description string
	Setup       func(ctx context.Context, env *TestEnv) error
	Faults      []Fault
	Invariants  []InvariantCheck
	// Verify is called after all faults are injected and reconciled.
	// It should assert on the final expected outcome.
	Verify func(ctx context.Context, t *testing.T, env *TestEnv)
}

// Fault describes a fault to inject.
type Fault struct {
	Name   string
	Inject func(ctx context.Context, env *TestEnv) error
	Delay  time.Duration // delay before injecting (unused in fake-client mode)
}

// InvariantCheck verifies system invariants after fault injection.
type InvariantCheck struct {
	Name  string
	Check func(ctx context.Context, env *TestEnv) error
}

// TestEnv wraps the test environment (fake client, reconciler, recorder).
type TestEnv struct {
	Client     client.Client
	Scheme     *runtime.Scheme
	Reconciler *controller.JobReconciler
	Recorder   *record.FakeRecorder
}

// ---------------------------------------------------------------------------
// TestEnv helpers
// ---------------------------------------------------------------------------

// buildScheme registers all types needed by the fake client.
func buildScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = v1alpha1.AddToScheme(s)
	return s
}

// NewTestEnv creates a fresh TestEnv backed by a controller-runtime fake client.
func NewTestEnv(t *testing.T) *TestEnv {
	t.Helper()
	scheme := buildScheme()
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.AgentJob{}, &v1alpha1.AgentAttempt{}).
		Build()

	rec := record.NewFakeRecorder(64)

	r := &controller.JobReconciler{
		Client:   cl,
		Scheme:   scheme,
		Recorder: rec,
	}

	return &TestEnv{
		Client:     cl,
		Scheme:     scheme,
		Reconciler: r,
		Recorder:   rec,
	}
}

// ---------------------------------------------------------------------------
// Job factory
// ---------------------------------------------------------------------------

// NewJob creates a minimal AgentJob suitable for testing.
func NewJob(name, ns string, maxAttempts int32) *v1alpha1.AgentJob {
	return &v1alpha1.AgentJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       types.UID(fmt.Sprintf("uid-%s", name)),
		},
		Spec: v1alpha1.AgentJobSpec{
			Runtime: v1alpha1.RuntimeSpec{
				Image:   "agent:latest",
				Command: []string{"/bin/agent"},
			},
			Execution: v1alpha1.ExecutionSpec{
				MaxAttempts: maxAttempts,
			},
		},
	}
}

// ---------------------------------------------------------------------------
// Reconcile helpers
// ---------------------------------------------------------------------------

// ReconcileOnce runs a single reconcile loop for the given job name.
func ReconcileOnce(ctx context.Context, env *TestEnv, jobName, ns string) (ctrl.Result, error) {
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: jobName, Namespace: ns}}
	return env.Reconciler.Reconcile(ctx, req)
}

// ReconcileN runs up to n reconcile loops, stopping early if the reconciler
// indicates no further work (Requeue=false, RequeueAfter=0) or an error occurs.
func ReconcileN(ctx context.Context, env *TestEnv, jobName, ns string, n int) error {
	for i := 0; i < n; i++ {
		result, err := ReconcileOnce(ctx, env, jobName, ns)
		if err != nil {
			return fmt.Errorf("reconcile iteration %d: %w", i, err)
		}
		if !result.Requeue && result.RequeueAfter == 0 {
			break
		}
	}
	return nil
}

// ReconcileUntilPhase reconciles until the job reaches the given human-readable
// phase (Pending, Scheduling, Running, Succeeded, Failed) or maxIter is hit.
func ReconcileUntilPhase(ctx context.Context, env *TestEnv, jobName, ns, phase string, maxIter int) error {
	for i := 0; i < maxIter; i++ {
		var job v1alpha1.AgentJob
		if err := env.Client.Get(ctx, types.NamespacedName{Name: jobName, Namespace: ns}, &job); err != nil {
			return fmt.Errorf("get job: %w", err)
		}
		if v1alpha1.JobPhaseFromConditions(job.Status.Conditions) == phase {
			return nil
		}
		if _, err := ReconcileOnce(ctx, env, jobName, ns); err != nil {
			return fmt.Errorf("reconcile iteration %d: %w", i, err)
		}
	}
	return fmt.Errorf("job %s did not reach phase %q within %d iterations", jobName, phase, maxIter)
}

// SetPodPhase finds the pod for the given attempt and sets its phase.
func SetPodPhase(ctx context.Context, env *TestEnv, podName, ns string, phase corev1.PodPhase) error {
	var pod corev1.Pod
	if err := env.Client.Get(ctx, types.NamespacedName{Name: podName, Namespace: ns}, &pod); err != nil {
		return fmt.Errorf("get pod %s: %w", podName, err)
	}
	pod.Status.Phase = phase
	if phase == corev1.PodSucceeded {
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{
			{
				Name: "agent",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						ExitCode: 0,
						Reason:   "Completed",
					},
				},
			},
		}
	}
	if phase == corev1.PodRunning {
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{
			{
				Name:  "agent",
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
				Ready: true,
			},
		}
	}
	return env.Client.Status().Update(ctx, &pod)
}

// GetJob fetches the current job from the fake client.
func GetJob(ctx context.Context, env *TestEnv, name, ns string) (*v1alpha1.AgentJob, error) {
	var job v1alpha1.AgentJob
	if err := env.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, &job); err != nil {
		return nil, err
	}
	return &job, nil
}

// GetAttempt fetches the current attempt from the fake client.
func GetAttempt(ctx context.Context, env *TestEnv, name, ns string) (*v1alpha1.AgentAttempt, error) {
	var attempt v1alpha1.AgentAttempt
	if err := env.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, &attempt); err != nil {
		return nil, err
	}
	return &attempt, nil
}

// IsJobPhase returns true if the job is in the given phase.
func IsJobPhase(job *v1alpha1.AgentJob, phase string) bool {
	return v1alpha1.JobPhaseFromConditions(job.Status.Conditions) == phase
}

// HasConditionTrue returns true if the job has the given condition set to True.
func HasConditionTrue(job *v1alpha1.AgentJob, condType string) bool {
	return apimeta.IsStatusConditionTrue(job.Status.Conditions, condType)
}
