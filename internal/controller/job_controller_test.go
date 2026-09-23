package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"durarun-operator/api/v1alpha1"
)

// buildScheme registers all types needed by the fake client.
func buildScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = v1alpha1.AddToScheme(s)
	return s
}

func newJob(name, ns string) *v1alpha1.AgentJob {
	return &v1alpha1.AgentJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       types.UID("job-uid-1"),
		},
		Spec: v1alpha1.AgentJobSpec{
			Image:      "agent:latest",
			Command:    []string{"/bin/agent"},
			MaxRetries: 2,
			Resources: v1alpha1.ResourceSpec{
				CPULimit:    "1",
				MemoryLimit: "512m",
			},
			Isolation: v1alpha1.IsolationSpec{
				Level: v1alpha1.L1GVisor,
			},
		},
	}
}

// TestReconcile_Pending_CreatesAttempt verifies that reconciling a new
// AgentJob (empty phase) creates an AgentAttempt and transitions to Scheduling.
func TestReconcile_Pending_CreatesAttempt(t *testing.T) {
	scheme := buildScheme()
	job := newJob("my-job", "default")

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(job).
		WithStatusSubresource(&v1alpha1.AgentJob{}, &v1alpha1.AgentAttempt{}).
		Build()

	r := &JobReconciler{
		Client: cl,
		Scheme: scheme,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "my-job", Namespace: "default"}}
	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}
	if !result.Requeue {
		t.Error("expected Requeue=true after pending->scheduling transition")
	}

	// Verify the AgentAttempt was created.
	var attempt v1alpha1.AgentAttempt
	key := types.NamespacedName{Name: "my-job-1", Namespace: "default"}
	if err := cl.Get(context.Background(), key, &attempt); err != nil {
		t.Fatalf("expected attempt 'my-job-1' to exist: %v", err)
	}
	if attempt.Spec.JobRef != "my-job" {
		t.Errorf("attempt JobRef = %q, want 'my-job'", attempt.Spec.JobRef)
	}
	if attempt.Spec.Number != 1 {
		t.Errorf("attempt Number = %d, want 1", attempt.Spec.Number)
	}

	// Verify job status was updated to Scheduling.
	var updatedJob v1alpha1.AgentJob
	if err := cl.Get(context.Background(), req.NamespacedName, &updatedJob); err != nil {
		t.Fatalf("failed to get updated job: %v", err)
	}
	if updatedJob.Status.Phase != v1alpha1.JobPhaseScheduling {
		t.Errorf("job phase = %q, want Scheduling", updatedJob.Status.Phase)
	}
	if updatedJob.Status.ActiveAttempt == nil || updatedJob.Status.ActiveAttempt.Name != "my-job-1" {
		t.Error("expected ActiveAttempt to reference my-job-1")
	}
}

// TestReconcile_Scheduling_CreatesPod verifies that reconciling a Scheduling
// job creates a Pod for the active attempt.
func TestReconcile_Scheduling_CreatesPod(t *testing.T) {
	scheme := buildScheme()
	job := newJob("my-job", "default")
	job.Status.Phase = v1alpha1.JobPhaseScheduling
	job.Status.ActiveAttempt = &v1alpha1.AttemptReference{
		Name:   "my-job-1",
		Number: 1,
		UID:    types.UID("attempt-uid-1"),
	}

	attempt := &v1alpha1.AgentAttempt{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-job-1",
			Namespace: "default",
			UID:       types.UID("attempt-uid-1"),
		},
		Spec: v1alpha1.AgentAttemptSpec{
			JobRef: "my-job",
			Number: 1,
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(job, attempt).
		WithStatusSubresource(&v1alpha1.AgentJob{}, &v1alpha1.AgentAttempt{}).
		Build()

	r := &JobReconciler{
		Client: cl,
		Scheme: scheme,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "my-job", Namespace: "default"}}
	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Error("expected RequeueAfter > 0 when pod is pending")
	}

	// Verify Pod was created.
	var pod corev1.Pod
	podKey := types.NamespacedName{Name: "my-job-1", Namespace: "default"}
	if err := cl.Get(context.Background(), podKey, &pod); err != nil {
		t.Fatalf("expected pod 'my-job-1' to exist: %v", err)
	}
	if pod.Spec.Containers[0].Image != "agent:latest" {
		t.Errorf("pod image = %q, want 'agent:latest'", pod.Spec.Containers[0].Image)
	}
	if pod.Labels["durarun.io/job"] != "my-job" {
		t.Errorf("pod missing label durarun.io/job")
	}

	// Verify attempt status was updated with PodName.
	var updatedAttempt v1alpha1.AgentAttempt
	if err := cl.Get(context.Background(), types.NamespacedName{Name: "my-job-1", Namespace: "default"}, &updatedAttempt); err != nil {
		t.Fatalf("failed to get updated attempt: %v", err)
	}
	if updatedAttempt.Status.PodName != "my-job-1" {
		t.Errorf("attempt PodName = %q, want 'my-job-1'", updatedAttempt.Status.PodName)
	}
}

// TestReconcile_NotFound verifies that reconciling a deleted job returns
// without error.
func TestReconcile_NotFound(t *testing.T) {
	scheme := buildScheme()
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	r := &JobReconciler{
		Client: cl,
		Scheme: scheme,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "gone", Namespace: "default"}}
	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile returned error for deleted job: %v", err)
	}
	if result.Requeue || result.RequeueAfter > 0 {
		t.Error("expected no requeue for deleted job")
	}
}

// TestReconcile_Terminal verifies that terminal-phase jobs are no-ops.
func TestReconcile_Terminal(t *testing.T) {
	scheme := buildScheme()

	for _, phase := range []v1alpha1.JobPhase{v1alpha1.JobPhaseSucceeded, v1alpha1.JobPhaseFailed, v1alpha1.JobPhaseTerminated} {
		job := newJob("done-job", "default")
		job.Status.Phase = phase

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(job).
			WithStatusSubresource(&v1alpha1.AgentJob{}).
			Build()

		r := &JobReconciler{Client: cl, Scheme: scheme}
		req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "done-job", Namespace: "default"}}
		result, err := r.Reconcile(context.Background(), req)
		if err != nil {
			t.Errorf("phase %s: unexpected error: %v", phase, err)
		}
		if result.Requeue || result.RequeueAfter > 0 {
			t.Errorf("phase %s: expected no requeue", phase)
		}
	}
}

// TestReconcile_Idempotent_AttemptAlreadyExists verifies that reconciling a
// Pending job when the Attempt already exists (re-entrant reconcile) succeeds
// without error and reuses the existing Attempt.
func TestReconcile_Idempotent_AttemptAlreadyExists(t *testing.T) {
	scheme := buildScheme()
	job := newJob("my-job", "default")

	// Pre-create the Attempt that reconcilePending would create.
	attempt := &v1alpha1.AgentAttempt{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-job-1",
			Namespace: "default",
			UID:       types.UID("preexisting-attempt-uid"),
			Labels: map[string]string{
				"durarun.io/job": "my-job",
			},
		},
		Spec: v1alpha1.AgentAttemptSpec{
			JobRef: "my-job",
			Number: 1,
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(job, attempt).
		WithStatusSubresource(&v1alpha1.AgentJob{}, &v1alpha1.AgentAttempt{}).
		Build()

	r := &JobReconciler{
		Client: cl,
		Scheme: scheme,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "my-job", Namespace: "default"}}
	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}
	if !result.Requeue {
		t.Error("expected Requeue=true after pending->scheduling transition")
	}

	// Verify job moved to Scheduling and references the pre-existing attempt.
	var updatedJob v1alpha1.AgentJob
	if err := cl.Get(context.Background(), req.NamespacedName, &updatedJob); err != nil {
		t.Fatalf("failed to get updated job: %v", err)
	}
	if updatedJob.Status.Phase != v1alpha1.JobPhaseScheduling {
		t.Errorf("job phase = %q, want Scheduling", updatedJob.Status.Phase)
	}
	if updatedJob.Status.ActiveAttempt == nil || updatedJob.Status.ActiveAttempt.Name != "my-job-1" {
		t.Error("expected ActiveAttempt to reference my-job-1")
	}

	// Verify no duplicate Attempt was created (still exactly one).
	var attemptList v1alpha1.AgentAttemptList
	if err := cl.List(context.Background(), &attemptList); err != nil {
		t.Fatalf("failed to list attempts: %v", err)
	}
	if len(attemptList.Items) != 1 {
		t.Errorf("expected exactly 1 attempt, got %d", len(attemptList.Items))
	}
}

// TestReconcile_Idempotent_PodAlreadyExists verifies that reconciling a
// Scheduling job when the Pod already exists (re-entrant reconcile) succeeds
// and correctly updates the Attempt status with the existing Pod reference.
func TestReconcile_Idempotent_PodAlreadyExists(t *testing.T) {
	scheme := buildScheme()
	job := newJob("my-job", "default")
	job.Status.Phase = v1alpha1.JobPhaseScheduling
	job.Status.ActiveAttempt = &v1alpha1.AttemptReference{
		Name:   "my-job-1",
		Number: 1,
		UID:    types.UID("attempt-uid-1"),
	}

	attempt := &v1alpha1.AgentAttempt{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-job-1",
			Namespace: "default",
			UID:       types.UID("attempt-uid-1"),
		},
		Spec: v1alpha1.AgentAttemptSpec{
			JobRef: "my-job",
			Number: 1,
		},
		// PodName is empty: simulates the case where the Pod was created but
		// the Attempt status update failed.
	}

	// Pre-create the Pod that reconcileScheduling would create.
	existingPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-job-1",
			Namespace: "default",
			UID:       types.UID("existing-pod-uid"),
			Labels: map[string]string{
				"durarun.io/job":     "my-job",
				"durarun.io/attempt": "my-job-1",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "agent", Image: "agent:latest"},
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(job, attempt, existingPod).
		WithStatusSubresource(&v1alpha1.AgentJob{}, &v1alpha1.AgentAttempt{}).
		Build()

	r := &JobReconciler{
		Client: cl,
		Scheme: scheme,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "my-job", Namespace: "default"}}
	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Error("expected RequeueAfter > 0 after pod discovery")
	}

	// Verify Attempt status was updated with the existing Pod reference.
	var updatedAttempt v1alpha1.AgentAttempt
	if err := cl.Get(context.Background(), types.NamespacedName{Name: "my-job-1", Namespace: "default"}, &updatedAttempt); err != nil {
		t.Fatalf("failed to get updated attempt: %v", err)
	}
	if updatedAttempt.Status.PodName != "my-job-1" {
		t.Errorf("attempt PodName = %q, want 'my-job-1'", updatedAttempt.Status.PodName)
	}
	if updatedAttempt.Status.PodUID != types.UID("existing-pod-uid") {
		t.Errorf("attempt PodUID = %q, want 'existing-pod-uid'", updatedAttempt.Status.PodUID)
	}

	// Verify no duplicate Pod was created (still exactly one).
	var podList corev1.PodList
	if err := cl.List(context.Background(), &podList); err != nil {
		t.Fatalf("failed to list pods: %v", err)
	}
	if len(podList.Items) != 1 {
		t.Errorf("expected exactly 1 pod, got %d", len(podList.Items))
	}
}

// TestReconcile_UIDFencing_StalePod verifies that when a Running job's Pod has
// a different UID than what the Attempt recorded (stale/replaced Pod), the
// reconciler treats it as a failure rather than updating status from the
// impostor Pod.
func TestReconcile_UIDFencing_StalePod(t *testing.T) {
	scheme := buildScheme()
	job := newJob("my-job", "default")
	job.Status.Phase = v1alpha1.JobPhaseRunning
	job.Status.ActiveAttempt = &v1alpha1.AttemptReference{
		Name:   "my-job-1",
		Number: 1,
		UID:    types.UID("attempt-uid-1"),
	}

	attempt := &v1alpha1.AgentAttempt{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-job-1",
			Namespace: "default",
			UID:       types.UID("attempt-uid-1"),
		},
		Spec: v1alpha1.AgentAttemptSpec{
			JobRef: "my-job",
			Number: 1,
		},
		Status: v1alpha1.AgentAttemptStatus{
			PodName: "my-job-1",
			PodUID:  types.UID("old-pod-uid"), // original Pod UID
			Phase:   v1alpha1.AttemptPhaseExecuting,
		},
	}

	// Create a Pod with a DIFFERENT UID (simulates K8s replacing the Pod).
	stalePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-job-1",
			Namespace: "default",
			UID:       types.UID("new-pod-uid"), // different from attempt's PodUID
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "agent", Image: "agent:latest"},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodSucceeded, // even though it "succeeded", UID doesn't match
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(job, attempt, stalePod).
		WithStatusSubresource(&v1alpha1.AgentJob{}, &v1alpha1.AgentAttempt{}).
		Build()

	r := &JobReconciler{
		Client: cl,
		Scheme: scheme,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "my-job", Namespace: "default"}}
	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}

	// The UID mismatch should be treated as a failure. With MaxRetries=2 and
	// 0 previous failures, it should retry (create attempt #2 and transition
	// to Scheduling).
	var updatedJob v1alpha1.AgentJob
	if err := cl.Get(context.Background(), req.NamespacedName, &updatedJob); err != nil {
		t.Fatalf("failed to get updated job: %v", err)
	}

	if updatedJob.Status.FailedAttempts != 1 {
		t.Errorf("expected FailedAttempts=1, got %d", updatedJob.Status.FailedAttempts)
	}

	// Should have retried -> Scheduling with a new attempt.
	if updatedJob.Status.Phase != v1alpha1.JobPhaseScheduling {
		t.Errorf("expected phase Scheduling (retry), got %q", updatedJob.Status.Phase)
	}
	if updatedJob.Status.ActiveAttempt == nil || updatedJob.Status.ActiveAttempt.Name != "my-job-2" {
		t.Error("expected ActiveAttempt to reference my-job-2 (retry attempt)")
	}

	// The original attempt should be marked Failed.
	var updatedAttempt v1alpha1.AgentAttempt
	if err := cl.Get(context.Background(), types.NamespacedName{Name: "my-job-1", Namespace: "default"}, &updatedAttempt); err != nil {
		t.Fatalf("failed to get updated attempt: %v", err)
	}
	if updatedAttempt.Status.Phase != v1alpha1.AttemptPhaseFailed {
		t.Errorf("expected attempt phase Failed, got %q", updatedAttempt.Status.Phase)
	}

	// The stale Pod's success should NOT have been recorded.
	if result.Requeue != true {
		t.Error("expected Requeue=true for retry")
	}
}
