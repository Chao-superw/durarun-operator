package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
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
			Runtime: v1alpha1.RuntimeSpec{
				Image:   "agent:latest",
				Command: []string{"/bin/agent"},
			},
			Execution: v1alpha1.ExecutionSpec{
				MaxAttempts: 3,
			},
		},
	}
}

// TestReconcile_Pending_CreatesAttempt verifies that reconciling a new
// AgentJob (no conditions) creates an AgentAttempt and transitions to Scheduling.
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
	if attempt.Spec.JobRef.Name != "my-job" {
		t.Errorf("attempt JobRef.Name = %q, want 'my-job'", attempt.Spec.JobRef.Name)
	}
	if attempt.Spec.Ordinal != 1 {
		t.Errorf("attempt Ordinal = %d, want 1", attempt.Spec.Ordinal)
	}

	// Verify job status was updated with Scheduled condition.
	var updatedJob v1alpha1.AgentJob
	if err := cl.Get(context.Background(), req.NamespacedName, &updatedJob); err != nil {
		t.Fatalf("failed to get updated job: %v", err)
	}
	if !apimeta.IsStatusConditionTrue(updatedJob.Status.Conditions, v1alpha1.JobConditionScheduled) {
		t.Error("expected Scheduled condition to be True")
	}
	if updatedJob.Status.ActiveAttempt == nil || updatedJob.Status.ActiveAttempt.Name != "my-job-1" {
		t.Error("expected ActiveAttempt to reference my-job-1")
	}

	// Verify spec hash was computed and stored.
	if updatedJob.Status.SpecHash == "" {
		t.Error("expected SpecHash to be set after reconcile")
	}
	expectedHash := ComputeSpecHash(&job.Spec)
	if updatedJob.Status.SpecHash != expectedHash {
		t.Errorf("SpecHash = %q, want %q", updatedJob.Status.SpecHash, expectedHash)
	}
}

// TestReconcile_Scheduling_CreatesPod verifies that reconciling a Scheduling
// job creates a Pod for the active attempt.
func TestReconcile_Scheduling_CreatesPod(t *testing.T) {
	scheme := buildScheme()
	job := newJob("my-job", "default")
	apimeta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type:               v1alpha1.JobConditionScheduled,
		Status:             metav1.ConditionTrue,
		Reason:             "AttemptCreated",
		LastTransitionTime: metav1.Now(),
	})
	job.Status.ActiveAttempt = &v1alpha1.AttemptReference{
		Name:    "my-job-1",
		Ordinal: 1,
		UID:     types.UID("attempt-uid-1"),
	}

	attempt := &v1alpha1.AgentAttempt{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-job-1",
			Namespace: "default",
			UID:       types.UID("attempt-uid-1"),
			Labels: map[string]string{
				"durarun.io/job": "my-job",
			},
		},
		Spec: v1alpha1.AgentAttemptSpec{
			JobRef:  v1alpha1.ObjectRef{Name: "my-job", Namespace: "default", UID: "job-uid-1"},
			Ordinal: 1,
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

// TestReconcile_Terminal verifies that terminal-condition jobs are no-ops.
func TestReconcile_Terminal(t *testing.T) {
	scheme := buildScheme()

	terminalConditions := []struct {
		condType string
		reason   string
	}{
		{v1alpha1.JobConditionComplete, "Succeeded"},
		{v1alpha1.JobConditionFailed, "Failed"},
	}

	for _, tc := range terminalConditions {
		job := newJob("done-job", "default")
		apimeta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
			Type:               tc.condType,
			Status:             metav1.ConditionTrue,
			Reason:             tc.reason,
			LastTransitionTime: metav1.Now(),
		})

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(job).
			WithStatusSubresource(&v1alpha1.AgentJob{}).
			Build()

		r := &JobReconciler{Client: cl, Scheme: scheme}
		req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "done-job", Namespace: "default"}}
		result, err := r.Reconcile(context.Background(), req)
		if err != nil {
			t.Errorf("condition %s: unexpected error: %v", tc.condType, err)
		}
		if result.Requeue || result.RequeueAfter > 0 {
			t.Errorf("condition %s: expected no requeue", tc.condType)
		}
	}
}

// TestReconcile_Idempotent_AttemptAlreadyExists verifies that reconciling a
// Pending job when the Attempt already exists succeeds and reuses the existing Attempt.
func TestReconcile_Idempotent_AttemptAlreadyExists(t *testing.T) {
	scheme := buildScheme()
	job := newJob("my-job", "default")

	// Pre-create the Attempt with proper labels.
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
			JobRef:  v1alpha1.ObjectRef{Name: "my-job", Namespace: "default", UID: "job-uid-1"},
			Ordinal: 1,
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

	// Verify job moved to Scheduled and references the pre-existing attempt.
	var updatedJob v1alpha1.AgentJob
	if err := cl.Get(context.Background(), req.NamespacedName, &updatedJob); err != nil {
		t.Fatalf("failed to get updated job: %v", err)
	}
	if !apimeta.IsStatusConditionTrue(updatedJob.Status.Conditions, v1alpha1.JobConditionScheduled) {
		t.Error("expected Scheduled condition to be True")
	}
	if updatedJob.Status.ActiveAttempt == nil || updatedJob.Status.ActiveAttempt.Name != "my-job-1" {
		t.Error("expected ActiveAttempt to reference my-job-1")
	}

	// Verify no duplicate Attempt was created.
	var attemptList v1alpha1.AgentAttemptList
	if err := cl.List(context.Background(), &attemptList); err != nil {
		t.Fatalf("failed to list attempts: %v", err)
	}
	if len(attemptList.Items) != 1 {
		t.Errorf("expected exactly 1 attempt, got %d", len(attemptList.Items))
	}
}

// TestReconcile_Idempotent_PodAlreadyExists verifies that reconciling a
// Scheduling job when the Pod already exists succeeds and correctly updates
// the Attempt status with the existing Pod reference.
func TestReconcile_Idempotent_PodAlreadyExists(t *testing.T) {
	scheme := buildScheme()
	job := newJob("my-job", "default")
	apimeta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type:               v1alpha1.JobConditionScheduled,
		Status:             metav1.ConditionTrue,
		Reason:             "AttemptCreated",
		LastTransitionTime: metav1.Now(),
	})
	job.Status.ActiveAttempt = &v1alpha1.AttemptReference{
		Name:    "my-job-1",
		Ordinal: 1,
		UID:     types.UID("attempt-uid-1"),
	}

	attempt := &v1alpha1.AgentAttempt{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-job-1",
			Namespace: "default",
			UID:       types.UID("attempt-uid-1"),
			Labels: map[string]string{
				"durarun.io/job": "my-job",
			},
		},
		Spec: v1alpha1.AgentAttemptSpec{
			JobRef:  v1alpha1.ObjectRef{Name: "my-job", Namespace: "default", UID: "job-uid-1"},
			Ordinal: 1,
		},
	}

	// Pre-create the Pod.
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

	// Verify no duplicate Pod was created.
	var podList corev1.PodList
	if err := cl.List(context.Background(), &podList); err != nil {
		t.Fatalf("failed to list pods: %v", err)
	}
	if len(podList.Items) != 1 {
		t.Errorf("expected exactly 1 pod, got %d", len(podList.Items))
	}
}

// TestReconcile_UIDFencing_StalePod verifies that when a Running job's Pod has
// a different UID than what the Attempt recorded, the reconciler treats it as
// a failure.
func TestReconcile_UIDFencing_StalePod(t *testing.T) {
	scheme := buildScheme()
	job := newJob("my-job", "default")
	apimeta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type:               v1alpha1.JobConditionRunning,
		Status:             metav1.ConditionTrue,
		Reason:             "PodRunning",
		LastTransitionTime: metav1.Now(),
	})
	apimeta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type:               v1alpha1.JobConditionScheduled,
		Status:             metav1.ConditionTrue,
		Reason:             "AttemptCreated",
		LastTransitionTime: metav1.Now(),
	})
	job.Status.ActiveAttempt = &v1alpha1.AttemptReference{
		Name:    "my-job-1",
		Ordinal: 1,
		UID:     types.UID("attempt-uid-1"),
	}

	attempt := &v1alpha1.AgentAttempt{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-job-1",
			Namespace: "default",
			UID:       types.UID("attempt-uid-1"),
			Labels: map[string]string{
				"durarun.io/job": "my-job",
			},
		},
		Spec: v1alpha1.AgentAttemptSpec{
			JobRef:  v1alpha1.ObjectRef{Name: "my-job", Namespace: "default", UID: "job-uid-1"},
			Ordinal: 1,
		},
		Status: v1alpha1.AgentAttemptStatus{
			PodName: "my-job-1",
			PodUID:  types.UID("old-pod-uid"), // original Pod UID
		},
	}

	// Create a Pod with a DIFFERENT UID (simulates K8s replacing the Pod).
	stalePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-job-1",
			Namespace: "default",
			UID:       types.UID("new-pod-uid"), // different from attempt's PodUID
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
		Status: corev1.PodStatus{
			Phase: corev1.PodSucceeded,
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

	// The UID mismatch should be treated as a failure. With MaxAttempts=3 and
	// 0 previous failures, it should retry.
	var updatedJob v1alpha1.AgentJob
	if err := cl.Get(context.Background(), req.NamespacedName, &updatedJob); err != nil {
		t.Fatalf("failed to get updated job: %v", err)
	}

	if updatedJob.Status.FailedAttempts != 1 {
		t.Errorf("expected FailedAttempts=1, got %d", updatedJob.Status.FailedAttempts)
	}

	// Should have retried -> Scheduling with a new attempt.
	if !apimeta.IsStatusConditionTrue(updatedJob.Status.Conditions, v1alpha1.JobConditionScheduled) {
		t.Error("expected Scheduled condition to be True (retry)")
	}
	if updatedJob.Status.ActiveAttempt == nil || updatedJob.Status.ActiveAttempt.Name != "my-job-2" {
		t.Error("expected ActiveAttempt to reference my-job-2 (retry attempt)")
	}

	// The original attempt should be marked Failed.
	var updatedAttempt v1alpha1.AgentAttempt
	if err := cl.Get(context.Background(), types.NamespacedName{Name: "my-job-1", Namespace: "default"}, &updatedAttempt); err != nil {
		t.Fatalf("failed to get updated attempt: %v", err)
	}
	if !apimeta.IsStatusConditionTrue(updatedAttempt.Status.Conditions, v1alpha1.AttemptConditionFailed) {
		t.Error("expected attempt Failed condition to be True")
	}

	if result.Requeue != true {
		t.Error("expected Requeue=true for retry")
	}
}

// TestReconcile_100Repeats_NoDuplicateAttempts verifies that running reconcile
// 100 times on a pending job does not create duplicate attempts.
func TestReconcile_100Repeats_NoDuplicateAttempts(t *testing.T) {
	scheme := buildScheme()
	job := newJob("idempotent-job", "default")

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(job).
		WithStatusSubresource(&v1alpha1.AgentJob{}, &v1alpha1.AgentAttempt{}).
		Build()

	r := &JobReconciler{
		Client: cl,
		Scheme: scheme,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "idempotent-job", Namespace: "default"}}

	for i := 0; i < 100; i++ {
		_, err := r.Reconcile(context.Background(), req)
		if err != nil {
			t.Fatalf("Reconcile iteration %d returned error: %v", i, err)
		}
	}

	// Verify exactly 1 attempt exists.
	var attemptList v1alpha1.AgentAttemptList
	if err := cl.List(context.Background(), &attemptList); err != nil {
		t.Fatalf("failed to list attempts: %v", err)
	}
	if len(attemptList.Items) != 1 {
		t.Errorf("expected exactly 1 attempt after 100 reconciles, got %d", len(attemptList.Items))
	}
}

// TestReconcile_SpecHash_ComputedAndStored verifies that the spec hash is
// computed deterministically and stored in job status.
func TestReconcile_SpecHash_ComputedAndStored(t *testing.T) {
	scheme := buildScheme()
	job := newJob("hash-job", "default")

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(job).
		WithStatusSubresource(&v1alpha1.AgentJob{}, &v1alpha1.AgentAttempt{}).
		Build()

	r := &JobReconciler{
		Client: cl,
		Scheme: scheme,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "hash-job", Namespace: "default"}}
	_, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}

	var updatedJob v1alpha1.AgentJob
	if err := cl.Get(context.Background(), req.NamespacedName, &updatedJob); err != nil {
		t.Fatalf("failed to get updated job: %v", err)
	}

	// Hash should be set.
	if updatedJob.Status.SpecHash == "" {
		t.Fatal("expected SpecHash to be set")
	}

	// Hash should be deterministic.
	expectedHash := ComputeSpecHash(&job.Spec)
	if updatedJob.Status.SpecHash != expectedHash {
		t.Errorf("SpecHash = %q, want %q", updatedJob.Status.SpecHash, expectedHash)
	}

	// Hash should be the same on subsequent reconciles.
	_, err = r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Second reconcile returned error: %v", err)
	}

	var job2 v1alpha1.AgentJob
	if err := cl.Get(context.Background(), req.NamespacedName, &job2); err != nil {
		t.Fatalf("failed to get job after second reconcile: %v", err)
	}
	if job2.Status.SpecHash != expectedHash {
		t.Errorf("SpecHash changed after second reconcile: %q != %q", job2.Status.SpecHash, expectedHash)
	}
}

// TestReconcile_PodCompletion_TriggersAttemptTerminal verifies that when a Pod
// succeeds, the attempt is marked as Complete and the Job transitions to Succeeded.
func TestReconcile_PodCompletion_TriggersAttemptTerminal(t *testing.T) {
	scheme := buildScheme()
	job := newJob("success-job", "default")
	apimeta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type:               v1alpha1.JobConditionScheduled,
		Status:             metav1.ConditionTrue,
		Reason:             "AttemptCreated",
		LastTransitionTime: metav1.Now(),
	})
	apimeta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type:               v1alpha1.JobConditionRunning,
		Status:             metav1.ConditionTrue,
		Reason:             "PodRunning",
		LastTransitionTime: metav1.Now(),
	})
	job.Status.ActiveAttempt = &v1alpha1.AttemptReference{
		Name:    "success-job-1",
		Ordinal: 1,
		UID:     types.UID("attempt-uid-1"),
	}

	attempt := &v1alpha1.AgentAttempt{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "success-job-1",
			Namespace: "default",
			UID:       types.UID("attempt-uid-1"),
			Labels: map[string]string{
				"durarun.io/job": "success-job",
			},
		},
		Spec: v1alpha1.AgentAttemptSpec{
			JobRef:  v1alpha1.ObjectRef{Name: "success-job", Namespace: "default", UID: "job-uid-1"},
			Ordinal: 1,
		},
		Status: v1alpha1.AgentAttemptStatus{
			PodName: "success-job-1",
			PodUID:  types.UID("pod-uid-1"),
		},
	}

	succeededPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "success-job-1",
			Namespace: "default",
			UID:       types.UID("pod-uid-1"),
			Labels: map[string]string{
				"durarun.io/job":     "success-job",
				"durarun.io/attempt": "success-job-1",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "agent", Image: "agent:latest"},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodSucceeded,
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(job, attempt, succeededPod).
		WithStatusSubresource(&v1alpha1.AgentJob{}, &v1alpha1.AgentAttempt{}).
		Build()

	r := &JobReconciler{
		Client: cl,
		Scheme: scheme,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "success-job", Namespace: "default"}}
	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}
	if result.Requeue || result.RequeueAfter > 0 {
		t.Error("expected no requeue for succeeded job")
	}

	// Verify attempt is marked Complete.
	var updatedAttempt v1alpha1.AgentAttempt
	if err := cl.Get(context.Background(), types.NamespacedName{Name: "success-job-1", Namespace: "default"}, &updatedAttempt); err != nil {
		t.Fatalf("failed to get updated attempt: %v", err)
	}
	if !apimeta.IsStatusConditionTrue(updatedAttempt.Status.Conditions, v1alpha1.AttemptConditionComplete) {
		t.Error("expected attempt Complete condition to be True")
	}
	if updatedAttempt.Status.ExitCode == nil || *updatedAttempt.Status.ExitCode != 0 {
		t.Error("expected attempt ExitCode=0")
	}

	// Verify job is marked Complete.
	var updatedJob v1alpha1.AgentJob
	if err := cl.Get(context.Background(), req.NamespacedName, &updatedJob); err != nil {
		t.Fatalf("failed to get updated job: %v", err)
	}
	if !apimeta.IsStatusConditionTrue(updatedJob.Status.Conditions, v1alpha1.JobConditionComplete) {
		t.Error("expected job Complete condition to be True")
	}
	if updatedJob.Status.CompletedAttempts != 1 {
		t.Errorf("expected CompletedAttempts=1, got %d", updatedJob.Status.CompletedAttempts)
	}
	if updatedJob.Status.CompletionTime == nil {
		t.Error("expected CompletionTime to be set")
	}
}

// TestReconcile_OwnershipMismatch_WrongUID verifies that an attempt with
// a mismatched JobRef UID is detected as an ownership violation.
func TestReconcile_OwnershipMismatch_WrongUID(t *testing.T) {
	scheme := buildScheme()
	job := newJob("owner-job", "default")

	// Create an attempt that claims to belong to a DIFFERENT job UID.
	staleAttempt := &v1alpha1.AgentAttempt{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "owner-job-1",
			Namespace: "default",
			UID:       types.UID("stale-attempt-uid"),
			Labels: map[string]string{
				"durarun.io/job": "owner-job",
			},
		},
		Spec: v1alpha1.AgentAttemptSpec{
			JobRef:  v1alpha1.ObjectRef{Name: "owner-job", Namespace: "default", UID: "wrong-job-uid"},
			Ordinal: 1,
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(job, staleAttempt).
		WithStatusSubresource(&v1alpha1.AgentJob{}, &v1alpha1.AgentAttempt{}).
		Build()

	r := &JobReconciler{
		Client: cl,
		Scheme: scheme,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "owner-job", Namespace: "default"}}
	_, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile returned unexpected error: %v", err)
	}

	// Job should be marked as Failed due to ownership violation.
	var updatedJob v1alpha1.AgentJob
	if err := cl.Get(context.Background(), req.NamespacedName, &updatedJob); err != nil {
		t.Fatalf("failed to get updated job: %v", err)
	}
	if !apimeta.IsStatusConditionTrue(updatedJob.Status.Conditions, v1alpha1.JobConditionFailed) {
		t.Error("expected job Failed condition to be True due to ownership mismatch")
	}
}

// TestComputeSpecHash_Deterministic verifies that ComputeSpecHash produces
// the same hash for the same spec.
func TestComputeSpecHash_Deterministic(t *testing.T) {
	spec := v1alpha1.AgentJobSpec{
		Runtime: v1alpha1.RuntimeSpec{
			Image:   "agent:latest",
			Command: []string{"/bin/agent"},
		},
		Execution: v1alpha1.ExecutionSpec{
			MaxAttempts: 3,
		},
	}

	hash1 := ComputeSpecHash(&spec)
	hash2 := ComputeSpecHash(&spec)

	if hash1 == "" {
		t.Fatal("hash should not be empty")
	}
	if hash1 != hash2 {
		t.Errorf("hash not deterministic: %q != %q", hash1, hash2)
	}

	// Different spec should produce different hash.
	spec2 := spec
	spec2.Execution.MaxAttempts = 5
	hash3 := ComputeSpecHash(&spec2)
	if hash1 == hash3 {
		t.Error("different specs should produce different hashes")
	}
}

// TestLoadJobSnapshot verifies the snapshot loading logic.
func TestLoadJobSnapshot(t *testing.T) {
	scheme := buildScheme()
	job := newJob("snap-job", "default")
	attempt1 := &v1alpha1.AgentAttempt{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "snap-job-1",
			Namespace: "default",
			Labels:    map[string]string{"durarun.io/job": "snap-job"},
		},
		Spec: v1alpha1.AgentAttemptSpec{
			JobRef:  v1alpha1.ObjectRef{Name: "snap-job", Namespace: "default", UID: "job-uid-1"},
			Ordinal: 1,
		},
	}
	attempt2 := &v1alpha1.AgentAttempt{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "snap-job-2",
			Namespace: "default",
			Labels:    map[string]string{"durarun.io/job": "snap-job"},
		},
		Spec: v1alpha1.AgentAttemptSpec{
			JobRef:  v1alpha1.ObjectRef{Name: "snap-job", Namespace: "default", UID: "job-uid-1"},
			Ordinal: 2,
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "snap-job-1",
			Namespace: "default",
			Labels: map[string]string{
				"durarun.io/job":     "snap-job",
				"durarun.io/attempt": "snap-job-1",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "c", Image: "img"}},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(job, attempt1, attempt2, pod).
		Build()

	snap, err := LoadJobSnapshot(context.Background(), cl, types.NamespacedName{Name: "snap-job", Namespace: "default"})
	if err != nil {
		t.Fatalf("LoadJobSnapshot error: %v", err)
	}
	if snap == nil {
		t.Fatal("expected non-nil snapshot")
	}
	if len(snap.Attempts) != 2 {
		t.Errorf("expected 2 attempts, got %d", len(snap.Attempts))
	}
	if snap.Attempts[0].Spec.Ordinal != 1 || snap.Attempts[1].Spec.Ordinal != 2 {
		t.Error("attempts not sorted by ordinal")
	}
	if len(snap.Pods) != 1 {
		t.Errorf("expected 1 pod, got %d", len(snap.Pods))
	}
	if snap.PodForAttempt("snap-job-1") == nil {
		t.Error("expected PodForAttempt to find pod for snap-job-1")
	}
	if snap.PodForAttempt("snap-job-2") != nil {
		t.Error("expected PodForAttempt to NOT find pod for snap-job-2")
	}
}

// TestLoadJobSnapshot_NotFound verifies that a deleted job returns nil.
func TestLoadJobSnapshot_NotFound(t *testing.T) {
	scheme := buildScheme()
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	snap, err := LoadJobSnapshot(context.Background(), cl, types.NamespacedName{Name: "gone", Namespace: "default"})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if snap != nil {
		t.Error("expected nil snapshot for deleted job")
	}
}

// TestVerifyOwnership verifies the ownership checking logic.
func TestVerifyOwnership_Mismatch(t *testing.T) {
	job := &v1alpha1.AgentJob{
		ObjectMeta: metav1.ObjectMeta{
			Name: "my-job",
			UID:  types.UID("correct-uid"),
		},
	}

	attempt := &v1alpha1.AgentAttempt{
		ObjectMeta: metav1.ObjectMeta{
			Name: "my-job-1",
		},
		Spec: v1alpha1.AgentAttemptSpec{
			JobRef: v1alpha1.ObjectRef{
				Name: "my-job",
				UID:  types.UID("wrong-uid"),
			},
		},
	}

	err := VerifyAttemptOwnership(job, attempt)
	if err == nil {
		t.Error("expected error for UID mismatch")
	}
}

func TestVerifyOwnership_Match(t *testing.T) {
	job := &v1alpha1.AgentJob{
		ObjectMeta: metav1.ObjectMeta{
			Name: "my-job",
			UID:  types.UID("correct-uid"),
		},
	}

	attempt := &v1alpha1.AgentAttempt{
		ObjectMeta: metav1.ObjectMeta{
			Name: "my-job-1",
		},
		Spec: v1alpha1.AgentAttemptSpec{
			JobRef: v1alpha1.ObjectRef{
				Name: "my-job",
				UID:  types.UID("correct-uid"),
			},
		},
	}

	err := VerifyAttemptOwnership(job, attempt)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}
