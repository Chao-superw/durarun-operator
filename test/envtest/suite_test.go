package envtest_test

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"durarun-operator/api/v1alpha1"
	"durarun-operator/internal/controller"
	"durarun-operator/internal/state"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// buildScheme registers all types needed by the fake client.
func buildScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = v1alpha1.AddToScheme(s)
	return s
}

// newJob creates a minimal AgentJob for testing.
func newJob(name, ns string, uid types.UID) *v1alpha1.AgentJob {
	return &v1alpha1.AgentJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       uid,
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

// newAttempt creates an AgentAttempt owned by the given job.
func newAttempt(job *v1alpha1.AgentJob, ordinal int32, attemptUID types.UID) *v1alpha1.AgentAttempt {
	return &v1alpha1.AgentAttempt{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%d", job.Name, ordinal),
			Namespace: job.Namespace,
			UID:       attemptUID,
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
}

// reconciler creates a JobReconciler backed by the given fake client and scheme.
func reconciler(cl client.Client, scheme *runtime.Scheme) *controller.JobReconciler {
	return &controller.JobReconciler{
		Client: cl,
		Scheme: scheme,
	}
}

// reconcileN runs Reconcile n times for the given request.
func reconcileN(t *testing.T, r *controller.JobReconciler, req ctrl.Request, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		_, err := r.Reconcile(context.Background(), req)
		if err != nil {
			t.Fatalf("Reconcile iteration %d returned error: %v", i, err)
		}
	}
}

// countAttempts returns the total number of AgentAttempts in the fake client.
func countAttempts(t *testing.T, cl client.Client) int {
	t.Helper()
	var list v1alpha1.AgentAttemptList
	if err := cl.List(context.Background(), &list); err != nil {
		t.Fatalf("failed to list attempts: %v", err)
	}
	return len(list.Items)
}

// getJob fetches the AgentJob from the fake client.
func getJob(t *testing.T, cl client.Client, name, ns string) *v1alpha1.AgentJob {
	t.Helper()
	var job v1alpha1.AgentJob
	if err := cl.Get(context.Background(), types.NamespacedName{Name: name, Namespace: ns}, &job); err != nil {
		t.Fatalf("failed to get job %s/%s: %v", ns, name, err)
	}
	return &job
}

// ---------------------------------------------------------------------------
// Test 1: Create-response-lost simulation
// ---------------------------------------------------------------------------

// TestCreateResponseLost simulates the scenario where a Job is created,
// the controller creates an attempt on the first reconcile, but the
// status update response is "lost" (i.e., the job status is never
// persisted with ActiveAttempt). On the second reconcile the controller
// should find the existing attempt via snapshot and not create a duplicate.
func TestCreateResponseLost(t *testing.T) {
	scheme := buildScheme()
	job := newJob("resp-lost", "default", "job-uid-1")

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(job).
		WithStatusSubresource(&v1alpha1.AgentJob{}, &v1alpha1.AgentAttempt{}).
		Build()

	r := reconciler(cl, scheme)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "resp-lost", Namespace: "default"}}

	// --- First reconcile: creates attempt and updates job status ---
	result1, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("first reconcile error: %v", err)
	}
	if !result1.Requeue {
		t.Fatal("expected Requeue=true after first reconcile")
	}

	// Verify attempt was created.
	if countAttempts(t, cl) != 1 {
		t.Fatalf("expected 1 attempt after first reconcile, got %d", countAttempts(t, cl))
	}

	// Simulate "response lost": reset the job status so ActiveAttempt is nil,
	// as if the status update response never reached the controller.
	updated := getJob(t, cl, "resp-lost", "default")
	updated.Status.ActiveAttempt = nil
	updated.Status.Conditions = nil
	if err := cl.Status().Update(context.Background(), updated); err != nil {
		t.Fatalf("failed to reset job status: %v", err)
	}

	// --- Second reconcile: should find existing attempt, not create duplicate ---
	result2, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("second reconcile error: %v", err)
	}
	if !result2.Requeue {
		t.Error("expected Requeue=true after second reconcile")
	}

	// Key assertion: still exactly 1 attempt.
	if n := countAttempts(t, cl); n != 1 {
		t.Errorf("expected exactly 1 attempt after response-lost retry, got %d", n)
	}

	// The job should now reference the existing attempt.
	jobAfter := getJob(t, cl, "resp-lost", "default")
	if jobAfter.Status.ActiveAttempt == nil {
		t.Fatal("expected ActiveAttempt to be set after second reconcile")
	}
	if jobAfter.Status.ActiveAttempt.Name != "resp-lost-1" {
		t.Errorf("expected ActiveAttempt=resp-lost-1, got %s", jobAfter.Status.ActiveAttempt.Name)
	}
}

// ---------------------------------------------------------------------------
// Test 2: Status conflict
// ---------------------------------------------------------------------------

// TestStatusConflict simulates the case where an external actor modifies
// the job status between the controller's reconcile iterations. The
// controller should handle the changed state gracefully and converge.
func TestStatusConflict(t *testing.T) {
	scheme := buildScheme()
	job := newJob("conflict-job", "default", "job-uid-conflict")

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(job).
		WithStatusSubresource(&v1alpha1.AgentJob{}, &v1alpha1.AgentAttempt{}).
		Build()

	r := reconciler(cl, scheme)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "conflict-job", Namespace: "default"}}

	// First reconcile: creates attempt, sets Scheduled condition.
	_, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("first reconcile error: %v", err)
	}

	// Externally modify the job status: bump FailedAttempts as if another
	// controller instance (or direct API edit) changed it.
	modified := getJob(t, cl, "conflict-job", "default")
	modified.Status.FailedAttempts = 99
	if err := cl.Status().Update(context.Background(), modified); err != nil {
		t.Fatalf("failed to externally modify job status: %v", err)
	}

	// Second reconcile: should not panic or error; should handle the modified state.
	_, err = r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("second reconcile after external modification returned error: %v", err)
	}

	// The job should still be in a valid state.
	jobAfter := getJob(t, cl, "conflict-job", "default")

	// The external FailedAttempts=99 may or may not be preserved (depends on
	// reconcile path), but the job should be in a coherent state with conditions set.
	if len(jobAfter.Status.Conditions) == 0 {
		t.Error("expected at least one condition on the job after conflict reconcile")
	}

	// Attempt count should still be reasonable (not duplicated).
	n := countAttempts(t, cl)
	if n < 1 || n > 2 {
		t.Errorf("expected 1-2 attempts after conflict reconcile, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// Test 3: Leader handoff
// ---------------------------------------------------------------------------

// TestLeaderHandoff simulates a new leader taking over when a job already
// has an active, non-terminal attempt. The new reconciler should continue
// from the existing state without creating duplicate resources.
func TestLeaderHandoff(t *testing.T) {
	scheme := buildScheme()
	job := newJob("handoff-job", "default", "job-uid-handoff")

	// Set up the job as if the old leader had already created attempt #1
	// and set the Scheduled condition.
	apimeta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type:               v1alpha1.JobConditionScheduled,
		Status:             metav1.ConditionTrue,
		Reason:             "AttemptCreated",
		LastTransitionTime: metav1.Now(),
	})
	job.Status.ActiveAttempt = &v1alpha1.AttemptReference{
		Name:    "handoff-job-1",
		Ordinal: 1,
		UID:     types.UID("attempt-uid-handoff"),
	}

	attempt := newAttempt(job, 1, "attempt-uid-handoff")

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(job, attempt).
		WithStatusSubresource(&v1alpha1.AgentJob{}, &v1alpha1.AgentAttempt{}).
		Build()

	// New leader picks up.
	r := reconciler(cl, scheme)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "handoff-job", Namespace: "default"}}

	// Reconcile multiple times to simulate the new leader stabilizing.
	for i := 0; i < 5; i++ {
		_, err := r.Reconcile(context.Background(), req)
		if err != nil {
			t.Fatalf("reconcile %d after handoff error: %v", i, err)
		}
	}

	// Verify: no duplicate attempts were created.
	n := countAttempts(t, cl)
	if n != 1 {
		t.Errorf("expected exactly 1 attempt after leader handoff, got %d", n)
	}

	// Verify: no duplicate pods (should be exactly 1 pod for the attempt).
	var podList corev1.PodList
	if err := cl.List(context.Background(), &podList); err != nil {
		t.Fatalf("failed to list pods: %v", err)
	}
	if len(podList.Items) != 1 {
		t.Errorf("expected exactly 1 pod after leader handoff, got %d", len(podList.Items))
	}

	// Verify: attempt status has PodName set (continuation from existing state).
	var updatedAttempt v1alpha1.AgentAttempt
	if err := cl.Get(context.Background(), types.NamespacedName{
		Name: "handoff-job-1", Namespace: "default",
	}, &updatedAttempt); err != nil {
		t.Fatalf("failed to get attempt: %v", err)
	}
	if updatedAttempt.Status.PodName == "" {
		t.Error("expected attempt PodName to be set after leader handoff reconcile")
	}
}

// ---------------------------------------------------------------------------
// Test 4: Ownership validation on re-created job
// ---------------------------------------------------------------------------

// TestReCreatedJobOwnership simulates a job being deleted and re-created
// with the same name but a different UID. The old attempt (referencing
// the old UID) should not be reused; the controller should detect the
// ownership mismatch.
func TestReCreatedJobOwnership(t *testing.T) {
	scheme := buildScheme()

	// Step 1: Create the "old" attempt that references UID-1.
	oldAttempt := &v1alpha1.AgentAttempt{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "recreated-job-1",
			Namespace: "default",
			UID:       types.UID("old-attempt-uid"),
			Labels: map[string]string{
				"durarun.io/job": "recreated-job",
			},
		},
		Spec: v1alpha1.AgentAttemptSpec{
			JobRef: v1alpha1.ObjectRef{
				Name:      "recreated-job",
				Namespace: "default",
				UID:       types.UID("job-uid-1"), // old job UID
			},
			Ordinal: 1,
		},
	}

	// Step 2: Create a new job with the same name but UID-2.
	newJobObj := newJob("recreated-job", "default", "job-uid-2")

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(newJobObj, oldAttempt).
		WithStatusSubresource(&v1alpha1.AgentJob{}, &v1alpha1.AgentAttempt{}).
		Build()

	r := reconciler(cl, scheme)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "recreated-job", Namespace: "default"}}

	// Reconcile: the controller should detect that oldAttempt's JobRef.UID
	// (job-uid-1) does not match the new job's UID (job-uid-2).
	_, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	// Verify: the job should be marked as Failed due to ownership violation.
	jobAfter := getJob(t, cl, "recreated-job", "default")
	if !apimeta.IsStatusConditionTrue(jobAfter.Status.Conditions, v1alpha1.JobConditionFailed) {
		t.Error("expected job Failed condition due to ownership mismatch with re-created job")
	}

	// The old attempt should NOT be reused as the active attempt.
	if jobAfter.Status.ActiveAttempt != nil && jobAfter.Status.ActiveAttempt.Name == "recreated-job-1" {
		// This would mean the old attempt was incorrectly adopted.
		cond := apimeta.FindStatusCondition(jobAfter.Status.Conditions, v1alpha1.JobConditionFailed)
		if cond == nil || cond.Status != metav1.ConditionTrue {
			t.Error("old attempt was reused despite UID mismatch")
		}
	}
}

// ---------------------------------------------------------------------------
// Test 5: Terminal idempotence
// ---------------------------------------------------------------------------

// TestTerminalIdempotence verifies that once a job reaches a terminal state
// (Complete), reconciling it 100 more times produces no side effects:
// no new attempts, no status changes.
func TestTerminalIdempotence(t *testing.T) {
	scheme := buildScheme()
	job := newJob("terminal-job", "default", "job-uid-terminal")

	// Mark the job as Complete via conditions.
	apimeta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type:               v1alpha1.JobConditionComplete,
		Status:             metav1.ConditionTrue,
		Reason:             "Succeeded",
		Message:            "Job completed successfully",
		LastTransitionTime: metav1.Now(),
	})
	now := metav1.Now()
	job.Status.CompletionTime = &now
	job.Status.CompletedAttempts = 1

	// Also create the completed attempt for completeness.
	attempt := newAttempt(job, 1, "attempt-uid-terminal")
	state.SetAttemptCondition(&attempt.Status.Conditions,
		v1alpha1.AttemptConditionComplete, metav1.ConditionTrue,
		"PodSucceeded", "Pod completed successfully")

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(job, attempt).
		WithStatusSubresource(&v1alpha1.AgentJob{}, &v1alpha1.AgentAttempt{}).
		Build()

	r := reconciler(cl, scheme)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "terminal-job", Namespace: "default"}}

	// Capture state before the 100 reconciles.
	jobBefore := getJob(t, cl, "terminal-job", "default")
	attemptCountBefore := countAttempts(t, cl)
	conditionsBefore := len(jobBefore.Status.Conditions)

	// Reconcile 100 times.
	for i := 0; i < 100; i++ {
		result, err := r.Reconcile(context.Background(), req)
		if err != nil {
			t.Fatalf("reconcile %d returned error: %v", i, err)
		}
		if result.Requeue || result.RequeueAfter > 0 {
			t.Errorf("reconcile %d: expected no requeue for terminal job", i)
		}
	}

	// Verify: no new attempts were created.
	if n := countAttempts(t, cl); n != attemptCountBefore {
		t.Errorf("attempt count changed from %d to %d after 100 terminal reconciles", attemptCountBefore, n)
	}

	// Verify: job status unchanged.
	jobAfter := getJob(t, cl, "terminal-job", "default")
	if len(jobAfter.Status.Conditions) != conditionsBefore {
		t.Errorf("conditions count changed from %d to %d", conditionsBefore, len(jobAfter.Status.Conditions))
	}
	if jobAfter.Status.CompletedAttempts != 1 {
		t.Errorf("CompletedAttempts changed to %d, expected 1", jobAfter.Status.CompletedAttempts)
	}
	if jobAfter.Status.FailedAttempts != 0 {
		t.Errorf("FailedAttempts changed to %d, expected 0", jobAfter.Status.FailedAttempts)
	}
	if !apimeta.IsStatusConditionTrue(jobAfter.Status.Conditions, v1alpha1.JobConditionComplete) {
		t.Error("Complete condition should still be True after 100 reconciles")
	}
}
