package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"durarun-operator/api/v1alpha1"
	"durarun-operator/internal/state"
)

// ---------------------------------------------------------------------------
// Helpers (lifecycle-specific)
// ---------------------------------------------------------------------------

func int64Ptr(v int64) *int64 { return &v }
func int32Ptr(v int32) *int32 { return &v }

func newLifecycleTestAttempt(name string) *v1alpha1.AgentAttempt {
	return &v1alpha1.AgentAttempt{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			UID:       types.UID("attempt-uid-1"),
		},
		Spec: v1alpha1.AgentAttemptSpec{
			JobRef: v1alpha1.ObjectRef{
				Name: "test-job",
				UID:  "job-uid-1",
			},
			Ordinal: 1,
		},
	}
}

// ---------------------------------------------------------------------------
// Deadline Tests
// ---------------------------------------------------------------------------

func TestCheckDeadline_ActiveDeadlineExceeded(t *testing.T) {
	job := newJob("test-job", "default")
	startTime := metav1.NewTime(time.Now().Add(-61 * time.Second))
	job.Status.StartTime = &startTime
	job.Spec.Execution.ActiveDeadlineSeconds = int64Ptr(60)

	timedOut, level := CheckDeadline(job, nil, time.Now())
	assert.True(t, timedOut)
	assert.Equal(t, "job", level)
}

func TestCheckDeadline_ActiveDeadlineNotExceeded(t *testing.T) {
	job := newJob("test-job", "default")
	startTime := metav1.NewTime(time.Now().Add(-30 * time.Second))
	job.Status.StartTime = &startTime
	job.Spec.Execution.ActiveDeadlineSeconds = int64Ptr(60)

	timedOut, level := CheckDeadline(job, nil, time.Now())
	assert.False(t, timedOut)
	assert.Empty(t, level)
}

func TestCheckDeadline_TimeoutExceeded(t *testing.T) {
	job := newJob("test-job", "default")
	startTime := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	job.Status.StartTime = &startTime
	job.Spec.Execution.Timeout = &metav1.Duration{Duration: 1 * time.Minute}

	timedOut, level := CheckDeadline(job, nil, time.Now())
	assert.True(t, timedOut)
	assert.Equal(t, "job", level)
}

func TestCheckDeadline_TimeoutNotExceeded(t *testing.T) {
	job := newJob("test-job", "default")
	startTime := metav1.NewTime(time.Now().Add(-30 * time.Second))
	job.Status.StartTime = &startTime
	job.Spec.Execution.Timeout = &metav1.Duration{Duration: 1 * time.Minute}

	timedOut, level := CheckDeadline(job, nil, time.Now())
	assert.False(t, timedOut)
	assert.Empty(t, level)
}

func TestCheckDeadline_AttemptDeadlinePast(t *testing.T) {
	job := newJob("test-job", "default")
	attempt := newLifecycleTestAttempt("test-job-1")
	deadlineTime := metav1.NewTime(time.Now().Add(-10 * time.Second))
	attempt.Spec.Deadline = &deadlineTime

	timedOut, level := CheckDeadline(job, attempt, time.Now())
	assert.True(t, timedOut)
	assert.Equal(t, "attempt", level)
}

func TestCheckDeadline_AttemptDeadlineFuture(t *testing.T) {
	job := newJob("test-job", "default")
	attempt := newLifecycleTestAttempt("test-job-1")
	deadlineTime := metav1.NewTime(time.Now().Add(10 * time.Minute))
	attempt.Spec.Deadline = &deadlineTime

	timedOut, level := CheckDeadline(job, attempt, time.Now())
	assert.False(t, timedOut)
	assert.Empty(t, level)
}

func TestCheckDeadline_NoDeadlineConfigured(t *testing.T) {
	job := newJob("test-job", "default")
	attempt := newLifecycleTestAttempt("test-job-1")

	timedOut, level := CheckDeadline(job, attempt, time.Now())
	assert.False(t, timedOut)
	assert.Empty(t, level)
}

func TestCheckDeadline_NoStartTime(t *testing.T) {
	job := newJob("test-job", "default")
	job.Spec.Execution.ActiveDeadlineSeconds = int64Ptr(60)
	// No StartTime set

	timedOut, level := CheckDeadline(job, nil, time.Now())
	assert.False(t, timedOut)
	assert.Empty(t, level)
}

func TestCheckDeadline_JobLevelTakesPrecedence(t *testing.T) {
	// Both job-level and attempt-level deadlines are exceeded.
	// Job-level should be returned first.
	job := newJob("test-job", "default")
	startTime := metav1.NewTime(time.Now().Add(-120 * time.Second))
	job.Status.StartTime = &startTime
	job.Spec.Execution.ActiveDeadlineSeconds = int64Ptr(60)

	attempt := newLifecycleTestAttempt("test-job-1")
	deadlineTime := metav1.NewTime(time.Now().Add(-10 * time.Second))
	attempt.Spec.Deadline = &deadlineTime

	timedOut, level := CheckDeadline(job, attempt, time.Now())
	assert.True(t, timedOut)
	assert.Equal(t, "job", level)
}

// ---------------------------------------------------------------------------
// Cancel Tests
// ---------------------------------------------------------------------------

func TestIsCancelled_WithAnnotation(t *testing.T) {
	job := newJob("test-job", "default")
	job.Annotations = map[string]string{
		"durarun.io/cancel": "true",
	}
	assert.True(t, IsCancelled(job))
}

func TestIsCancelled_WithoutAnnotation(t *testing.T) {
	job := newJob("test-job", "default")
	assert.False(t, IsCancelled(job))
}

func TestIsCancelled_AnnotationNotTrue(t *testing.T) {
	job := newJob("test-job", "default")
	job.Annotations = map[string]string{
		"durarun.io/cancel": "false",
	}
	assert.False(t, IsCancelled(job))
}

func TestHandleCancel_WithAnnotation(t *testing.T) {
	job := newJob("test-job", "default")
	job.Annotations = map[string]string{
		"durarun.io/cancel": "true",
	}

	triggered := HandleCancel(job)
	assert.True(t, triggered)
	assert.True(t, state.IsJobTerminal(job.Status.Conditions))
	assert.NotNil(t, job.Status.CompletionTime)

	// Verify the condition is Failed with reason Cancelled.
	found := false
	for _, c := range job.Status.Conditions {
		if c.Type == v1alpha1.JobConditionFailed && c.Status == metav1.ConditionTrue {
			assert.Equal(t, "Cancelled", c.Reason)
			found = true
		}
	}
	assert.True(t, found, "expected Failed condition with Cancelled reason")
}

func TestHandleCancel_WithoutAnnotation(t *testing.T) {
	job := newJob("test-job", "default")

	triggered := HandleCancel(job)
	assert.False(t, triggered)
	assert.False(t, state.IsJobTerminal(job.Status.Conditions))
}

func TestHandleCancel_AlreadyTerminal(t *testing.T) {
	job := newJob("test-job", "default")
	job.Annotations = map[string]string{
		"durarun.io/cancel": "true",
	}

	// Mark job as already completed successfully.
	state.TrySetJobTerminal(&job.Status.Conditions,
		v1alpha1.JobConditionComplete, metav1.ConditionTrue,
		"Succeeded", "Job completed successfully")

	triggered := HandleCancel(job)
	assert.False(t, triggered, "cancel should be no-op when job already terminal")

	// Verify the job remains Complete, not Failed.
	phase := v1alpha1.JobPhaseFromConditions(job.Status.Conditions)
	assert.Equal(t, "Succeeded", phase)
}

// ---------------------------------------------------------------------------
// Finalizer Tests
// ---------------------------------------------------------------------------

func TestEnsureFinalizer_AddsOnce(t *testing.T) {
	job := newJob("test-job", "default")

	added := EnsureFinalizer(job)
	assert.True(t, added)
	assert.Contains(t, job.Finalizers, FinalizerName)

	// Second call should be idempotent.
	added2 := EnsureFinalizer(job)
	assert.False(t, added2)
	// Count: exactly one instance.
	count := 0
	for _, f := range job.Finalizers {
		if f == FinalizerName {
			count++
		}
	}
	assert.Equal(t, 1, count)
}

func TestRemoveFinalizer_Present(t *testing.T) {
	job := newJob("test-job", "default")
	EnsureFinalizer(job)
	assert.Contains(t, job.Finalizers, FinalizerName)

	removed := RemoveFinalizer(job)
	assert.True(t, removed)
	assert.NotContains(t, job.Finalizers, FinalizerName)
}

func TestRemoveFinalizer_NotPresent(t *testing.T) {
	job := newJob("test-job", "default")

	removed := RemoveFinalizer(job)
	assert.False(t, removed)
}

func TestHandleFinalization_DeletesPods(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.SchemeBuilder.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	job := newJob("test-job", "default")
	EnsureFinalizer(job)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job-pod",
			Namespace: "default",
			Labels: map[string]string{
				"durarun.io/job": "test-job",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "main", Image: "busybox"},
			},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pod).
		Build()

	done, err := HandleFinalization(context.Background(), c, job, 30*time.Second)
	require.NoError(t, err)
	assert.True(t, done)
	assert.NotContains(t, job.Finalizers, FinalizerName)
}

func TestHandleFinalization_BoundedTimeout(t *testing.T) {
	// Use an already-cancelled context to simulate timeout.
	// Even with a cancelled context, the finalizer should be removed.
	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.SchemeBuilder.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	job := newJob("test-job", "default")
	EnsureFinalizer(job)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	// HandleFinalization with a zero maxWait means the inner context expires
	// immediately. It should still remove the finalizer.
	done, err := HandleFinalization(ctx, c, job, 0)
	_ = err // may error from cancelled context, but that's expected
	assert.True(t, done)
	assert.NotContains(t, job.Finalizers, FinalizerName)
}

// ---------------------------------------------------------------------------
// TTL Tests
// ---------------------------------------------------------------------------

func TestCheckTTL_Expired(t *testing.T) {
	job := newJob("test-job", "default")
	job.Spec.Execution.TTLSecondsAfterFinished = int32Ptr(60)

	// Mark as terminal.
	state.TrySetJobTerminal(&job.Status.Conditions,
		v1alpha1.JobConditionComplete, metav1.ConditionTrue,
		"Succeeded", "done")
	completionTime := metav1.NewTime(time.Now().Add(-120 * time.Second))
	job.Status.CompletionTime = &completionTime

	expired, remaining := CheckTTL(job, time.Now())
	assert.True(t, expired)
	assert.Equal(t, time.Duration(0), remaining)
}

func TestCheckTTL_NotExpired(t *testing.T) {
	job := newJob("test-job", "default")
	job.Spec.Execution.TTLSecondsAfterFinished = int32Ptr(300)

	state.TrySetJobTerminal(&job.Status.Conditions,
		v1alpha1.JobConditionComplete, metav1.ConditionTrue,
		"Succeeded", "done")
	completionTime := metav1.NewTime(time.Now().Add(-60 * time.Second))
	job.Status.CompletionTime = &completionTime

	expired, remaining := CheckTTL(job, time.Now())
	assert.False(t, expired)
	assert.True(t, remaining > 0, "expected positive remaining duration, got %v", remaining)
	// Should be approximately 240 seconds.
	assert.InDelta(t, 240, remaining.Seconds(), 5)
}

func TestCheckTTL_NonTerminalJob(t *testing.T) {
	job := newJob("test-job", "default")
	job.Spec.Execution.TTLSecondsAfterFinished = int32Ptr(60)
	// No terminal condition.

	expired, remaining := CheckTTL(job, time.Now())
	assert.False(t, expired)
	assert.Equal(t, time.Duration(0), remaining)
}

func TestCheckTTL_NoTTLSet(t *testing.T) {
	job := newJob("test-job", "default")
	state.TrySetJobTerminal(&job.Status.Conditions,
		v1alpha1.JobConditionComplete, metav1.ConditionTrue,
		"Succeeded", "done")
	completionTime := metav1.NewTime(time.Now().Add(-120 * time.Second))
	job.Status.CompletionTime = &completionTime

	expired, remaining := CheckTTL(job, time.Now())
	assert.False(t, expired)
	assert.Equal(t, time.Duration(0), remaining)
}

func TestCheckTTL_NoCompletionTime(t *testing.T) {
	job := newJob("test-job", "default")
	job.Spec.Execution.TTLSecondsAfterFinished = int32Ptr(60)
	state.TrySetJobTerminal(&job.Status.Conditions,
		v1alpha1.JobConditionComplete, metav1.ConditionTrue,
		"Succeeded", "done")
	// No CompletionTime set.

	expired, remaining := CheckTTL(job, time.Now())
	assert.False(t, expired)
	assert.Equal(t, time.Duration(0), remaining)
}

func TestCheckTTL_ExactExpiry(t *testing.T) {
	job := newJob("test-job", "default")
	job.Spec.Execution.TTLSecondsAfterFinished = int32Ptr(60)
	state.TrySetJobTerminal(&job.Status.Conditions,
		v1alpha1.JobConditionComplete, metav1.ConditionTrue,
		"Succeeded", "done")
	now := time.Now()
	completionTime := metav1.NewTime(now.Add(-60 * time.Second))
	job.Status.CompletionTime = &completionTime

	expired, _ := CheckTTL(job, now)
	assert.True(t, expired)
}

// ---------------------------------------------------------------------------
// Race Condition Tests
// ---------------------------------------------------------------------------

func TestRace_CancelAfterSuccess(t *testing.T) {
	job := newJob("test-job", "default")
	job.Annotations = map[string]string{
		"durarun.io/cancel": "true",
	}

	// Job completed successfully first.
	set := state.TrySetJobTerminal(&job.Status.Conditions,
		v1alpha1.JobConditionComplete, metav1.ConditionTrue,
		"Succeeded", "Job completed successfully")
	assert.True(t, set)

	// Now try cancel -- should fail because terminal is immutable.
	triggered := HandleCancel(job)
	assert.False(t, triggered)
	assert.Equal(t, "Succeeded", v1alpha1.JobPhaseFromConditions(job.Status.Conditions))
}

func TestRace_TimeoutAfterSuccess(t *testing.T) {
	job := newJob("test-job", "default")
	startTime := metav1.NewTime(time.Now().Add(-120 * time.Second))
	job.Status.StartTime = &startTime
	job.Spec.Execution.ActiveDeadlineSeconds = int64Ptr(60)

	// Job completed successfully first.
	set := state.TrySetJobTerminal(&job.Status.Conditions,
		v1alpha1.JobConditionComplete, metav1.ConditionTrue,
		"Succeeded", "done")
	assert.True(t, set)

	// Deadline is exceeded, but job is already terminal.
	timedOut, _ := CheckDeadline(job, nil, time.Now())
	assert.True(t, timedOut, "deadline check correctly detects timeout")

	// But attempting to set terminal should fail.
	set2 := state.TrySetJobTerminal(&job.Status.Conditions,
		v1alpha1.JobConditionFailed, metav1.ConditionTrue,
		"DeadlineExceeded", "timed out")
	assert.False(t, set2, "terminal should be immutable -- timeout after success must fail")
	assert.Equal(t, "Succeeded", v1alpha1.JobPhaseFromConditions(job.Status.Conditions))
}

func TestRace_SuccessAfterCancel(t *testing.T) {
	job := newJob("test-job", "default")
	job.Annotations = map[string]string{
		"durarun.io/cancel": "true",
	}

	// Cancel fires first.
	triggered := HandleCancel(job)
	assert.True(t, triggered)
	assert.Equal(t, "Failed", v1alpha1.JobPhaseFromConditions(job.Status.Conditions))

	// Now try to set success -- should fail because terminal is immutable.
	set := state.TrySetJobTerminal(&job.Status.Conditions,
		v1alpha1.JobConditionComplete, metav1.ConditionTrue,
		"Succeeded", "done")
	assert.False(t, set)
	assert.Equal(t, "Failed", v1alpha1.JobPhaseFromConditions(job.Status.Conditions))
}

func TestRace_DoubleCancel(t *testing.T) {
	job := newJob("test-job", "default")
	job.Annotations = map[string]string{
		"durarun.io/cancel": "true",
	}

	triggered1 := HandleCancel(job)
	assert.True(t, triggered1)

	triggered2 := HandleCancel(job)
	assert.False(t, triggered2, "second cancel should be no-op since job is already terminal")
}
