package controller

import (
	"testing"
	"time"

	v1alpha1 "durarun-operator/api/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ---------------------------------------------------------------------------
// Loss detector tests
// ---------------------------------------------------------------------------

func TestDetectPodLoss_NilPod(t *testing.T) {
	lost, reason := DetectPodLoss(nil)
	if !lost || reason != PodLossReasonNotFound {
		t.Errorf("nil pod: expected (true, NotFound), got (%v, %v)", lost, reason)
	}
}

func TestDetectPodLoss_Evicted(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			Phase:  corev1.PodFailed,
			Reason: "Evicted",
		},
	}
	lost, reason := DetectPodLoss(pod)
	if !lost || reason != PodLossReasonEvicted {
		t.Errorf("evicted pod: expected (true, Evicted), got (%v, %v)", lost, reason)
	}
}

func TestDetectPodLoss_Preempted(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			Phase:  corev1.PodFailed,
			Reason: "Preempted",
		},
	}
	lost, reason := DetectPodLoss(pod)
	if !lost || reason != PodLossReasonPreempted {
		t.Errorf("preempted pod: expected (true, Preempted), got (%v, %v)", lost, reason)
	}
}

func TestDetectPodLoss_OOMKilled(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			Phase: corev1.PodFailed,
			ContainerStatuses: []corev1.ContainerStatus{
				{
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							Reason: "OOMKilled",
						},
					},
				},
			},
		},
	}
	lost, reason := DetectPodLoss(pod)
	if !lost || reason != PodLossReasonOOMKilled {
		t.Errorf("OOMKilled pod: expected (true, OOMKilled), got (%v, %v)", lost, reason)
	}
}

func TestDetectPodLoss_NodeLost_DisruptionTarget(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{
					Type:   "DisruptionTarget",
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
	lost, reason := DetectPodLoss(pod)
	if !lost || reason != PodLossReasonNodeLost {
		t.Errorf("node lost (disruption): expected (true, NodeLost), got (%v, %v)", lost, reason)
	}
}

func TestDetectPodLoss_NodeLost_ReadyFalse(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionFalse,
					Reason: "NodeLost",
				},
			},
		},
	}
	lost, reason := DetectPodLoss(pod)
	if !lost || reason != PodLossReasonNodeLost {
		t.Errorf("node lost (ready=false): expected (true, NodeLost), got (%v, %v)", lost, reason)
	}
}

func TestDetectPodLoss_FailedUnknown(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			Phase: corev1.PodFailed,
		},
	}
	lost, reason := DetectPodLoss(pod)
	if !lost || reason != PodLossReasonUnknown {
		t.Errorf("failed (no reason): expected (true, Unknown), got (%v, %v)", lost, reason)
	}
}

func TestDetectPodLoss_RunningPod(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}
	lost, reason := DetectPodLoss(pod)
	if lost {
		t.Errorf("running pod: expected not lost, got (%v, %v)", lost, reason)
	}
}

func TestDetectPodLoss_PendingPod(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
		},
	}
	lost, reason := DetectPodLoss(pod)
	if lost {
		t.Errorf("pending pod: expected not lost, got (%v, %v)", lost, reason)
	}
}

func TestDetectPodLoss_SucceededPod(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			Phase: corev1.PodSucceeded,
		},
	}
	lost, reason := DetectPodLoss(pod)
	if lost {
		t.Errorf("succeeded pod: expected not lost, got (%v, %v)", lost, reason)
	}
}

// ---------------------------------------------------------------------------
// Retry decision tests
// ---------------------------------------------------------------------------

func newJobWithMaxAttempts(max int32) *v1alpha1.AgentJob {
	return &v1alpha1.AgentJob{
		ObjectMeta: metav1.ObjectMeta{Name: "test-job", Namespace: "default"},
		Spec: v1alpha1.AgentJobSpec{
			Execution: v1alpha1.ExecutionSpec{
				MaxAttempts: max,
			},
		},
	}
}

func TestShouldRetry_FirstFailureInfra(t *testing.T) {
	job := newJobWithMaxAttempts(3)
	d := ShouldRetry(job, 1, v1alpha1.ErrorCategoryInfra)
	if !d.ShouldRetry {
		t.Fatal("expected ShouldRetry=true for first infra failure")
	}
	if d.Delay != 10*time.Second {
		t.Errorf("expected 10s delay (ordinal 1), got %v", d.Delay)
	}
}

func TestShouldRetry_ZeroFailedAttempts(t *testing.T) {
	job := newJobWithMaxAttempts(3)
	d := ShouldRetry(job, 0, v1alpha1.ErrorCategoryInfra)
	if !d.ShouldRetry {
		t.Fatal("expected ShouldRetry=true for zero failed attempts")
	}
	if d.Delay != 5*time.Second {
		t.Errorf("expected 5s delay (ordinal 0), got %v", d.Delay)
	}
}

func TestShouldRetry_SecondFailure(t *testing.T) {
	job := newJobWithMaxAttempts(3)
	d := ShouldRetry(job, 1, v1alpha1.ErrorCategoryUser)
	if !d.ShouldRetry {
		t.Fatal("expected ShouldRetry=true for second failure")
	}
	if d.Delay != 10*time.Second {
		t.Errorf("expected 10s delay, got %v", d.Delay)
	}
}

func TestShouldRetry_AtLimit(t *testing.T) {
	job := newJobWithMaxAttempts(3)
	d := ShouldRetry(job, 3, v1alpha1.ErrorCategoryInfra)
	if d.ShouldRetry {
		t.Fatal("expected ShouldRetry=false when failedAttempts == maxAttempts")
	}
}

func TestShouldRetry_UserErrorAtLimit(t *testing.T) {
	job := newJobWithMaxAttempts(3)
	d := ShouldRetry(job, 3, v1alpha1.ErrorCategoryUser)
	if d.ShouldRetry {
		t.Fatal("expected ShouldRetry=false when user error at limit")
	}
}

func TestShouldRetry_InfraRespectsMaxAttempts(t *testing.T) {
	job := newJobWithMaxAttempts(2)
	d := ShouldRetry(job, 2, v1alpha1.ErrorCategoryInfra)
	if d.ShouldRetry {
		t.Fatal("expected ShouldRetry=false: infra error also respects maxAttempts")
	}
}

func TestShouldRetry_TimeoutRetries(t *testing.T) {
	job := newJobWithMaxAttempts(3)
	d := ShouldRetry(job, 1, v1alpha1.ErrorCategoryTimeout)
	if !d.ShouldRetry {
		t.Fatal("expected ShouldRetry=true for timeout error under limit")
	}
}

func TestShouldRetry_OOMRetries(t *testing.T) {
	job := newJobWithMaxAttempts(3)
	d := ShouldRetry(job, 0, v1alpha1.ErrorCategoryOOM)
	if !d.ShouldRetry {
		t.Fatal("expected ShouldRetry=true for OOM error under limit")
	}
}

func TestShouldRetry_EmptyCategoryNoRetry(t *testing.T) {
	job := newJobWithMaxAttempts(3)
	d := ShouldRetry(job, 0, "")
	if d.ShouldRetry {
		t.Fatal("expected ShouldRetry=false for empty (success) category")
	}
}

func TestShouldRetry_DefaultMaxAttempts(t *testing.T) {
	job := newJobWithMaxAttempts(0) // 0 → should default to 3
	d := ShouldRetry(job, 2, v1alpha1.ErrorCategoryUser)
	if !d.ShouldRetry {
		t.Fatal("expected ShouldRetry=true: 2 < default 3")
	}
	d2 := ShouldRetry(job, 3, v1alpha1.ErrorCategoryUser)
	if d2.ShouldRetry {
		t.Fatal("expected ShouldRetry=false: 3 >= default 3")
	}
}

// ---------------------------------------------------------------------------
// Backoff computation tests
// ---------------------------------------------------------------------------

func TestComputeBackoff_Ordinals(t *testing.T) {
	cases := []struct {
		ordinal  int32
		expected time.Duration
	}{
		{0, 5 * time.Second},
		{1, 10 * time.Second},
		{2, 20 * time.Second},
		{3, 40 * time.Second},
		{4, 80 * time.Second},
		{5, 160 * time.Second},
		{6, 300 * time.Second}, // capped at 5m
		{10, 300 * time.Second},
		{20, 300 * time.Second},
	}
	for _, tc := range cases {
		got := ComputeBackoff(tc.ordinal)
		if got != tc.expected {
			t.Errorf("ComputeBackoff(%d) = %v, want %v", tc.ordinal, got, tc.expected)
		}
	}
}

func TestComputeBackoff_Deterministic(t *testing.T) {
	for ordinal := int32(0); ordinal < 15; ordinal++ {
		a := ComputeBackoff(ordinal)
		b := ComputeBackoff(ordinal)
		if a != b {
			t.Errorf("ComputeBackoff(%d) not deterministic: %v vs %v", ordinal, a, b)
		}
	}
}
