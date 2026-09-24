package v1alpha1

import (
	"encoding/json"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// --- Defaulting tests ---

func TestDefaultAgentJobSpec_EmptySpec(t *testing.T) {
	spec := &AgentJobSpec{}
	DefaultAgentJobSpec(spec)

	if spec.Execution.MaxAttempts != DefaultMaxAttempts {
		t.Errorf("expected MaxAttempts=%d, got %d", DefaultMaxAttempts, spec.Execution.MaxAttempts)
	}
	if spec.Execution.Timeout == nil {
		t.Fatal("expected Timeout to be set")
	}
	if spec.Execution.Timeout.Duration != DefaultTimeout {
		t.Errorf("expected Timeout=%v, got %v", DefaultTimeout, spec.Execution.Timeout.Duration)
	}
	// Checkpoint not enabled, so IntervalSeconds should remain 0.
	if spec.Checkpoint.IntervalSeconds != 0 {
		t.Errorf("expected CheckpointInterval=0 when not enabled, got %d", spec.Checkpoint.IntervalSeconds)
	}
}

func TestDefaultAgentJobSpec_CheckpointEnabled(t *testing.T) {
	spec := &AgentJobSpec{
		Checkpoint: CheckpointPolicy{Enabled: true},
	}
	DefaultAgentJobSpec(spec)

	if spec.Checkpoint.IntervalSeconds != DefaultCheckpointIntervalSecs {
		t.Errorf("expected CheckpointInterval=%d, got %d", DefaultCheckpointIntervalSecs, spec.Checkpoint.IntervalSeconds)
	}
}

func TestDefaultAgentJobSpec_PreserveExisting(t *testing.T) {
	timeout := metav1.Duration{Duration: 10 * time.Minute}
	spec := &AgentJobSpec{
		Execution: ExecutionSpec{
			MaxAttempts: 5,
			Timeout:     &timeout,
		},
	}
	DefaultAgentJobSpec(spec)

	if spec.Execution.MaxAttempts != 5 {
		t.Errorf("expected MaxAttempts=5, got %d", spec.Execution.MaxAttempts)
	}
	if spec.Execution.Timeout.Duration != 10*time.Minute {
		t.Errorf("expected Timeout=10m, got %v", spec.Execution.Timeout.Duration)
	}
}

// --- Validation tests ---

func TestValidateAgentJobSpec_Valid(t *testing.T) {
	spec := &AgentJobSpec{
		Runtime: RuntimeSpec{Image: "my-agent:latest"},
		Execution: ExecutionSpec{
			MaxAttempts: 3,
		},
	}
	if err := ValidateAgentJobSpec(spec); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestValidateAgentJobSpec_MissingImage(t *testing.T) {
	spec := &AgentJobSpec{
		Execution: ExecutionSpec{MaxAttempts: 1},
	}
	if err := ValidateAgentJobSpec(spec); err == nil {
		t.Fatal("expected error for missing image")
	}
}

func TestValidateAgentJobSpec_ZeroMaxAttempts(t *testing.T) {
	spec := &AgentJobSpec{
		Runtime:   RuntimeSpec{Image: "img:latest"},
		Execution: ExecutionSpec{MaxAttempts: 0},
	}
	if err := ValidateAgentJobSpec(spec); err == nil {
		t.Fatal("expected error for maxAttempts=0")
	}
}

func TestValidateAgentJobSpec_NegativeMaxAttempts(t *testing.T) {
	spec := &AgentJobSpec{
		Runtime:   RuntimeSpec{Image: "img:latest"},
		Execution: ExecutionSpec{MaxAttempts: -1},
	}
	if err := ValidateAgentJobSpec(spec); err == nil {
		t.Fatal("expected error for negative maxAttempts")
	}
}

func TestValidateAgentAttemptSpec_Valid(t *testing.T) {
	spec := &AgentAttemptSpec{
		JobRef:  ObjectRef{Name: "job-1", UID: "uid-123"},
		Ordinal: 0,
	}
	if err := ValidateAgentAttemptSpec(spec); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestValidateAgentAttemptSpec_NegativeOrdinal(t *testing.T) {
	spec := &AgentAttemptSpec{
		JobRef:  ObjectRef{Name: "job-1", UID: "uid-123"},
		Ordinal: -1,
	}
	if err := ValidateAgentAttemptSpec(spec); err == nil {
		t.Fatal("expected error for negative ordinal")
	}
}

func TestValidateAgentAttemptSpec_EmptyUID(t *testing.T) {
	spec := &AgentAttemptSpec{
		JobRef:  ObjectRef{Name: "job-1"},
		Ordinal: 0,
	}
	if err := ValidateAgentAttemptSpec(spec); err == nil {
		t.Fatal("expected error for empty jobRef.uid")
	}
}

// --- DeepCopy tests ---

func TestAgentJob_DeepCopy(t *testing.T) {
	original := &AgentJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job",
			Namespace: "default",
		},
		Spec: AgentJobSpec{
			Runtime: RuntimeSpec{
				Image:   "img:v1",
				Command: []string{"run", "--flag"},
				Env:     []corev1.EnvVar{{Name: "KEY", Value: "VALUE"}},
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("2"),
					},
				},
			},
			Execution: ExecutionSpec{
				MaxAttempts: 3,
			},
		},
		Status: AgentJobStatus{
			FailedAttempts: 1,
			ActiveAttempt: &AttemptReference{
				Name:    "attempt-1",
				Ordinal: 1,
				UID:     types.UID("uid-123"),
			},
		},
	}

	copied := original.DeepCopy()

	// Mutate the copy and verify the original is not affected.
	copied.Spec.Runtime.Command[0] = "mutated"
	if original.Spec.Runtime.Command[0] == "mutated" {
		t.Error("DeepCopy: mutating Command slice affected the original")
	}

	copied.Spec.Runtime.Env[0].Value = "CHANGED"
	if original.Spec.Runtime.Env[0].Value == "CHANGED" {
		t.Error("DeepCopy: mutating Env slice affected the original")
	}

	copied.Status.ActiveAttempt.Name = "mutated"
	if original.Status.ActiveAttempt.Name == "mutated" {
		t.Error("DeepCopy: mutating ActiveAttempt affected the original")
	}
}

func TestAgentAttempt_DeepCopy(t *testing.T) {
	exitCode := int32(0)
	signal := int32(15)
	original := &AgentAttempt{
		ObjectMeta: metav1.ObjectMeta{
			Name: "attempt-1",
		},
		Spec: AgentAttemptSpec{
			JobRef:  ObjectRef{Name: "job-1", UID: "uid-123"},
			Ordinal: 1,
		},
		Status: AgentAttemptStatus{
			ExitCode: &exitCode,
			Signal:   &signal,
			ArtifactRef: &ArtifactRef{
				Bucket: "my-bucket",
				Key:    "my-key",
			},
		},
	}

	copied := original.DeepCopy()

	// Mutate exit code.
	*copied.Status.ExitCode = 1
	if *original.Status.ExitCode != 0 {
		t.Error("DeepCopy: mutating ExitCode affected the original")
	}

	// Mutate signal.
	*copied.Status.Signal = 9
	if *original.Status.Signal != 15 {
		t.Error("DeepCopy: mutating Signal affected the original")
	}

	// Mutate artifact ref.
	copied.Status.ArtifactRef.Key = "changed"
	if original.Status.ArtifactRef.Key == "changed" {
		t.Error("DeepCopy: mutating ArtifactRef affected the original")
	}
}

func TestSandboxPool_DeepCopy(t *testing.T) {
	original := &SandboxPool{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pool-1",
		},
		Spec: SandboxPoolSpec{
			MinSize: 2,
			MaxSize: 10,
			Template: PoolPodTemplate{
				Image:   "sandbox:v1",
				Command: []string{"init"},
				Env:     map[string]string{"MODE": "warm"},
			},
		},
		Status: SandboxPoolStatus{
			TotalPods: 5,
			IdlePods:  3,
			Ready:     true,
		},
	}

	copied := original.DeepCopy()

	copied.Spec.Template.Command[0] = "mutated"
	if original.Spec.Template.Command[0] == "mutated" {
		t.Error("DeepCopy: mutating Template.Command affected the original")
	}

	copied.Spec.Template.Env["MODE"] = "cold"
	if original.Spec.Template.Env["MODE"] == "cold" {
		t.Error("DeepCopy: mutating Template.Env affected the original")
	}
}

// --- Round-trip (marshal/unmarshal) tests ---

func TestAgentJob_RoundTrip(t *testing.T) {
	timeout := metav1.Duration{Duration: 30 * time.Minute}
	original := &AgentJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "roundtrip-job",
			Namespace: "default",
		},
		Spec: AgentJobSpec{
			Runtime: RuntimeSpec{
				Image:   "agent:v2",
				Command: []string{"/bin/agent"},
				Args:    []string{"--verbose"},
			},
			Execution: ExecutionSpec{
				MaxAttempts: 5,
				Timeout:     &timeout,
			},
			Checkpoint: CheckpointPolicy{
				Enabled:         true,
				IntervalSeconds: 120,
				StorageRef:      "my-bucket",
			},
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	restored := &AgentJob{}
	if err := json.Unmarshal(data, restored); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if restored.Spec.Runtime.Image != original.Spec.Runtime.Image {
		t.Errorf("image mismatch: got %q, want %q", restored.Spec.Runtime.Image, original.Spec.Runtime.Image)
	}
	if restored.Spec.Execution.MaxAttempts != original.Spec.Execution.MaxAttempts {
		t.Errorf("maxAttempts mismatch: got %d, want %d", restored.Spec.Execution.MaxAttempts, original.Spec.Execution.MaxAttempts)
	}
	if restored.Spec.Checkpoint.IntervalSeconds != original.Spec.Checkpoint.IntervalSeconds {
		t.Errorf("checkpoint interval mismatch: got %d, want %d", restored.Spec.Checkpoint.IntervalSeconds, original.Spec.Checkpoint.IntervalSeconds)
	}
}

func TestAgentAttempt_RoundTrip(t *testing.T) {
	exitCode := int32(42)
	original := &AgentAttempt{
		ObjectMeta: metav1.ObjectMeta{
			Name: "roundtrip-attempt",
		},
		Spec: AgentAttemptSpec{
			JobRef:  ObjectRef{Name: "job-1", Namespace: "ns-1", UID: "uid-abc"},
			Ordinal: 3,
		},
		Status: AgentAttemptStatus{
			PodName:       "pod-xyz",
			ExitCode:      &exitCode,
			ErrorCategory: ErrorCategoryTimeout,
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	restored := &AgentAttempt{}
	if err := json.Unmarshal(data, restored); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if restored.Spec.JobRef.Name != "job-1" || restored.Spec.JobRef.UID != "uid-abc" {
		t.Errorf("jobRef mismatch: %+v", restored.Spec.JobRef)
	}
	if restored.Spec.Ordinal != 3 {
		t.Errorf("ordinal mismatch: got %d, want 3", restored.Spec.Ordinal)
	}
	if restored.Status.ErrorCategory != ErrorCategoryTimeout {
		t.Errorf("errorCategory mismatch: got %q, want %q", restored.Status.ErrorCategory, ErrorCategoryTimeout)
	}
}

// --- Scheme registration test ---

func TestSchemeRegistration(t *testing.T) {
	s := runtime.NewScheme()
	if err := AddToScheme(s); err != nil {
		t.Fatalf("failed to add types to scheme: %v", err)
	}

	// Verify all top-level types are registered.
	for _, kind := range []string{"AgentJob", "AgentJobList", "AgentAttempt", "AgentAttemptList", "SandboxPool", "SandboxPoolList"} {
		gvk := GroupVersion.WithKind(kind)
		if _, err := s.New(gvk); err != nil {
			t.Fatalf("%s not registered in scheme: %v", kind, err)
		}
	}
}

// --- DeepCopyObject interface check ---

func TestDeepCopyObject_Interface(t *testing.T) {
	var _ runtime.Object = &AgentJob{}
	var _ runtime.Object = &AgentJobList{}
	var _ runtime.Object = &AgentAttempt{}
	var _ runtime.Object = &AgentAttemptList{}
	var _ runtime.Object = &SandboxPool{}
	var _ runtime.Object = &SandboxPoolList{}
}

// --- JobPhaseFromConditions tests ---

func TestJobPhaseFromConditions(t *testing.T) {
	tests := []struct {
		name       string
		conditions []metav1.Condition
		want       string
	}{
		{"empty", nil, "Pending"},
		{"scheduled", []metav1.Condition{{Type: JobConditionScheduled, Status: metav1.ConditionTrue}}, "Scheduling"},
		{"running", []metav1.Condition{{Type: JobConditionRunning, Status: metav1.ConditionTrue}}, "Running"},
		{"complete", []metav1.Condition{{Type: JobConditionComplete, Status: metav1.ConditionTrue}}, "Succeeded"},
		{"failed", []metav1.Condition{{Type: JobConditionFailed, Status: metav1.ConditionTrue}}, "Failed"},
		{"terminal overrides running", []metav1.Condition{
			{Type: JobConditionRunning, Status: metav1.ConditionTrue},
			{Type: JobConditionComplete, Status: metav1.ConditionTrue},
		}, "Succeeded"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := JobPhaseFromConditions(tt.conditions)
			if got != tt.want {
				t.Errorf("JobPhaseFromConditions() = %q, want %q", got, tt.want)
			}
		})
	}
}
