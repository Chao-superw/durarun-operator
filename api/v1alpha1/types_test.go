package v1alpha1

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// --- ValidateJobSpec tests ---

func TestValidateJobSpec_Valid(t *testing.T) {
	spec := &AgentJobSpec{
		Image:  "my-agent:latest",
		Prompt: "do something",
	}
	if err := ValidateJobSpec(spec); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	// Verify defaults were applied.
	if spec.Timeout != "5m" {
		t.Errorf("expected default timeout '5m', got %q", spec.Timeout)
	}
	if spec.Resources.CPULimit != "1" {
		t.Errorf("expected default cpuLimit '1', got %q", spec.Resources.CPULimit)
	}
	if spec.Resources.MemoryLimit != "512m" {
		t.Errorf("expected default memoryLimit '512m', got %q", spec.Resources.MemoryLimit)
	}
	if spec.Isolation.Level != L1GVisor {
		t.Errorf("expected default isolation L1GVisor, got %q", spec.Isolation.Level)
	}
	if !spec.StepTracking.Enabled || spec.StepTracking.Protocol != ProtocolEnv {
		t.Errorf("expected default step tracking enabled/env, got %v/%q", spec.StepTracking.Enabled, spec.StepTracking.Protocol)
	}
	if spec.PoolRef != "default" {
		t.Errorf("expected default poolRef 'default', got %q", spec.PoolRef)
	}
}

func TestValidateJobSpec_MissingImage(t *testing.T) {
	spec := &AgentJobSpec{}
	if err := ValidateJobSpec(spec); err == nil {
		t.Fatal("expected error for missing image")
	}
}

func TestValidateJobSpec_NegativeRetries(t *testing.T) {
	spec := &AgentJobSpec{
		Image:      "img:latest",
		MaxRetries: -3,
	}
	if err := ValidateJobSpec(spec); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if spec.MaxRetries != 0 {
		t.Errorf("expected maxRetries clamped to 0, got %d", spec.MaxRetries)
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
			Image:   "img:v1",
			Command: []string{"run", "--flag"},
			Env:     map[string]string{"KEY": "VALUE"},
		},
		Status: AgentJobStatus{
			Phase:          JobPhaseRunning,
			FailedAttempts: 1,
			ActiveAttempt: &AttemptReference{
				Name:   "attempt-1",
				Number: 1,
				UID:    types.UID("uid-123"),
			},
		},
	}

	copied := original.DeepCopy()

	// Mutate the copy and verify the original is not affected.
	copied.Spec.Command[0] = "mutated"
	if original.Spec.Command[0] == "mutated" {
		t.Error("DeepCopy: mutating Command slice affected the original")
	}

	copied.Spec.Env["KEY"] = "CHANGED"
	if original.Spec.Env["KEY"] == "CHANGED" {
		t.Error("DeepCopy: mutating Env map affected the original")
	}

	copied.Status.ActiveAttempt.Name = "mutated"
	if original.Status.ActiveAttempt.Name == "mutated" {
		t.Error("DeepCopy: mutating ActiveAttempt affected the original")
	}
}

func TestAgentAttempt_DeepCopy(t *testing.T) {
	exitCode := int32(0)
	original := &AgentAttempt{
		ObjectMeta: metav1.ObjectMeta{
			Name: "attempt-1",
		},
		Spec: AgentAttemptSpec{
			JobRef: "job-1",
			Number: 1,
		},
		Status: AgentAttemptStatus{
			Phase:    AttemptPhaseExecuting,
			ExitCode: &exitCode,
			StepProgress: []StepStatus{
				{StepID: "step-1", Phase: "Running"},
			},
		},
	}

	copied := original.DeepCopy()

	// Mutate exit code.
	*copied.Status.ExitCode = 1
	if *original.Status.ExitCode != 0 {
		t.Error("DeepCopy: mutating ExitCode affected the original")
	}

	// Mutate step progress.
	copied.Status.StepProgress[0].StepID = "mutated"
	if original.Status.StepProgress[0].StepID == "mutated" {
		t.Error("DeepCopy: mutating StepProgress affected the original")
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

// --- Scheme registration test ---

func TestSchemeRegistration(t *testing.T) {
	s := runtime.NewScheme()
	if err := AddToScheme(s); err != nil {
		t.Fatalf("failed to add types to scheme: %v", err)
	}

	// Verify all top-level types are registered.
	gvk := GroupVersion.WithKind("AgentJob")
	obj, err := s.New(gvk)
	if err != nil {
		t.Fatalf("AgentJob not registered in scheme: %v", err)
	}
	if _, ok := obj.(*AgentJob); !ok {
		t.Fatalf("expected *AgentJob, got %T", obj)
	}

	gvk = GroupVersion.WithKind("AgentJobList")
	obj, err = s.New(gvk)
	if err != nil {
		t.Fatalf("AgentJobList not registered in scheme: %v", err)
	}
	if _, ok := obj.(*AgentJobList); !ok {
		t.Fatalf("expected *AgentJobList, got %T", obj)
	}

	gvk = GroupVersion.WithKind("AgentAttempt")
	obj, err = s.New(gvk)
	if err != nil {
		t.Fatalf("AgentAttempt not registered in scheme: %v", err)
	}
	if _, ok := obj.(*AgentAttempt); !ok {
		t.Fatalf("expected *AgentAttempt, got %T", obj)
	}

	gvk = GroupVersion.WithKind("AgentAttemptList")
	obj, err = s.New(gvk)
	if err != nil {
		t.Fatalf("AgentAttemptList not registered in scheme: %v", err)
	}
	if _, ok := obj.(*AgentAttemptList); !ok {
		t.Fatalf("expected *AgentAttemptList, got %T", obj)
	}

	gvk = GroupVersion.WithKind("SandboxPool")
	obj, err = s.New(gvk)
	if err != nil {
		t.Fatalf("SandboxPool not registered in scheme: %v", err)
	}
	if _, ok := obj.(*SandboxPool); !ok {
		t.Fatalf("expected *SandboxPool, got %T", obj)
	}

	gvk = GroupVersion.WithKind("SandboxPoolList")
	obj, err = s.New(gvk)
	if err != nil {
		t.Fatalf("SandboxPoolList not registered in scheme: %v", err)
	}
	if _, ok := obj.(*SandboxPoolList); !ok {
		t.Fatalf("expected *SandboxPoolList, got %T", obj)
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
