package executor

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"durarun-operator/api/v1alpha1"
)

func newTestJob() *v1alpha1.AgentJob {
	return &v1alpha1.AgentJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job",
			Namespace: "default",
		},
		Spec: v1alpha1.AgentJobSpec{
			Image:   "agent:latest",
			Command: []string{"/bin/agent", "run"},
			Prompt:  "solve the task",
			Env: map[string]string{
				"CUSTOM_VAR": "hello",
			},
			Resources: v1alpha1.ResourceSpec{
				CPULimit:    "2",
				MemoryLimit: "1024m",
			},
			MaxRetries: 3,
			Isolation: v1alpha1.IsolationSpec{
				Level: v1alpha1.L1GVisor,
			},
		},
	}
}

func newTestAttempt(number int) *v1alpha1.AgentAttempt {
	return &v1alpha1.AgentAttempt{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job-1",
			Namespace: "default",
		},
		Spec: v1alpha1.AgentAttemptSpec{
			JobRef: "test-job",
			Number: number,
		},
	}
}

func TestBuildPod_BasicFields(t *testing.T) {
	job := newTestJob()
	attempt := newTestAttempt(1)

	pod := BuildPod(job, attempt)

	// Name and namespace.
	if pod.Name != "test-job-1" {
		t.Errorf("expected pod name 'test-job-1', got %q", pod.Name)
	}
	if pod.Namespace != "default" {
		t.Errorf("expected namespace 'default', got %q", pod.Namespace)
	}

	// Labels.
	if pod.Labels["durarun.io/job"] != "test-job" {
		t.Errorf("missing or wrong label durarun.io/job: %v", pod.Labels)
	}
	if pod.Labels["durarun.io/attempt"] != "test-job-1" {
		t.Errorf("missing or wrong label durarun.io/attempt: %v", pod.Labels)
	}

	// RestartPolicy.
	if pod.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("expected RestartPolicyNever, got %v", pod.Spec.RestartPolicy)
	}

	// AutomountServiceAccountToken.
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Error("AutomountServiceAccountToken should be false")
	}

	// Containers.
	if len(pod.Spec.Containers) != 1 {
		t.Fatalf("expected 1 container, got %d", len(pod.Spec.Containers))
	}
	c := pod.Spec.Containers[0]
	if c.Name != "agent" {
		t.Errorf("expected container name 'agent', got %q", c.Name)
	}
	if c.Image != "agent:latest" {
		t.Errorf("expected image 'agent:latest', got %q", c.Image)
	}
	if len(c.Command) != 2 || c.Command[0] != "/bin/agent" {
		t.Errorf("unexpected command: %v", c.Command)
	}

	// Volumes: workspace + tmp (for ReadOnlyRootFilesystem).
	volNames := make(map[string]bool)
	for _, v := range pod.Spec.Volumes {
		volNames[v.Name] = true
	}
	if !volNames["workspace"] {
		t.Errorf("expected workspace volume, got %v", pod.Spec.Volumes)
	}
	if !volNames["tmp"] {
		t.Errorf("expected tmp volume, got %v", pod.Spec.Volumes)
	}
	mountPaths := make(map[string]bool)
	for _, m := range c.VolumeMounts {
		mountPaths[m.MountPath] = true
	}
	if !mountPaths["/workspace"] {
		t.Errorf("expected /workspace mount, got %v", c.VolumeMounts)
	}
	if !mountPaths["/tmp"] {
		t.Errorf("expected /tmp mount, got %v", c.VolumeMounts)
	}
}

func TestBuildPod_EnvVars(t *testing.T) {
	job := newTestJob()
	attempt := newTestAttempt(1)

	pod := BuildPod(job, attempt)
	envs := pod.Spec.Containers[0].Env

	envMap := make(map[string]string)
	for _, e := range envs {
		envMap[e.Name] = e.Value
	}

	// Standard vars.
	if envMap["AGENT_PROMPT"] != "solve the task" {
		t.Errorf("AGENT_PROMPT = %q, want 'solve the task'", envMap["AGENT_PROMPT"])
	}
	if envMap["AF_JOB_NAME"] != "test-job" {
		t.Errorf("AF_JOB_NAME = %q, want 'test-job'", envMap["AF_JOB_NAME"])
	}
	if envMap["AF_ATTEMPT_NUMBER"] != "1" {
		t.Errorf("AF_ATTEMPT_NUMBER = %q, want '1'", envMap["AF_ATTEMPT_NUMBER"])
	}

	// Custom var.
	if envMap["CUSTOM_VAR"] != "hello" {
		t.Errorf("CUSTOM_VAR = %q, want 'hello'", envMap["CUSTOM_VAR"])
	}
}

func TestBuildPod_Resources(t *testing.T) {
	job := newTestJob()
	attempt := newTestAttempt(1)

	pod := BuildPod(job, attempt)
	res := pod.Spec.Containers[0].Resources

	// CPU: "2" should parse to 2 cores.
	expectedCPU := resource.MustParse("2")
	if !res.Limits[corev1.ResourceCPU].Equal(expectedCPU) {
		t.Errorf("CPU limit = %s, want %s", res.Limits.Cpu().String(), expectedCPU.String())
	}
	if !res.Requests[corev1.ResourceCPU].Equal(expectedCPU) {
		t.Errorf("CPU request = %s, want %s", res.Requests.Cpu().String(), expectedCPU.String())
	}

	// Memory: "1024m" -> 1024 * 1024 * 1024 = 1073741824 bytes (1Gi).
	memLimit := res.Limits[corev1.ResourceMemory]
	memBytes := memLimit.Value()
	expectedBytes := int64(1024 * 1024 * 1024) // 1024 MiB
	if memBytes != expectedBytes {
		t.Errorf("Memory limit = %d bytes, want %d", memBytes, expectedBytes)
	}
}

func TestBuildPod_Resources_MilliCPU(t *testing.T) {
	job := newTestJob()
	job.Spec.Resources.CPULimit = "500m"
	job.Spec.Resources.MemoryLimit = "256m"
	attempt := newTestAttempt(1)

	pod := BuildPod(job, attempt)
	res := pod.Spec.Containers[0].Resources

	expectedCPU := resource.MustParse("500m")
	if !res.Limits[corev1.ResourceCPU].Equal(expectedCPU) {
		t.Errorf("CPU limit = %s, want %s", res.Limits.Cpu().String(), expectedCPU.String())
	}

	memLimit := res.Limits[corev1.ResourceMemory]
	expectedMem := int64(256 * 1024 * 1024) // 256 MiB
	if memLimit.Value() != expectedMem {
		t.Errorf("Memory limit = %d bytes, want %d", memLimit.Value(), expectedMem)
	}
}

func TestBuildPod_SecurityContext_GVisor(t *testing.T) {
	job := newTestJob()
	job.Spec.Isolation.Level = v1alpha1.L1GVisor
	attempt := newTestAttempt(1)

	pod := BuildPod(job, attempt)

	// Pod-level.
	if pod.Spec.SecurityContext == nil || pod.Spec.SecurityContext.RunAsNonRoot == nil || !*pod.Spec.SecurityContext.RunAsNonRoot {
		t.Error("expected RunAsNonRoot=true for gVisor")
	}

	// Container-level.
	sc := pod.Spec.Containers[0].SecurityContext
	if sc == nil {
		t.Fatal("expected container security context for gVisor")
	}
	if sc.ReadOnlyRootFilesystem == nil || !*sc.ReadOnlyRootFilesystem {
		t.Error("expected ReadOnlyRootFilesystem=true for gVisor")
	}
	if sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation {
		t.Error("expected AllowPrivilegeEscalation=false for gVisor")
	}
	if sc.Capabilities == nil || len(sc.Capabilities.Drop) == 0 {
		t.Error("expected capabilities Drop ALL for gVisor")
	}
	if sc.SeccompProfile != nil {
		t.Error("gVisor should not set SeccompProfile")
	}

	// RuntimeClass.
	if pod.Spec.RuntimeClassName == nil || *pod.Spec.RuntimeClassName != "runsc" {
		t.Errorf("expected RuntimeClassName=runsc for gVisor, got %v", pod.Spec.RuntimeClassName)
	}
}

func TestBuildPod_SecurityContext_Firecracker(t *testing.T) {
	job := newTestJob()
	job.Spec.Isolation.Level = v1alpha1.L2Firecracker
	attempt := newTestAttempt(1)

	pod := BuildPod(job, attempt)

	sc := pod.Spec.Containers[0].SecurityContext
	if sc == nil {
		t.Fatal("expected container security context for Firecracker")
	}
	if sc.SeccompProfile == nil || sc.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Error("expected SeccompProfile RuntimeDefault for Firecracker")
	}

	if pod.Spec.RuntimeClassName == nil || *pod.Spec.RuntimeClassName != "firecracker-containerd" {
		t.Errorf("expected RuntimeClassName=firecracker-containerd, got %v", pod.Spec.RuntimeClassName)
	}
}

func TestBuildPod_SecurityContext_Default(t *testing.T) {
	job := newTestJob()
	job.Spec.Isolation.Level = v1alpha1.L0Process
	attempt := newTestAttempt(1)

	pod := BuildPod(job, attempt)

	// No RuntimeClass for default levels.
	if pod.Spec.RuntimeClassName != nil {
		t.Errorf("expected no RuntimeClassName for L0Process, got %v", *pod.Spec.RuntimeClassName)
	}

	sc := pod.Spec.Containers[0].SecurityContext
	if sc == nil {
		t.Fatal("expected container security context")
	}
	// Default should still deny privilege escalation.
	if sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation {
		t.Error("expected AllowPrivilegeEscalation=false even for default")
	}
}

func TestBuildPod_AttemptNumber(t *testing.T) {
	job := newTestJob()
	attempt := newTestAttempt(3)

	pod := BuildPod(job, attempt)
	if pod.Name != "test-job-3" {
		t.Errorf("expected pod name 'test-job-3', got %q", pod.Name)
	}
}
