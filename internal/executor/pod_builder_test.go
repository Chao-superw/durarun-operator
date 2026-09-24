package executor

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"durarun-operator/api/v1alpha1"
)

func newTestJob() *v1alpha1.AgentJob {
	return &v1alpha1.AgentJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job",
			Namespace: "default",
			UID:       types.UID("job-uid-1234"),
		},
		Spec: v1alpha1.AgentJobSpec{
			Runtime: v1alpha1.RuntimeSpec{
				Image:   "agent:latest",
				Command: []string{"/bin/agent", "run"},
				Env: []corev1.EnvVar{
					{Name: "CUSTOM_VAR", Value: "hello"},
				},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("2"),
						corev1.ResourceMemory: resource.MustParse("1Gi"),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("2"),
						corev1.ResourceMemory: resource.MustParse("1Gi"),
					},
				},
			},
			Execution: v1alpha1.ExecutionSpec{
				MaxAttempts: 4,
			},
		},
	}
}

func newTestAttempt(ordinal int32) *v1alpha1.AgentAttempt {
	return &v1alpha1.AgentAttempt{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job-1",
			Namespace: "default",
			UID:       types.UID("attempt-uid-5678"),
		},
		Spec: v1alpha1.AgentAttemptSpec{
			JobRef:  v1alpha1.ObjectRef{Name: "test-job", UID: "test-uid"},
			Ordinal: ordinal,
		},
	}
}

// ---------------------------------------------------------------------------
// Basic pod fields
// ---------------------------------------------------------------------------

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

	// Main container count (only agent, no sidecar).
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

	// Volumes: workspace + tmp + tools.
	volNames := make(map[string]bool)
	for _, v := range pod.Spec.Volumes {
		volNames[v.Name] = true
	}
	for _, expected := range []string{"workspace", "tmp", "tools"} {
		if !volNames[expected] {
			t.Errorf("expected volume %q, got %v", expected, pod.Spec.Volumes)
		}
	}
	mountPaths := make(map[string]bool)
	for _, m := range c.VolumeMounts {
		mountPaths[m.MountPath] = true
	}
	for _, expected := range []string{"/workspace", "/tmp", "/tools"} {
		if !mountPaths[expected] {
			t.Errorf("expected mount path %q, got %v", expected, c.VolumeMounts)
		}
	}
}

// ---------------------------------------------------------------------------
// Environment variables
// ---------------------------------------------------------------------------

func TestBuildPod_EnvVars(t *testing.T) {
	job := newTestJob()
	attempt := newTestAttempt(1)

	pod := BuildPod(job, attempt)
	envs := pod.Spec.Containers[0].Env

	envMap := make(map[string]string)
	for _, e := range envs {
		envMap[e.Name] = e.Value
	}

	// New DURARUN standard vars.
	if envMap["DURARUN_JOB_NAME"] != "test-job" {
		t.Errorf("DURARUN_JOB_NAME = %q, want 'test-job'", envMap["DURARUN_JOB_NAME"])
	}
	if envMap["DURARUN_JOB_UID"] != "job-uid-1234" {
		t.Errorf("DURARUN_JOB_UID = %q, want 'job-uid-1234'", envMap["DURARUN_JOB_UID"])
	}
	if envMap["DURARUN_ATTEMPT_ORDINAL"] != "1" {
		t.Errorf("DURARUN_ATTEMPT_ORDINAL = %q, want '1'", envMap["DURARUN_ATTEMPT_ORDINAL"])
	}
	if envMap["DURARUN_ATTEMPT_UID"] != "attempt-uid-5678" {
		t.Errorf("DURARUN_ATTEMPT_UID = %q, want 'attempt-uid-5678'", envMap["DURARUN_ATTEMPT_UID"])
	}

	// Legacy aliases.
	if envMap["AF_JOB_NAME"] != "test-job" {
		t.Errorf("AF_JOB_NAME = %q, want 'test-job'", envMap["AF_JOB_NAME"])
	}
	if envMap["AF_ATTEMPT_NUMBER"] != "1" {
		t.Errorf("AF_ATTEMPT_NUMBER = %q, want '1'", envMap["AF_ATTEMPT_NUMBER"])
	}

	// Custom var from runtime env.
	if envMap["CUSTOM_VAR"] != "hello" {
		t.Errorf("CUSTOM_VAR = %q, want 'hello'", envMap["CUSTOM_VAR"])
	}
}

// ---------------------------------------------------------------------------
// Resources
// ---------------------------------------------------------------------------

func TestBuildPod_Resources(t *testing.T) {
	job := newTestJob()
	attempt := newTestAttempt(1)

	pod := BuildPod(job, attempt)
	res := pod.Spec.Containers[0].Resources

	// CPU: 2 cores.
	expectedCPU := resource.MustParse("2")
	if !res.Limits[corev1.ResourceCPU].Equal(expectedCPU) {
		t.Errorf("CPU limit = %s, want %s", res.Limits.Cpu().String(), expectedCPU.String())
	}
	if !res.Requests[corev1.ResourceCPU].Equal(expectedCPU) {
		t.Errorf("CPU request = %s, want %s", res.Requests.Cpu().String(), expectedCPU.String())
	}

	// Memory: 1Gi.
	expectedMem := resource.MustParse("1Gi")
	if !res.Limits[corev1.ResourceMemory].Equal(expectedMem) {
		t.Errorf("Memory limit = %s, want %s", res.Limits.Memory().String(), expectedMem.String())
	}
}

// ---------------------------------------------------------------------------
// Security context
// ---------------------------------------------------------------------------

func TestBuildPod_SecurityContext_Default(t *testing.T) {
	job := newTestJob()
	attempt := newTestAttempt(1)

	pod := BuildPod(job, attempt)

	// Pod-level security context.
	if pod.Spec.SecurityContext == nil || pod.Spec.SecurityContext.RunAsNonRoot == nil || !*pod.Spec.SecurityContext.RunAsNonRoot {
		t.Error("expected RunAsNonRoot=true")
	}

	// RunAsUser must be 65534 (nobody).
	if pod.Spec.SecurityContext.RunAsUser == nil || *pod.Spec.SecurityContext.RunAsUser != 65534 {
		t.Errorf("expected RunAsUser=65534, got %v", pod.Spec.SecurityContext.RunAsUser)
	}

	// Container-level.
	sc := pod.Spec.Containers[0].SecurityContext
	if sc == nil {
		t.Fatal("expected container security context")
	}
	if sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation {
		t.Error("expected AllowPrivilegeEscalation=false")
	}
	if sc.ReadOnlyRootFilesystem == nil || !*sc.ReadOnlyRootFilesystem {
		t.Error("expected ReadOnlyRootFilesystem=true")
	}

	// Seccomp profile.
	if sc.SeccompProfile == nil || sc.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Error("expected SeccompProfile RuntimeDefault")
	}

	// Capabilities: drop ALL.
	if sc.Capabilities == nil || len(sc.Capabilities.Drop) == 0 {
		t.Error("expected capabilities to drop ALL")
	} else if sc.Capabilities.Drop[0] != "ALL" {
		t.Errorf("expected drop ALL, got %v", sc.Capabilities.Drop)
	}
}

// ---------------------------------------------------------------------------
// No hostPath volumes
// ---------------------------------------------------------------------------

func TestBuildPod_NoHostPathVolumes(t *testing.T) {
	job := newTestJob()
	job.Spec.Checkpoint.Enabled = true
	attempt := newTestAttempt(1)

	pod := BuildPod(job, attempt)

	for _, v := range pod.Spec.Volumes {
		if v.VolumeSource.HostPath != nil {
			t.Errorf("pod should not contain hostPath volumes, found %q with path %q",
				v.Name, v.VolumeSource.HostPath.Path)
		}
	}
}

// ---------------------------------------------------------------------------
// Init container injects runner
// ---------------------------------------------------------------------------

func TestBuildPod_InitContainerInjectsRunner(t *testing.T) {
	job := newTestJob()
	attempt := newTestAttempt(1)

	pod := BuildPod(job, attempt, WithRunnerImage("my-runner:v1"))

	if len(pod.Spec.InitContainers) != 1 {
		t.Fatalf("expected 1 init container, got %d", len(pod.Spec.InitContainers))
	}

	ic := pod.Spec.InitContainers[0]
	if ic.Name != "inject-runner" {
		t.Errorf("expected init container name 'inject-runner', got %q", ic.Name)
	}
	if ic.Image != "my-runner:v1" {
		t.Errorf("expected image 'my-runner:v1', got %q", ic.Image)
	}
	if len(ic.Command) != 3 || ic.Command[0] != "cp" || ic.Command[1] != "/agent-runner" || ic.Command[2] != "/tools/agent-runner" {
		t.Errorf("unexpected init container command: %v", ic.Command)
	}

	// Verify init container mounts the tools volume.
	hasToolsMount := false
	for _, m := range ic.VolumeMounts {
		if m.Name == "tools" && m.MountPath == "/tools" {
			hasToolsMount = true
		}
	}
	if !hasToolsMount {
		t.Error("init container should mount the tools volume at /tools")
	}
}

// ---------------------------------------------------------------------------
// Runner as PID 1
// ---------------------------------------------------------------------------

func TestBuildPod_RunnerAsPID1(t *testing.T) {
	job := newTestJob()
	attempt := newTestAttempt(1)

	pod := BuildPod(job, attempt)
	c := pod.Spec.Containers[0]

	if len(c.Command) != 1 || c.Command[0] != "/tools/agent-runner" {
		t.Errorf("expected command [/tools/agent-runner], got %v", c.Command)
	}

	// Args should be: -- /bin/agent run
	expectedArgs := []string{"--", "/bin/agent", "run"}
	if len(c.Args) != len(expectedArgs) {
		t.Fatalf("expected args %v, got %v", expectedArgs, c.Args)
	}
	for i, a := range expectedArgs {
		if c.Args[i] != a {
			t.Errorf("args[%d] = %q, want %q", i, c.Args[i], a)
		}
	}
}

// ---------------------------------------------------------------------------
// Default runner image
// ---------------------------------------------------------------------------

func TestBuildPod_DefaultRunnerImage(t *testing.T) {
	job := newTestJob()
	attempt := newTestAttempt(1)

	pod := BuildPod(job, attempt)

	if len(pod.Spec.InitContainers) != 1 {
		t.Fatalf("expected 1 init container, got %d", len(pod.Spec.InitContainers))
	}
	if pod.Spec.InitContainers[0].Image != "durarun-runner:latest" {
		t.Errorf("expected default runner image 'durarun-runner:latest', got %q",
			pod.Spec.InitContainers[0].Image)
	}
}

// ---------------------------------------------------------------------------
// Attempt ordinal in pod name
// ---------------------------------------------------------------------------

func TestBuildPod_AttemptOrdinal(t *testing.T) {
	job := newTestJob()
	attempt := newTestAttempt(3)

	pod := BuildPod(job, attempt)
	if pod.Name != "test-job-3" {
		t.Errorf("expected pod name 'test-job-3', got %q", pod.Name)
	}
}

// ---------------------------------------------------------------------------
// AutomountServiceAccountToken disabled
// ---------------------------------------------------------------------------

func TestBuildPod_NoServiceAccountToken(t *testing.T) {
	job := newTestJob()
	attempt := newTestAttempt(1)

	pod := BuildPod(job, attempt)

	if pod.Spec.AutomountServiceAccountToken == nil {
		t.Fatal("AutomountServiceAccountToken should not be nil")
	}
	if *pod.Spec.AutomountServiceAccountToken {
		t.Error("AutomountServiceAccountToken should be false to prevent K8s token injection")
	}
}

// ---------------------------------------------------------------------------
// NetworkPolicy tests
// ---------------------------------------------------------------------------

func TestBuildNetworkPolicy_DefaultDenyAndDNS(t *testing.T) {
	job := newTestJob()
	np := BuildNetworkPolicy(job, "artifact-gateway")

	// Name and namespace.
	if np.Name != "test-job-egress" {
		t.Errorf("expected name 'test-job-egress', got %q", np.Name)
	}
	if np.Namespace != "default" {
		t.Errorf("expected namespace 'default', got %q", np.Namespace)
	}

	// PodSelector targets the job.
	if np.Spec.PodSelector.MatchLabels["durarun.io/job"] != "test-job" {
		t.Errorf("PodSelector should target durarun.io/job=test-job, got %v", np.Spec.PodSelector.MatchLabels)
	}

	// PolicyTypes includes Egress (default deny).
	foundEgress := false
	for _, pt := range np.Spec.PolicyTypes {
		if pt == "Egress" {
			foundEgress = true
		}
	}
	if !foundEgress {
		t.Error("expected PolicyType Egress for default deny")
	}

	// Must have at least 2 egress rules: DNS + gateway.
	if len(np.Spec.Egress) < 2 {
		t.Fatalf("expected at least 2 egress rules, got %d", len(np.Spec.Egress))
	}

	// First rule: DNS (port 53).
	dnsRule := np.Spec.Egress[0]
	if len(dnsRule.Ports) < 2 {
		t.Fatalf("DNS rule should have at least 2 ports (UDP+TCP), got %d", len(dnsRule.Ports))
	}
	for _, p := range dnsRule.Ports {
		if p.Port.IntValue() != 53 {
			t.Errorf("DNS port should be 53, got %d", p.Port.IntValue())
		}
	}

	// Second rule: gateway.
	gwRule := np.Spec.Egress[1]
	if len(gwRule.To) == 0 {
		t.Fatal("gateway rule should have at least one To peer")
	}
	if gwRule.To[0].PodSelector == nil || gwRule.To[0].PodSelector.MatchLabels["app"] != "artifact-gateway" {
		t.Errorf("gateway rule should target app=artifact-gateway, got %v", gwRule.To)
	}
}

// ---------------------------------------------------------------------------
// Attempt secret tests
// ---------------------------------------------------------------------------

func TestBuildAttemptSecret_Structure(t *testing.T) {
	job := newTestJob()
	attempt := newTestAttempt(1)
	token := "secret-token-xyz"

	secret := BuildAttemptSecret(job, attempt, token)

	expectedName := "test-job-1-token"
	if secret.Name != expectedName {
		t.Errorf("expected secret name %q, got %q", expectedName, secret.Name)
	}
	if secret.Namespace != "default" {
		t.Errorf("expected namespace 'default', got %q", secret.Namespace)
	}
	if secret.Type != corev1.SecretTypeOpaque {
		t.Errorf("expected SecretTypeOpaque, got %v", secret.Type)
	}

	// Labels.
	if secret.Labels["durarun.io/job"] != "test-job" {
		t.Errorf("missing or wrong label durarun.io/job: %v", secret.Labels)
	}
	if secret.Labels["durarun.io/attempt"] != "test-job-1" {
		t.Errorf("missing or wrong label durarun.io/attempt: %v", secret.Labels)
	}

	// Token data.
	tokenData, ok := secret.Data["token"]
	if !ok {
		t.Fatal("secret should contain 'token' key")
	}
	if string(tokenData) != token {
		t.Errorf("token data = %q, want %q", string(tokenData), token)
	}
}

// ---------------------------------------------------------------------------
// Attempt secret mounted into pod
// ---------------------------------------------------------------------------

func TestBuildPod_WithAttemptSecret(t *testing.T) {
	job := newTestJob()
	attempt := newTestAttempt(1)

	pod := BuildPod(job, attempt, WithAttemptSecret("test-job-1-token"))

	// Check the token volume exists.
	foundVol := false
	for _, v := range pod.Spec.Volumes {
		if v.Name == "durarun-token" {
			foundVol = true
			if v.Secret == nil || v.Secret.SecretName != "test-job-1-token" {
				t.Errorf("expected secret volume with name 'test-job-1-token', got %v", v)
			}
		}
	}
	if !foundVol {
		t.Error("expected durarun-token volume")
	}

	// Check mount in agent container.
	foundMount := false
	for _, m := range pod.Spec.Containers[0].VolumeMounts {
		if m.Name == "durarun-token" && m.MountPath == "/var/run/durarun" && m.ReadOnly {
			foundMount = true
		}
	}
	if !foundMount {
		t.Error("expected durarun-token mount at /var/run/durarun (read-only)")
	}
}

// ---------------------------------------------------------------------------
// Runner args with job args
// ---------------------------------------------------------------------------

func TestBuildPod_RunnerWithJobArgs(t *testing.T) {
	job := newTestJob()
	job.Spec.Runtime.Args = []string{"--verbose", "--debug"}
	attempt := newTestAttempt(1)

	pod := BuildPod(job, attempt)
	c := pod.Spec.Containers[0]

	expectedArgs := []string{"--", "/bin/agent", "run", "--verbose", "--debug"}
	if len(c.Args) != len(expectedArgs) {
		t.Fatalf("expected args %v, got %v", expectedArgs, c.Args)
	}
	for i, a := range expectedArgs {
		if c.Args[i] != a {
			t.Errorf("args[%d] = %q, want %q", i, c.Args[i], a)
		}
	}
}
