package executor

import (
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"durarun-operator/api/v1alpha1"
)

// BuildPod constructs a corev1.Pod from an AgentJob and its AgentAttempt.
// The returned Pod is not yet created; the caller must set OwnerReferences
// and submit it to the API server.
func BuildPod(job *v1alpha1.AgentJob, attempt *v1alpha1.AgentAttempt) *corev1.Pod {
	podName := fmt.Sprintf("%s-%d", job.Name, attempt.Spec.Number)

	podSecCtx, containerSecCtx := BuildSecurityContext(job.Spec.Isolation.Level)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: job.Namespace,
			Labels: map[string]string{
				"durarun.io/job":     job.Name,
				"durarun.io/attempt": attempt.Name,
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:            "agent",
					Image:           job.Spec.Image,
					Command:         job.Spec.Command,
					Env:             buildEnvVars(job, attempt),
					Resources:       buildResources(job.Spec.Resources),
					SecurityContext: containerSecCtx,
					VolumeMounts: []corev1.VolumeMount{
						{Name: "workspace", MountPath: "/workspace"},
						{Name: "tmp", MountPath: "/tmp"},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "workspace",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
				{
					Name: "tmp",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{
							Medium: corev1.StorageMediumMemory,
						},
					},
				},
			},
			SecurityContext:               podSecCtx,
			AutomountServiceAccountToken: boolPtr(false),
		},
	}

	// Set RuntimeClassName based on isolation level.
	if rc := runtimeClassForLevel(job.Spec.Isolation.Level); rc != "" {
		pod.Spec.RuntimeClassName = &rc
	}

	// Checkpoint support
	if job.Spec.Checkpoint.Enabled {
		// Wrap the agent command so the main container writes an exit marker when it finishes.
		if len(job.Spec.Command) > 0 {
			cmd := job.Spec.Command
			if len(cmd) >= 3 && (cmd[0] == "sh" || cmd[0] == "/bin/sh" || cmd[0] == "bash" || cmd[0] == "/bin/bash") && cmd[1] == "-c" {
				cmd[len(cmd)-1] = cmd[len(cmd)-1] + "; echo $? > /workspace/.exit-code"
				pod.Spec.Containers[0].Command = cmd
			} else {
				original := strings.Join(cmd, " ")
				pod.Spec.Containers[0].Command = []string{"sh", "-c", original + "; echo $? > /workspace/.exit-code"}
			}
		}

		// Add volumes for checkpoints and agent-runner binary.
		pod.Spec.Volumes = append(pod.Spec.Volumes,
			corev1.Volume{
				Name: "checkpoints",
				VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{
						Path: "/tmp/durarun-checkpoints",
						Type: hostPathTypePtr(corev1.HostPathDirectoryOrCreate),
					},
				},
			},
			corev1.Volume{
				Name: "agent-runner-bin",
				VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{
						Path: "/home/wangchaoyu.v2/durarun-operator/bin",
						Type: hostPathTypePtr(corev1.HostPathDirectory),
					},
				},
			},
		)

		// Add initContainer to restore checkpoint before the agent starts.
		pod.Spec.InitContainers = append(pod.Spec.InitContainers, corev1.Container{
			Name:    "restore-checkpoint",
			Image:   "busybox:latest",
			Command: []string{"/tools/agent-runner"},
			Args: []string{
				"--mode=restore",
				"--work-dir=/workspace",
				"--checkpoint-dir=/checkpoints",
				fmt.Sprintf("--job-name=%s", job.Name),
			},
			VolumeMounts: []corev1.VolumeMount{
				{Name: "workspace", MountPath: "/workspace"},
				{Name: "checkpoints", MountPath: "/checkpoints"},
				{Name: "agent-runner-bin", MountPath: "/tools"},
			},
		})

		// Add sidecar container for periodic checkpointing.
		interval := 30
		if job.Spec.Checkpoint.IntervalSeconds > 0 {
			interval = job.Spec.Checkpoint.IntervalSeconds
		}
		pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{
			Name:    "runner-sidecar",
			Image:   "busybox:latest",
			Command: []string{"/tools/agent-runner"},
			Args: []string{
				"--mode=sidecar",
				"--work-dir=/workspace",
				"--checkpoint-dir=/checkpoints",
				fmt.Sprintf("--job-name=%s", job.Name),
				fmt.Sprintf("--attempt-number=%d", attempt.Spec.Number),
				fmt.Sprintf("--checkpoint-interval=%d", interval),
			},
			VolumeMounts: []corev1.VolumeMount{
				{Name: "workspace", MountPath: "/workspace"},
				{Name: "checkpoints", MountPath: "/checkpoints"},
				{Name: "agent-runner-bin", MountPath: "/tools"},
			},
		})
	}

	return pod
}

// buildEnvVars produces the environment variable list for the agent container.
// It includes user-supplied env vars from job.Spec.Env plus standard vars.
func buildEnvVars(job *v1alpha1.AgentJob, attempt *v1alpha1.AgentAttempt) []corev1.EnvVar {
	var envs []corev1.EnvVar

	// Standard variables.
	envs = append(envs, corev1.EnvVar{Name: "AGENT_PROMPT", Value: job.Spec.Prompt})
	envs = append(envs, corev1.EnvVar{Name: "AF_JOB_NAME", Value: job.Name})
	envs = append(envs, corev1.EnvVar{Name: "AF_ATTEMPT_NUMBER", Value: strconv.Itoa(attempt.Spec.Number)})

	// User-supplied variables.
	for k, v := range job.Spec.Env {
		envs = append(envs, corev1.EnvVar{Name: k, Value: v})
	}

	return envs
}

// buildResources converts a v1alpha1.ResourceSpec into Kubernetes
// ResourceRequirements.  It sets both requests and limits to the same value
// to guarantee QoS class "Guaranteed".
func buildResources(spec v1alpha1.ResourceSpec) corev1.ResourceRequirements {
	reqs := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{},
		Limits:   corev1.ResourceList{},
	}

	if spec.CPULimit != "" {
		q := parseCPUQuantity(spec.CPULimit)
		reqs.Requests[corev1.ResourceCPU] = q
		reqs.Limits[corev1.ResourceCPU] = q
	}

	if spec.MemoryLimit != "" {
		q := parseMemoryQuantity(spec.MemoryLimit)
		reqs.Requests[corev1.ResourceMemory] = q
		reqs.Limits[corev1.ResourceMemory] = q
	}

	return reqs
}

// parseCPUQuantity converts a CPU string (e.g. "1", "0.5", "500m") into a
// resource.Quantity.
func parseCPUQuantity(s string) resource.Quantity {
	s = strings.TrimSpace(s)
	// If the string already has a suffix recognized by resource.Quantity, use it directly.
	if strings.HasSuffix(s, "m") {
		q, err := resource.ParseQuantity(s)
		if err == nil {
			return q
		}
	}
	// Plain number: interpret as whole or fractional cores.
	q, err := resource.ParseQuantity(s)
	if err != nil {
		// Fallback to zero.
		return resource.MustParse("0")
	}
	return q
}

// parseMemoryQuantity converts a memory string (e.g. "512m", "1g", "256Mi")
// into a resource.Quantity.  It handles the project's legacy lowercase
// suffixes (m=MiB, g=GiB, k=KiB) as well as standard K8s suffixes.
//
// Legacy lowercase suffixes are checked FIRST because K8s resource.ParseQuantity
// treats "m" as "milli" (10^-3) which is not what we want for memory.
func parseMemoryQuantity(s string) resource.Quantity {
	s = strings.TrimSpace(s)

	// Check for legacy single-char lowercase suffixes first.
	// These come from the project's Docker-era conventions where
	// "m" = MiB, "g" = GiB, "k" = KiB.
	if len(s) > 1 {
		suffix := s[len(s)-1]
		numPart := s[:len(s)-1]
		switch suffix {
		case 'm':
			n, _ := strconv.ParseFloat(numPart, 64)
			return *resource.NewQuantity(int64(n*1024*1024), resource.BinarySI)
		case 'g':
			n, _ := strconv.ParseFloat(numPart, 64)
			return *resource.NewQuantity(int64(n*1024*1024*1024), resource.BinarySI)
		case 'k':
			n, _ := strconv.ParseFloat(numPart, 64)
			return *resource.NewQuantity(int64(n*1024), resource.BinarySI)
		}
	}

	// Try standard K8s format (e.g., "512Mi", "1Gi", "1000").
	if q, err := resource.ParseQuantity(s); err == nil {
		return q
	}

	// Fallback: treat as raw bytes.
	n, _ := strconv.ParseInt(s, 10, 64)
	return *resource.NewQuantity(n, resource.BinarySI)
}

// runtimeClassForLevel returns the RuntimeClassName to use for the given
// isolation level, or "" if no special runtime is needed.
func runtimeClassForLevel(level v1alpha1.IsolationLevel) string {
	switch level {
	case v1alpha1.L1GVisor:
		return "runsc"
	case v1alpha1.L2Firecracker:
		return "firecracker-containerd"
	default:
		return ""
	}
}

func boolPtr(b bool) *bool {
	return &b
}

func hostPathTypePtr(t corev1.HostPathType) *corev1.HostPathType {
	return &t
}
