package executor

import (
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"durarun-operator/api/v1alpha1"
)

// BuildPod constructs a corev1.Pod from an AgentJob and its AgentAttempt.
// runnerImage specifies the image that contains the /agent-runner binary.
// The returned Pod is not yet created; the caller must set OwnerReferences
// and submit it to the API server.
func BuildPod(job *v1alpha1.AgentJob, attempt *v1alpha1.AgentAttempt, opts ...PodBuildOption) *corev1.Pod {
	cfg := defaultPodBuildConfig()
	for _, o := range opts {
		o(&cfg)
	}

	podName := fmt.Sprintf("%s-%d", job.Name, attempt.Spec.Ordinal)

	podSecCtx, containerSecCtx := BuildSecurityContext("")

	// Build the main container command: runner as PID 1, then user command/args.
	mainCommand := []string{"/tools/agent-runner"}
	var mainArgs []string
	mainArgs = append(mainArgs, "--")
	mainArgs = append(mainArgs, job.Spec.Runtime.Command...)
	if len(job.Spec.Runtime.Args) > 0 {
		mainArgs = append(mainArgs, job.Spec.Runtime.Args...)
	}

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
			InitContainers: []corev1.Container{
				{
					Name:    "inject-runner",
					Image:   cfg.RunnerImage,
					Command: []string{"cp", "/agent-runner", "/tools/agent-runner"},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "tools", MountPath: "/tools"},
					},
				},
			},
			Containers: []corev1.Container{
				{
					Name:            "agent",
					Image:           job.Spec.Runtime.Image,
					Command:         mainCommand,
					Args:            mainArgs,
					Env:             buildEnvVars(job, attempt),
					Resources:       job.Spec.Runtime.Resources,
					SecurityContext: containerSecCtx,
					VolumeMounts: []corev1.VolumeMount{
						{Name: "workspace", MountPath: "/workspace"},
						{Name: "tmp", MountPath: "/tmp"},
						{Name: "tools", MountPath: "/tools", ReadOnly: true},
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
				{
					Name: "tools",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
			},
			SecurityContext:              podSecCtx,
			AutomountServiceAccountToken: boolPtr(false),
		},
	}

	// If an attempt secret name is provided, mount the token volume.
	if cfg.AttemptSecretName != "" {
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
			Name: "durarun-token",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: cfg.AttemptSecretName,
				},
			},
		})
		pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts,
			corev1.VolumeMount{
				Name:      "durarun-token",
				MountPath: "/var/run/durarun",
				ReadOnly:  true,
			},
		)
	}

	return pod
}

// PodBuildConfig holds optional configuration for BuildPod.
type PodBuildConfig struct {
	RunnerImage       string
	AttemptSecretName string
}

// PodBuildOption is a functional option for BuildPod.
type PodBuildOption func(*PodBuildConfig)

func defaultPodBuildConfig() PodBuildConfig {
	return PodBuildConfig{
		RunnerImage: "durarun-runner:latest",
	}
}

// WithRunnerImage sets the runner image used by the init container.
func WithRunnerImage(image string) PodBuildOption {
	return func(c *PodBuildConfig) {
		if image != "" {
			c.RunnerImage = image
		}
	}
}

// WithAttemptSecret sets the secret name to mount as the attempt token.
func WithAttemptSecret(name string) PodBuildOption {
	return func(c *PodBuildConfig) {
		c.AttemptSecretName = name
	}
}

// buildEnvVars produces the environment variable list for the agent container.
// It includes standard vars plus user-supplied env vars from runtime spec.
func buildEnvVars(job *v1alpha1.AgentJob, attempt *v1alpha1.AgentAttempt) []corev1.EnvVar {
	var envs []corev1.EnvVar

	// Standard DURARUN variables.
	envs = append(envs,
		corev1.EnvVar{Name: "DURARUN_JOB_NAME", Value: job.Name},
		corev1.EnvVar{Name: "DURARUN_JOB_UID", Value: string(job.UID)},
		corev1.EnvVar{Name: "DURARUN_ATTEMPT_ORDINAL", Value: strconv.FormatInt(int64(attempt.Spec.Ordinal), 10)},
		corev1.EnvVar{Name: "DURARUN_ATTEMPT_UID", Value: string(attempt.UID)},
	)

	// Legacy aliases for backward compatibility.
	envs = append(envs,
		corev1.EnvVar{Name: "AF_JOB_NAME", Value: job.Name},
		corev1.EnvVar{Name: "AF_ATTEMPT_NUMBER", Value: strconv.FormatInt(int64(attempt.Spec.Ordinal), 10)},
	)

	// User-supplied variables from runtime spec.
	envs = append(envs, job.Spec.Runtime.Env...)

	return envs
}

func boolPtr(b bool) *bool {
	return &b
}
