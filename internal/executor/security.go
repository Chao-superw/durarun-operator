package executor

import (
	corev1 "k8s.io/api/core/v1"

	"durarun-operator/api/v1alpha1"
)

// BuildSecurityContext returns PodSecurityContext and container-level
// SecurityContext appropriate for the requested isolation level.
func BuildSecurityContext(level v1alpha1.IsolationLevel) (*corev1.PodSecurityContext, *corev1.SecurityContext) {
	switch level {
	case v1alpha1.L1GVisor:
		return gvisorSecurityContext()
	case v1alpha1.L2Firecracker:
		return firecrackerSecurityContext()
	default:
		// L0Process, L3Docker, or unset: minimal hardening.
		return defaultSecurityContext()
	}
}

// defaultSecurityContext provides a baseline security posture.
func defaultSecurityContext() (*corev1.PodSecurityContext, *corev1.SecurityContext) {
	uid := int64(1000)
	return &corev1.PodSecurityContext{
			RunAsNonRoot: boolPtr(true),
			RunAsUser:    &uid,
		}, &corev1.SecurityContext{
			ReadOnlyRootFilesystem:   boolPtr(true),
			AllowPrivilegeEscalation: boolPtr(false),
			Capabilities: &corev1.Capabilities{
				Drop: []corev1.Capability{"ALL"},
			},
		}
}

// gvisorSecurityContext configures security for gVisor (runsc) runtime.
func gvisorSecurityContext() (*corev1.PodSecurityContext, *corev1.SecurityContext) {
	uid := int64(1000)
	return &corev1.PodSecurityContext{
			RunAsNonRoot: boolPtr(true),
			RunAsUser:    &uid,
		}, &corev1.SecurityContext{
			ReadOnlyRootFilesystem:   boolPtr(true),
			AllowPrivilegeEscalation: boolPtr(false),
			Capabilities: &corev1.Capabilities{
				Drop: []corev1.Capability{"ALL"},
				Add:  []corev1.Capability{"NET_BIND_SERVICE"},
			},
		}
}

// firecrackerSecurityContext configures security for Firecracker micro-VMs.
// Stricter than gVisor: adds a Seccomp profile.
func firecrackerSecurityContext() (*corev1.PodSecurityContext, *corev1.SecurityContext) {
	uid := int64(1000)
	return &corev1.PodSecurityContext{
			RunAsNonRoot: boolPtr(true),
			RunAsUser:    &uid,
		}, &corev1.SecurityContext{
			ReadOnlyRootFilesystem:   boolPtr(true),
			AllowPrivilegeEscalation: boolPtr(false),
			Capabilities: &corev1.Capabilities{
				Drop: []corev1.Capability{"ALL"},
				Add:  []corev1.Capability{"NET_BIND_SERVICE"},
			},
			SeccompProfile: &corev1.SeccompProfile{
				Type: corev1.SeccompProfileTypeRuntimeDefault,
			},
		}
}
