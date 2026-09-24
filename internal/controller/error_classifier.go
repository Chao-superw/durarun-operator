package controller

import (
	v1alpha1 "durarun-operator/api/v1alpha1"
	"durarun-operator/internal/protocol"

	corev1 "k8s.io/api/core/v1"
)

// ClassifyError determines the ErrorCategory from a ProcessResult and pod status.
//
// Classification rules (evaluated in priority order):
//   - Exit code 0                → no error (empty category)
//   - OOMKilled (from ProcessResult or pod container status) → ErrorCategoryOOM
//   - Timeout (ProcessResult.Error contains "timeout")       → ErrorCategoryTimeout
//   - Exit code 137 (SIGKILL)   → ErrorCategoryInfra (likely OOM or preemption)
//   - Exit code 1-126           → ErrorCategoryUser  (application error)
//   - Exit code 127             → ErrorCategoryUser  (command not found)
//   - Exit code 128+            → ErrorCategoryInfra (signal-based termination)
//   - Otherwise                 → ErrorCategoryUnknown
func ClassifyError(result *protocol.ProcessResult, podStatus *corev1.PodStatus) v1alpha1.ErrorCategory {
	if result == nil {
		return v1alpha1.ErrorCategoryUnknown
	}

	// Success: no error
	if result.ExitCode == 0 && !result.OOMKilled {
		return ""
	}

	// OOMKilled flag from the runner
	if result.OOMKilled {
		return v1alpha1.ErrorCategoryOOM
	}

	// OOMKilled detected from pod container statuses
	if podStatus != nil && isOOMKilledFromPod(podStatus) {
		return v1alpha1.ErrorCategoryOOM
	}

	// Timeout detection
	if result.Error == "timeout" {
		return v1alpha1.ErrorCategoryTimeout
	}

	// Exit code based classification
	code := result.ExitCode

	// SIGKILL (137 = 128 + 9) → likely OOM or preemption
	if code == 137 {
		return v1alpha1.ErrorCategoryInfra
	}

	// Application errors (1-127)
	if code >= 1 && code <= 127 {
		return v1alpha1.ErrorCategoryUser
	}

	// Signal-based termination (128+)
	if code >= 128 {
		return v1alpha1.ErrorCategoryInfra
	}

	return v1alpha1.ErrorCategoryUnknown
}

// isOOMKilledFromPod checks if any container in the pod was OOMKilled.
func isOOMKilledFromPod(podStatus *corev1.PodStatus) bool {
	for i := range podStatus.ContainerStatuses {
		terminated := podStatus.ContainerStatuses[i].State.Terminated
		if terminated != nil && terminated.Reason == "OOMKilled" {
			return true
		}
		lastTerminated := podStatus.ContainerStatuses[i].LastTerminationState.Terminated
		if lastTerminated != nil && lastTerminated.Reason == "OOMKilled" {
			return true
		}
	}
	return false
}
