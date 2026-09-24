package controller

import (
	corev1 "k8s.io/api/core/v1"
)

// PodLossReason describes why a pod was lost.
type PodLossReason string

const (
	PodLossReasonNotFound  PodLossReason = "NotFound"
	PodLossReasonNodeLost  PodLossReason = "NodeLost"
	PodLossReasonEvicted   PodLossReason = "Evicted"
	PodLossReasonPreempted PodLossReason = "Preempted"
	PodLossReasonOOMKilled PodLossReason = "OOMKilled"
	PodLossReasonUnknown   PodLossReason = "Unknown"
)

// DetectPodLoss determines if and why a pod was lost.
// Returns (lost bool, reason PodLossReason).
// A nil pod is treated as NotFound.
func DetectPodLoss(pod *corev1.Pod) (bool, PodLossReason) {
	// Pod not found (nil pointer means caller could not fetch the pod).
	if pod == nil {
		return true, PodLossReasonNotFound
	}

	// Evicted pod: status.reason set by kubelet / API server.
	if pod.Status.Reason == "Evicted" {
		return true, PodLossReasonEvicted
	}

	// Preempted pod.
	if pod.Status.Reason == "Preempted" {
		return true, PodLossReasonPreempted
	}

	// Node lost: check pod conditions for DisruptionTarget or a generic
	// "NodeLost" reason, or the Ready condition being False with reason
	// "NodeLost".
	if isNodeLost(pod) {
		return true, PodLossReasonNodeLost
	}

	// OOMKilled container.
	if hasOOMKilledContainer(pod) {
		return true, PodLossReasonOOMKilled
	}

	// Pod in Failed phase with no specific reason → Unknown loss.
	if pod.Status.Phase == corev1.PodFailed {
		return true, PodLossReasonUnknown
	}

	// Pod is not lost (Running, Pending, Succeeded, etc.).
	return false, ""
}

// isNodeLost checks pod conditions that indicate the node is lost or unready.
func isNodeLost(pod *corev1.Pod) bool {
	for i := range pod.Status.Conditions {
		c := &pod.Status.Conditions[i]
		// DisruptionTarget condition with reason "DeletionByTaintManager" or similar.
		if c.Type == "DisruptionTarget" && c.Status == corev1.ConditionTrue {
			return true
		}
		// Explicit NodeLost reason on Ready condition.
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionFalse && c.Reason == "NodeLost" {
			return true
		}
	}
	return false
}

// hasOOMKilledContainer checks if any container in the pod was OOMKilled.
func hasOOMKilledContainer(pod *corev1.Pod) bool {
	for i := range pod.Status.ContainerStatuses {
		cs := &pod.Status.ContainerStatuses[i]
		if cs.State.Terminated != nil && cs.State.Terminated.Reason == "OOMKilled" {
			return true
		}
		if cs.LastTerminationState.Terminated != nil && cs.LastTerminationState.Terminated.Reason == "OOMKilled" {
			return true
		}
	}
	return false
}
