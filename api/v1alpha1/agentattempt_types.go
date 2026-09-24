package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Condition type constants for AgentAttempt.
const (
	AttemptConditionPodCreated = "PodCreated"
	AttemptConditionRunning    = "Running"
	AttemptConditionComplete   = "Complete"
	AttemptConditionFailed     = "Failed"
)

// AgentAttempt is the Schema for the agentattempts API.
type AgentAttempt struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AgentAttemptSpec   `json:"spec"`
	Status AgentAttemptStatus `json:"status,omitempty"`
}

// AgentAttemptSpec defines the desired state of an AgentAttempt.
type AgentAttemptSpec struct {
	JobRef   ObjectRef    `json:"jobRef"`
	Ordinal  int32        `json:"ordinal"`
	Deadline *metav1.Time `json:"deadline,omitempty"`
}

// AgentAttemptStatus defines the observed state of an AgentAttempt.
type AgentAttemptStatus struct {
	Conditions     []metav1.Condition `json:"conditions,omitempty"`
	PodName        string             `json:"podName,omitempty"`
	PodUID         types.UID          `json:"podUID,omitempty"`
	StartTime      *metav1.Time       `json:"startTime,omitempty"`
	CompletionTime *metav1.Time       `json:"completionTime,omitempty"`
	ExitCode       *int32             `json:"exitCode,omitempty"`
	Signal         *int32             `json:"signal,omitempty"`
	ErrorCategory  ErrorCategory      `json:"errorCategory,omitempty"`
	ArtifactRef    *ArtifactRef       `json:"artifactRef,omitempty"`
}

// AgentAttemptList contains a list of AgentAttempt.
type AgentAttemptList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AgentAttempt `json:"items"`
}

// AttemptPhaseFromConditions derives a human-readable phase string from conditions.
func AttemptPhaseFromConditions(conditions []metav1.Condition) string {
	for _, c := range conditions {
		if c.Status != metav1.ConditionTrue {
			continue
		}
		switch c.Type {
		case AttemptConditionComplete:
			return "Succeeded"
		case AttemptConditionFailed:
			return "Failed"
		case AttemptConditionRunning:
			return "Running"
		case AttemptConditionPodCreated:
			return "Pending"
		}
	}
	return "Pending"
}

func init() {
	SchemeBuilder.Register(&AgentAttempt{}, &AgentAttemptList{})
}
