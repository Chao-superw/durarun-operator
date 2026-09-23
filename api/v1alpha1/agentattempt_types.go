package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
	JobRef string `json:"jobRef"`
	Number int    `json:"number"`
}

// AttemptPhase represents the lifecycle phase of an AgentAttempt.
type AttemptPhase string

const (
	AttemptPhaseNew       AttemptPhase = "Pending"
	AttemptPhaseExecuting AttemptPhase = "Running"
	AttemptPhaseCompleted AttemptPhase = "Succeeded"
	AttemptPhaseFailed    AttemptPhase = "Failed"
	AttemptPhaseTimedOut  AttemptPhase = "Failed"
)

// StepStatus tracks the status of an individual step within an attempt.
type StepStatus struct {
	StepID         string       `json:"stepId"`
	Phase          string       `json:"phase"`
	StartTime      *metav1.Time `json:"startTime,omitempty"`
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`
}

// AgentAttemptStatus defines the observed state of an AgentAttempt.
type AgentAttemptStatus struct {
	Phase          AttemptPhase `json:"phase,omitempty"`
	PodName        string       `json:"podName,omitempty"`
	PodUID         types.UID    `json:"podUID,omitempty"`
	StartTime      *metav1.Time `json:"startTime,omitempty"`
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`
	ExitCode       *int32       `json:"exitCode,omitempty"`
	CheckpointRef  string       `json:"checkpointRef,omitempty"`
	StepProgress   []StepStatus `json:"stepProgress,omitempty"`
}

// AgentAttemptList contains a list of AgentAttempt.
type AgentAttemptList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AgentAttempt `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AgentAttempt{}, &AgentAttemptList{})
}
