package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Condition type constants for AgentJob.
const (
	JobConditionScheduled = "Scheduled"
	JobConditionRunning   = "Running"
	JobConditionComplete  = "Complete"
	JobConditionFailed    = "Failed"
)

// AgentJob is the Schema for the agentjobs API.
type AgentJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AgentJobSpec   `json:"spec"`
	Status AgentJobStatus `json:"status,omitempty"`
}

// AgentJobSpec defines the desired state of an AgentJob.
type AgentJobSpec struct {
	Runtime    RuntimeSpec      `json:"runtime"`
	Execution  ExecutionSpec    `json:"execution"`
	Checkpoint CheckpointPolicy `json:"checkpoint,omitempty"`
}

// RuntimeSpec defines the container runtime configuration.
type RuntimeSpec struct {
	Image     string                      `json:"image"`
	Command   []string                    `json:"command,omitempty"`
	Args      []string                    `json:"args,omitempty"`
	Env       []corev1.EnvVar             `json:"env,omitempty"`
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

// ExecutionSpec defines execution parameters for the job.
type ExecutionSpec struct {
	Timeout                 *metav1.Duration `json:"timeout,omitempty"`
	MaxAttempts             int32            `json:"maxAttempts,omitempty"`
	BackoffLimit            *metav1.Duration `json:"backoffLimit,omitempty"`
	ActiveDeadlineSeconds   *int64           `json:"activeDeadlineSeconds,omitempty"`
	TTLSecondsAfterFinished *int32           `json:"ttlSecondsAfterFinished,omitempty"`
}

// CheckpointPolicy configures checkpoint persistence.
type CheckpointPolicy struct {
	Enabled         bool   `json:"enabled,omitempty"`
	IntervalSeconds int32  `json:"intervalSeconds,omitempty"`
	StorageRef      string `json:"storageRef,omitempty"`
}

// AttemptReference points to a specific AgentAttempt.
type AttemptReference struct {
	Name    string    `json:"name"`
	Ordinal int32     `json:"ordinal"`
	UID     types.UID `json:"uid,omitempty"`
}

// AgentJobStatus defines the observed state of an AgentJob.
type AgentJobStatus struct {
	Conditions        []metav1.Condition `json:"conditions,omitempty"`
	ActiveAttempt     *AttemptReference  `json:"activeAttempt,omitempty"`
	CompletedAttempts int32              `json:"completedAttempts"`
	FailedAttempts    int32              `json:"failedAttempts"`
	StartTime         *metav1.Time       `json:"startTime,omitempty"`
	CompletionTime    *metav1.Time       `json:"completionTime,omitempty"`
	SpecHash          string             `json:"specHash,omitempty"`
}

// AgentJobList contains a list of AgentJob.
type AgentJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AgentJob `json:"items"`
}

// JobPhaseFromConditions derives a human-readable phase string from conditions.
// Terminal conditions (Complete, Failed) take priority over non-terminal ones.
func JobPhaseFromConditions(conditions []metav1.Condition) string {
	for _, c := range conditions {
		if c.Status != metav1.ConditionTrue {
			continue
		}
		switch c.Type {
		case JobConditionComplete:
			return "Succeeded"
		case JobConditionFailed:
			return "Failed"
		}
	}
	for _, c := range conditions {
		if c.Status != metav1.ConditionTrue {
			continue
		}
		switch c.Type {
		case JobConditionRunning:
			return "Running"
		case JobConditionScheduled:
			return "Scheduling"
		}
	}
	return "Pending"
}

func init() {
	SchemeBuilder.Register(&AgentJob{}, &AgentJobList{})
}
