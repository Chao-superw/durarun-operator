package v1alpha1

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
	Image        string            `json:"image"`
	Command      []string          `json:"command,omitempty"`
	Prompt       string            `json:"prompt,omitempty"`
	Timeout      string            `json:"timeout,omitempty"`
	Resources    ResourceSpec      `json:"resources,omitempty"`
	Env          map[string]string `json:"env,omitempty"`
	MaxRetries   int               `json:"maxRetries,omitempty"`
	Isolation    IsolationSpec     `json:"isolation,omitempty"`
	PoolRef      string            `json:"poolRef,omitempty"`
	StepTracking StepTrackingSpec  `json:"stepTracking,omitempty"`
	Checkpoint   CheckpointSpec    `json:"checkpoint,omitempty"`
}

// ResourceSpec defines compute resource constraints.
type ResourceSpec struct {
	CPULimit    string `json:"cpuLimit,omitempty"`
	MemoryLimit string `json:"memoryLimit,omitempty"`
}

// IsolationLevel describes the sandbox isolation tier.
type IsolationLevel string

const (
	L0Process     IsolationLevel = "L0Process"
	L1GVisor      IsolationLevel = "L1GVisor"
	L2Firecracker IsolationLevel = "L2Firecracker"
	L3Docker      IsolationLevel = "L3Docker"
)

// IsolationSpec configures sandbox isolation for the job.
type IsolationSpec struct {
	Level IsolationLevel `json:"level,omitempty"`
}

// StepTrackingProtocol defines how step progress is reported.
type StepTrackingProtocol string

const (
	ProtocolEnv     StepTrackingProtocol = "env"
	ProtocolHook    StepTrackingProtocol = "hook"
	ProtocolAdapter StepTrackingProtocol = "adapter"
)

// StepTrackingSpec configures step-level progress tracking.
type StepTrackingSpec struct {
	Enabled  bool                 `json:"enabled,omitempty"`
	Protocol StepTrackingProtocol `json:"protocol,omitempty"`
}

// CheckpointSpec configures checkpoint persistence.
type CheckpointSpec struct {
	Enabled         bool   `json:"enabled,omitempty"`
	Bucket          string `json:"bucket,omitempty"`
	Endpoint        string `json:"endpoint,omitempty"`
	IntervalSeconds int    `json:"intervalSeconds,omitempty"`
}

// JobPhase represents the overall lifecycle phase of an AgentJob.
type JobPhase string

const (
	JobPhasePending    JobPhase = "Pending"
	JobPhaseScheduling JobPhase = "Scheduling"
	JobPhaseRunning    JobPhase = "Running"
	JobPhaseRecovering JobPhase = "Recovering"
	JobPhaseSucceeded  JobPhase = "Succeeded"
	JobPhaseFailed     JobPhase = "Failed"
	JobPhaseTerminated JobPhase = "Terminated"
)

// AttemptReference points to a specific AgentAttempt.
type AttemptReference struct {
	Name   string    `json:"name"`
	Number int       `json:"number"`
	UID    types.UID `json:"uid,omitempty"`
}

// AgentJobStatus defines the observed state of an AgentJob.
type AgentJobStatus struct {
	Phase             JobPhase           `json:"phase,omitempty"`
	ActiveAttempt     *AttemptReference  `json:"activeAttempt,omitempty"`
	CompletedAttempts int                `json:"completedAttempts"`
	FailedAttempts    int                `json:"failedAttempts"`
	Conditions        []metav1.Condition `json:"conditions,omitempty"`
	StartTime         *metav1.Time       `json:"startTime,omitempty"`
	CompletionTime    *metav1.Time       `json:"completionTime,omitempty"`
}

// AgentJobList contains a list of AgentJob.
type AgentJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AgentJob `json:"items"`
}

// ValidateJobSpec validates an AgentJobSpec and applies defaults where needed.
func ValidateJobSpec(spec *AgentJobSpec) error {
	if spec.Image == "" {
		return fmt.Errorf("image is required")
	}
	if spec.MaxRetries < 0 {
		spec.MaxRetries = 0
	}
	if spec.Timeout == "" {
		spec.Timeout = "5m"
	}
	if spec.Resources.CPULimit == "" {
		spec.Resources.CPULimit = "1"
	}
	if spec.Resources.MemoryLimit == "" {
		spec.Resources.MemoryLimit = "512m"
	}
	if spec.Isolation.Level == "" {
		spec.Isolation.Level = L1GVisor
	}
	if spec.StepTracking.Protocol == "" {
		spec.StepTracking.Enabled = true
		spec.StepTracking.Protocol = ProtocolEnv
	}
	if spec.PoolRef == "" {
		spec.PoolRef = "default"
	}
	return nil
}

func init() {
	SchemeBuilder.Register(&AgentJob{}, &AgentJobList{})
}
