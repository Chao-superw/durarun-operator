package model

import (
	"encoding/json"
	"fmt"
	"time"
)

type JobState string

const (
	JobPending    JobState = "Pending"
	JobScheduled  JobState = "Scheduled"
	JobRunning    JobState = "Running"
	JobCompleted  JobState = "Completed"
	JobFailed     JobState = "Failed"
	JobTerminated JobState = "Terminated"
	JobClaiming   JobState = "Claiming"
	JobActivating JobState = "Activating"
	JobRecovering JobState = "Recovering"
)

var validJobTransitions = map[JobState][]JobState{
	JobPending:    {JobScheduled, JobClaiming, JobTerminated},
	JobScheduled:  {JobRunning, JobFailed, JobTerminated},
	JobRunning:    {JobCompleted, JobFailed, JobScheduled, JobRecovering, JobTerminated},
	JobCompleted:  {},
	JobFailed:     {},
	JobClaiming:   {JobActivating, JobScheduled, JobTerminated},
	JobActivating: {JobRunning, JobFailed, JobTerminated},
	JobRecovering: {JobRunning, JobFailed, JobTerminated},
}

func (s JobState) CanTransitionTo(target JobState) bool {
	for _, t := range validJobTransitions[s] {
		if t == target {
			return true
		}
	}
	return false
}

type AttemptState string

const (
	AttemptNew       AttemptState = "New"
	AttemptExecuting AttemptState = "Executing"
	AttemptCompleted AttemptState = "Completed"
	AttemptFailed    AttemptState = "Failed"
	AttemptTimedOut  AttemptState = "TimedOut"
)

var validAttemptTransitions = map[AttemptState][]AttemptState{
	AttemptNew:       {AttemptExecuting},
	AttemptExecuting: {AttemptCompleted, AttemptFailed, AttemptTimedOut},
}

func (s AttemptState) CanTransitionTo(target AttemptState) bool {
	for _, t := range validAttemptTransitions[s] {
		if t == target {
			return true
		}
	}
	return false
}

type ResourceSpec struct {
	CPULimit    string `json:"cpuLimit" yaml:"cpuLimit"`
	MemoryLimit string `json:"memoryLimit" yaml:"memoryLimit"`
}

type JobSpec struct {
	Name         string            `json:"name" yaml:"name"`
	Image        string            `json:"image" yaml:"image"`
	Prompt       string            `json:"prompt" yaml:"prompt"`
	Timeout      string            `json:"timeout" yaml:"timeout"`
	Resources    ResourceSpec      `json:"resources" yaml:"resources"`
	Artifacts    []string          `json:"artifacts" yaml:"artifacts"`
	Env          map[string]string `json:"env,omitempty" yaml:"env,omitempty"`
	MaxRetries   int               `json:"maxRetries" yaml:"maxRetries"`
	Isolation    IsolationSpec     `json:"isolation,omitempty" yaml:"isolation,omitempty"`
	StepTracking StepTrackingSpec  `json:"stepTracking,omitempty" yaml:"stepTracking,omitempty"`
	PoolRef      string            `json:"poolRef,omitempty" yaml:"poolRef,omitempty"`
}

func (s *JobSpec) TimeoutDuration() time.Duration {
	d, err := time.ParseDuration(s.Timeout)
	if err != nil {
		return 5 * time.Minute
	}
	return d
}

type Job struct {
	ID        string   `json:"id"`
	Spec      JobSpec  `json:"spec"`
	State     JobState `json:"state"`
	CreatedAt string   `json:"createdAt"`
	UpdatedAt string   `json:"updatedAt"`
}

type Attempt struct {
	ID            string       `json:"id"`
	JobID         string       `json:"jobId"`
	Number        int          `json:"number"`
	WorkerID      string       `json:"workerId"`
	State         AttemptState `json:"state"`
	StartedAt     string       `json:"startedAt"`
	FinishedAt    *string      `json:"finishedAt,omitempty"`
	LastHeartbeat string       `json:"lastHeartbeat"`
	CheckpointID  *string      `json:"checkpointId,omitempty"`
}

type Checkpoint struct {
	ID        string          `json:"id"`
	AttemptID string          `json:"attemptId"`
	JobID     string          `json:"jobId"`
	StepIndex int             `json:"stepIndex"`
	Data      json.RawMessage `json:"data"`
	CreatedAt string          `json:"createdAt"`
}

type StateTransition struct {
	ID         string `json:"id"`
	EntityType string `json:"entityType"`
	EntityID   string `json:"entityId"`
	FromState  string `json:"fromState"`
	ToState    string `json:"toState"`
	Trigger    string `json:"trigger"`
	CreatedAt  string `json:"createdAt"`
}

type Artifact struct {
	JobID string `json:"jobId"`
	Name  string `json:"name"`
}

type SubmitResponse struct {
	JobID string `json:"jobId"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

func ValidateJobSpec(spec *JobSpec) error {
	if spec.Name == "" {
		return fmt.Errorf("name is required")
	}
	if spec.Image == "" {
		return fmt.Errorf("image is required")
	}
	if spec.Prompt == "" {
		return fmt.Errorf("prompt is required")
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

type IsolationLevel string

const (
	L0Process     IsolationLevel = "L0Process"
	L1GVisor      IsolationLevel = "L1GVisor"
	L2Firecracker IsolationLevel = "L2Firecracker"
	L3Docker      IsolationLevel = "L3Docker"
)

type StepTrackingProtocol string

const (
	ProtocolEnv     StepTrackingProtocol = "env"
	ProtocolHook    StepTrackingProtocol = "hook"
	ProtocolAdapter StepTrackingProtocol = "adapter"
)

type IsolationSpec struct {
	Level IsolationLevel `json:"level" yaml:"level"`
}

type StepTrackingSpec struct {
	Enabled  bool                 `json:"enabled" yaml:"enabled"`
	Protocol StepTrackingProtocol `json:"protocol" yaml:"protocol"`
}

type PoolClaimStatus struct {
	PoolName       string `json:"poolName"`
	ClaimLatencyMs int64  `json:"claimLatencyMs"`
	WarmHit        bool   `json:"warmHit"`
}

type StepProgressStatus struct {
	LastCompletedStep string `json:"lastCompletedStep"`
	CompletedStepCount int   `json:"completedStepCount"`
	TotalStepCount     int   `json:"totalStepCount"`
	RecoveredFromStep  string `json:"recoveredFromStep,omitempty"`
}

type PoolPodTemplate struct {
	Image            string       `json:"image" yaml:"image"`
	Resources        ResourceSpec `json:"resources" yaml:"resources"`
	RuntimeClassName string       `json:"runtimeClassName" yaml:"runtimeClassName"`
}

type SandboxPoolSpec struct {
	MinSize            int             `json:"minSize" yaml:"minSize"`
	MaxSize            int             `json:"maxSize" yaml:"maxSize"`
	ScaleUpThreshold   float64         `json:"scaleUpThreshold" yaml:"scaleUpThreshold"`
	ScaleDownThreshold float64         `json:"scaleDownThreshold" yaml:"scaleDownThreshold"`
	ScaleUpStep        int             `json:"scaleUpStep" yaml:"scaleUpStep"`
	CooldownSeconds    int             `json:"cooldownSeconds" yaml:"cooldownSeconds"`
	Template           PoolPodTemplate `json:"template" yaml:"template"`
}

type SandboxPoolStatus struct {
	TotalPods     int    `json:"totalPods"`
	IdlePods      int    `json:"idlePods"`
	ClaimedPods   int    `json:"claimedPods"`
	LastScaleTime string `json:"lastScaleTime,omitempty"`
	Ready         bool   `json:"ready"`
}

type SandboxPool struct {
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	Spec      SandboxPoolSpec   `json:"spec"`
	Status    SandboxPoolStatus `json:"status"`
	CreatedAt string            `json:"createdAt"`
	UpdatedAt string            `json:"updatedAt"`
}

type SandboxPolicySpec struct {
	AllowedLevels     []IsolationLevel `json:"allowedLevels" yaml:"allowedLevels"`
	DefaultLevel      IsolationLevel   `json:"defaultLevel" yaml:"defaultLevel"`
	ForceLevel        *IsolationLevel  `json:"forceLevel,omitempty" yaml:"forceLevel,omitempty"`
	MaxConcurrentJobs int              `json:"maxConcurrentJobs" yaml:"maxConcurrentJobs"`
	MaxPoolSize       int              `json:"maxPoolSize" yaml:"maxPoolSize"`
}

type SandboxPolicy struct {
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	Namespace string            `json:"namespace"`
	Spec      SandboxPolicySpec `json:"spec"`
	CreatedAt string            `json:"createdAt"`
}

type PoolPodState string

const (
	PodIdle        PoolPodState = "Idle"
	PodClaimed     PoolPodState = "Claimed"
	PodTerminating PoolPodState = "Terminating"
)

type PoolPod struct {
	ID             string       `json:"id"`
	PoolID         string       `json:"poolId"`
	State          PoolPodState `json:"state"`
	ClaimedByJobID string       `json:"claimedByJobId,omitempty"`
	RuntimeClass   string       `json:"runtimeClass"`
	CreatedAt      string       `json:"createdAt"`
	ClaimedAt      string       `json:"claimedAt,omitempty"`
}

type ErrorCategory string

const (
	ErrPoolExhausted        ErrorCategory = "PoolExhausted"
	ErrPoolClaimConflict    ErrorCategory = "PoolClaimConflict"
	ErrWALCorrupt           ErrorCategory = "WALCorrupt"
	ErrIsolationUnavailable ErrorCategory = "IsolationUnavailable"
	ErrActivationFailed     ErrorCategory = "ActivationFailed"
)

type WALRecord struct {
	Seq       int64  `json:"seq"`
	Type      string `json:"type"`
	StepID    string `json:"step_id"`
	Timestamp int64  `json:"ts"`
	Exit      *int   `json:"exit,omitempty"`
	OutputRef string `json:"output_ref,omitempty"`
}

type CompletedStep struct {
	StepID    string `json:"stepId"`
	Seq       int64  `json:"seq"`
	OutputRef string `json:"outputRef"`
}

type WALManifest struct {
	SchemaVersion    int             `json:"schemaVersion"`
	LastCompletedStep string         `json:"lastCompletedStep"`
	CompletedSteps   []CompletedStep `json:"completedSteps"`
	WALFileCount     int             `json:"walFileCount"`
	WALTotalRecords  int64           `json:"walTotalRecords"`
}

type ArchiveRef struct {
	URI       string `json:"uri"`
	SHA256    string `json:"sha256"`
	SizeBytes int64  `json:"sizeBytes"`
}

type CheckpointManifest struct {
	SchemaVersion int          `json:"schemaVersion"`
	JobUID        string       `json:"jobUID"`
	AttemptUID    string       `json:"attemptUID"`
	CheckpointSeq int          `json:"checkpointSeq"`
	Archive       ArchiveRef   `json:"archive"`
	StepProgress  *WALManifest `json:"stepProgress,omitempty"`
	CreatedAt     string       `json:"createdAt"`
	RunnerVersion string       `json:"runnerVersion"`
}

func ValidateIsolationLevel(level IsolationLevel) error {
	switch level {
	case L0Process, L1GVisor, L2Firecracker, L3Docker:
		return nil
	default:
		return fmt.Errorf("invalid isolation level: %s", level)
	}
}

func DefaultJobSpec() JobSpec {
	return JobSpec{
		Timeout:    "5m",
		MaxRetries: 0,
		Resources: ResourceSpec{
			CPULimit:    "1",
			MemoryLimit: "512m",
		},
		Isolation: IsolationSpec{
			Level: L1GVisor,
		},
		StepTracking: StepTrackingSpec{
			Enabled:  true,
			Protocol: ProtocolEnv,
		},
		PoolRef: "default",
	}
}
