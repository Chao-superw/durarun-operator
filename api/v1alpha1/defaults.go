package v1alpha1

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	DefaultMaxAttempts            = int32(3)
	DefaultTimeout                = 30 * time.Minute
	DefaultCheckpointIntervalSecs = int32(60)
)

// DefaultAgentJobSpec applies default values to an AgentJobSpec.
func DefaultAgentJobSpec(spec *AgentJobSpec) {
	if spec.Execution.MaxAttempts == 0 {
		spec.Execution.MaxAttempts = DefaultMaxAttempts
	}
	if spec.Execution.Timeout == nil {
		d := metav1.Duration{Duration: DefaultTimeout}
		spec.Execution.Timeout = &d
	}
	if spec.Checkpoint.Enabled && spec.Checkpoint.IntervalSeconds == 0 {
		spec.Checkpoint.IntervalSeconds = DefaultCheckpointIntervalSecs
	}
}
