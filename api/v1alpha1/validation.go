package v1alpha1

import "fmt"

// ValidateAgentJobSpec validates an AgentJobSpec.
func ValidateAgentJobSpec(spec *AgentJobSpec) error {
	if spec.Runtime.Image == "" {
		return fmt.Errorf("runtime.image is required")
	}
	if spec.Execution.MaxAttempts < 1 {
		return fmt.Errorf("execution.maxAttempts must be >= 1, got %d", spec.Execution.MaxAttempts)
	}
	return nil
}

// ValidateAgentAttemptSpec validates an AgentAttemptSpec.
func ValidateAgentAttemptSpec(spec *AgentAttemptSpec) error {
	if spec.Ordinal < 0 {
		return fmt.Errorf("ordinal must be >= 0, got %d", spec.Ordinal)
	}
	if spec.JobRef.UID == "" {
		return fmt.Errorf("jobRef.uid is required")
	}
	return nil
}
