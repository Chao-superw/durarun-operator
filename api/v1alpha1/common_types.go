package v1alpha1

import (
	"k8s.io/apimachinery/pkg/types"
)

// ObjectRef is a reference to a Kubernetes object by name, namespace and UID.
type ObjectRef struct {
	Name      string    `json:"name"`
	Namespace string    `json:"namespace,omitempty"`
	UID       types.UID `json:"uid"`
}

// ArtifactRef points to an artifact stored in object storage.
type ArtifactRef struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

// ErrorCategory classifies the root cause of an attempt failure.
type ErrorCategory string

const (
	ErrorCategoryInfra   ErrorCategory = "Infra"
	ErrorCategoryUser    ErrorCategory = "User"
	ErrorCategoryTimeout ErrorCategory = "Timeout"
	ErrorCategoryOOM     ErrorCategory = "OOM"
	ErrorCategoryUnknown ErrorCategory = "Unknown"
)

// ResourceSpec defines compute resource constraints.
// Retained for backward compatibility with SandboxPool.
type ResourceSpec struct {
	CPULimit    string `json:"cpuLimit,omitempty"`
	MemoryLimit string `json:"memoryLimit,omitempty"`
}

// IsolationLevel describes the sandbox isolation tier.
// Retained for backward compatibility with SandboxPool and security contexts.
type IsolationLevel string

const (
	L0Process     IsolationLevel = "L0Process"
	L1GVisor      IsolationLevel = "L1GVisor"
	L2Firecracker IsolationLevel = "L2Firecracker"
	L3Docker      IsolationLevel = "L3Docker"
)

// IsolationSpec configures sandbox isolation for a workload.
// Retained for backward compatibility with SandboxPool.
type IsolationSpec struct {
	Level IsolationLevel `json:"level,omitempty"`
}
