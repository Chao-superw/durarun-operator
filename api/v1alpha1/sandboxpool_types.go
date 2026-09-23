package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SandboxPool is the Schema for the sandboxpools API.
type SandboxPool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SandboxPoolSpec   `json:"spec"`
	Status SandboxPoolStatus `json:"status,omitempty"`
}

// SandboxPoolSpec defines the desired state of a SandboxPool.
type SandboxPoolSpec struct {
	MinSize            int             `json:"minSize"`
	MaxSize            int             `json:"maxSize"`
	ScaleUpThreshold   float64         `json:"scaleUpThreshold"`
	ScaleDownThreshold float64         `json:"scaleDownThreshold"`
	ScaleUpStep        int             `json:"scaleUpStep"`
	CooldownSeconds    int             `json:"cooldownSeconds"`
	Template           PoolPodTemplate `json:"template"`
	RuntimeClassName   string          `json:"runtimeClassName,omitempty"`
}

// PoolPodTemplate defines the template for pods in the pool.
type PoolPodTemplate struct {
	Image     string            `json:"image"`
	Resources ResourceSpec      `json:"resources"`
	Command   []string          `json:"command,omitempty"`
	Env       map[string]string `json:"env,omitempty"`
}

// SandboxPoolStatus defines the observed state of a SandboxPool.
type SandboxPoolStatus struct {
	TotalPods     int          `json:"totalPods"`
	IdlePods      int          `json:"idlePods"`
	ClaimedPods   int          `json:"claimedPods"`
	Ready         bool         `json:"ready"`
	LastScaleTime *metav1.Time `json:"lastScaleTime,omitempty"`
}

// SandboxPoolList contains a list of SandboxPool.
type SandboxPoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SandboxPool `json:"items"`
}

func init() {
	SchemeBuilder.Register(&SandboxPool{}, &SandboxPoolList{})
}
