package state

import (
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "durarun-operator/api/v1alpha1"
)

// SetJobCondition sets a condition on the job status.
// If the condition already exists with the same status, no update is performed.
func SetJobCondition(conditions *[]metav1.Condition, conditionType string, status metav1.ConditionStatus, reason, message string) {
	apimeta.SetStatusCondition(conditions, metav1.Condition{
		Type:               conditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.NewTime(time.Now()),
	})
}

// TrySetJobTerminal attempts to set a terminal condition (Complete or Failed).
// Returns true if the terminal was set, false if already terminal (immutable).
// Key invariant: once terminal, no other terminal condition can be set.
func TrySetJobTerminal(conditions *[]metav1.Condition, conditionType string, status metav1.ConditionStatus, reason, message string) bool {
	if IsJobTerminal(*conditions) {
		return false
	}
	SetJobCondition(conditions, conditionType, status, reason, message)
	return true
}

// IsJobTerminal checks if the job has reached a terminal state.
// A job is terminal if it has JobConditionComplete=True or JobConditionFailed=True.
func IsJobTerminal(conditions []metav1.Condition) bool {
	for _, c := range conditions {
		if c.Status != metav1.ConditionTrue {
			continue
		}
		if c.Type == v1alpha1.JobConditionComplete || c.Type == v1alpha1.JobConditionFailed {
			return true
		}
	}
	return false
}
