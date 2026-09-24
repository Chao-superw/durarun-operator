package state

import (
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "durarun-operator/api/v1alpha1"
)

// SetAttemptCondition sets a condition on the attempt status.
// If the condition already exists with the same status, no update is performed.
func SetAttemptCondition(conditions *[]metav1.Condition, conditionType string, status metav1.ConditionStatus, reason, message string) {
	apimeta.SetStatusCondition(conditions, metav1.Condition{
		Type:               conditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.NewTime(time.Now()),
	})
}

// IsAttemptTerminal checks if the attempt has reached a terminal state.
// An attempt is terminal if it has AttemptConditionComplete=True or AttemptConditionFailed=True.
func IsAttemptTerminal(conditions []metav1.Condition) bool {
	for _, c := range conditions {
		if c.Status != metav1.ConditionTrue {
			continue
		}
		if c.Type == v1alpha1.AttemptConditionComplete || c.Type == v1alpha1.AttemptConditionFailed {
			return true
		}
	}
	return false
}

// IsAttemptSucceeded checks if the attempt completed successfully.
func IsAttemptSucceeded(conditions []metav1.Condition) bool {
	for _, c := range conditions {
		if c.Status == metav1.ConditionTrue && c.Type == v1alpha1.AttemptConditionComplete {
			return true
		}
	}
	return false
}
