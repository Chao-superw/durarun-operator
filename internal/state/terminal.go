package state

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TryTerminalCAS attempts an atomic terminal transition using Compare-And-Swap semantics.
// If the conditions slice is not yet terminal, it applies the new terminal condition
// and returns (newConditions, true).
// If already terminal, it returns (oldConditions, false) without modification.
func TryTerminalCAS(conditions []metav1.Condition, conditionType string, status metav1.ConditionStatus, reason, message string) ([]metav1.Condition, bool) {
	// Copy the slice to avoid mutating the caller's original
	copied := make([]metav1.Condition, len(conditions))
	copy(copied, conditions)

	ok := TrySetJobTerminal(&copied, conditionType, status, reason, message)
	if !ok {
		return conditions, false
	}
	return copied, true
}
