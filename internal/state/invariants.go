package state

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "durarun-operator/api/v1alpha1"
)

// JobSnapshot captures the state of a Job and its Attempts for invariant validation.
type JobSnapshot struct {
	JobUID        types.UID
	Conditions    []metav1.Condition
	ActiveAttempt *v1alpha1.AttemptReference
	Attempts      []AttemptSnapshot
}

// AttemptSnapshot captures the state of a single Attempt for invariant validation.
type AttemptSnapshot struct {
	UID       types.UID
	Ordinal   int32
	JobRefUID types.UID
	Terminal  bool
}

// ValidateSnapshot checks the following invariants on the snapshot:
//  1. At most one non-terminal attempt (single-active invariant)
//  2. Ordinals are monotonically increasing (strictly)
//  3. All attempt JobRef UIDs match the Job UID
//
// Returns nil if all invariants hold, or an error describing the first violation found.
func ValidateSnapshot(snap *JobSnapshot) error {
	// Invariant 1: at most one non-terminal attempt
	activeCount := 0
	for _, a := range snap.Attempts {
		if !a.Terminal {
			activeCount++
		}
	}
	if activeCount > 1 {
		return fmt.Errorf("invariant violation: found %d non-terminal attempts, expected at most 1", activeCount)
	}

	// Invariant 2: ordinals are monotonically increasing
	for i := 1; i < len(snap.Attempts); i++ {
		if snap.Attempts[i].Ordinal <= snap.Attempts[i-1].Ordinal {
			return fmt.Errorf(
				"invariant violation: ordinal %d at index %d is not greater than ordinal %d at index %d",
				snap.Attempts[i].Ordinal, i, snap.Attempts[i-1].Ordinal, i-1,
			)
		}
	}

	// Invariant 3: all attempt JobRef UIDs match the Job UID
	for i, a := range snap.Attempts {
		if a.JobRefUID != snap.JobUID {
			return fmt.Errorf(
				"invariant violation: attempt at index %d has JobRefUID %q, expected %q",
				i, a.JobRefUID, snap.JobUID,
			)
		}
	}

	return nil
}
