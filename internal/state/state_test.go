package state

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "durarun-operator/api/v1alpha1"
)

// ---------------------------------------------------------------------------
// TrySetJobTerminal tests
// ---------------------------------------------------------------------------

func TestTrySetJobTerminal_CompleteBlocksFailed(t *testing.T) {
	var conditions []metav1.Condition

	ok := TrySetJobTerminal(&conditions, v1alpha1.JobConditionComplete, metav1.ConditionTrue, "Done", "job completed")
	assert.True(t, ok, "first terminal (Complete) should succeed")
	assert.True(t, IsJobTerminal(conditions))

	ok = TrySetJobTerminal(&conditions, v1alpha1.JobConditionFailed, metav1.ConditionTrue, "Err", "job failed")
	assert.False(t, ok, "second terminal (Failed) should be rejected")

	// Verify the Complete condition is still there and Failed was not added as True
	for _, c := range conditions {
		if c.Type == v1alpha1.JobConditionFailed && c.Status == metav1.ConditionTrue {
			t.Fatal("Failed condition should not have been set after Complete")
		}
	}
}

func TestTrySetJobTerminal_FailedBlocksComplete(t *testing.T) {
	var conditions []metav1.Condition

	ok := TrySetJobTerminal(&conditions, v1alpha1.JobConditionFailed, metav1.ConditionTrue, "Err", "job failed")
	assert.True(t, ok, "first terminal (Failed) should succeed")
	assert.True(t, IsJobTerminal(conditions))

	ok = TrySetJobTerminal(&conditions, v1alpha1.JobConditionComplete, metav1.ConditionTrue, "Done", "job completed")
	assert.False(t, ok, "second terminal (Complete) should be rejected")

	for _, c := range conditions {
		if c.Type == v1alpha1.JobConditionComplete && c.Status == metav1.ConditionTrue {
			t.Fatal("Complete condition should not have been set after Failed")
		}
	}
}

// ---------------------------------------------------------------------------
// SetJobCondition idempotency test
// ---------------------------------------------------------------------------

func TestSetJobCondition_Idempotent(t *testing.T) {
	var conditions []metav1.Condition

	SetJobCondition(&conditions, v1alpha1.JobConditionRunning, metav1.ConditionTrue, "Running", "pod is running")
	first := make([]metav1.Condition, len(conditions))
	copy(first, conditions)

	SetJobCondition(&conditions, v1alpha1.JobConditionRunning, metav1.ConditionTrue, "Running", "pod is running")

	assert.Equal(t, len(first), len(conditions), "condition count should not change on idempotent set")
	// The condition should still exist exactly once
	count := 0
	for _, c := range conditions {
		if c.Type == v1alpha1.JobConditionRunning {
			count++
		}
	}
	assert.Equal(t, 1, count, "Running condition should appear exactly once")
}

// ---------------------------------------------------------------------------
// SetAttemptCondition / IsAttemptTerminal tests
// ---------------------------------------------------------------------------

func TestSetAttemptCondition_And_IsAttemptTerminal(t *testing.T) {
	var conditions []metav1.Condition

	SetAttemptCondition(&conditions, v1alpha1.AttemptConditionRunning, metav1.ConditionTrue, "Running", "running")
	assert.False(t, IsAttemptTerminal(conditions), "Running is not terminal")

	SetAttemptCondition(&conditions, v1alpha1.AttemptConditionComplete, metav1.ConditionTrue, "Done", "done")
	assert.True(t, IsAttemptTerminal(conditions), "Complete is terminal")
}

func TestIsAttemptTerminal_Failed(t *testing.T) {
	var conditions []metav1.Condition
	SetAttemptCondition(&conditions, v1alpha1.AttemptConditionFailed, metav1.ConditionTrue, "Err", "error")
	assert.True(t, IsAttemptTerminal(conditions), "Failed is terminal")
}

// ---------------------------------------------------------------------------
// TryTerminalCAS tests
// ---------------------------------------------------------------------------

func TestTryTerminalCAS_Success(t *testing.T) {
	conditions := []metav1.Condition{}

	newConds, ok := TryTerminalCAS(conditions, v1alpha1.JobConditionComplete, metav1.ConditionTrue, "Done", "ok")
	assert.True(t, ok)
	assert.True(t, IsJobTerminal(newConds))
	// Original should not be modified
	assert.False(t, IsJobTerminal(conditions))
}

func TestTryTerminalCAS_AlreadyTerminal(t *testing.T) {
	var conditions []metav1.Condition
	SetJobCondition(&conditions, v1alpha1.JobConditionFailed, metav1.ConditionTrue, "Err", "failed")

	returned, ok := TryTerminalCAS(conditions, v1alpha1.JobConditionComplete, metav1.ConditionTrue, "Done", "ok")
	assert.False(t, ok)
	assert.Equal(t, conditions, returned, "should return original conditions on failure")
}

// ---------------------------------------------------------------------------
// ValidateSnapshot tests
// ---------------------------------------------------------------------------

func TestValidateSnapshot_Valid(t *testing.T) {
	snap := &JobSnapshot{
		JobUID: "job-uid-1",
		Attempts: []AttemptSnapshot{
			{UID: "a1", Ordinal: 0, JobRefUID: "job-uid-1", Terminal: true},
			{UID: "a2", Ordinal: 1, JobRefUID: "job-uid-1", Terminal: true},
			{UID: "a3", Ordinal: 2, JobRefUID: "job-uid-1", Terminal: false},
		},
	}
	require.NoError(t, ValidateSnapshot(snap))
}

func TestValidateSnapshot_TwoActiveAttempts(t *testing.T) {
	snap := &JobSnapshot{
		JobUID: "job-uid-1",
		Attempts: []AttemptSnapshot{
			{UID: "a1", Ordinal: 0, JobRefUID: "job-uid-1", Terminal: false},
			{UID: "a2", Ordinal: 1, JobRefUID: "job-uid-1", Terminal: false},
		},
	}
	err := ValidateSnapshot(snap)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "non-terminal attempts")
}

func TestValidateSnapshot_NonMonotonicOrdinals(t *testing.T) {
	snap := &JobSnapshot{
		JobUID: "job-uid-1",
		Attempts: []AttemptSnapshot{
			{UID: "a1", Ordinal: 0, JobRefUID: "job-uid-1", Terminal: true},
			{UID: "a2", Ordinal: 0, JobRefUID: "job-uid-1", Terminal: true},
		},
	}
	err := ValidateSnapshot(snap)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not greater than")
}

func TestValidateSnapshot_MismatchedUID(t *testing.T) {
	snap := &JobSnapshot{
		JobUID: "job-uid-1",
		Attempts: []AttemptSnapshot{
			{UID: "a1", Ordinal: 0, JobRefUID: "wrong-uid", Terminal: true},
		},
	}
	err := ValidateSnapshot(snap)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "JobRefUID")
}

func TestValidateSnapshot_EmptyIsValid(t *testing.T) {
	snap := &JobSnapshot{
		JobUID:   "job-uid-1",
		Attempts: []AttemptSnapshot{},
	}
	require.NoError(t, ValidateSnapshot(snap))
}

// ---------------------------------------------------------------------------
// Property test: random event permutations — at most one terminal state
// ---------------------------------------------------------------------------

func TestProperty_OnlyOneTerminalState(t *testing.T) {
	type event struct {
		condType string
		status   metav1.ConditionStatus
		reason   string
		message  string
	}

	baseEvents := []event{
		{v1alpha1.JobConditionScheduled, metav1.ConditionTrue, "Scheduled", "scheduled"},
		{v1alpha1.JobConditionRunning, metav1.ConditionTrue, "Running", "running"},
		{v1alpha1.JobConditionComplete, metav1.ConditionTrue, "Done", "completed"},
		{v1alpha1.JobConditionFailed, metav1.ConditionTrue, "Err", "failed"},
	}

	rng := rand.New(rand.NewSource(42))

	for i := 0; i < 100; i++ {
		// Shuffle events
		events := make([]event, len(baseEvents))
		copy(events, baseEvents)
		rng.Shuffle(len(events), func(a, b int) {
			events[a], events[b] = events[b], events[a]
		})

		var conditions []metav1.Condition
		terminalCount := 0

		for _, ev := range events {
			if ev.condType == v1alpha1.JobConditionComplete || ev.condType == v1alpha1.JobConditionFailed {
				ok := TrySetJobTerminal(&conditions, ev.condType, ev.status, ev.reason, ev.message)
				if ok {
					terminalCount++
				}
			} else {
				SetJobCondition(&conditions, ev.condType, ev.status, ev.reason, ev.message)
			}
		}

		assert.LessOrEqual(t, terminalCount, 1,
			"iteration %d: at most one terminal condition should be accepted, got %d", i, terminalCount)

		// Also verify IsJobTerminal is consistent
		if terminalCount == 1 {
			assert.True(t, IsJobTerminal(conditions), "iteration %d: should be terminal after one terminal accepted", i)
		}
	}
}

// Property test: TryTerminalCAS never mutates the original slice
func TestProperty_TryTerminalCAS_NoMutation(t *testing.T) {
	rng := rand.New(rand.NewSource(99))

	terminals := []string{v1alpha1.JobConditionComplete, v1alpha1.JobConditionFailed}

	for i := 0; i < 100; i++ {
		var original []metav1.Condition
		// Optionally pre-set a terminal condition
		if rng.Intn(2) == 0 {
			SetJobCondition(&original, terminals[rng.Intn(2)], metav1.ConditionTrue, "Pre", "pre-set")
		}

		var snapshot []metav1.Condition
		if original != nil {
			snapshot = make([]metav1.Condition, len(original))
			copy(snapshot, original)
		}

		condType := terminals[rng.Intn(2)]
		_, _ = TryTerminalCAS(original, condType, metav1.ConditionTrue, "CAS", "cas attempt")

		assert.Equal(t, snapshot, original,
			"iteration %d: TryTerminalCAS should not mutate the original conditions", i)
	}
}

// Property test: ValidateSnapshot detects any random invalid state
func TestProperty_ValidateSnapshot_RandomInvalid(t *testing.T) {
	rng := rand.New(rand.NewSource(77))

	for i := 0; i < 100; i++ {
		jobUID := types.UID("job-uid")
		nAttempts := rng.Intn(5) + 2 // at least 2
		attempts := make([]AttemptSnapshot, nAttempts)

		// Build a potentially invalid snapshot
		for j := 0; j < nAttempts; j++ {
			uid := jobUID
			if rng.Intn(10) == 0 {
				uid = "wrong-uid" // ~10% chance of wrong UID
			}
			attempts[j] = AttemptSnapshot{
				UID:       types.UID(fmt.Sprintf("a%d", j)),
				Ordinal:   int32(rng.Intn(nAttempts)), // may not be monotonic
				JobRefUID: uid,
				Terminal:  rng.Intn(2) == 0,
			}
		}

		snap := &JobSnapshot{
			JobUID:   jobUID,
			Attempts: attempts,
		}

		err := ValidateSnapshot(snap)

		// Check: if ValidateSnapshot says OK, manually verify all invariants hold
		if err == nil {
			activeCount := 0
			for _, a := range attempts {
				if !a.Terminal {
					activeCount++
				}
			}
			assert.LessOrEqual(t, activeCount, 1, "iter %d: passed but has %d active attempts", i, activeCount)

			for j := 1; j < len(attempts); j++ {
				assert.Greater(t, attempts[j].Ordinal, attempts[j-1].Ordinal,
					"iter %d: passed but ordinals not monotonic", i)
			}

			for j, a := range attempts {
				assert.Equal(t, jobUID, a.JobRefUID,
					"iter %d: passed but attempt %d has wrong UID", i, j)
			}
		}
	}
}
