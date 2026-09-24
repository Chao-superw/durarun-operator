package controller

import (
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "durarun-operator/api/v1alpha1"
	"durarun-operator/internal/protocol"
)

// newTestJob creates a minimal AgentJob for fencing tests.
func newTestJob(uid types.UID, specHash string, terminal bool) *v1alpha1.AgentJob {
	job := &v1alpha1.AgentJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job",
			Namespace: "default",
			UID:       uid,
		},
		Status: v1alpha1.AgentJobStatus{
			SpecHash: specHash,
		},
	}
	if terminal {
		job.Status.Conditions = []metav1.Condition{
			{
				Type:   v1alpha1.JobConditionComplete,
				Status: metav1.ConditionTrue,
			},
		}
	}
	return job
}

// newTestAttempt creates a minimal AgentAttempt for fencing tests.
func newTestAttempt(uid types.UID, ordinal int32) *v1alpha1.AgentAttempt {
	return &v1alpha1.AgentAttempt{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("test-attempt-%d", ordinal),
			Namespace: "default",
			UID:       uid,
		},
		Spec: v1alpha1.AgentAttemptSpec{
			Ordinal: ordinal,
		},
	}
}

// newTestEnvelope creates a ManifestEnvelope that matches the given job/attempt.
func newTestEnvelope(jobUID, attemptUID types.UID, ordinal int32, specHash string) *protocol.ManifestEnvelope {
	return &protocol.ManifestEnvelope{
		JobUID:     jobUID,
		AttemptUID: attemptUID,
		Ordinal:    ordinal,
		SpecHash:   specHash,
		Result: protocol.ResultManifest{
			ExitCode:    0,
			ContentHash: "sha256:test",
		},
	}
}

func TestFencing_ValidEnvelope(t *testing.T) {
	job := newTestJob("job-1", "hash-1", false)
	attempt := newTestAttempt("attempt-1", 0)
	envelope := newTestEnvelope("job-1", "attempt-1", 0, "hash-1")

	result := ValidateFencing(job, attempt, envelope)
	assert.True(t, result.Accepted)
	assert.Empty(t, result.Reason)
}

func TestFencing_Layer1_JobUIDMismatch(t *testing.T) {
	job := newTestJob("job-1", "hash-1", false)
	attempt := newTestAttempt("attempt-1", 0)
	envelope := newTestEnvelope("wrong-job-uid", "attempt-1", 0, "hash-1")

	result := ValidateFencing(job, attempt, envelope)
	assert.False(t, result.Accepted)
	assert.Contains(t, result.Reason, "job UID mismatch")
}

func TestFencing_Layer2_AttemptUIDMismatch(t *testing.T) {
	job := newTestJob("job-1", "hash-1", false)
	attempt := newTestAttempt("attempt-1", 0)
	envelope := newTestEnvelope("job-1", "wrong-attempt-uid", 0, "hash-1")

	result := ValidateFencing(job, attempt, envelope)
	assert.False(t, result.Accepted)
	assert.Contains(t, result.Reason, "attempt UID mismatch")
}

func TestFencing_Layer3_OrdinalMismatch(t *testing.T) {
	job := newTestJob("job-1", "hash-1", false)
	attempt := newTestAttempt("attempt-1", 0)
	envelope := newTestEnvelope("job-1", "attempt-1", 99, "hash-1")

	result := ValidateFencing(job, attempt, envelope)
	assert.False(t, result.Accepted)
	assert.Contains(t, result.Reason, "ordinal mismatch")
}

func TestFencing_Layer4_SpecHashMismatch(t *testing.T) {
	job := newTestJob("job-1", "hash-1", false)
	attempt := newTestAttempt("attempt-1", 0)
	envelope := newTestEnvelope("job-1", "attempt-1", 0, "wrong-hash")

	result := ValidateFencing(job, attempt, envelope)
	assert.False(t, result.Accepted)
	assert.Contains(t, result.Reason, "spec hash mismatch")
}

func TestFencing_Layer5_AlreadyTerminal(t *testing.T) {
	job := newTestJob("job-1", "hash-1", true)
	attempt := newTestAttempt("attempt-1", 0)
	envelope := newTestEnvelope("job-1", "attempt-1", 0, "hash-1")

	result := ValidateFencing(job, attempt, envelope)
	assert.False(t, result.Accepted)
	assert.Contains(t, result.Reason, "terminal")
}

func TestFencing_LateSuccessFromOldAttemptRejected(t *testing.T) {
	// Simulate: old attempt-0 sends a late result, but attempt-1 is now active.
	job := newTestJob("job-1", "hash-1", false)
	activeAttempt := newTestAttempt("attempt-1-new", 1)

	// Envelope from old attempt (attempt-0) with different UID
	envelope := newTestEnvelope("job-1", "attempt-0-old", 0, "hash-1")

	result := ValidateFencing(job, activeAttempt, envelope)
	assert.False(t, result.Accepted)
	assert.Contains(t, result.Reason, "attempt UID mismatch")
}

func TestFencing_PropertyRandomUIDs(t *testing.T) {
	// Generate 100 random UIDs; only the matching one should pass.
	correctJobUID := types.UID("correct-job-uid")
	correctAttemptUID := types.UID("correct-attempt-uid")
	specHash := "sha256:correct"

	job := newTestJob(correctJobUID, specHash, false)
	attempt := newTestAttempt(correctAttemptUID, 0)

	// Correct envelope passes
	correctEnvelope := newTestEnvelope(correctJobUID, correctAttemptUID, 0, specHash)
	result := ValidateFencing(job, attempt, correctEnvelope)
	assert.True(t, result.Accepted, "correct envelope should pass")

	// 100 random UIDs should all be rejected
	for i := 0; i < 100; i++ {
		randomUID := randomUID(t)
		envelope := newTestEnvelope(randomUID, correctAttemptUID, 0, specHash)

		result := ValidateFencing(job, attempt, envelope)
		assert.False(t, result.Accepted, "random job UID %q should be rejected", randomUID)
	}

	// Also test random attempt UIDs
	for i := 0; i < 100; i++ {
		randomUID := randomUID(t)
		envelope := newTestEnvelope(correctJobUID, randomUID, 0, specHash)

		result := ValidateFencing(job, attempt, envelope)
		assert.False(t, result.Accepted, "random attempt UID %q should be rejected", randomUID)
	}
}

func randomUID(t *testing.T) types.UID {
	t.Helper()
	buf := make([]byte, 16)
	_, err := rand.Read(buf)
	if err != nil {
		t.Fatalf("failed to generate random UID: %v", err)
	}
	return types.UID(fmt.Sprintf("%x", buf))
}
