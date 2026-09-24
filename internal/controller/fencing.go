package controller

import (
	"fmt"

	v1alpha1 "durarun-operator/api/v1alpha1"
	"durarun-operator/internal/protocol"
	"durarun-operator/internal/state"
)

// FencingResult describes whether a result envelope was accepted or rejected,
// and if rejected, the reason why.
type FencingResult struct {
	Accepted bool
	Reason   string
}

// ValidateFencing performs five-layer fencing check on an incoming result envelope.
//
// The five layers are:
//  1. Job UID match: envelope.JobUID must match the current job's UID.
//  2. Attempt UID match: envelope.AttemptUID must match the active attempt's UID.
//  3. Ordinal match: envelope.Ordinal must match the active attempt's ordinal.
//  4. Spec hash match: envelope.SpecHash must match job.Status.SpecHash.
//  5. Terminal check: the job must not already be in a terminal state.
func ValidateFencing(job *v1alpha1.AgentJob, attempt *v1alpha1.AgentAttempt, envelope *protocol.ManifestEnvelope) FencingResult {
	// Layer 1: Job UID
	if envelope.JobUID != job.UID {
		return FencingResult{
			Accepted: false,
			Reason:   fmt.Sprintf("job UID mismatch: envelope has %q, job has %q", envelope.JobUID, job.UID),
		}
	}

	// Layer 2: Attempt UID
	if envelope.AttemptUID != attempt.UID {
		return FencingResult{
			Accepted: false,
			Reason:   fmt.Sprintf("attempt UID mismatch: envelope has %q, attempt has %q", envelope.AttemptUID, attempt.UID),
		}
	}

	// Layer 3: Ordinal
	if envelope.Ordinal != attempt.Spec.Ordinal {
		return FencingResult{
			Accepted: false,
			Reason:   fmt.Sprintf("ordinal mismatch: envelope has %d, attempt has %d", envelope.Ordinal, attempt.Spec.Ordinal),
		}
	}

	// Layer 4: Spec hash
	if envelope.SpecHash != job.Status.SpecHash {
		return FencingResult{
			Accepted: false,
			Reason:   fmt.Sprintf("spec hash mismatch: envelope has %q, job has %q", envelope.SpecHash, job.Status.SpecHash),
		}
	}

	// Layer 5: Terminal check
	if state.IsJobTerminal(job.Status.Conditions) {
		return FencingResult{
			Accepted: false,
			Reason:   "job is already in a terminal state",
		}
	}

	return FencingResult{Accepted: true}
}
