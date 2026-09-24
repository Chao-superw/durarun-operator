package controller

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "durarun-operator/api/v1alpha1"
	"durarun-operator/internal/protocol"
	"durarun-operator/internal/state"
)

// ReconcileResult processes a ManifestEnvelope, applying fencing validation
// and updating the attempt status accordingly.
//
// Steps:
//  1. Validate fencing (five-layer check)
//  2. Classify error from the result
//  3. Set attempt conditions (Complete or Failed)
//  4. Update attempt status fields (ExitCode, Signal, ErrorCategory)
func ReconcileResult(
	_ context.Context,
	job *v1alpha1.AgentJob,
	attempt *v1alpha1.AgentAttempt,
	envelope *protocol.ManifestEnvelope,
) error {
	// Step 1: Validate fencing
	fencing := ValidateFencing(job, attempt, envelope)
	if !fencing.Accepted {
		return fmt.Errorf("fencing rejected: %s", fencing.Reason)
	}

	// Step 2: Classify error
	pr := &protocol.ProcessResult{
		ExitCode:  envelope.Result.ExitCode,
		Signal:    envelope.Result.Signal,
		OOMKilled: false,
	}
	category := ClassifyError(pr, nil)

	// Override with envelope's explicit error category if provided and we
	// detected no specific category from the exit code classification.
	if envelope.Result.ErrorCategory != "" && category == v1alpha1.ErrorCategoryUnknown {
		category = v1alpha1.ErrorCategory(envelope.Result.ErrorCategory)
	}

	// Step 3 & 4: Update attempt status
	exitCode := int32(envelope.Result.ExitCode)
	attempt.Status.ExitCode = &exitCode

	if envelope.Result.Signal != 0 {
		signal := int32(envelope.Result.Signal)
		attempt.Status.Signal = &signal
	}

	attempt.Status.ErrorCategory = category

	now := metav1.Now()
	attempt.Status.CompletionTime = &now

	if envelope.Result.ExitCode == 0 {
		// Success: set Complete condition
		state.SetAttemptCondition(
			&attempt.Status.Conditions,
			v1alpha1.AttemptConditionComplete,
			metav1.ConditionTrue,
			"ProcessExitZero",
			"process exited with code 0",
		)
	} else {
		// Failure: set Failed condition
		state.SetAttemptCondition(
			&attempt.Status.Conditions,
			v1alpha1.AttemptConditionFailed,
			metav1.ConditionTrue,
			"ProcessFailed",
			fmt.Sprintf("process exited with code %d, category %s", envelope.Result.ExitCode, category),
		)
	}

	return nil
}
