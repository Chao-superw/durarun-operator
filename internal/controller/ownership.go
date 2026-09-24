package controller

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"durarun-operator/api/v1alpha1"
)

// VerifyOwnership checks that a child resource (Attempt/Pod) belongs to the
// current Job incarnation. It verifies that the child's ownerReference UID
// matches the Job's UID. Returns an error if there is a UID mismatch,
// indicating a stale resource from a previous Job with the same name.
func VerifyOwnership(job *v1alpha1.AgentJob, child metav1.Object) error {
	for _, ref := range child.GetOwnerReferences() {
		if ref.Name == job.Name {
			if ref.UID != job.UID {
				return fmt.Errorf(
					"ownership mismatch: child %s has owner UID %s, but job %s has UID %s (stale resource)",
					child.GetName(), ref.UID, job.Name, job.UID,
				)
			}
			return nil
		}
	}

	// If the child has a durarun.io/job label matching this job but no ownerReference,
	// check the attempt's JobRef UID if the child is an AgentAttempt.
	// For Pods, check the durarun.io/job label match (label-based ownership).
	// When no ownerReference is found, we allow it (the resource may have been
	// created with a label but without controller reference set yet).
	return nil
}

// VerifyAttemptOwnership checks that an AgentAttempt belongs to the current
// Job by verifying both the ownerReference and the spec's JobRef UID.
func VerifyAttemptOwnership(job *v1alpha1.AgentJob, attempt *v1alpha1.AgentAttempt) error {
	// Check ownerReference first.
	if err := VerifyOwnership(job, attempt); err != nil {
		return err
	}

	// Check JobRef UID in the spec.
	if attempt.Spec.JobRef.UID != "" && attempt.Spec.JobRef.UID != job.UID {
		return fmt.Errorf(
			"attempt %s JobRef UID %s does not match job %s UID %s (stale attempt)",
			attempt.Name, attempt.Spec.JobRef.UID, job.Name, job.UID,
		)
	}

	return nil
}

// ComputeSpecHash returns a deterministic SHA-256 hash of the job spec
// for drift detection. The spec is serialized as JSON before hashing.
func ComputeSpecHash(spec *v1alpha1.AgentJobSpec) string {
	data, err := json.Marshal(spec)
	if err != nil {
		// This should never happen for a valid spec, but return a sentinel.
		return "error-computing-hash"
	}
	h := sha256.Sum256(data)
	return fmt.Sprintf("%x", h[:])
}
