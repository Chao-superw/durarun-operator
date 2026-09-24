// Package webhook implements admission webhooks for the durarun.io API.
package webhook

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	v1alpha1 "durarun-operator/api/v1alpha1"
)

// AgentJobValidator implements admission.CustomValidator for AgentJob resources.
type AgentJobValidator struct{}

var _ admission.CustomValidator = &AgentJobValidator{}

// ValidateCreate validates AgentJob on creation.
func (v *AgentJobValidator) ValidateCreate(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	job, ok := obj.(*v1alpha1.AgentJob)
	if !ok {
		return nil, fmt.Errorf("expected *AgentJob, got %T", obj)
	}

	if job.Spec.Runtime.Image == "" {
		return nil, fmt.Errorf("spec.runtime.image must not be empty")
	}

	if job.Spec.Execution.MaxAttempts < 0 {
		return nil, fmt.Errorf("spec.execution.maxAttempts must be non-negative")
	}

	return nil, nil
}

// ValidateUpdate validates AgentJob updates - enforces immutability of key fields.
func (v *AgentJobValidator) ValidateUpdate(_ context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	oldJob, ok := oldObj.(*v1alpha1.AgentJob)
	if !ok {
		return nil, fmt.Errorf("expected *AgentJob for old object, got %T", oldObj)
	}
	newJob, ok := newObj.(*v1alpha1.AgentJob)
	if !ok {
		return nil, fmt.Errorf("expected *AgentJob for new object, got %T", newObj)
	}

	errs := ValidateImmutableFields(oldJob, newJob)
	if len(errs) > 0 {
		return nil, fmt.Errorf("validation failed: %s", errs.ToAggregate().Error())
	}

	return nil, nil
}

// ValidateDelete allows all deletes.
func (v *AgentJobValidator) ValidateDelete(_ context.Context, _ runtime.Object) (admission.Warnings, error) {
	return nil, nil
}
