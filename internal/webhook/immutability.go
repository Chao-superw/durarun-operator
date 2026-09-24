package webhook

import (
	"reflect"

	"k8s.io/apimachinery/pkg/util/validation/field"

	v1alpha1 "durarun-operator/api/v1alpha1"
)

// ValidateImmutableFields checks that immutable fields have not been changed
// after creation. The fields spec.runtime.image and spec.runtime.command are
// fully immutable; spec.execution.maxAttempts can only increase, not decrease.
func ValidateImmutableFields(oldJob, newJob *v1alpha1.AgentJob) field.ErrorList {
	var allErrs field.ErrorList

	runtimePath := field.NewPath("spec", "runtime")

	// spec.runtime.image is immutable.
	if oldJob.Spec.Runtime.Image != newJob.Spec.Runtime.Image {
		allErrs = append(allErrs, field.Forbidden(
			runtimePath.Child("image"),
			"field is immutable after creation",
		))
	}

	// spec.runtime.command is immutable.
	if !reflect.DeepEqual(oldJob.Spec.Runtime.Command, newJob.Spec.Runtime.Command) {
		allErrs = append(allErrs, field.Forbidden(
			runtimePath.Child("command"),
			"field is immutable after creation",
		))
	}

	// spec.execution.maxAttempts can only increase, not decrease.
	execPath := field.NewPath("spec", "execution")
	if newJob.Spec.Execution.MaxAttempts < oldJob.Spec.Execution.MaxAttempts {
		allErrs = append(allErrs, field.Forbidden(
			execPath.Child("maxAttempts"),
			"maxAttempts can only be increased, not decreased",
		))
	}

	return allErrs
}
