package webhook

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "durarun-operator/api/v1alpha1"
)

// ---------------------------------------------------------------------------
// ValidateCreate tests
// ---------------------------------------------------------------------------

func TestValidateCreate_ValidJob(t *testing.T) {
	v := &AgentJobValidator{}
	job := &v1alpha1.AgentJob{
		ObjectMeta: metav1.ObjectMeta{Name: "test-job", Namespace: "default"},
		Spec: v1alpha1.AgentJobSpec{
			Runtime: v1alpha1.RuntimeSpec{
				Image:   "busybox:latest",
				Command: []string{"echo", "hello"},
			},
			Execution: v1alpha1.ExecutionSpec{
				MaxAttempts: 3,
			},
		},
	}
	warnings, err := v.ValidateCreate(context.Background(), job)
	assert.NoError(t, err)
	assert.Nil(t, warnings)
}

func TestValidateCreate_EmptyImageFails(t *testing.T) {
	v := &AgentJobValidator{}
	job := &v1alpha1.AgentJob{
		ObjectMeta: metav1.ObjectMeta{Name: "test-job", Namespace: "default"},
		Spec: v1alpha1.AgentJobSpec{
			Runtime: v1alpha1.RuntimeSpec{
				Image: "", // empty
			},
			Execution: v1alpha1.ExecutionSpec{
				MaxAttempts: 1,
			},
		},
	}
	_, err := v.ValidateCreate(context.Background(), job)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "image")
}

// ---------------------------------------------------------------------------
// ValidateUpdate tests
// ---------------------------------------------------------------------------

func TestValidateUpdate_ChangeImageRejected(t *testing.T) {
	v := &AgentJobValidator{}
	oldJob := &v1alpha1.AgentJob{
		Spec: v1alpha1.AgentJobSpec{
			Runtime:   v1alpha1.RuntimeSpec{Image: "busybox:1.0", Command: []string{"sh"}},
			Execution: v1alpha1.ExecutionSpec{MaxAttempts: 3},
		},
	}
	newJob := oldJob.DeepCopy()
	newJob.Spec.Runtime.Image = "busybox:2.0"

	_, err := v.ValidateUpdate(context.Background(), oldJob, newJob)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "image")
}

func TestValidateUpdate_IncreaseMaxAttemptsAllowed(t *testing.T) {
	v := &AgentJobValidator{}
	oldJob := &v1alpha1.AgentJob{
		Spec: v1alpha1.AgentJobSpec{
			Runtime:   v1alpha1.RuntimeSpec{Image: "busybox:1.0"},
			Execution: v1alpha1.ExecutionSpec{MaxAttempts: 3},
		},
	}
	newJob := oldJob.DeepCopy()
	newJob.Spec.Execution.MaxAttempts = 5

	warnings, err := v.ValidateUpdate(context.Background(), oldJob, newJob)
	assert.NoError(t, err)
	assert.Nil(t, warnings)
}

func TestValidateUpdate_DecreaseMaxAttemptsRejected(t *testing.T) {
	v := &AgentJobValidator{}
	oldJob := &v1alpha1.AgentJob{
		Spec: v1alpha1.AgentJobSpec{
			Runtime:   v1alpha1.RuntimeSpec{Image: "busybox:1.0"},
			Execution: v1alpha1.ExecutionSpec{MaxAttempts: 5},
		},
	}
	newJob := oldJob.DeepCopy()
	newJob.Spec.Execution.MaxAttempts = 2

	_, err := v.ValidateUpdate(context.Background(), oldJob, newJob)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "maxAttempts")
}

// ---------------------------------------------------------------------------
// ValidateDelete test
// ---------------------------------------------------------------------------

func TestValidateDelete_AlwaysSucceeds(t *testing.T) {
	v := &AgentJobValidator{}
	job := &v1alpha1.AgentJob{
		ObjectMeta: metav1.ObjectMeta{Name: "test-job"},
	}
	warnings, err := v.ValidateDelete(context.Background(), job)
	assert.NoError(t, err)
	assert.Nil(t, warnings)
}

// ---------------------------------------------------------------------------
// ValidateImmutableFields tests
// ---------------------------------------------------------------------------

func TestValidateImmutableFields_NoChange(t *testing.T) {
	job := &v1alpha1.AgentJob{
		Spec: v1alpha1.AgentJobSpec{
			Runtime:   v1alpha1.RuntimeSpec{Image: "img:1", Command: []string{"run"}},
			Execution: v1alpha1.ExecutionSpec{MaxAttempts: 3},
		},
	}
	errs := ValidateImmutableFields(job, job.DeepCopy())
	assert.Empty(t, errs)
}

func TestValidateImmutableFields_ImageChanged(t *testing.T) {
	oldJob := &v1alpha1.AgentJob{
		Spec: v1alpha1.AgentJobSpec{
			Runtime: v1alpha1.RuntimeSpec{Image: "img:1"},
		},
	}
	newJob := oldJob.DeepCopy()
	newJob.Spec.Runtime.Image = "img:2"

	errs := ValidateImmutableFields(oldJob, newJob)
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0].Detail, "immutable")
}

func TestValidateImmutableFields_CommandChanged(t *testing.T) {
	oldJob := &v1alpha1.AgentJob{
		Spec: v1alpha1.AgentJobSpec{
			Runtime: v1alpha1.RuntimeSpec{Image: "img:1", Command: []string{"a"}},
		},
	}
	newJob := oldJob.DeepCopy()
	newJob.Spec.Runtime.Command = []string{"b"}

	errs := ValidateImmutableFields(oldJob, newJob)
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0].Detail, "immutable")
}

func TestValidateImmutableFields_MaxAttemptsIncrease(t *testing.T) {
	oldJob := &v1alpha1.AgentJob{
		Spec: v1alpha1.AgentJobSpec{
			Runtime:   v1alpha1.RuntimeSpec{Image: "img:1"},
			Execution: v1alpha1.ExecutionSpec{MaxAttempts: 3},
		},
	}
	newJob := oldJob.DeepCopy()
	newJob.Spec.Execution.MaxAttempts = 5

	errs := ValidateImmutableFields(oldJob, newJob)
	assert.Empty(t, errs)
}

func TestValidateImmutableFields_MaxAttemptsDecrease(t *testing.T) {
	oldJob := &v1alpha1.AgentJob{
		Spec: v1alpha1.AgentJobSpec{
			Runtime:   v1alpha1.RuntimeSpec{Image: "img:1"},
			Execution: v1alpha1.ExecutionSpec{MaxAttempts: 5},
		},
	}
	newJob := oldJob.DeepCopy()
	newJob.Spec.Execution.MaxAttempts = 2

	errs := ValidateImmutableFields(oldJob, newJob)
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0].Detail, "maxAttempts")
}

func TestValidateImmutableFields_MultipleViolations(t *testing.T) {
	oldJob := &v1alpha1.AgentJob{
		Spec: v1alpha1.AgentJobSpec{
			Runtime:   v1alpha1.RuntimeSpec{Image: "img:1", Command: []string{"a"}},
			Execution: v1alpha1.ExecutionSpec{MaxAttempts: 5},
		},
	}
	newJob := oldJob.DeepCopy()
	newJob.Spec.Runtime.Image = "img:2"
	newJob.Spec.Runtime.Command = []string{"b"}
	newJob.Spec.Execution.MaxAttempts = 1

	errs := ValidateImmutableFields(oldJob, newJob)
	assert.Len(t, errs, 3)
}

// ---------------------------------------------------------------------------
// CapacityAdmission tests
// ---------------------------------------------------------------------------

func TestCapacityAdmission_AdmitUntilMax(t *testing.T) {
	ca := &CapacityAdmission{MaxActiveJobs: 2}
	job := &v1alpha1.AgentJob{}

	// First two should be admitted.
	require.NoError(t, ca.Admit(job))
	require.NoError(t, ca.Admit(job))

	// Third should be rejected.
	err := ca.Admit(job)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "backpressure")
}

func TestCapacityAdmission_ReleaseAndAdmitAgain(t *testing.T) {
	ca := &CapacityAdmission{MaxActiveJobs: 1}
	job := &v1alpha1.AgentJob{}

	require.NoError(t, ca.Admit(job))

	// At capacity - reject.
	require.Error(t, ca.Admit(job))

	// Release one slot.
	ca.Release(job)

	// Should be able to admit again.
	require.NoError(t, ca.Admit(job))
}

func TestCapacityAdmission_ReleaseNeverNegative(t *testing.T) {
	ca := &CapacityAdmission{MaxActiveJobs: 1}
	job := &v1alpha1.AgentJob{}

	// Release without any admit should not go negative.
	ca.Release(job)
	ca.Release(job)

	// Should still be able to admit exactly MaxActiveJobs.
	require.NoError(t, ca.Admit(job))
	require.Error(t, ca.Admit(job))
}

// ---------------------------------------------------------------------------
// CheckSAR tests
// ---------------------------------------------------------------------------

func TestCheckSAR_EmptySA(t *testing.T) {
	err := CheckSAR(context.Background(), nil, "default", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "service account")
}

func TestCheckSAR_ValidSA(t *testing.T) {
	err := CheckSAR(context.Background(), nil, "default", "my-sa")
	assert.NoError(t, err)
}
