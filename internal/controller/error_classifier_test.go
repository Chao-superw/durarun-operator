package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"

	v1alpha1 "durarun-operator/api/v1alpha1"
	"durarun-operator/internal/protocol"
)

func TestClassifyError_ExitZero(t *testing.T) {
	result := &protocol.ProcessResult{ExitCode: 0}
	assert.Equal(t, v1alpha1.ErrorCategory(""), ClassifyError(result, nil))
}

func TestClassifyError_OOMKilledFlag(t *testing.T) {
	result := &protocol.ProcessResult{ExitCode: 137, OOMKilled: true}
	assert.Equal(t, v1alpha1.ErrorCategoryOOM, ClassifyError(result, nil))
}

func TestClassifyError_OOMKilledFromPod(t *testing.T) {
	result := &protocol.ProcessResult{ExitCode: 137}
	podStatus := &corev1.PodStatus{
		ContainerStatuses: []corev1.ContainerStatus{
			{
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						Reason: "OOMKilled",
					},
				},
			},
		},
	}
	assert.Equal(t, v1alpha1.ErrorCategoryOOM, ClassifyError(result, podStatus))
}

func TestClassifyError_OOMKilledFromPodLastTermination(t *testing.T) {
	result := &protocol.ProcessResult{ExitCode: 137}
	podStatus := &corev1.PodStatus{
		ContainerStatuses: []corev1.ContainerStatus{
			{
				LastTerminationState: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						Reason: "OOMKilled",
					},
				},
			},
		},
	}
	assert.Equal(t, v1alpha1.ErrorCategoryOOM, ClassifyError(result, podStatus))
}

func TestClassifyError_Timeout(t *testing.T) {
	result := &protocol.ProcessResult{ExitCode: 1, Error: "timeout"}
	assert.Equal(t, v1alpha1.ErrorCategoryTimeout, ClassifyError(result, nil))
}

func TestClassifyError_ExitCode137_NoOOM(t *testing.T) {
	// 137 without OOM flag → Infra (likely OOM or preemption)
	result := &protocol.ProcessResult{ExitCode: 137}
	assert.Equal(t, v1alpha1.ErrorCategoryInfra, ClassifyError(result, nil))
}

func TestClassifyError_ExitCode1_User(t *testing.T) {
	result := &protocol.ProcessResult{ExitCode: 1}
	assert.Equal(t, v1alpha1.ErrorCategoryUser, ClassifyError(result, nil))
}

func TestClassifyError_ExitCode126_User(t *testing.T) {
	result := &protocol.ProcessResult{ExitCode: 126}
	assert.Equal(t, v1alpha1.ErrorCategoryUser, ClassifyError(result, nil))
}

func TestClassifyError_ExitCode127_User(t *testing.T) {
	result := &protocol.ProcessResult{ExitCode: 127}
	assert.Equal(t, v1alpha1.ErrorCategoryUser, ClassifyError(result, nil))
}

func TestClassifyError_ExitCode128Plus_Infra(t *testing.T) {
	// 128 + signal (not 137/SIGKILL specifically)
	result := &protocol.ProcessResult{ExitCode: 130} // SIGINT
	assert.Equal(t, v1alpha1.ErrorCategoryInfra, ClassifyError(result, nil))
}

func TestClassifyError_ExitCode143_Infra(t *testing.T) {
	// 143 = 128 + 15 (SIGTERM)
	result := &protocol.ProcessResult{ExitCode: 143}
	assert.Equal(t, v1alpha1.ErrorCategoryInfra, ClassifyError(result, nil))
}

func TestClassifyError_NilResult(t *testing.T) {
	assert.Equal(t, v1alpha1.ErrorCategoryUnknown, ClassifyError(nil, nil))
}

func TestClassifyError_NilPodStatus(t *testing.T) {
	result := &protocol.ProcessResult{ExitCode: 1}
	assert.Equal(t, v1alpha1.ErrorCategoryUser, ClassifyError(result, nil))
}

func TestClassifyError_SignalBasedRange(t *testing.T) {
	// Test a range of signal-based exit codes
	for _, code := range []int{128, 129, 131, 134, 139, 141, 143, 255} {
		result := &protocol.ProcessResult{ExitCode: code}
		cat := ClassifyError(result, nil)
		assert.Equal(t, v1alpha1.ErrorCategoryInfra, cat,
			"exit code %d should be classified as Infra", code)
	}
}

func TestClassifyError_UserErrorRange(t *testing.T) {
	// Test a range of user error exit codes
	for _, code := range []int{1, 2, 42, 100, 126, 127} {
		result := &protocol.ProcessResult{ExitCode: code}
		cat := ClassifyError(result, nil)
		assert.Equal(t, v1alpha1.ErrorCategoryUser, cat,
			"exit code %d should be classified as User", code)
	}
}
