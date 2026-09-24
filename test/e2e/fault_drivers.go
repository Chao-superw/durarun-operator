package e2e

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"durarun-operator/api/v1alpha1"
)

// ---------------------------------------------------------------------------
// 1. PodDeleteFault - simulates a pod being deleted (kubectl delete pod)
// ---------------------------------------------------------------------------

// PodDeleteFault simulates the pod for the active attempt being deleted.
// After injection the pod is removed from the fake client, causing the
// reconciler to detect PodLoss=NotFound on the next loop.
func PodDeleteFault(jobName string) Fault {
	return Fault{
		Name: "PodDelete",
		Inject: func(ctx context.Context, env *TestEnv) error {
			ns := "default"
			job, err := GetJob(ctx, env, jobName, ns)
			if err != nil {
				return fmt.Errorf("get job: %w", err)
			}
			if job.Status.ActiveAttempt == nil {
				return fmt.Errorf("no active attempt on job %s", jobName)
			}
			podName := job.Status.ActiveAttempt.Name
			var pod corev1.Pod
			key := types.NamespacedName{Name: podName, Namespace: ns}
			if err := env.Client.Get(ctx, key, &pod); err != nil {
				return fmt.Errorf("get pod %s: %w", podName, err)
			}
			return env.Client.Delete(ctx, &pod)
		},
	}
}

// ---------------------------------------------------------------------------
// 2. PodOOMFault - simulates pod being OOM killed
// ---------------------------------------------------------------------------

// PodOOMFault sets the pod's container status to OOMKilled and transitions
// the pod to Failed phase.
func PodOOMFault(jobName string) Fault {
	return Fault{
		Name: "PodOOM",
		Inject: func(ctx context.Context, env *TestEnv) error {
			ns := "default"
			job, err := GetJob(ctx, env, jobName, ns)
			if err != nil {
				return fmt.Errorf("get job: %w", err)
			}
			if job.Status.ActiveAttempt == nil {
				return fmt.Errorf("no active attempt on job %s", jobName)
			}
			podName := job.Status.ActiveAttempt.Name
			var pod corev1.Pod
			key := types.NamespacedName{Name: podName, Namespace: ns}
			if err := env.Client.Get(ctx, key, &pod); err != nil {
				return fmt.Errorf("get pod %s: %w", podName, err)
			}
			pod.Status.Phase = corev1.PodFailed
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{
				{
					Name: "agent",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							ExitCode: 137,
							Reason:   "OOMKilled",
						},
					},
				},
			}
			return env.Client.Status().Update(ctx, &pod)
		},
	}
}

// ---------------------------------------------------------------------------
// 3. NodeLossFault - simulates node becoming unreachable
// ---------------------------------------------------------------------------

// NodeLossFault marks the pod with a DisruptionTarget condition and sets
// its Ready condition to False with reason NodeLost, simulating a node failure.
func NodeLossFault(jobName string) Fault {
	return Fault{
		Name: "NodeLoss",
		Inject: func(ctx context.Context, env *TestEnv) error {
			ns := "default"
			job, err := GetJob(ctx, env, jobName, ns)
			if err != nil {
				return fmt.Errorf("get job: %w", err)
			}
			if job.Status.ActiveAttempt == nil {
				return fmt.Errorf("no active attempt on job %s", jobName)
			}
			podName := job.Status.ActiveAttempt.Name
			var pod corev1.Pod
			key := types.NamespacedName{Name: podName, Namespace: ns}
			if err := env.Client.Get(ctx, key, &pod); err != nil {
				return fmt.Errorf("get pod %s: %w", podName, err)
			}
			pod.Status.Phase = corev1.PodFailed
			pod.Status.Reason = "NodeLost"
			pod.Status.Conditions = []corev1.PodCondition{
				{
					Type:   "DisruptionTarget",
					Status: corev1.ConditionTrue,
					Reason: "DeletionByTaintManager",
				},
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionFalse,
					Reason: "NodeLost",
				},
			}
			return env.Client.Status().Update(ctx, &pod)
		},
	}
}

// ---------------------------------------------------------------------------
// 4. StatusConflictFault - simulates concurrent status update conflict
// ---------------------------------------------------------------------------

// StatusConflictFault modifies the job's resource version to simulate a
// conflict that would happen during concurrent status updates. In real
// clusters this triggers a retry; with the fake client we bump the
// generation to verify the reconciler handles stale-read situations.
func StatusConflictFault(jobName string) Fault {
	return Fault{
		Name: "StatusConflict",
		Inject: func(ctx context.Context, env *TestEnv) error {
			ns := "default"
			var job v1alpha1.AgentJob
			key := types.NamespacedName{Name: jobName, Namespace: ns}
			if err := env.Client.Get(ctx, key, &job); err != nil {
				return fmt.Errorf("get job: %w", err)
			}
			// Bump the status to simulate a concurrent writer, which forces
			// the next reconcile to re-read the object.
			job.Status.FailedAttempts += 0 // no-op change; we just want to update
			return env.Client.Status().Update(ctx, &job)
		},
	}
}

// ---------------------------------------------------------------------------
// 5. CancelFault - simulates job cancellation mid-execution
// ---------------------------------------------------------------------------

// CancelFault annotates the job with the cancel annotation, simulating a
// user issuing a cancel while the job is running.
func CancelFault(jobName string) Fault {
	return Fault{
		Name: "Cancel",
		Inject: func(ctx context.Context, env *TestEnv) error {
			ns := "default"
			var job v1alpha1.AgentJob
			key := types.NamespacedName{Name: jobName, Namespace: ns}
			if err := env.Client.Get(ctx, key, &job); err != nil {
				return fmt.Errorf("get job: %w", err)
			}
			if job.Annotations == nil {
				job.Annotations = make(map[string]string)
			}
			job.Annotations["durarun.io/cancel"] = "true"
			return env.Client.Update(ctx, &job)
		},
	}
}

// ---------------------------------------------------------------------------
// 6. TimeoutFault - simulates job exceeding deadline
// ---------------------------------------------------------------------------

// TimeoutFault sets the job's ActiveDeadlineSeconds to 1 and backdates
// the StartTime so that CheckDeadline will fire on the next reconcile.
func TimeoutFault(jobName string) Fault {
	return Fault{
		Name: "Timeout",
		Inject: func(ctx context.Context, env *TestEnv) error {
			ns := "default"
			var job v1alpha1.AgentJob
			key := types.NamespacedName{Name: jobName, Namespace: ns}
			if err := env.Client.Get(ctx, key, &job); err != nil {
				return fmt.Errorf("get job: %w", err)
			}
			// Set a very short deadline and backdate start time.
			deadlineSec := int64(1)
			job.Spec.Execution.ActiveDeadlineSeconds = &deadlineSec
			pastTime := metav1.NewTime(time.Now().Add(-10 * time.Second))
			job.Status.StartTime = &pastTime
			if err := env.Client.Update(ctx, &job); err != nil {
				return fmt.Errorf("update job spec: %w", err)
			}
			return env.Client.Status().Update(ctx, &job)
		},
	}
}

// ---------------------------------------------------------------------------
// 7. DoubleCompleteFault - simulates two attempts trying to complete
// ---------------------------------------------------------------------------

// DoubleCompleteFault creates a second attempt in a non-terminal state that
// also has a succeeded pod, simulating a race where two attempts both try
// to report completion. The invariant checker should detect the violation
// if both remain non-terminal.
func DoubleCompleteFault(jobName string) Fault {
	return Fault{
		Name: "DoubleComplete",
		Inject: func(ctx context.Context, env *TestEnv) error {
			ns := "default"
			job, err := GetJob(ctx, env, jobName, ns)
			if err != nil {
				return fmt.Errorf("get job: %w", err)
			}

			// Create a second attempt that looks non-terminal.
			secondAttempt := &v1alpha1.AgentAttempt{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-extra", jobName),
					Namespace: ns,
					UID:       types.UID(fmt.Sprintf("uid-%s-extra", jobName)),
					Labels: map[string]string{
						"durarun.io/job": jobName,
					},
				},
				Spec: v1alpha1.AgentAttemptSpec{
					JobRef: v1alpha1.ObjectRef{
						Name:      jobName,
						Namespace: ns,
						UID:       job.UID,
					},
					Ordinal: 99, // high ordinal to avoid collision
				},
			}
			if err := env.Client.Create(ctx, secondAttempt); err != nil {
				return fmt.Errorf("create extra attempt: %w", err)
			}

			// Create a succeeded pod for the extra attempt.
			extraPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-extra", jobName),
					Namespace: ns,
					UID:       types.UID(fmt.Sprintf("uid-pod-%s-extra", jobName)),
					Labels: map[string]string{
						"durarun.io/job":     jobName,
						"durarun.io/attempt": fmt.Sprintf("%s-extra", jobName),
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "agent", Image: "agent:latest"}},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodSucceeded,
				},
			}
			return env.Client.Create(ctx, extraPod)
		},
	}
}

// ---------------------------------------------------------------------------
// 8. StaleResultFault - simulates a late result from an old attempt
// ---------------------------------------------------------------------------

// StaleResultFault marks the active attempt as failed, then creates a new
// attempt via reconcile. It then sets the OLD (now-terminal) attempt's pod
// back to Succeeded, simulating a stale result arriving after the attempt
// was already superseded. The reconciler should ignore this stale success
// because the attempt is already terminal.
func StaleResultFault(jobName string) Fault {
	return Fault{
		Name: "StaleResult",
		Inject: func(ctx context.Context, env *TestEnv) error {
			ns := "default"
			job, err := GetJob(ctx, env, jobName, ns)
			if err != nil {
				return fmt.Errorf("get job: %w", err)
			}
			if job.Status.ActiveAttempt == nil {
				return fmt.Errorf("no active attempt on job %s", jobName)
			}
			oldAttemptName := job.Status.ActiveAttempt.Name

			// Fail the current attempt's pod.
			var pod corev1.Pod
			key := types.NamespacedName{Name: oldAttemptName, Namespace: ns}
			if err := env.Client.Get(ctx, key, &pod); err != nil {
				return fmt.Errorf("get pod: %w", err)
			}
			pod.Status.Phase = corev1.PodFailed
			exitCode := int32(1)
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{
				{
					Name: "agent",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							ExitCode: exitCode,
							Reason:   "Error",
						},
					},
				},
			}
			if err := env.Client.Status().Update(ctx, &pod); err != nil {
				return fmt.Errorf("update pod to failed: %w", err)
			}

			// Reconcile so the controller detects the failure and creates a
			// retry attempt.
			if err := ReconcileN(ctx, env, jobName, ns, 5); err != nil {
				return fmt.Errorf("reconcile after failure: %w", err)
			}

			// Now "undelete" / flip the old pod back to Succeeded (stale result).
			if err := env.Client.Get(ctx, key, &pod); err != nil {
				// Pod might not exist if deleted; re-create it.
				stalePod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      oldAttemptName,
						Namespace: ns,
						UID:       types.UID(fmt.Sprintf("uid-pod-%s-stale", oldAttemptName)),
						Labels: map[string]string{
							"durarun.io/job":     jobName,
							"durarun.io/attempt": oldAttemptName,
						},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "agent", Image: "agent:latest"}},
					},
					Status: corev1.PodStatus{Phase: corev1.PodSucceeded},
				}
				return env.Client.Create(ctx, stalePod)
			}
			// Pod still exists; flip it to Succeeded.
			pod.Status.Phase = corev1.PodSucceeded
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{
				{
					Name: "agent",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							ExitCode: 0,
							Reason:   "Completed",
						},
					},
				},
			}
			return env.Client.Status().Update(ctx, &pod)
		},
	}
}
