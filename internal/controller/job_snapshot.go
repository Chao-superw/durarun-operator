package controller

import (
	"context"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"durarun-operator/api/v1alpha1"
)

// JobSnapshot captures the complete state of a Job, its owned Attempts,
// and its owned Pods at a single point in time. All reconcile decisions
// are made from the snapshot, avoiding mid-reconcile re-fetches.
type JobSnapshot struct {
	Job      *v1alpha1.AgentJob
	Attempts []*v1alpha1.AgentAttempt
	Pods     []*corev1.Pod
}

// LoadJobSnapshot fetches the Job and all owned Attempts and Pods.
// Attempts and Pods are selected using the label selector durarun.io/job=<name>
// and sorted by ordinal (attempts) or name (pods).
// Returns nil, nil if the Job is not found (deleted).
func LoadJobSnapshot(ctx context.Context, c client.Client, key types.NamespacedName) (*JobSnapshot, error) {
	var job v1alpha1.AgentJob
	if err := c.Get(ctx, key, &job); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get job %s: %w", key, err)
	}

	selector := labels.SelectorFromSet(labels.Set{
		"durarun.io/job": key.Name,
	})
	listOpts := &client.ListOptions{
		Namespace:     key.Namespace,
		LabelSelector: selector,
	}

	// List all Attempts owned by this job.
	var attemptList v1alpha1.AgentAttemptList
	if err := c.List(ctx, &attemptList, listOpts); err != nil {
		return nil, fmt.Errorf("list attempts for job %s: %w", key, err)
	}

	// Sort by ordinal ascending.
	sort.Slice(attemptList.Items, func(i, j int) bool {
		return attemptList.Items[i].Spec.Ordinal < attemptList.Items[j].Spec.Ordinal
	})

	attempts := make([]*v1alpha1.AgentAttempt, len(attemptList.Items))
	for i := range attemptList.Items {
		attempts[i] = &attemptList.Items[i]
	}

	// List all Pods owned by this job.
	var podList corev1.PodList
	if err := c.List(ctx, &podList, listOpts); err != nil {
		return nil, fmt.Errorf("list pods for job %s: %w", key, err)
	}

	// Sort pods by name for deterministic ordering.
	sort.Slice(podList.Items, func(i, j int) bool {
		return podList.Items[i].Name < podList.Items[j].Name
	})

	pods := make([]*corev1.Pod, len(podList.Items))
	for i := range podList.Items {
		pods[i] = &podList.Items[i]
	}

	return &JobSnapshot{
		Job:      &job,
		Attempts: attempts,
		Pods:     pods,
	}, nil
}

// ActiveAttempt returns the current non-terminal attempt from the snapshot,
// or nil if none exists.
func (s *JobSnapshot) ActiveAttempt() *v1alpha1.AgentAttempt {
	if s.Job.Status.ActiveAttempt == nil {
		return nil
	}
	for _, a := range s.Attempts {
		if a.Name == s.Job.Status.ActiveAttempt.Name {
			return a
		}
	}
	return nil
}

// PodForAttempt returns the Pod associated with the given attempt name, or nil.
func (s *JobSnapshot) PodForAttempt(attemptName string) *corev1.Pod {
	for _, p := range s.Pods {
		if p.Labels["durarun.io/attempt"] == attemptName {
			return p
		}
	}
	return nil
}

// NextOrdinal returns the ordinal for the next attempt to create.
func (s *JobSnapshot) NextOrdinal() int32 {
	if len(s.Attempts) == 0 {
		return 1
	}
	return s.Attempts[len(s.Attempts)-1].Spec.Ordinal + 1
}
