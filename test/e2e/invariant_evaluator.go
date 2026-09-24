package e2e

import (
	"context"
	"fmt"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"durarun-operator/api/v1alpha1"
	"durarun-operator/internal/state"
)

// StandardInvariants returns the standard set of invariant checks that
// should hold after any fault injection scenario.
func StandardInvariants() []InvariantCheck {
	return []InvariantCheck{
		SingleActiveAttemptInvariant(),
		MonotonicOrdinalsInvariant(),
		UIDConsistencyInvariant(),
		TerminalImmutabilityInvariant(),
		AtMostOneTerminalInvariant(),
	}
}

// ---------------------------------------------------------------------------
// 1. SingleActiveAttempt - at most one non-terminal attempt
// ---------------------------------------------------------------------------

// SingleActiveAttemptInvariant verifies that at most one attempt is in a
// non-terminal state at any point in time.
func SingleActiveAttemptInvariant() InvariantCheck {
	return InvariantCheck{
		Name: "SingleActiveAttempt",
		Check: func(ctx context.Context, env *TestEnv) error {
			var attemptList v1alpha1.AgentAttemptList
			if err := env.Client.List(ctx, &attemptList); err != nil {
				return fmt.Errorf("list attempts: %w", err)
			}

			active := 0
			for _, a := range attemptList.Items {
				if !state.IsAttemptTerminal(a.Status.Conditions) {
					active++
				}
			}
			if active > 1 {
				return fmt.Errorf("found %d non-terminal attempts, expected at most 1", active)
			}
			return nil
		},
	}
}

// ---------------------------------------------------------------------------
// 2. MonotonicOrdinals - ordinals strictly increase
// ---------------------------------------------------------------------------

// MonotonicOrdinalsInvariant verifies that attempt ordinals are strictly
// monotonically increasing when sorted by creation.
func MonotonicOrdinalsInvariant() InvariantCheck {
	return InvariantCheck{
		Name: "MonotonicOrdinals",
		Check: func(ctx context.Context, env *TestEnv) error {
			var attemptList v1alpha1.AgentAttemptList
			if err := env.Client.List(ctx, &attemptList); err != nil {
				return fmt.Errorf("list attempts: %w", err)
			}

			// Build a map of ordinals we have seen. Since attempts may
			// belong to different jobs in theory, group by job name.
			type key struct {
				ns, job string
			}
			groups := make(map[key][]int32)
			for _, a := range attemptList.Items {
				k := key{ns: a.Namespace, job: a.Spec.JobRef.Name}
				groups[k] = append(groups[k], a.Spec.Ordinal)
			}

			for k, ords := range groups {
				for i := 1; i < len(ords); i++ {
					if ords[i] <= ords[i-1] {
						return fmt.Errorf(
							"job %s/%s: ordinal %d at index %d is not greater than %d at index %d",
							k.ns, k.job, ords[i], i, ords[i-1], i-1,
						)
					}
				}
			}
			return nil
		},
	}
}

// ---------------------------------------------------------------------------
// 3. UIDConsistency - all attempt JobRef UIDs match their job UID
// ---------------------------------------------------------------------------

// UIDConsistencyInvariant verifies that every attempt's JobRef UID matches
// its parent job's UID.
func UIDConsistencyInvariant() InvariantCheck {
	return InvariantCheck{
		Name: "UIDConsistency",
		Check: func(ctx context.Context, env *TestEnv) error {
			var jobList v1alpha1.AgentJobList
			if err := env.Client.List(ctx, &jobList); err != nil {
				return fmt.Errorf("list jobs: %w", err)
			}

			jobUIDs := make(map[string]string) // name -> UID
			for _, j := range jobList.Items {
				jobUIDs[j.Name] = string(j.UID)
			}

			var attemptList v1alpha1.AgentAttemptList
			if err := env.Client.List(ctx, &attemptList); err != nil {
				return fmt.Errorf("list attempts: %w", err)
			}

			for _, a := range attemptList.Items {
				expectedUID, ok := jobUIDs[a.Spec.JobRef.Name]
				if !ok {
					// Job may have been deleted; skip.
					continue
				}
				if string(a.Spec.JobRef.UID) != expectedUID {
					return fmt.Errorf(
						"attempt %s has JobRef UID %q but job %s has UID %q",
						a.Name, a.Spec.JobRef.UID, a.Spec.JobRef.Name, expectedUID,
					)
				}
			}
			return nil
		},
	}
}

// ---------------------------------------------------------------------------
// 4. TerminalImmutability - terminal conditions do not change
// ---------------------------------------------------------------------------

// TerminalImmutabilityInvariant verifies that once a job enters a terminal
// state (Complete or Failed), the terminal condition type does not change
// across successive reconcile loops. It reconciles twice more after observing
// a terminal condition and asserts stability.
func TerminalImmutabilityInvariant() InvariantCheck {
	return InvariantCheck{
		Name: "TerminalImmutability",
		Check: func(ctx context.Context, env *TestEnv) error {
			var jobList v1alpha1.AgentJobList
			if err := env.Client.List(ctx, &jobList); err != nil {
				return fmt.Errorf("list jobs: %w", err)
			}

			for _, job := range jobList.Items {
				if !state.IsJobTerminal(job.Status.Conditions) {
					continue
				}

				// Record which terminal condition is set.
				wasComplete := apimeta.IsStatusConditionTrue(job.Status.Conditions, v1alpha1.JobConditionComplete)
				wasFailed := apimeta.IsStatusConditionTrue(job.Status.Conditions, v1alpha1.JobConditionFailed)

				// Reconcile twice more.
				for i := 0; i < 2; i++ {
					_, _ = ReconcileOnce(ctx, env, job.Name, job.Namespace)
				}

				// Re-fetch and verify conditions haven't flipped.
				updated, err := GetJob(ctx, env, job.Name, job.Namespace)
				if err != nil {
					return fmt.Errorf("get job %s after re-reconcile: %w", job.Name, err)
				}
				nowComplete := apimeta.IsStatusConditionTrue(updated.Status.Conditions, v1alpha1.JobConditionComplete)
				nowFailed := apimeta.IsStatusConditionTrue(updated.Status.Conditions, v1alpha1.JobConditionFailed)

				if wasComplete != nowComplete || wasFailed != nowFailed {
					return fmt.Errorf(
						"job %s terminal condition changed: Complete %v->%v, Failed %v->%v",
						job.Name, wasComplete, nowComplete, wasFailed, nowFailed,
					)
				}
			}
			return nil
		},
	}
}

// ---------------------------------------------------------------------------
// 5. AtMostOneTerminal - job has at most one terminal condition
// ---------------------------------------------------------------------------

// AtMostOneTerminalInvariant verifies that a job never has both Complete=True
// and Failed=True simultaneously.
func AtMostOneTerminalInvariant() InvariantCheck {
	return InvariantCheck{
		Name: "AtMostOneTerminal",
		Check: func(ctx context.Context, env *TestEnv) error {
			var jobList v1alpha1.AgentJobList
			if err := env.Client.List(ctx, &jobList); err != nil {
				return fmt.Errorf("list jobs: %w", err)
			}

			for _, job := range jobList.Items {
				count := 0
				for _, c := range job.Status.Conditions {
					if c.Status != metav1.ConditionTrue {
						continue
					}
					if c.Type == v1alpha1.JobConditionComplete || c.Type == v1alpha1.JobConditionFailed {
						count++
					}
				}
				if count > 1 {
					return fmt.Errorf(
						"job %s has %d terminal conditions set to True (expected at most 1)",
						job.Name, count,
					)
				}
			}
			return nil
		},
	}
}

// ---------------------------------------------------------------------------
// Snapshot-based validator (delegates to internal/state.ValidateSnapshot)
// ---------------------------------------------------------------------------

// ValidateSnapshotInvariant builds a state.JobSnapshot from the current
// fake-client state and delegates validation to state.ValidateSnapshot.
func ValidateSnapshotInvariant() InvariantCheck {
	return InvariantCheck{
		Name: "ValidateSnapshot",
		Check: func(ctx context.Context, env *TestEnv) error {
			var jobList v1alpha1.AgentJobList
			if err := env.Client.List(ctx, &jobList); err != nil {
				return fmt.Errorf("list jobs: %w", err)
			}

			for _, job := range jobList.Items {
				var attemptList v1alpha1.AgentAttemptList
				if err := env.Client.List(ctx, &attemptList); err != nil {
					return fmt.Errorf("list attempts: %w", err)
				}

				var snapAttempts []state.AttemptSnapshot
				for _, a := range attemptList.Items {
					if a.Spec.JobRef.Name != job.Name {
						continue
					}
					snapAttempts = append(snapAttempts, state.AttemptSnapshot{
						UID:       a.UID,
						Ordinal:   a.Spec.Ordinal,
						JobRefUID: a.Spec.JobRef.UID,
						Terminal:  state.IsAttemptTerminal(a.Status.Conditions),
					})
				}

				snap := &state.JobSnapshot{
					JobUID:        job.UID,
					Conditions:    job.Status.Conditions,
					ActiveAttempt: job.Status.ActiveAttempt,
					Attempts:      snapAttempts,
				}
				if err := state.ValidateSnapshot(snap); err != nil {
					return fmt.Errorf("job %s: %w", job.Name, err)
				}
			}
			return nil
		},
	}
}
