# Fault Model

This document describes every failure mode handled by the durarun-operator, the recovery strategy for each, and the invariants that are maintained throughout.

## Handled Failure Modes

### 1. Runner Pod Crash / OOMKill

**Symptom**: Pod transitions to Failed phase or is evicted by kubelet.

**Recovery**: The operator's loss detector (`internal/controller/loss_detector.go`) observes the Pod status change. If `failedAttempts < maxRetries`, a new AgentAttempt is created. The new runner Pod restores from the latest checkpoint in S3 and resumes execution.

**Invariant**: The failed attempt is marked terminal before the new attempt is created. At most one active attempt exists at any time.

### 2. Runner Pod Deleted (Node Drain / Preemption)

**Symptom**: Pod disappears from the API server (not just Failed, but gone).

**Recovery**: The controller's informer cache fires a delete event. The operator treats this the same as a crash: marks the attempt as failed with reason `PodLost`, then creates a new attempt if retries allow.

**Invariant**: Fencing rejects any late results from the deleted Pod because the attempt UID no longer matches the active attempt.

### 3. Node Failure

**Symptom**: Node becomes NotReady; kubelet stops updating Pod status.

**Recovery**: After the Kubernetes node controller marks Pods on that node as Unknown/Terminated, the operator's loss detector picks up the status change. A new attempt is created, potentially on a different node. Cross-node recovery is enabled by S3-based checkpoints.

**Invariant**: Checkpoints in object storage are node-independent. The manifest-last protocol ensures only fully-uploaded checkpoints are visible.

### 4. Stale Result Delivery

**Symptom**: A result envelope arrives from a Pod that was part of a previous attempt (e.g., slow network delivery after Pod replacement).

**Recovery**: The five-layer fencing check (`internal/controller/fencing.go`) rejects the envelope because the attempt UID does not match the current active attempt. The rejection is logged and an Event is emitted.

**Invariant**: Only results matching all five fencing layers (job UID, attempt UID, ordinal, spec hash, non-terminal) can mutate job state.

### 5. Duplicate Job Name (Delete + Recreate)

**Symptom**: A user deletes an AgentJob and creates a new one with the same `.metadata.name`. A lingering Pod from the old job sends a result.

**Recovery**: The job UID fencing layer rejects the result because `metadata.uid` differs between the old and new job.

**Invariant**: Job UID is a globally unique identifier; name reuse does not cause state corruption.

### 6. Operator Crash / Restart

**Symptom**: The operator Pod crashes or is restarted (deployment rollout, OOM, leader election loss).

**Recovery**: On restart, the controller-manager re-lists all AgentJob and AgentAttempt resources and reconciles them to their desired state. Leader election ensures only one operator instance is active at a time. The reconcile loop is fully idempotent: re-running it on the same state produces no additional side effects.

**Invariant**: All state is stored in etcd (CRD status fields). The operator is stateless; no in-memory state is required to resume.

### 7. Checkpoint Upload Failure

**Symptom**: S3 upload fails (network error, bucket full, credentials expired).

**Recovery**: The runner logs the error and retries on the next checkpoint interval. Because the manifest is written last (manifest-last protocol), a partial upload is invisible to restore logic. The runner continues executing; checkpoint failure does not kill the user process.

**Invariant**: Only checkpoints with a valid manifest are considered for restore. Partial uploads without manifests are orphaned and can be garbage-collected.

### 8. Checkpoint Restore Failure

**Symptom**: On a new attempt, the latest checkpoint archive is corrupt or the S3 endpoint is unreachable.

**Recovery**: The runner attempts to restore. If restore fails, it starts the user process from scratch (clean `/workspace`). The attempt proceeds without prior state. A Kubernetes Event is emitted to alert the user.

**Invariant**: A failed restore does not block execution. The job may redo work, but it does not hang.

### 9. Timeout Expiry

**Symptom**: The job's `spec.timeout` duration elapses.

**Recovery**: The deadline controller (`internal/controller/deadline.go`) detects the timeout, sends SIGTERM to the runner Pod (via delete with grace period), and sets the job's terminal condition to Failed with reason `DeadlineExceeded`.

**Invariant**: Timeout racing with normal completion is resolved by the terminal-once rule: whichever event is processed first sets the terminal condition, and the other is rejected by `TrySetJobTerminal`.

### 10. User Cancellation

**Symptom**: User sets a cancellation annotation or deletes the AgentJob.

**Recovery**: The cancel controller (`internal/controller/cancel.go`) detects the annotation and initiates graceful shutdown: SIGTERM to the runner, wait for grace period, then SIGKILL. The job is marked Failed with reason `Cancelled`. If the job is deleted, the finalizer (`internal/controller/finalizer.go`) ensures cleanup of Pods, Secrets, and NetworkPolicies before the resource is removed.

**Invariant**: Finalizer guarantees no orphaned resources remain after job deletion.

### 11. Webhook Rejection (Admission Failure)

**Symptom**: A user submits an invalid AgentJob spec or attempts to mutate an immutable field.

**Recovery**: The admission webhook (`internal/webhook/`) rejects the request with a descriptive error message. No CRD is created or modified.

**Invariant**: Only valid, well-formed AgentJobs enter the system. Immutable fields on AgentAttempt cannot be changed after creation.

### 12. Resource Exhaustion (Capacity Backpressure)

**Symptom**: Too many concurrent AgentJobs for the cluster to handle.

**Recovery**: The capacity webhook (`internal/webhook/capacity.go`) rejects new job submissions when the cluster is at capacity. Existing jobs are not affected.

**Invariant**: The system does not over-commit resources beyond configured limits.

### 13. Concurrent Terminal Race

**Symptom**: A timeout fires at the same moment the runner reports success (or two failure paths race).

**Recovery**: The `TrySetJobTerminal` function (`internal/state/job.go`) uses an atomic check-and-set pattern: it checks if any terminal condition already exists before setting one. The first writer wins; subsequent attempts return false.

**Invariant**: Exactly one terminal condition is set per job. No job can be both Complete and Failed.

## Maintained Invariants Summary

| Invariant | Enforcement |
|---|---|
| At most one active attempt per job | JobController creates attempts sequentially |
| Terminal state is irreversible | `TrySetJobTerminal` in `internal/state/` |
| Only fencing-valid results mutate state | `ValidateFencing` in `internal/controller/fencing.go` |
| No orphaned Pods/Secrets/NetworkPolicies | Finalizer in `internal/controller/finalizer.go` |
| Checkpoint atomicity | Manifest-last write protocol in `internal/artifact/commit.go` |
| Leader-elected singleton operator | controller-manager leader election via Lease |
| Idempotent reconcile | All controllers are written to be re-entrant |
