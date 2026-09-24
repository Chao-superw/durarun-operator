# Architecture

## System Overview

```
                          +------------------+
                          |   kube-apiserver |
                          +--------+---------+
                                   |
                    watch/patch CRDs & Pods
                                   |
               +-------------------+-------------------+
               |                                       |
    +----------v-----------+              +------------v-----------+
    |   Operator (ctrl-mgr)|              |   Artifact Gateway     |
    |                      |              |                        |
    |  - JobController     |              |  - Token auth (scoped) |
    |  - AttemptReconciler |              |  - Upload/download     |
    |  - PoolController    |              |  - S3/memory backend   |
    |  - ResultReconciler  |              +------------+-----------+
    |  - Webhooks          |                           |
    +----------+-----------+                    S3 / MinIO
               |                                       ^
        create/delete Pods                             |
               |                              checkpoint upload
               v                                       |
    +----------+-----------+              +------------+-----------+
    |   Runner Pod         |              |   Runner Pod           |
    |                      |              |                        |
    |  PID 1: supervisor   |   ...more   |  PID 1: supervisor     |
    |  PID 2: user process |    pods     |  PID 2: user process   |
    |  hook server :19090  |              |  hook server :19090    |
    +----------------------+              +------------------------+
```

## Component Descriptions

### Operator (cmd/operator)

The operator is a standard controller-manager that watches AgentJob and AgentAttempt custom resources. It runs as a Deployment with leader election. Key sub-controllers:

- **JobController** (`internal/controller/job_controller.go`): Main reconcile loop for AgentJob. Creates AgentAttempts, manages retries, enforces deadlines, handles cancellation and TTL cleanup.
- **AttemptReconciler** (`internal/controller/attempt_reconciler.go`): Reconciles AgentAttempt resources. Creates runner Pods, watches Pod phase transitions, collects results.
- **ResultReconciler** (`internal/controller/result_reconciler.go`): Processes incoming result envelopes from runners, applies fencing validation before accepting.
- **PoolController** (`internal/controller/pool_controller.go`): Manages SandboxPool warm pools for pre-provisioned Pods.
- **Webhooks** (`internal/webhook/`): Admission webhooks for immutability validation, SAR checks, and capacity backpressure.

### Runner (cmd/agent-runner)

The runner binary runs as PID 1 inside each agent Pod. It supervises the user process and manages its full lifecycle:

- **Supervisor** (`internal/runner/supervisor.go`): Execs the user command as a child process, forwards signals, captures exit code and signal information.
- **Hook Server** (`internal/runner/hookserver.go`): HTTP server on port 19090 for step-tracking callbacks from the user process.
- **Checkpoint** (`internal/runner/checkpoint.go`): Periodically snapshots `/workspace` to S3/MinIO using the manifest-last protocol.
- **Restore** (`internal/runner/restore.go`): On startup, restores the latest checkpoint into `/workspace` before launching the user process.
- **Result Reporting** (`internal/runner/result.go`): Reports process exit status back to the operator via the result protocol.

### Artifact Gateway (cmd/artifact-gateway)

The gateway provides an HTTP API for uploading and downloading job artifacts. Each AgentAttempt receives a short-lived, scoped token that restricts access to only that attempt's artifact namespace.

- **Auth** (`internal/gateway/auth.go`): Token validation, scope enforcement.
- **Uploads/Downloads** (`internal/gateway/uploads.go`, `internal/gateway/results.go`): Streamed artifact transfer.
- **Session** (`internal/gateway/session.go`): Attempt-scoped session management.

## CRD Schema Summary

### AgentJob (`durarun.io/v1alpha1`)

The primary user-facing resource. Users create an AgentJob to request execution of a containerized agent workload.

**Key spec fields:**
- `image` (required): Container image to run
- `command`: Override entrypoint
- `prompt`: Agent prompt text (injected as `AGENT_PROMPT` env var)
- `timeout`: Maximum run duration (default: 5m)
- `maxRetries`: Retry limit before marking as Failed
- `checkpoint`: Checkpoint configuration (enabled, bucket, endpoint, intervalSeconds)
- `isolation`: Sandbox isolation level (L0Process through L3Docker)

**Key status fields:**
- `conditions`: Array of metav1.Condition -- the authoritative state signal
- `activeAttempt`: Reference to the current AgentAttempt (name, number, uid)
- `completedAttempts` / `failedAttempts`: Counters

### AgentAttempt (`durarun.io/v1alpha1`)

An immutable-spec record of a single execution attempt. Created by the operator; one per retry.

**Key spec fields:**
- `jobRef`: Parent AgentJob name
- `number`: 1-based attempt ordinal

**Key status fields:**
- `phase`: Pending | Running | Succeeded | Failed
- `podName` / `podUID`: The runner Pod
- `exitCode`: Process exit code

## State Machine

The system uses Kubernetes Conditions as the single source of truth for job state. The state machine enforces the following rules:

```
                       +----------+
                       |  Pending |  (no conditions set)
                       +----+-----+
                            |
                     create Attempt
                            |
                       +----v-----+
                   +-->| Running  |  (Scheduled=True)
                   |   +----+-----+
                   |        |
             retry |   +----+----+
             (if   |   |         |
             under |   v         v
             limit)+--Failed  Succeeded
                      attempt   attempt
                         |         |
                         v         v
                   +-----+---+ +--+--------+
                   |  Failed  | | Complete  |  (terminal conditions)
                   +----------+ +-----------+
```

**Terminal invariant**: Once `JobConditionComplete=True` or `JobConditionFailed=True` is set, no further condition transitions are allowed. This is enforced by `internal/state/job.go:TrySetJobTerminal()`.

## Fencing Protocol

The five-layer fencing protocol (`internal/controller/fencing.go`) prevents stale or misrouted results from corrupting job state:

1. **Job UID match**: The result envelope's `jobUID` must match the current AgentJob's `metadata.uid`. Prevents results from a deleted-and-recreated job with the same name.
2. **Attempt UID match**: The envelope's `attemptUID` must match the active AgentAttempt's `metadata.uid`. Prevents results from a previous attempt.
3. **Ordinal match**: The envelope's ordinal must match the active attempt number. Belt-and-suspenders check alongside UID.
4. **Spec hash match**: The envelope's `specHash` must match the job's recorded spec hash. Detects mid-flight spec mutations.
5. **Terminal check**: The job must not already be in a terminal state. Prevents double-completion.

Any fencing failure causes the result to be rejected with a logged reason, and the operator emits a Kubernetes Event for observability.

## Checkpoint / Recovery Flow

1. **Periodic snapshot**: The runner's checkpoint goroutine periodically tars `/workspace`, computes SHA-256, uploads the archive to S3, then writes the manifest (manifest-last protocol). The manifest is only written after the archive upload succeeds, ensuring atomic commit.

2. **Manifest format** (`internal/protocol/manifest.go`):
   ```
   {jobName}/{attemptNumber}/checkpoint-{seq}.tar.gz
   {jobName}/{attemptNumber}/checkpoint-{seq}.manifest.json
   ```

3. **Recovery trigger**: When the operator detects a runner Pod loss (via `internal/controller/loss_detector.go`), it creates a new AgentAttempt if retries remain.

4. **Restore**: The new runner Pod starts, queries S3 for the latest manifest (highest `checkpointSeq`), downloads and extracts the archive into `/workspace`, then launches the user process. The user process (via the SDK) reads its WAL from `/workspace/.wal/` and skips completed steps.

5. **Cross-node**: Because checkpoints are in object storage (not hostPath), recovery works across nodes without node affinity constraints.

## Fault Model

See [fault-model.md](fault-model.md) for the complete fault model with handled failure modes and recovery strategies.
