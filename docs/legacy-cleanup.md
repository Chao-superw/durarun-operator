# Legacy Code Cleanup

This document lists old code that can be removed or deprecated as part of the v0.1 operator migration. These packages and binaries have been superseded by the new operator-pattern implementation. **Do not delete them yet** -- this list is for tracking purposes to coordinate a future cleanup.

## Packages

### `wal/`
- **Status**: Superseded
- **Replaced by**: `internal/runner/checkpoint.go` + `internal/artifact/` (S3-based checkpoint)
- **Reason**: The old WAL (Write-Ahead Log) implementation was used for local step tracking. In the new architecture, checkpoints are stored in S3/MinIO using the manifest-last protocol, and step tracking is handled by the runner's hook server.
- **Files**: `wal/writer.go`, `wal/reader.go`, `wal/record.go`, `wal/sync_darwin.go`, `wal/sync_other.go`, `wal/wal_test.go`

### `protect/`
- **Status**: Superseded
- **Replaced by**: `internal/controller/retry.go` and `internal/controller/deadline.go`
- **Reason**: The old retry/timeout utilities were standalone helpers. The new controller implements retry and deadline logic as part of the Kubernetes reconcile loop with proper condition-based state tracking.
- **Files**: `protect/retry.go`, `protect/timeout.go`, `protect/protect_test.go`

### `runtime/`
- **Status**: Superseded
- **Replaced by**: `internal/runner/`
- **Reason**: The old runtime package provided a step-based execution framework. The new runner is a PID 1 supervisor that execs the user command directly and manages lifecycle through signals and hook server callbacks.
- **Files**: `runtime/runtime.go`, `runtime/step.go`, `runtime/context.go`, `runtime/config.go`, `runtime/runtime_test.go`

### `observe/`
- **Status**: Superseded
- **Replaced by**: `internal/observability/`
- **Reason**: The old observability package has been replaced with a new implementation that integrates OTLP tracing, Prometheus metrics, and structured logging in the operator-native style.
- **Files**: `observe/metrics.go`, `observe/trace.go`, `observe/config.go`, `observe/observe_test.go`

### `internal/sandbox/`
- **Status**: Superseded
- **Replaced by**: `internal/executor/security.go` + `internal/isolation/`
- **Reason**: The old sandbox package handled Docker-based isolation directly. The new executor builds Pod specs with the appropriate security context and runtime class based on the isolation level, delegating actual isolation to the container runtime.
- **Files**: `internal/sandbox/docker.go`

### `internal/pool/`
- **Status**: Superseded
- **Replaced by**: `internal/controller/pool_controller.go`
- **Reason**: The old pool package managed sandbox pre-warming as a standalone component. The new pool controller is a proper Kubernetes controller that reconciles SandboxPool CRDs.
- **Files**: `internal/pool/pool.go`, `internal/pool/pool_test.go`

### `internal/store/`
- **Status**: Superseded
- **Replaced by**: `internal/artifact/`
- **Reason**: The old store package provided a simple key-value abstraction. The new artifact package implements S3-compatible storage with manifest-last atomic commit, scoped keys, and proper content addressing.
- **Files**: `internal/store/store.go`

### `internal/worker/`
- **Status**: Superseded
- **Replaced by**: `internal/controller/` (operator pattern)
- **Reason**: The old worker package implemented a pull-based job execution model. In the operator pattern, the controller creates Pods declaratively; there is no worker polling loop.
- **Files**: `internal/worker/worker.go`

### `internal/scheduler/`
- **Status**: Superseded
- **Replaced by**: Kubernetes scheduler (native) + operator Pod creation
- **Reason**: The old scheduler manually assigned jobs to workers. The operator pattern delegates scheduling to the Kubernetes scheduler by creating Pods directly.
- **Files**: `internal/scheduler/reconciler.go`

### `internal/api/`
- **Status**: Superseded
- **Replaced by**: CRD + `internal/webhook/` + `cmd/artifact-gateway/`
- **Reason**: The old REST API server is replaced by the Kubernetes CRD API (for job management) and the artifact gateway (for artifact operations). Admission webhooks handle validation.
- **Files**: `internal/api/server.go`

### `internal/model/`
- **Status**: Superseded
- **Replaced by**: `api/v1alpha1/`
- **Reason**: The old model package defined internal domain types. The new approach uses Kubernetes API types in `api/v1alpha1/` as the canonical data model, with proper deep copy generation.
- **Files**: `internal/model/model.go`

## Binaries (cmd/)

### `cmd/fab/`
- **Status**: Superseded
- **Replaced by**: `cmd/operator/`
- **Reason**: The old `fab` binary was the original entry point. The operator binary is the new standard entry point for the controller-manager.

### `cmd/scheduler/`
- **Status**: Superseded
- **Replaced by**: Kubernetes-native scheduling via operator
- **Reason**: No longer needed; the operator creates Pods and Kubernetes handles scheduling.

### `cmd/apiserver/`
- **Status**: Superseded
- **Replaced by**: CRD API + `cmd/artifact-gateway/`
- **Reason**: The custom API server is replaced by the Kubernetes API (CRDs) for job management and the artifact gateway for file operations.

### `cmd/worker/`
- **Status**: Superseded
- **Replaced by**: `cmd/agent-runner/`
- **Reason**: The worker binary polled for jobs. The runner binary is started by the operator inside each Pod.

### `cmd/experiment/`
- **Status**: Superseded
- **Replaced by**: `test/load/` and `test/envtest/`
- **Reason**: The experiment harness is replaced by structured load tests and envtest-based integration tests.

## Migration Notes

- All legacy packages still compile and are included in `go build ./...`. They do not break the build.
- Before removing any package, verify no import paths reference it from active code paths.
- The `experiment/` top-level package may still be useful for benchmarking reference; consider archiving rather than deleting.
- The `internal/checkpoint/` package (`fs.go`, `store.go`, `tar.go`) bridges old and new: `tar.go` is used by the new runner, while `fs.go` is legacy. Review individually before removal.
