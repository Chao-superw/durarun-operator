# Workload Protocol

This document defines the contract between `durarun` (Python SDK) and `durarun-operator` (Kubernetes control plane). Any container image that follows this protocol can be scheduled by the operator.

## 1. Workspace Convention

The operator mounts an `emptyDir` volume at `/workspace` in both the main container and the agent-runner sidecar. All agent working state (WAL files, intermediate outputs) must live under this directory.

When the main container finishes, it must write its numeric exit code to:

```
/workspace/.exit-code
```

The sidecar watches for this file to trigger a final checkpoint and signal job completion.

## 2. Environment Variables

The operator injects the following environment variables into the main container:

| Variable | Description | Example |
|---|---|---|
| `AF_JOB_NAME` | The name of the AgentJob CR | `my-research-agent` |
| `AF_ATTEMPT_NUMBER` | Current attempt number (1-based) | `2` |
| `AGENT_PROMPT` | The prompt string from `spec.prompt` | `Summarize the report` |

The Python SDK reads `AF_JOB_NAME` and `AF_ATTEMPT_NUMBER` to tag WAL files and OTel spans. The SDK does not depend on running inside Kubernetes; when these variables are absent, it falls back to local defaults.

## 3. Checkpoint Format

A checkpoint is a gzip-compressed tar archive of the `/workspace` directory, accompanied by a JSON manifest.

### 3.1 Manifest Schema

```json
{
  "schemaVersion": 1,
  "jobName": "my-research-agent",
  "attemptNumber": 1,
  "checkpointSeq": 3,
  "sha256": "e3b0c44298fc1c149afbf4c8996fb924...",
  "createdAt": "2026-09-23T10:30:00Z",
  "completedSteps": ["fetch_data", "parse_results"]
}
```

| Field | Type | Description |
|---|---|---|
| `schemaVersion` | int | Always `1` for now |
| `jobName` | string | Matches `AF_JOB_NAME` |
| `attemptNumber` | int | Which attempt produced this checkpoint |
| `checkpointSeq` | int | Monotonically increasing sequence within an attempt |
| `sha256` | string | SHA-256 hex digest of the tar archive |
| `createdAt` | string | RFC 3339 UTC timestamp |
| `completedSteps` | []string | Step names the SDK has marked as completed in WAL |

### 3.2 Storage Layout

On a filesystem-backed store, checkpoints are laid out as:

```
<checkpoint-dir>/
  <jobName>/
    checkpoint-<seq>.tar.gz
    checkpoint-<seq>.manifest.json
```

### 3.3 Restore Behavior

On recovery, the agent-runner init container restores the latest checkpoint (highest `checkpointSeq`) into `/workspace`. The SDK then reads its WAL from `/workspace/.wal/` and skips already-completed steps.

## 4. AgentJob CRD Spec

The minimal CRD fields that form the contract:

```yaml
apiVersion: agentfabric.io/v1alpha1
kind: AgentJob
metadata:
  name: my-research-agent
spec:
  image: "my-registry/research-agent:latest"
  command: ["python", "-m", "my_agent"]
  prompt: "Summarize the quarterly report"
  timeout: "10m"
  maxRetries: 3
  env:
    CUSTOM_KEY: "value"
  resources:
    cpuLimit: "2"
    memoryLimit: "1Gi"
  checkpoint:
    enabled: true
    intervalSeconds: 30
  isolation:
    level: L1GVisor
  poolRef: "default"
  stepTracking:
    enabled: true
    protocol: env
```

The operator merges `spec.env` with the protocol environment variables (`AF_JOB_NAME`, `AF_ATTEMPT_NUMBER`, `AGENT_PROMPT`) and injects them into the Pod spec. User-supplied env keys must not collide with the `AF_` prefix.

## 5. Step Tracking Protocols

| Protocol | How the SDK reports step progress |
|---|---|
| `env` | SDK writes step status to WAL files under `/workspace/.wal/` |
| `hook` | SDK calls the sidecar's HTTP hook server at `localhost:19090` |
| `adapter` | Reserved for future use |

## 6. Version Compatibility

The `schemaVersion` field in manifests allows forward-compatible evolution. The operator must be able to read any manifest with `schemaVersion <= currentVersion`. When the schema changes in a breaking way, the version number increments.
