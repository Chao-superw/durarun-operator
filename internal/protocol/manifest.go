package protocol

import "k8s.io/apimachinery/pkg/types"

// ManifestEnvelope wraps a result manifest with UID fencing metadata.
// Every result submitted by a runner must include matching UIDs, ordinal,
// and spec hash so the controller can reject stale or misdirected results.
type ManifestEnvelope struct {
	JobUID     types.UID      `json:"jobUID"`
	AttemptUID types.UID      `json:"attemptUID"`
	Ordinal    int32          `json:"ordinal"`
	SpecHash   string         `json:"specHash"`
	Result     ResultManifest `json:"result"`
}

// ResultManifest describes the outcome of an attempt.
type ResultManifest struct {
	ExitCode      int             `json:"exitCode"`
	Signal        int             `json:"signal,omitempty"`
	ErrorCategory string          `json:"errorCategory,omitempty"`
	Artifacts     []ArtifactEntry `json:"artifacts,omitempty"`
	CheckpointKey string          `json:"checkpointKey,omitempty"`
	ContentHash   string          `json:"contentHash"`
}

// ArtifactEntry describes a single artifact produced by an attempt.
type ArtifactEntry struct {
	Key         string `json:"key"`
	ContentHash string `json:"contentHash"`
	SizeBytes   int64  `json:"sizeBytes"`
}
