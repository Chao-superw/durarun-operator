package checkpoint

import (
	"context"
	"io"
)

// Manifest describes the metadata of a single checkpoint snapshot.
type Manifest struct {
	SchemaVersion  int      `json:"schemaVersion"`
	JobName        string   `json:"jobName"`
	AttemptNumber  int      `json:"attemptNumber"`
	CheckpointSeq  int      `json:"checkpointSeq"`
	SHA256         string   `json:"sha256"`
	CreatedAt      string   `json:"createdAt"`
	CompletedSteps []string `json:"completedSteps,omitempty"`
}

// Store defines the interface for checkpoint storage backends.
type Store interface {
	// Save persists a workspace snapshot together with its manifest.
	Save(ctx context.Context, jobName string, seq int, manifest *Manifest, workspace io.Reader) error

	// RestoreLatest extracts the most recent checkpoint into destDir.
	// Returns (nil, nil) when no checkpoint exists for the given job.
	RestoreLatest(ctx context.Context, jobName string, destDir string) (*Manifest, error)

	// GetLatestManifest returns the manifest of the most recent checkpoint
	// without extracting the workspace archive.
	// Returns (nil, nil) when no checkpoint exists for the given job.
	GetLatestManifest(ctx context.Context, jobName string) (*Manifest, error)
}
