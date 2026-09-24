package runner

import (
	"context"
	"fmt"

	"durarun-operator/internal/artifact"

	"k8s.io/apimachinery/pkg/types"
)

// RestoreCheckpoint downloads and extracts the latest checkpoint into the work directory.
// Returns the restored ordinal, or -1 if no checkpoint was found.
func RestoreCheckpoint(
	ctx context.Context,
	store artifact.Store,
	namespace string,
	jobUID types.UID,
	workDir string,
) (int32, error) {
	mgr := &CheckpointManager{
		Store:     store,
		Namespace: namespace,
		JobUID:    jobUID,
		WorkDir:   workDir,
	}

	ordinal, key, err := mgr.LatestCheckpoint(ctx)
	if err != nil {
		return -1, fmt.Errorf("restore: find latest: %w", err)
	}
	if ordinal < 0 {
		return -1, nil
	}

	reader, err := store.Get(ctx, key)
	if err != nil {
		return -1, fmt.Errorf("restore: download checkpoint %s: %w", key, err)
	}
	defer reader.Close()

	if err := ExtractArchive(reader, workDir, 0); err != nil {
		return -1, fmt.Errorf("restore: extract checkpoint: %w", err)
	}

	return ordinal, nil
}
