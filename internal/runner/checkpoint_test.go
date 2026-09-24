package runner

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"durarun-operator/internal/artifact"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
)

// =========================================================================
// CreateArchive / ExtractArchive round-trip
// =========================================================================

func TestCreateExtractArchive_RoundTrip(t *testing.T) {
	// Create a source directory with some files.
	srcDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(srcDir, "subdir"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "file1.txt"), []byte("hello"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "subdir", "file2.txt"), []byte("world"), 0o644))

	// Create archive.
	var buf bytes.Buffer
	require.NoError(t, CreateArchive(srcDir, &buf, 0))

	// Extract into a different directory.
	dstDir := t.TempDir()
	require.NoError(t, ExtractArchive(&buf, dstDir, 0))

	// Verify contents.
	data1, err := os.ReadFile(filepath.Join(dstDir, "file1.txt"))
	require.NoError(t, err)
	assert.Equal(t, "hello", string(data1))

	data2, err := os.ReadFile(filepath.Join(dstDir, "subdir", "file2.txt"))
	require.NoError(t, err)
	assert.Equal(t, "world", string(data2))
}

func TestCreateArchive_EmptyDir(t *testing.T) {
	srcDir := t.TempDir()

	var buf bytes.Buffer
	require.NoError(t, CreateArchive(srcDir, &buf, 0))

	// Should still produce a valid gzipped tar (empty).
	dstDir := t.TempDir()
	require.NoError(t, ExtractArchive(&buf, dstDir, 0))

	entries, err := os.ReadDir(dstDir)
	require.NoError(t, err)
	assert.Empty(t, entries)
}

// =========================================================================
// ExtractArchive security: rejects path traversal
// =========================================================================

func TestExtractArchive_RejectsPathTraversal(t *testing.T) {
	// Manually craft a tar.gz with a malicious "../etc/passwd" path.
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)

	header := &tar.Header{
		Name: "../etc/passwd",
		Mode: 0o644,
		Size: int64(len("malicious")),
	}
	require.NoError(t, tw.WriteHeader(header))
	_, err := tw.Write([]byte("malicious"))
	require.NoError(t, err)
	require.NoError(t, tw.Close())
	require.NoError(t, gw.Close())

	dstDir := t.TempDir()
	err = ExtractArchive(&buf, dstDir, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "path traversal")
}

func TestExtractArchive_RejectsAbsolutePath(t *testing.T) {
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)

	header := &tar.Header{
		Name: "/etc/passwd",
		Mode: 0o644,
		Size: int64(len("malicious")),
	}
	require.NoError(t, tw.WriteHeader(header))
	_, err := tw.Write([]byte("malicious"))
	require.NoError(t, err)
	require.NoError(t, tw.Close())
	require.NoError(t, gw.Close())

	dstDir := t.TempDir()
	err = ExtractArchive(&buf, dstDir, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "absolute path")
}

// =========================================================================
// CheckpointManager: CreateCheckpoint
// =========================================================================

func TestCreateCheckpoint(t *testing.T) {
	ctx := context.Background()
	store := artifact.NewMemoryStore()

	workDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(workDir, "state.txt"), []byte("step 3 done"), 0o644))

	mgr := &CheckpointManager{
		Store:     store,
		Namespace: "default",
		JobUID:    types.UID("job-abc"),
		WorkDir:   workDir,
	}

	require.NoError(t, mgr.CreateCheckpoint(ctx, 0, 0))

	// Verify the checkpoint was uploaded.
	key := artifact.CheckpointKey("default", "job-abc", 0, "ckpt-0.tar.gz")
	exists, err := store.Exists(ctx, key)
	require.NoError(t, err)
	assert.True(t, exists, "checkpoint should exist in store")

	// Verify the uploaded archive is valid by extracting it.
	reader, err := store.Get(ctx, key)
	require.NoError(t, err)
	defer reader.Close()

	extractDir := t.TempDir()
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, ExtractArchive(bytes.NewReader(data), extractDir, 0))

	content, err := os.ReadFile(filepath.Join(extractDir, "state.txt"))
	require.NoError(t, err)
	assert.Equal(t, "step 3 done", string(content))
}

// =========================================================================
// CheckpointManager: LatestCheckpoint
// =========================================================================

func TestLatestCheckpoint_FindsCorrectLatest(t *testing.T) {
	ctx := context.Background()
	store := artifact.NewMemoryStore()

	workDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(workDir, "data.txt"), []byte("data"), 0o644))

	mgr := &CheckpointManager{
		Store:     store,
		Namespace: "default",
		JobUID:    types.UID("job-xyz"),
		WorkDir:   workDir,
	}

	// Create multiple checkpoints at different ordinals and sequences.
	require.NoError(t, mgr.CreateCheckpoint(ctx, 0, 0))
	require.NoError(t, mgr.CreateCheckpoint(ctx, 0, 1))
	require.NoError(t, mgr.CreateCheckpoint(ctx, 1, 0))
	require.NoError(t, mgr.CreateCheckpoint(ctx, 1, 2))

	ordinal, key, err := mgr.LatestCheckpoint(ctx)
	require.NoError(t, err)
	assert.Equal(t, int32(1), ordinal)
	assert.True(t, strings.Contains(key, "checkpoints/1/ckpt-2.tar.gz"), "should find ordinal 1, seq 2: got %s", key)
}

func TestLatestCheckpoint_NoCheckpoint(t *testing.T) {
	ctx := context.Background()
	store := artifact.NewMemoryStore()

	mgr := &CheckpointManager{
		Store:     store,
		Namespace: "default",
		JobUID:    types.UID("job-empty"),
		WorkDir:   t.TempDir(),
	}

	ordinal, key, err := mgr.LatestCheckpoint(ctx)
	require.NoError(t, err)
	assert.Equal(t, int32(-1), ordinal)
	assert.Empty(t, key)
}

// =========================================================================
// RestoreCheckpoint
// =========================================================================

func TestRestoreCheckpoint_DownloadsAndExtracts(t *testing.T) {
	ctx := context.Background()
	store := artifact.NewMemoryStore()

	// Create a checkpoint from a source directory.
	srcDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "model.bin"), []byte("trained-model"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(srcDir, "logs"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "logs", "train.log"), []byte("epoch 1 done"), 0o644))

	mgr := &CheckpointManager{
		Store:     store,
		Namespace: "prod",
		JobUID:    types.UID("job-restore"),
		WorkDir:   srcDir,
	}
	require.NoError(t, mgr.CreateCheckpoint(ctx, 2, 5))

	// Restore into a fresh directory.
	dstDir := t.TempDir()
	ordinal, err := RestoreCheckpoint(ctx, store, "prod", types.UID("job-restore"), dstDir)
	require.NoError(t, err)
	assert.Equal(t, int32(2), ordinal)

	// Verify extracted contents.
	data, err := os.ReadFile(filepath.Join(dstDir, "model.bin"))
	require.NoError(t, err)
	assert.Equal(t, "trained-model", string(data))

	logData, err := os.ReadFile(filepath.Join(dstDir, "logs", "train.log"))
	require.NoError(t, err)
	assert.Equal(t, "epoch 1 done", string(logData))
}

func TestRestoreCheckpoint_NoCheckpointFound(t *testing.T) {
	ctx := context.Background()
	store := artifact.NewMemoryStore()

	dstDir := t.TempDir()
	ordinal, err := RestoreCheckpoint(ctx, store, "default", types.UID("no-such-job"), dstDir)
	require.NoError(t, err)
	assert.Equal(t, int32(-1), ordinal)
}

func TestRestoreCheckpoint_CorruptedArchive(t *testing.T) {
	ctx := context.Background()
	store := artifact.NewMemoryStore()

	// Upload corrupted data as a checkpoint.
	key := artifact.CheckpointKey("default", "job-corrupt", 0, "ckpt-0.tar.gz")
	corruptData := []byte("this is not a valid tar.gz")
	require.NoError(t, store.Put(ctx, key, bytes.NewReader(corruptData), int64(len(corruptData))))

	dstDir := t.TempDir()
	_, err := RestoreCheckpoint(ctx, store, "default", types.UID("job-corrupt"), dstDir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "extract")
}

// =========================================================================
// Cross-Attempt recovery: checkpoint from ordinal 0, restore in ordinal 1
// =========================================================================

func TestCrossAttemptRecovery(t *testing.T) {
	ctx := context.Background()
	store := artifact.NewMemoryStore()
	jobUID := types.UID("job-cross-attempt")

	// --- Attempt 0: create workspace and checkpoint ---
	attempt0Dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(attempt0Dir, "progress.txt"), []byte("step 5 of 10"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(attempt0Dir, "cache"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(attempt0Dir, "cache", "embeddings.bin"), []byte("cached-vectors"), 0o644))

	mgr0 := &CheckpointManager{
		Store:     store,
		Namespace: "default",
		JobUID:    jobUID,
		WorkDir:   attempt0Dir,
	}
	require.NoError(t, mgr0.CreateCheckpoint(ctx, 0, 0))
	require.NoError(t, mgr0.CreateCheckpoint(ctx, 0, 1))

	// --- Attempt 1: restore from latest checkpoint of ordinal 0 ---
	attempt1Dir := t.TempDir()
	ordinal, err := RestoreCheckpoint(ctx, store, "default", jobUID, attempt1Dir)
	require.NoError(t, err)
	assert.Equal(t, int32(0), ordinal, "should restore from ordinal 0")

	// Verify the restored workspace matches the original.
	progress, err := os.ReadFile(filepath.Join(attempt1Dir, "progress.txt"))
	require.NoError(t, err)
	assert.Equal(t, "step 5 of 10", string(progress))

	embeddings, err := os.ReadFile(filepath.Join(attempt1Dir, "cache", "embeddings.bin"))
	require.NoError(t, err)
	assert.Equal(t, "cached-vectors", string(embeddings))

	// --- Attempt 1: create a new checkpoint at ordinal 1 ---
	require.NoError(t, os.WriteFile(filepath.Join(attempt1Dir, "progress.txt"), []byte("step 8 of 10"), 0o644))

	mgr1 := &CheckpointManager{
		Store:     store,
		Namespace: "default",
		JobUID:    jobUID,
		WorkDir:   attempt1Dir,
	}
	require.NoError(t, mgr1.CreateCheckpoint(ctx, 1, 0))

	// Verify latest checkpoint is now ordinal 1.
	latestOrd, latestKey, err := mgr1.LatestCheckpoint(ctx)
	require.NoError(t, err)
	assert.Equal(t, int32(1), latestOrd)
	assert.Contains(t, latestKey, "checkpoints/1/")
}

// =========================================================================
// CheckpointManager: Start/Stop periodic checkpointing
// =========================================================================

func TestCheckpointManager_StartStop(t *testing.T) {
	ctx := context.Background()
	store := artifact.NewMemoryStore()

	workDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(workDir, "live.txt"), []byte("live data"), 0o644))

	mgr := &CheckpointManager{
		Store:     store,
		Namespace: "default",
		JobUID:    types.UID("job-periodic"),
		WorkDir:   workDir,
		Interval:  50 * time.Millisecond,
	}

	stop := mgr.Start(ctx, 0)

	// Wait long enough for at least one checkpoint to be created.
	time.Sleep(200 * time.Millisecond)

	stop()

	// Verify at least one checkpoint was created.
	ordinal, _, err := mgr.LatestCheckpoint(ctx)
	require.NoError(t, err)
	assert.Equal(t, int32(0), ordinal, "should have created at least one checkpoint")
}
