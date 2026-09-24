package artifact

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"durarun-operator/internal/protocol"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
)

// =========================================================================
// Key generation tests
// =========================================================================

func TestArtifactKey(t *testing.T) {
	key := ArtifactKey("default", "job-123", "attempt-456", "output.json")
	assert.Equal(t, "default/job-123/attempt-456/output.json", key)
}

func TestCheckpointKey(t *testing.T) {
	key := CheckpointKey("default", "job-123", 3, "state.tar.gz")
	assert.Equal(t, "default/job-123/checkpoints/3/state.tar.gz", key)
}

func TestManifestKey(t *testing.T) {
	key := ManifestKey("default", "job-123", "attempt-456")
	assert.Equal(t, "default/job-123/attempt-456/manifest.json", key)
}

func TestArtifactKey_NamespaceIsolation(t *testing.T) {
	key1 := ArtifactKey("ns1", "job-1", "attempt-1", "file.txt")
	key2 := ArtifactKey("ns2", "job-1", "attempt-1", "file.txt")
	assert.NotEqual(t, key1, key2)
	assert.True(t, strings.HasPrefix(key1, "ns1/"))
	assert.True(t, strings.HasPrefix(key2, "ns2/"))
}

func TestArtifactKey_UIDIsolation(t *testing.T) {
	key1 := ArtifactKey("default", "job-1", "attempt-1", "file.txt")
	key2 := ArtifactKey("default", "job-2", "attempt-1", "file.txt")
	key3 := ArtifactKey("default", "job-1", "attempt-2", "file.txt")
	assert.NotEqual(t, key1, key2, "different jobUID should produce different keys")
	assert.NotEqual(t, key1, key3, "different attemptUID should produce different keys")
}

// =========================================================================
// ParseKey tests
// =========================================================================

func TestParseKey_Valid(t *testing.T) {
	ns, jobUID, rest, err := ParseKey("default/job-123/attempt-456/output.json")
	require.NoError(t, err)
	assert.Equal(t, "default", ns)
	assert.Equal(t, "job-123", jobUID)
	assert.Equal(t, "attempt-456/output.json", rest)
}

func TestParseKey_ManifestKey(t *testing.T) {
	key := ManifestKey("prod", "j-1", "a-1")
	ns, jobUID, rest, err := ParseKey(key)
	require.NoError(t, err)
	assert.Equal(t, "prod", ns)
	assert.Equal(t, "j-1", jobUID)
	assert.Equal(t, "a-1/manifest.json", rest)
}

func TestParseKey_InvalidTooFewSegments(t *testing.T) {
	_, _, _, err := ParseKey("only-one-segment")
	assert.Error(t, err)

	_, _, _, err = ParseKey("two/segments")
	assert.Error(t, err)
}

func TestParseKey_InvalidEmpty(t *testing.T) {
	_, _, _, err := ParseKey("")
	assert.Error(t, err)
}

// =========================================================================
// MemoryStore tests
// =========================================================================

func TestMemoryStore_PutGet(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	data := []byte("hello world")
	require.NoError(t, store.Put(ctx, "test/key", bytes.NewReader(data), int64(len(data))))

	reader, err := store.Get(ctx, "test/key")
	require.NoError(t, err)
	defer reader.Close()

	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestMemoryStore_GetNotFound(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	_, err := store.Get(ctx, "nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestMemoryStore_Exists(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	exists, err := store.Exists(ctx, "test/key")
	require.NoError(t, err)
	assert.False(t, exists)

	require.NoError(t, store.Put(ctx, "test/key", bytes.NewReader([]byte("data")), 4))

	exists, err = store.Exists(ctx, "test/key")
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestMemoryStore_Delete(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	require.NoError(t, store.Put(ctx, "test/key", bytes.NewReader([]byte("data")), 4))

	require.NoError(t, store.Delete(ctx, "test/key"))

	exists, err := store.Exists(ctx, "test/key")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestMemoryStore_List(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	require.NoError(t, store.Put(ctx, "ns/job1/file1.txt", bytes.NewReader([]byte("a")), 1))
	require.NoError(t, store.Put(ctx, "ns/job1/file2.txt", bytes.NewReader([]byte("b")), 1))
	require.NoError(t, store.Put(ctx, "ns/job2/file1.txt", bytes.NewReader([]byte("c")), 1))

	keys, err := store.List(ctx, "ns/job1/")
	require.NoError(t, err)
	assert.Equal(t, []string{"ns/job1/file1.txt", "ns/job1/file2.txt"}, keys)

	keys, err = store.List(ctx, "ns/")
	require.NoError(t, err)
	assert.Len(t, keys, 3)
}

func TestMemoryStore_ListEmpty(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	keys, err := store.List(ctx, "anything/")
	require.NoError(t, err)
	assert.Empty(t, keys)
}

// =========================================================================
// CommitArtifacts tests
// =========================================================================

func TestCommitArtifacts_Success(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	manifest := &protocol.ResultManifest{ExitCode: 0}
	blobs := map[string]io.Reader{
		"output.json": bytes.NewReader([]byte(`{"result":"ok"}`)),
		"model.bin":   bytes.NewReader([]byte("model data")),
	}

	err := CommitArtifacts(ctx, store, manifest, "default",
		types.UID("job-uid"), types.UID("attempt-uid"), blobs)
	require.NoError(t, err)

	// Manifest fields should be populated.
	assert.Len(t, manifest.Artifacts, 2)
	assert.NotEmpty(t, manifest.ContentHash)
	assert.True(t, strings.HasPrefix(manifest.ContentHash, "sha256:"))

	// Manifest file should exist in the store.
	mKey := ManifestKey("default", "job-uid", "attempt-uid")
	exists, err := store.Exists(ctx, mKey)
	require.NoError(t, err)
	assert.True(t, exists)

	// Every artifact entry should exist and have valid metadata.
	for _, entry := range manifest.Artifacts {
		exists, err := store.Exists(ctx, entry.Key)
		require.NoError(t, err)
		assert.True(t, exists, "artifact %s should exist", entry.Key)
		assert.NotEmpty(t, entry.ContentHash)
		assert.True(t, entry.SizeBytes > 0)
	}
}

func TestCommitArtifacts_EmptyBlobs(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	manifest := &protocol.ResultManifest{ExitCode: 0}
	blobs := map[string]io.Reader{}

	err := CommitArtifacts(ctx, store, manifest, "default",
		types.UID("job-uid"), types.UID("attempt-uid"), blobs)
	require.NoError(t, err)

	assert.Empty(t, manifest.Artifacts)
	assert.NotEmpty(t, manifest.ContentHash)
}

// failingStore wraps a Store and injects Put failures for keys matching a
// substring, allowing simulation of interrupted uploads.
type failingStore struct {
	Store
	failOnKey string
}

func (f *failingStore) Put(ctx context.Context, key string, reader io.Reader, sizeBytes int64) error {
	if strings.Contains(key, f.failOnKey) {
		return fmt.Errorf("simulated failure for key: %s", key)
	}
	return f.Store.Put(ctx, key, reader, sizeBytes)
}

func TestCommitArtifacts_InterruptedUpload(t *testing.T) {
	ctx := context.Background()
	underlying := NewMemoryStore()
	store := &failingStore{Store: underlying, failOnKey: "model.bin"}

	manifest := &protocol.ResultManifest{ExitCode: 0}
	blobs := map[string]io.Reader{
		"model.bin":   bytes.NewReader([]byte("model data")),
		"output.json": bytes.NewReader([]byte(`{"result":"ok"}`)),
	}

	err := CommitArtifacts(ctx, store, manifest, "default",
		types.UID("job-uid"), types.UID("attempt-uid"), blobs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "simulated failure")

	// Manifest must NOT have been uploaded — the upload was interrupted
	// before the manifest-last step.
	mKey := ManifestKey("default", "job-uid", "attempt-uid")
	exists, err := underlying.Exists(ctx, mKey)
	require.NoError(t, err)
	assert.False(t, exists, "manifest should not exist after interrupted upload")
}

// =========================================================================
// ValidateCommit tests
// =========================================================================

func TestValidateCommit_Valid(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	manifest := &protocol.ResultManifest{ExitCode: 0}
	blobs := map[string]io.Reader{
		"output.json": bytes.NewReader([]byte(`{"result":"ok"}`)),
		"data.bin":    bytes.NewReader([]byte("binary data")),
	}

	err := CommitArtifacts(ctx, store, manifest, "default",
		types.UID("job-uid"), types.UID("attempt-uid"), blobs)
	require.NoError(t, err)

	validated, err := ValidateCommit(ctx, store, "default",
		types.UID("job-uid"), types.UID("attempt-uid"))
	require.NoError(t, err)
	assert.Equal(t, 0, validated.ExitCode)
	assert.Len(t, validated.Artifacts, 2)
}

func TestValidateCommit_MissingManifest(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	_, err := ValidateCommit(ctx, store, "default",
		types.UID("job-uid"), types.UID("attempt-uid"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "manifest not found")
}

func TestValidateCommit_MissingArtifact(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	manifest := &protocol.ResultManifest{ExitCode: 0}
	blobs := map[string]io.Reader{
		"output.json": bytes.NewReader([]byte(`{"result":"ok"}`)),
	}
	err := CommitArtifacts(ctx, store, manifest, "default",
		types.UID("job-uid"), types.UID("attempt-uid"), blobs)
	require.NoError(t, err)

	// Delete the artifact to simulate a missing file.
	for _, entry := range manifest.Artifacts {
		require.NoError(t, store.Delete(ctx, entry.Key))
	}

	_, err = ValidateCommit(ctx, store, "default",
		types.UID("job-uid"), types.UID("attempt-uid"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestValidateCommit_CorruptedHash(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	manifest := &protocol.ResultManifest{ExitCode: 0}
	blobs := map[string]io.Reader{
		"output.json": bytes.NewReader([]byte(`{"result":"ok"}`)),
	}
	err := CommitArtifacts(ctx, store, manifest, "default",
		types.UID("job-uid"), types.UID("attempt-uid"), blobs)
	require.NoError(t, err)

	// Overwrite the artifact with different data to simulate corruption.
	artKey := manifest.Artifacts[0].Key
	require.NoError(t, store.Put(ctx, artKey, bytes.NewReader([]byte("corrupted data")), 14))

	_, err = ValidateCommit(ctx, store, "default",
		types.UID("job-uid"), types.UID("attempt-uid"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "hash mismatch")
}
