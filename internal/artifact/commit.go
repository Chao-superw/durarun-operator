package artifact

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	"durarun-operator/internal/protocol"

	"k8s.io/apimachinery/pkg/types"
)

// contentHash computes a "sha256:<hex>" hash of the given data.
func contentHash(data []byte) string {
	h := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(h[:])
}

// computeManifestContentHash produces a deterministic hash over all artifact
// content hashes, sorted by key for reproducibility.
func computeManifestContentHash(entries []protocol.ArtifactEntry) string {
	sorted := make([]protocol.ArtifactEntry, len(entries))
	copy(sorted, entries)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Key < sorted[j].Key
	})
	parts := make([]string, 0, len(sorted))
	for _, e := range sorted {
		parts = append(parts, e.ContentHash)
	}
	combined := strings.Join(parts, "\n")
	h := sha256.Sum256([]byte(combined))
	return "sha256:" + hex.EncodeToString(h[:])
}

// CommitArtifacts atomically commits artifacts using manifest-last ordering:
//  1. Upload all artifact blobs (computing content hashes as we go).
//  2. Verify all uploads exist.
//  3. Upload manifest as final step — this makes the commit visible.
//
// The manifest's Artifacts and ContentHash fields are populated by this
// function; the caller is responsible for setting result-level fields such
// as ExitCode, Signal, ErrorCategory, and CheckpointKey before calling.
func CommitArtifacts(
	ctx context.Context,
	store Store,
	manifest *protocol.ResultManifest,
	namespace string,
	jobUID, attemptUID types.UID,
	blobs map[string]io.Reader,
) error {
	// Deterministic iteration order.
	filenames := make([]string, 0, len(blobs))
	for fn := range blobs {
		filenames = append(filenames, fn)
	}
	sort.Strings(filenames)

	// Step 1: read, hash, and upload each blob.
	entries := make([]protocol.ArtifactEntry, 0, len(blobs))
	for _, filename := range filenames {
		data, err := io.ReadAll(blobs[filename])
		if err != nil {
			return fmt.Errorf("reading blob %q: %w", filename, err)
		}

		key := ArtifactKey(namespace, jobUID, attemptUID, filename)
		hash := contentHash(data)

		if err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
			return fmt.Errorf("uploading artifact %q: %w", filename, err)
		}

		entries = append(entries, protocol.ArtifactEntry{
			Key:         key,
			ContentHash: hash,
			SizeBytes:   int64(len(data)),
		})
	}

	// Step 2: verify every blob was persisted.
	for _, entry := range entries {
		exists, err := store.Exists(ctx, entry.Key)
		if err != nil {
			return fmt.Errorf("verifying artifact %q: %w", entry.Key, err)
		}
		if !exists {
			return fmt.Errorf("artifact %q missing after upload", entry.Key)
		}
	}

	// Step 3: build manifest metadata.
	manifest.Artifacts = entries
	manifest.ContentHash = computeManifestContentHash(entries)

	// Step 4 (manifest-last): serialise and upload the manifest.
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("marshaling manifest: %w", err)
	}
	mKey := ManifestKey(namespace, jobUID, attemptUID)
	if err := store.Put(ctx, mKey, bytes.NewReader(manifestData), int64(len(manifestData))); err != nil {
		return fmt.Errorf("uploading manifest: %w", err)
	}

	return nil
}

// ValidateCommit checks whether a previously committed attempt is complete
// and consistent:
//   - Manifest exists
//   - All referenced artifacts exist
//   - Content hashes match
//   - Overall content hash matches
func ValidateCommit(
	ctx context.Context,
	store Store,
	namespace string,
	jobUID, attemptUID types.UID,
) (*protocol.ResultManifest, error) {
	mKey := ManifestKey(namespace, jobUID, attemptUID)

	// 1. Manifest must exist.
	exists, err := store.Exists(ctx, mKey)
	if err != nil {
		return nil, fmt.Errorf("checking manifest existence: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf("manifest not found at %s", mKey)
	}

	// 2. Download and decode manifest.
	reader, err := store.Get(ctx, mKey)
	if err != nil {
		return nil, fmt.Errorf("downloading manifest: %w", err)
	}
	defer reader.Close()

	var manifest protocol.ResultManifest
	if err := json.NewDecoder(reader).Decode(&manifest); err != nil {
		return nil, fmt.Errorf("decoding manifest: %w", err)
	}

	// 3. For each referenced artifact, verify existence and hash.
	for _, entry := range manifest.Artifacts {
		ok, err := store.Exists(ctx, entry.Key)
		if err != nil {
			return nil, fmt.Errorf("checking artifact %q: %w", entry.Key, err)
		}
		if !ok {
			return nil, fmt.Errorf("artifact %q referenced in manifest but not found", entry.Key)
		}

		artReader, err := store.Get(ctx, entry.Key)
		if err != nil {
			return nil, fmt.Errorf("downloading artifact %q for hash verification: %w", entry.Key, err)
		}
		data, readErr := io.ReadAll(artReader)
		artReader.Close()
		if readErr != nil {
			return nil, fmt.Errorf("reading artifact %q: %w", entry.Key, readErr)
		}

		actual := contentHash(data)
		if actual != entry.ContentHash {
			return nil, fmt.Errorf("artifact %q hash mismatch: expected %s, got %s",
				entry.Key, entry.ContentHash, actual)
		}
	}

	// 4. Verify overall content hash.
	expectedHash := computeManifestContentHash(manifest.Artifacts)
	if expectedHash != manifest.ContentHash {
		return nil, fmt.Errorf("manifest content hash mismatch: expected %s, got %s",
			expectedHash, manifest.ContentHash)
	}

	return &manifest, nil
}
