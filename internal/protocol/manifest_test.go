package protocol

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
)

func TestManifestEnvelopeJSONRoundTrip(t *testing.T) {
	envelope := ManifestEnvelope{
		JobUID:     types.UID("job-uid-123"),
		AttemptUID: types.UID("attempt-uid-456"),
		Ordinal:    2,
		SpecHash:   "sha256:abc123",
		Result: ResultManifest{
			ExitCode:    0,
			ContentHash: "sha256:def456",
		},
	}

	data, err := json.Marshal(envelope)
	require.NoError(t, err)

	var decoded ManifestEnvelope
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, envelope.JobUID, decoded.JobUID)
	assert.Equal(t, envelope.AttemptUID, decoded.AttemptUID)
	assert.Equal(t, envelope.Ordinal, decoded.Ordinal)
	assert.Equal(t, envelope.SpecHash, decoded.SpecHash)
	assert.Equal(t, envelope.Result.ExitCode, decoded.Result.ExitCode)
	assert.Equal(t, envelope.Result.ContentHash, decoded.Result.ContentHash)
}

func TestManifestEnvelopeWithSignalAndError(t *testing.T) {
	envelope := ManifestEnvelope{
		JobUID:     types.UID("job-uid-789"),
		AttemptUID: types.UID("attempt-uid-012"),
		Ordinal:    0,
		SpecHash:   "sha256:xyz",
		Result: ResultManifest{
			ExitCode:      137,
			Signal:        9,
			ErrorCategory: "Infra",
			ContentHash:   "sha256:content",
		},
	}

	data, err := json.Marshal(envelope)
	require.NoError(t, err)

	var decoded ManifestEnvelope
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, 137, decoded.Result.ExitCode)
	assert.Equal(t, 9, decoded.Result.Signal)
	assert.Equal(t, "Infra", decoded.Result.ErrorCategory)
}

func TestResultManifestWithArtifacts(t *testing.T) {
	manifest := ResultManifest{
		ExitCode:      0,
		CheckpointKey: "checkpoints/run-42",
		ContentHash:   "sha256:overall",
		Artifacts: []ArtifactEntry{
			{
				Key:         "output/model.bin",
				ContentHash: "sha256:model123",
				SizeBytes:   1048576,
			},
			{
				Key:         "output/metrics.json",
				ContentHash: "sha256:metrics456",
				SizeBytes:   2048,
			},
		},
	}

	data, err := json.Marshal(manifest)
	require.NoError(t, err)

	var decoded ResultManifest
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, manifest.ExitCode, decoded.ExitCode)
	assert.Equal(t, manifest.CheckpointKey, decoded.CheckpointKey)
	assert.Equal(t, manifest.ContentHash, decoded.ContentHash)
	require.Len(t, decoded.Artifacts, 2)
	assert.Equal(t, "output/model.bin", decoded.Artifacts[0].Key)
	assert.Equal(t, "sha256:model123", decoded.Artifacts[0].ContentHash)
	assert.Equal(t, int64(1048576), decoded.Artifacts[0].SizeBytes)
	assert.Equal(t, "output/metrics.json", decoded.Artifacts[1].Key)
	assert.Equal(t, int64(2048), decoded.Artifacts[1].SizeBytes)
}

func TestManifestEnvelopeOmitsEmptyFields(t *testing.T) {
	envelope := ManifestEnvelope{
		JobUID:     types.UID("j1"),
		AttemptUID: types.UID("a1"),
		Ordinal:    0,
		SpecHash:   "h1",
		Result: ResultManifest{
			ExitCode:    0,
			ContentHash: "sha256:empty",
		},
	}

	data, err := json.Marshal(envelope)
	require.NoError(t, err)

	// Signal, ErrorCategory, Artifacts, CheckpointKey should be omitted
	var raw map[string]json.RawMessage
	err = json.Unmarshal(data, &raw)
	require.NoError(t, err)

	var result map[string]json.RawMessage
	err = json.Unmarshal(raw["result"], &result)
	require.NoError(t, err)

	_, hasSignal := result["signal"]
	assert.False(t, hasSignal, "signal should be omitted when zero")

	_, hasErrorCategory := result["errorCategory"]
	assert.False(t, hasErrorCategory, "errorCategory should be omitted when empty")

	_, hasArtifacts := result["artifacts"]
	assert.False(t, hasArtifacts, "artifacts should be omitted when nil")

	_, hasCheckpointKey := result["checkpointKey"]
	assert.False(t, hasCheckpointKey, "checkpointKey should be omitted when empty")
}
