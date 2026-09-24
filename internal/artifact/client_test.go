package artifact

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"durarun-operator/internal/protocol"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testGateway creates a minimal artifact gateway for client tests.
// It uses bearer-token auth and an in-memory store. The token is hard-coded.
func testGateway(t *testing.T) (*httptest.Server, *MemoryStore) {
	t.Helper()
	store := NewMemoryStore()
	const validToken = "test-token-123"

	mux := http.NewServeMux()

	// PUT/GET /api/v1/artifacts/{key}
	mux.HandleFunc("/api/v1/artifacts/", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+validToken {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		key := strings.TrimPrefix(r.URL.Path, "/api/v1/artifacts/")
		switch r.Method {
		case http.MethodPut:
			data, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if err := store.Put(r.Context(), key, bytes.NewReader(data), int64(len(data))); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusCreated)
		case http.MethodGet:
			reader, err := store.Get(r.Context(), key)
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			defer reader.Close()
			w.WriteHeader(http.StatusOK)
			_, _ = io.Copy(w, reader)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// POST /api/v1/results
	mux.HandleFunc("/api/v1/results", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+validToken {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := store.Put(r.Context(), "_results/latest.json", bytes.NewReader(data), int64(len(data))); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
	})

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return ts, store
}

func TestGatewayClient_UploadDownloadRoundTrip(t *testing.T) {
	ts, _ := testGateway(t)
	client := NewGatewayClient(ts.URL, "test-token-123")

	ctx := context.Background()
	key := "default/job-1/attempt-1/output.json"
	data := []byte(`{"hello":"world"}`)

	// Upload
	err := client.Upload(ctx, key, bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)

	// Download
	reader, err := client.Download(ctx, key)
	require.NoError(t, err)
	defer reader.Close()

	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestGatewayClient_Upload_Unauthorized(t *testing.T) {
	ts, _ := testGateway(t)
	client := NewGatewayClient(ts.URL, "bad-token")

	ctx := context.Background()
	err := client.Upload(ctx, "k", bytes.NewReader([]byte("data")), 4)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "401")
}

func TestGatewayClient_Download_NotFound(t *testing.T) {
	ts, _ := testGateway(t)
	client := NewGatewayClient(ts.URL, "test-token-123")

	ctx := context.Background()
	_, err := client.Download(ctx, "no/such/key/file.txt")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "404")
}

func TestGatewayClient_SubmitResult(t *testing.T) {
	ts, store := testGateway(t)
	client := NewGatewayClient(ts.URL, "test-token-123")

	ctx := context.Background()
	envelope := &protocol.ManifestEnvelope{
		JobUID:     "job-1",
		AttemptUID: "attempt-1",
		Ordinal:    0,
		SpecHash:   "hash-abc",
		Result: protocol.ResultManifest{
			ExitCode:    0,
			ContentHash: "sha256:000",
		},
	}

	err := client.SubmitResult(ctx, envelope)
	require.NoError(t, err)

	// Verify the result was stored.
	reader, err := store.Get(ctx, "_results/latest.json")
	require.NoError(t, err)
	defer reader.Close()

	var stored protocol.ManifestEnvelope
	require.NoError(t, json.NewDecoder(reader).Decode(&stored))
	assert.Equal(t, envelope.JobUID, stored.JobUID)
	assert.Equal(t, envelope.AttemptUID, stored.AttemptUID)
	assert.Equal(t, envelope.Result.ExitCode, stored.Result.ExitCode)
}
