package gateway

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"durarun-operator/internal/artifact"
	"durarun-operator/internal/protocol"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
)

// helper to create a test server and return a running httptest.Server plus the gateway Server.
func newTestServer(t *testing.T) (*httptest.Server, *Server) {
	t.Helper()
	store := artifact.NewMemoryStore()
	srv := NewServer(store)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts, srv
}

// helper to create a session token for a given scope.
func createToken(srv *Server, jobUID, attemptUID types.UID, namespace string, ttl time.Duration) string {
	return srv.Sessions().Create(jobUID, attemptUID, namespace, ttl)
}

// =========================================================================
// Health check
// =========================================================================

func TestHealthCheck(t *testing.T) {
	ts, _ := newTestServer(t)

	resp, err := http.Get(ts.URL + "/healthz")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "ok")
}

// =========================================================================
// Session creation
// =========================================================================

func TestCreateSession(t *testing.T) {
	ts, _ := newTestServer(t)

	reqBody := `{"jobUID":"job-1","attemptUID":"attempt-1","namespace":"default","ttlSeconds":60}`
	resp, err := http.Post(ts.URL+"/api/v1/sessions", "application/json", bytes.NewBufferString(reqBody))
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	var sessResp sessionResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&sessResp))
	assert.NotEmpty(t, sessResp.Token)
	assert.False(t, sessResp.ExpiresAt.IsZero())
}

func TestCreateSession_MissingFields(t *testing.T) {
	ts, _ := newTestServer(t)

	reqBody := `{"jobUID":"job-1"}`
	resp, err := http.Post(ts.URL+"/api/v1/sessions", "application/json", bytes.NewBufferString(reqBody))
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// =========================================================================
// Upload and download
// =========================================================================

func TestUploadAndDownload(t *testing.T) {
	ts, srv := newTestServer(t)

	token := createToken(srv, "job-1", "attempt-1", "default", 5*time.Minute)
	key := "default/job-1/attempt-1/output.json"
	data := []byte(`{"result":"hello"}`)

	// Upload
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/api/v1/artifacts/"+key, bytes.NewReader(data))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	req.ContentLength = int64(len(data))

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// Download
	req2, err := http.NewRequest(http.MethodGet, ts.URL+"/api/v1/artifacts/"+key, nil)
	require.NoError(t, err)
	req2.Header.Set("Authorization", "Bearer "+token)

	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusOK, resp2.StatusCode)

	got, err := io.ReadAll(resp2.Body)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

// =========================================================================
// Authentication failures
// =========================================================================

func TestUpload_NoToken(t *testing.T) {
	ts, _ := newTestServer(t)

	key := "default/job-1/attempt-1/output.json"
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/api/v1/artifacts/"+key, bytes.NewReader([]byte("data")))
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestUpload_ExpiredToken(t *testing.T) {
	ts, srv := newTestServer(t)

	// Create a token with a TTL that has already expired.
	token := srv.Sessions().Create("job-1", "attempt-1", "default", -1*time.Second)

	key := "default/job-1/attempt-1/output.json"
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/api/v1/artifacts/"+key, bytes.NewReader([]byte("data")))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

// =========================================================================
// Authorization / scope enforcement
// =========================================================================

func TestUpload_WrongScope(t *testing.T) {
	ts, srv := newTestServer(t)

	// Token is scoped to attempt-A.
	token := createToken(srv, "job-1", "attempt-A", "default", 5*time.Minute)

	// Try uploading to attempt-B's key.
	key := "default/job-1/attempt-B/output.json"
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/api/v1/artifacts/"+key, bytes.NewReader([]byte("data")))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestCrossScopeAccess(t *testing.T) {
	ts, srv := newTestServer(t)

	tokenA := createToken(srv, "job-1", "attempt-A", "default", 5*time.Minute)
	tokenB := createToken(srv, "job-1", "attempt-B", "default", 5*time.Minute)

	keyA := "default/job-1/attempt-A/secret.txt"
	data := []byte("attempt-A secret")

	// Upload with token A.
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/api/v1/artifacts/"+keyA, bytes.NewReader(data))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+tokenA)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	// Attempt to download keyA with token B — should be forbidden.
	req2, err := http.NewRequest(http.MethodGet, ts.URL+"/api/v1/artifacts/"+keyA, nil)
	require.NoError(t, err)
	req2.Header.Set("Authorization", "Bearer "+tokenB)

	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusForbidden, resp2.StatusCode)

	// Token A can still download its own artifact.
	req3, err := http.NewRequest(http.MethodGet, ts.URL+"/api/v1/artifacts/"+keyA, nil)
	require.NoError(t, err)
	req3.Header.Set("Authorization", "Bearer "+tokenA)

	resp3, err := http.DefaultClient.Do(req3)
	require.NoError(t, err)
	defer resp3.Body.Close()
	assert.Equal(t, http.StatusOK, resp3.StatusCode)

	got, err := io.ReadAll(resp3.Body)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

// =========================================================================
// Result manifest submission
// =========================================================================

func TestSubmitResult(t *testing.T) {
	ts, srv := newTestServer(t)

	token := createToken(srv, "job-1", "attempt-1", "default", 5*time.Minute)

	envelope := protocol.ManifestEnvelope{
		JobUID:     "job-1",
		AttemptUID: "attempt-1",
		Ordinal:    0,
		SpecHash:   "abc123",
		Result: protocol.ResultManifest{
			ExitCode:    0,
			ContentHash: "sha256:deadbeef",
		},
	}

	data, err := json.Marshal(envelope)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/results", bytes.NewReader(data))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
}

func TestSubmitResult_WrongScope(t *testing.T) {
	ts, srv := newTestServer(t)

	// Token scoped to attempt-1.
	token := createToken(srv, "job-1", "attempt-1", "default", 5*time.Minute)

	// Envelope references a different attempt.
	envelope := protocol.ManifestEnvelope{
		JobUID:     "job-1",
		AttemptUID: "attempt-OTHER",
		Ordinal:    0,
		SpecHash:   "abc123",
		Result: protocol.ResultManifest{
			ExitCode:    0,
			ContentHash: "sha256:deadbeef",
		},
	}

	data, err := json.Marshal(envelope)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/results", bytes.NewReader(data))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestSubmitResult_NoAuth(t *testing.T) {
	ts, _ := newTestServer(t)

	envelope := protocol.ManifestEnvelope{
		JobUID:     "job-1",
		AttemptUID: "attempt-1",
	}
	data, _ := json.Marshal(envelope)

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/results", bytes.NewReader(data))
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}
