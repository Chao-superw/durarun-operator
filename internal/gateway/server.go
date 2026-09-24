package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"durarun-operator/internal/artifact"

	"k8s.io/apimachinery/pkg/types"
)

// Server is the artifact gateway HTTP server.
type Server struct {
	store artifact.Store
	auth  *AuthManager
	mux   *http.ServeMux
}

// NewServer creates a new gateway Server backed by the given Store.
func NewServer(store artifact.Store) *Server {
	sm := NewSessionManager()
	s := &Server{
		store: store,
		auth:  NewAuthManager(sm),
		mux:   http.NewServeMux(),
	}
	s.registerRoutes()
	return s
}

// Sessions returns the underlying SessionManager so callers (e.g. the
// operator controller) can create tokens for pods.
func (s *Server) Sessions() *SessionManager {
	return s.auth.sessions
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/healthz", s.handleHealth)
	s.mux.HandleFunc("/api/v1/sessions", s.handleSession)
	s.mux.HandleFunc("/api/v1/artifacts/", s.handleArtifacts)
	s.mux.HandleFunc("/api/v1/results", s.HandleResult)
}

// handleHealth responds to health check probes.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// sessionRequest is the body for POST /api/v1/sessions.
type sessionRequest struct {
	JobUID     string `json:"jobUID"`
	AttemptUID string `json:"attemptUID"`
	Namespace  string `json:"namespace"`
	TTLSeconds int    `json:"ttlSeconds,omitempty"`
}

// sessionResponse is the response for POST /api/v1/sessions.
type sessionResponse struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expiresAt"`
}

// handleSession handles POST /api/v1/sessions to create session tokens.
func (s *Server) handleSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req sessionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	if req.JobUID == "" || req.AttemptUID == "" || req.Namespace == "" {
		http.Error(w, "jobUID, attemptUID, and namespace are required", http.StatusBadRequest)
		return
	}

	ttl := 30 * time.Minute
	if req.TTLSeconds > 0 {
		ttl = time.Duration(req.TTLSeconds) * time.Second
	}

	token := s.auth.sessions.Create(
		types.UID(req.JobUID),
		types.UID(req.AttemptUID),
		req.Namespace,
		ttl,
	)

	// Look up the session to get ExpiresAt.
	info, _ := s.auth.sessions.Validate(token)

	resp := sessionResponse{
		Token:     token,
		ExpiresAt: info.ExpiresAt,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(resp)
}

// handleArtifacts routes PUT/GET to the upload/download handlers.
func (s *Server) handleArtifacts(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		s.HandleUpload(w, r)
	case http.MethodGet:
		s.HandleDownload(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// jsonReader wraps a byte slice as an io.Reader.
func jsonReader(data []byte) io.Reader {
	return bytes.NewReader(data)
}

// extractKeyScope parses an artifact key and returns scope fields.
// Returns namespace, jobUID, attemptUID, and any error.
func extractKeyScope(key string) (namespace string, jobUID types.UID, attemptUID types.UID, err error) {
	ns, job, _, parseErr := artifact.ParseKey(key)
	if parseErr != nil {
		return "", "", "", parseErr
	}
	parts := strings.SplitN(key, "/", 4)
	if len(parts) < 4 {
		return "", "", "", fmt.Errorf("invalid key: expected namespace/jobUID/attemptUID/filename")
	}
	return ns, types.UID(job), types.UID(parts[2]), nil
}
