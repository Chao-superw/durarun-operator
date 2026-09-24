package gateway

import (
	"fmt"
	"net/http"
	"strings"

	"durarun-operator/internal/artifact"

	"k8s.io/apimachinery/pkg/types"
)

// HandleUpload handles artifact upload (PUT) requests.
// The key path encodes namespace/jobUID/attemptUID/filename.
func (s *Server) HandleUpload(w http.ResponseWriter, r *http.Request) {
	session, err := s.auth.Authenticate(r)
	if err != nil {
		http.Error(w, fmt.Sprintf("authentication failed: %v", err), http.StatusUnauthorized)
		return
	}

	key := strings.TrimPrefix(r.URL.Path, "/api/v1/artifacts/")
	if key == "" {
		http.Error(w, "missing artifact key", http.StatusBadRequest)
		return
	}

	// Parse the key to extract scope fields.
	namespace, jobUID, _, parseErr := artifact.ParseKey(key)
	if parseErr != nil {
		http.Error(w, fmt.Sprintf("invalid key: %v", parseErr), http.StatusBadRequest)
		return
	}

	// The rest (3rd segment onward) includes attemptUID/filename.
	// We need to extract attemptUID from the key.
	parts := strings.SplitN(key, "/", 4)
	if len(parts) < 4 {
		http.Error(w, "invalid key: expected namespace/jobUID/attemptUID/filename", http.StatusBadRequest)
		return
	}
	attemptUID := parts[2]

	if err := s.auth.AuthorizeScope(session, namespace, types.UID(jobUID), types.UID(attemptUID)); err != nil {
		http.Error(w, fmt.Sprintf("authorization failed: %v", err), http.StatusForbidden)
		return
	}

	defer r.Body.Close()
	if err := s.store.Put(r.Context(), key, r.Body, r.ContentLength); err != nil {
		http.Error(w, fmt.Sprintf("upload failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

// HandleDownload handles artifact download (GET) requests.
func (s *Server) HandleDownload(w http.ResponseWriter, r *http.Request) {
	session, err := s.auth.Authenticate(r)
	if err != nil {
		http.Error(w, fmt.Sprintf("authentication failed: %v", err), http.StatusUnauthorized)
		return
	}

	key := strings.TrimPrefix(r.URL.Path, "/api/v1/artifacts/")
	if key == "" {
		http.Error(w, "missing artifact key", http.StatusBadRequest)
		return
	}

	namespace, jobUID, _, parseErr := artifact.ParseKey(key)
	if parseErr != nil {
		http.Error(w, fmt.Sprintf("invalid key: %v", parseErr), http.StatusBadRequest)
		return
	}

	parts := strings.SplitN(key, "/", 4)
	if len(parts) < 4 {
		http.Error(w, "invalid key: expected namespace/jobUID/attemptUID/filename", http.StatusBadRequest)
		return
	}
	attemptUID := parts[2]

	if err := s.auth.AuthorizeScope(session, namespace, types.UID(jobUID), types.UID(attemptUID)); err != nil {
		http.Error(w, fmt.Sprintf("authorization failed: %v", err), http.StatusForbidden)
		return
	}

	reader, err := s.store.Get(r.Context(), key)
	if err != nil {
		http.Error(w, fmt.Sprintf("download failed: %v", err), http.StatusNotFound)
		return
	}
	defer reader.Close()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)

	buf := make([]byte, 32*1024)
	for {
		n, readErr := reader.Read(buf)
		if n > 0 {
			if _, writeErr := w.Write(buf[:n]); writeErr != nil {
				return
			}
		}
		if readErr != nil {
			break
		}
	}
}
