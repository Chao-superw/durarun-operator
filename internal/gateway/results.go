package gateway

import (
	"encoding/json"
	"fmt"
	"net/http"

	"durarun-operator/internal/protocol"
)

// HandleResult handles result manifest submission (POST /api/v1/results).
func (s *Server) HandleResult(w http.ResponseWriter, r *http.Request) {
	session, err := s.auth.Authenticate(r)
	if err != nil {
		http.Error(w, fmt.Sprintf("authentication failed: %v", err), http.StatusUnauthorized)
		return
	}

	var envelope protocol.ManifestEnvelope
	if err := json.NewDecoder(r.Body).Decode(&envelope); err != nil {
		http.Error(w, fmt.Sprintf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	// Verify envelope UIDs match the session scope.
	if err := s.auth.AuthorizeScope(session, session.Namespace, envelope.JobUID, envelope.AttemptUID); err != nil {
		http.Error(w, fmt.Sprintf("authorization failed: %v", err), http.StatusForbidden)
		return
	}

	// Serialise and store the envelope as the manifest.
	manifestData, err := json.Marshal(envelope)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to marshal manifest: %v", err), http.StatusInternalServerError)
		return
	}

	key := fmt.Sprintf("%s/%s/%s/result-manifest.json",
		session.Namespace, session.JobUID, session.AttemptUID)

	if err := s.store.Put(r.Context(), key, jsonReader(manifestData), int64(len(manifestData))); err != nil {
		http.Error(w, fmt.Sprintf("failed to store manifest: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}
