package gateway

import (
	"fmt"
	"net/http"
	"strings"

	"k8s.io/apimachinery/pkg/types"
)

// AuthManager handles authentication and authorization for the gateway.
type AuthManager struct {
	sessions *SessionManager
}

// NewAuthManager creates an AuthManager backed by the given SessionManager.
func NewAuthManager(sessions *SessionManager) *AuthManager {
	return &AuthManager{sessions: sessions}
}

// Authenticate extracts and validates the Bearer token from the request.
func (a *AuthManager) Authenticate(r *http.Request) (*SessionInfo, error) {
	header := r.Header.Get("Authorization")
	if header == "" {
		return nil, fmt.Errorf("missing Authorization header")
	}
	if !strings.HasPrefix(header, "Bearer ") {
		return nil, fmt.Errorf("invalid Authorization header format")
	}
	token := strings.TrimPrefix(header, "Bearer ")
	return a.sessions.Validate(token)
}

// AuthorizeScope checks that the session has access to the requested resource scope.
func (a *AuthManager) AuthorizeScope(session *SessionInfo, namespace string, jobUID, attemptUID types.UID) error {
	if session.Namespace != namespace {
		return fmt.Errorf("namespace mismatch: session scoped to %q, requested %q", session.Namespace, namespace)
	}
	if session.JobUID != jobUID {
		return fmt.Errorf("jobUID mismatch: session scoped to %q, requested %q", session.JobUID, jobUID)
	}
	if session.AttemptUID != attemptUID {
		return fmt.Errorf("attemptUID mismatch: session scoped to %q, requested %q", session.AttemptUID, attemptUID)
	}
	return nil
}
