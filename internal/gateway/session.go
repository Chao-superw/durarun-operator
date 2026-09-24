package gateway

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// SessionInfo holds the metadata associated with an authenticated session.
type SessionInfo struct {
	JobUID     types.UID
	AttemptUID types.UID
	Namespace  string
	ExpiresAt  time.Time
}

// SessionManager manages attempt-scoped session tokens.
type SessionManager struct {
	mu       sync.RWMutex
	sessions map[string]*SessionInfo
}

// NewSessionManager returns an initialised SessionManager.
func NewSessionManager() *SessionManager {
	return &SessionManager{
		sessions: make(map[string]*SessionInfo),
	}
}

// Create issues a new session token scoped to the given job/attempt with a TTL.
func (m *SessionManager) Create(jobUID, attemptUID types.UID, namespace string, ttl time.Duration) string {
	token := generateToken()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessions[token] = &SessionInfo{
		JobUID:     jobUID,
		AttemptUID: attemptUID,
		Namespace:  namespace,
		ExpiresAt:  time.Now().Add(ttl),
	}
	return token
}

// Validate checks that the token exists and has not expired.
func (m *SessionManager) Validate(token string) (*SessionInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	info, ok := m.sessions[token]
	if !ok {
		return nil, fmt.Errorf("invalid session token")
	}
	if time.Now().After(info.ExpiresAt) {
		return nil, fmt.Errorf("session token expired")
	}
	return info, nil
}

// generateToken returns a cryptographically random hex token.
func generateToken() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("failed to generate random token: %v", err))
	}
	return hex.EncodeToString(b)
}
