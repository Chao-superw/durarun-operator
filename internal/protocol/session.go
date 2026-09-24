package protocol

import "time"

// SessionCredentials contains the short-lived, attempt-scoped credentials
// injected into a sandbox pod for artifact gateway authentication.
type SessionCredentials struct {
	Token      string    `json:"token"`
	GatewayURL string    `json:"gatewayURL"`
	JobUID     string    `json:"jobUID"`
	AttemptUID string    `json:"attemptUID"`
	ExpiresAt  time.Time `json:"expiresAt"`
}

// Expired reports whether the credentials have passed their expiration time.
func (s *SessionCredentials) Expired() bool {
	return time.Now().After(s.ExpiresAt)
}
