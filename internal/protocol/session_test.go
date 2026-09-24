package protocol

import (
	"testing"
	"time"
)

func TestSessionCredentials_Expired(t *testing.T) {
	tests := []struct {
		name      string
		expiresAt time.Time
		want      bool
	}{
		{
			name:      "future expiry is not expired",
			expiresAt: time.Now().Add(1 * time.Hour),
			want:      false,
		},
		{
			name:      "past expiry is expired",
			expiresAt: time.Now().Add(-1 * time.Hour),
			want:      true,
		},
		{
			name:      "zero time is expired",
			expiresAt: time.Time{},
			want:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := &SessionCredentials{
				Token:      "tok",
				GatewayURL: "http://gw:8080",
				JobUID:     "j-1",
				AttemptUID: "a-1",
				ExpiresAt:  tt.expiresAt,
			}
			got := sc.Expired()
			if got != tt.want {
				t.Errorf("Expired() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSessionCredentials_Fields(t *testing.T) {
	now := time.Now().Add(30 * time.Minute)
	sc := SessionCredentials{
		Token:      "my-token",
		GatewayURL: "http://artifact-gw:9090",
		JobUID:     "job-uid-abc",
		AttemptUID: "attempt-uid-def",
		ExpiresAt:  now,
	}

	if sc.Token != "my-token" {
		t.Errorf("Token = %q, want %q", sc.Token, "my-token")
	}
	if sc.GatewayURL != "http://artifact-gw:9090" {
		t.Errorf("GatewayURL = %q, want %q", sc.GatewayURL, "http://artifact-gw:9090")
	}
	if sc.JobUID != "job-uid-abc" {
		t.Errorf("JobUID = %q, want %q", sc.JobUID, "job-uid-abc")
	}
	if sc.AttemptUID != "attempt-uid-def" {
		t.Errorf("AttemptUID = %q, want %q", sc.AttemptUID, "attempt-uid-def")
	}
	if !sc.ExpiresAt.Equal(now) {
		t.Errorf("ExpiresAt = %v, want %v", sc.ExpiresAt, now)
	}
}
