package artifact

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"durarun-operator/internal/protocol"
)

// GatewayClient is an HTTP client for the artifact gateway.
type GatewayClient struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
}

// NewGatewayClient creates a GatewayClient with sensible defaults.
func NewGatewayClient(baseURL, token string) *GatewayClient {
	return &GatewayClient{
		BaseURL: baseURL,
		Token:   token,
		HTTP:    &http.Client{},
	}
}

// Upload uploads an artifact to the gateway.
func (c *GatewayClient) Upload(ctx context.Context, key string, reader io.Reader, size int64) error {
	url := fmt.Sprintf("%s/api/v1/artifacts/%s", c.BaseURL, key)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, reader)
	if err != nil {
		return fmt.Errorf("creating upload request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	if size >= 0 {
		req.ContentLength = size
	}

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("executing upload request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upload failed: status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// Download downloads an artifact from the gateway.
func (c *GatewayClient) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	url := fmt.Sprintf("%s/api/v1/artifacts/%s", c.BaseURL, key)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating download request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing download request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("download failed: status %d: %s", resp.StatusCode, string(body))
	}
	return resp.Body, nil
}

// SubmitResult submits a result manifest envelope to the gateway.
func (c *GatewayClient) SubmitResult(ctx context.Context, envelope *protocol.ManifestEnvelope) error {
	data, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshaling manifest envelope: %w", err)
	}

	url := fmt.Sprintf("%s/api/v1/results", c.BaseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("creating result request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = int64(len(data))

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("executing result request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("submit result failed: status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}
