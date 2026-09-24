package artifact

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
)

// ---------------------------------------------------------------------------
// MemoryStore — in-memory implementation for testing
// ---------------------------------------------------------------------------

// MemoryStore is an in-memory Store implementation suitable for unit tests.
type MemoryStore struct {
	mu      sync.RWMutex
	objects map[string][]byte
}

// NewMemoryStore returns an initialised MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{objects: make(map[string][]byte)}
}

func (m *MemoryStore) Put(_ context.Context, key string, reader io.Reader, _ int64) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("reading data: %w", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[key] = data
	return nil
}

func (m *MemoryStore) Get(_ context.Context, key string) (io.ReadCloser, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.objects[key]
	if !ok {
		return nil, fmt.Errorf("object not found: %s", key)
	}
	// Return a copy so the caller cannot mutate the stored slice.
	cp := make([]byte, len(data))
	copy(cp, data)
	return io.NopCloser(bytes.NewReader(cp)), nil
}

func (m *MemoryStore) Exists(_ context.Context, key string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.objects[key]
	return ok, nil
}

func (m *MemoryStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	return nil
}

func (m *MemoryStore) List(_ context.Context, prefix string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var keys []string
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

// ---------------------------------------------------------------------------
// S3Store — minimal S3-compatible HTTP implementation
// ---------------------------------------------------------------------------

// S3Store implements Store against an S3-compatible endpoint using plain
// HTTP requests.  It uses path-style addressing (endpoint/bucket/key).
//
// NOTE: This implementation does NOT perform AWS Signature V4 signing.
// It works with anonymous-access or pre-signed URLs.  For production use,
// replace with a proper AWS SDK client or add V4 signing.
type S3Store struct {
	Endpoint  string
	Bucket    string
	AccessKey string
	SecretKey string
	Client    *http.Client
}

// NewS3Store creates a new S3Store.
func NewS3Store(endpoint, bucket, accessKey, secretKey string) *S3Store {
	return &S3Store{
		Endpoint:  strings.TrimRight(endpoint, "/"),
		Bucket:    bucket,
		AccessKey: accessKey,
		SecretKey: secretKey,
		Client:    &http.Client{},
	}
}

// objectURL builds the path-style URL for a given object key.
func (s *S3Store) objectURL(key string) string {
	return fmt.Sprintf("%s/%s/%s", s.Endpoint, s.Bucket, url.PathEscape(key))
}

func (s *S3Store) Put(ctx context.Context, key string, reader io.Reader, sizeBytes int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, s.objectURL(key), reader)
	if err != nil {
		return fmt.Errorf("creating PUT request: %w", err)
	}
	if sizeBytes >= 0 {
		req.ContentLength = sizeBytes
	}
	resp, err := s.Client.Do(req)
	if err != nil {
		return fmt.Errorf("executing PUT request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("PUT %s returned status %d: %s", key, resp.StatusCode, string(body))
	}
	return nil
}

func (s *S3Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.objectURL(key), nil)
	if err != nil {
		return nil, fmt.Errorf("creating GET request: %w", err)
	}
	resp, err := s.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing GET request: %w", err)
	}
	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, fmt.Errorf("object not found: %s", key)
	}
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("GET %s returned status %d: %s", key, resp.StatusCode, string(body))
	}
	return resp.Body, nil
}

func (s *S3Store) Exists(ctx context.Context, key string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, s.objectURL(key), nil)
	if err != nil {
		return false, fmt.Errorf("creating HEAD request: %w", err)
	}
	resp, err := s.Client.Do(req)
	if err != nil {
		return false, fmt.Errorf("executing HEAD request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if resp.StatusCode >= 300 {
		return false, fmt.Errorf("HEAD %s returned status %d", key, resp.StatusCode)
	}
	return true, nil
}

func (s *S3Store) Delete(ctx context.Context, key string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, s.objectURL(key), nil)
	if err != nil {
		return fmt.Errorf("creating DELETE request: %w", err)
	}
	resp, err := s.Client.Do(req)
	if err != nil {
		return fmt.Errorf("executing DELETE request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("DELETE %s returned status %d: %s", key, resp.StatusCode, string(body))
	}
	return nil
}

// listBucketResult models the S3 ListObjectsV2 XML response.
type listBucketResult struct {
	XMLName  xml.Name   `xml:"ListBucketResult"`
	Contents []s3Object `xml:"Contents"`
}

type s3Object struct {
	Key string `xml:"Key"`
}

func (s *S3Store) List(ctx context.Context, prefix string) ([]string, error) {
	u := fmt.Sprintf("%s/%s?list-type=2&prefix=%s",
		s.Endpoint, s.Bucket, url.QueryEscape(prefix))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("creating LIST request: %w", err)
	}
	resp, err := s.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing LIST request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("LIST returned status %d: %s", resp.StatusCode, string(body))
	}
	var result listBucketResult
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding LIST response: %w", err)
	}
	keys := make([]string, 0, len(result.Contents))
	for _, obj := range result.Contents {
		keys = append(keys, obj.Key)
	}
	return keys, nil
}

// Compile-time interface conformance checks.
var (
	_ Store = (*MemoryStore)(nil)
	_ Store = (*S3Store)(nil)
)
