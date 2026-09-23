package checkpoint

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// FSStore is a filesystem-backed checkpoint Store.
//
// Directory layout under baseDir:
//
//	<baseDir>/<jobName>/
//	├── LATEST              # contains the seq number of the latest checkpoint
//	├── ckpt-1/
//	│   ├── workspace.tar.gz
//	│   └── manifest.json
//	├── ckpt-2/
//	│   └── ...
type FSStore struct {
	baseDir string
}

// NewFSStore creates a new filesystem-backed checkpoint store rooted at baseDir.
func NewFSStore(baseDir string) *FSStore {
	return &FSStore{baseDir: baseDir}
}

// Save implements Store.Save using a manifest-last atomic commit protocol:
//  1. Create the ckpt-<seq> directory
//  2. Write workspace.tar.gz (copy from reader)
//  3. Compute SHA256 of the written file
//  4. Write manifest.json with the SHA256
//  5. Write LATEST file with seq number
//  6. fsync each file for durability
func (s *FSStore) Save(_ context.Context, jobName string, seq int, manifest *Manifest, workspace io.Reader) error {
	ckptDir := s.ckptDir(jobName, seq)
	if err := os.MkdirAll(ckptDir, 0o755); err != nil {
		return fmt.Errorf("checkpoint: mkdir %s: %w", ckptDir, err)
	}

	// Step 1-2: write workspace archive.
	wsPath := filepath.Join(ckptDir, "workspace.tar.gz")
	if err := writeAndSync(wsPath, workspace); err != nil {
		return fmt.Errorf("checkpoint: write workspace: %w", err)
	}

	// Step 3: compute SHA256 of the workspace file.
	hash, err := HashFile(wsPath)
	if err != nil {
		return fmt.Errorf("checkpoint: hash workspace: %w", err)
	}

	// Step 4: write manifest.json (with hash filled in).
	manifest.SHA256 = hash
	manifestPath := filepath.Join(ckptDir, "manifest.json")
	manifestData, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("checkpoint: marshal manifest: %w", err)
	}
	if err := writeAndSyncBytes(manifestPath, manifestData); err != nil {
		return fmt.Errorf("checkpoint: write manifest: %w", err)
	}

	// Step 5: write LATEST pointer.
	latestPath := filepath.Join(s.jobDir(jobName), "LATEST")
	if err := writeAndSyncBytes(latestPath, []byte(strconv.Itoa(seq))); err != nil {
		return fmt.Errorf("checkpoint: write LATEST: %w", err)
	}

	return nil
}

// RestoreLatest implements Store.RestoreLatest.
// Returns (nil, nil) when no checkpoint exists.
func (s *FSStore) RestoreLatest(_ context.Context, jobName string, destDir string) (*Manifest, error) {
	seq, err := s.readLatestSeq(jobName)
	if err != nil {
		return nil, err
	}
	if seq < 0 {
		return nil, nil
	}

	ckptDir := s.ckptDir(jobName, seq)

	// Read manifest.
	m, err := s.readManifest(ckptDir)
	if err != nil {
		return nil, fmt.Errorf("checkpoint: read manifest seq %d: %w", seq, err)
	}

	// Verify SHA256 of workspace archive.
	wsPath := filepath.Join(ckptDir, "workspace.tar.gz")
	hash, err := HashFile(wsPath)
	if err != nil {
		return nil, fmt.Errorf("checkpoint: hash workspace seq %d: %w", seq, err)
	}
	if hash != m.SHA256 {
		return nil, fmt.Errorf("checkpoint: integrity check failed for seq %d: expected %s, got %s", seq, m.SHA256, hash)
	}

	// Extract workspace.
	wsFile, err := os.Open(wsPath)
	if err != nil {
		return nil, fmt.Errorf("checkpoint: open workspace seq %d: %w", seq, err)
	}
	defer wsFile.Close()

	if err := UntarWorkspace(destDir, wsFile); err != nil {
		return nil, fmt.Errorf("checkpoint: untar workspace seq %d: %w", seq, err)
	}

	return m, nil
}

// GetLatestManifest implements Store.GetLatestManifest.
// Returns (nil, nil) when no checkpoint exists.
func (s *FSStore) GetLatestManifest(_ context.Context, jobName string) (*Manifest, error) {
	seq, err := s.readLatestSeq(jobName)
	if err != nil {
		return nil, err
	}
	if seq < 0 {
		return nil, nil
	}

	ckptDir := s.ckptDir(jobName, seq)
	m, err := s.readManifest(ckptDir)
	if err != nil {
		return nil, fmt.Errorf("checkpoint: read manifest seq %d: %w", seq, err)
	}
	return m, nil
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

func (s *FSStore) jobDir(jobName string) string {
	return filepath.Join(s.baseDir, jobName)
}

func (s *FSStore) ckptDir(jobName string, seq int) string {
	return filepath.Join(s.jobDir(jobName), fmt.Sprintf("ckpt-%d", seq))
}

// readLatestSeq reads the LATEST file for a job. Returns -1 if the file does
// not exist (indicating no checkpoint has been written yet).
func (s *FSStore) readLatestSeq(jobName string) (int, error) {
	latestPath := filepath.Join(s.jobDir(jobName), "LATEST")
	data, err := os.ReadFile(latestPath)
	if err != nil {
		if os.IsNotExist(err) {
			return -1, nil
		}
		return -1, fmt.Errorf("checkpoint: read LATEST: %w", err)
	}
	seq, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return -1, fmt.Errorf("checkpoint: parse LATEST: %w", err)
	}
	return seq, nil
}

// readManifest reads and decodes a manifest.json from the given checkpoint directory.
func (s *FSStore) readManifest(ckptDir string) (*Manifest, error) {
	data, err := os.ReadFile(filepath.Join(ckptDir, "manifest.json"))
	if err != nil {
		return nil, fmt.Errorf("checkpoint: read manifest.json: %w", err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("checkpoint: unmarshal manifest: %w", err)
	}
	return &m, nil
}

// writeAndSync copies all data from r into a new file at path and fsyncs it.
func writeAndSync(path string, r io.Reader) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, r); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// writeAndSyncBytes writes data to a file at path and fsyncs it.
func writeAndSyncBytes(path string, data []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
