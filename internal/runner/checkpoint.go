package runner

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"durarun-operator/internal/artifact"

	"k8s.io/apimachinery/pkg/types"
)

// CheckpointManager manages periodic checkpoint creation and upload to an artifact store.
type CheckpointManager struct {
	Store     artifact.Store
	Namespace string
	JobUID    types.UID
	WorkDir   string
	Interval  time.Duration
}

// Start begins periodic checkpointing in a goroutine.
// Returns a stop function that cancels the periodic loop.
func (m *CheckpointManager) Start(ctx context.Context, ordinal int32) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	go func() {
		defer close(done)
		seq := 0
		ticker := time.NewTicker(m.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Best-effort: log errors but keep going.
				_ = m.CreateCheckpoint(ctx, ordinal, seq)
				seq++
			}
		}
	}()

	return func() {
		cancel()
		<-done
	}
}

// CreateCheckpoint creates a single checkpoint and uploads to the store.
func (m *CheckpointManager) CreateCheckpoint(ctx context.Context, ordinal int32, seq int) error {
	var buf bytes.Buffer
	if err := CreateArchive(m.WorkDir, &buf, 0); err != nil {
		return fmt.Errorf("checkpoint: create archive: %w", err)
	}

	key := artifact.CheckpointKey(m.Namespace, m.JobUID, ordinal, fmt.Sprintf("ckpt-%d.tar.gz", seq))

	data := buf.Bytes()
	if err := m.Store.Put(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
		return fmt.Errorf("checkpoint: upload: %w", err)
	}
	return nil
}

// LatestCheckpoint finds the latest checkpoint for this job.
// Returns the ordinal and key, or -1/"" if no checkpoint exists.
func (m *CheckpointManager) LatestCheckpoint(ctx context.Context) (ordinal int32, key string, err error) {
	prefix := fmt.Sprintf("%s/%s/checkpoints/", m.Namespace, m.JobUID)
	keys, err := m.Store.List(ctx, prefix)
	if err != nil {
		return -1, "", fmt.Errorf("checkpoint: list: %w", err)
	}
	if len(keys) == 0 {
		return -1, "", nil
	}

	// Parse ordinals and seq numbers to find the latest.
	type ckptEntry struct {
		ordinal int32
		seq     int
		key     string
	}
	var entries []ckptEntry

	for _, k := range keys {
		// Key format: {namespace}/{jobUID}/checkpoints/{ordinal}/ckpt-{seq}.tar.gz
		rest := strings.TrimPrefix(k, prefix)
		parts := strings.SplitN(rest, "/", 2)
		if len(parts) != 2 {
			continue
		}
		ord, err := strconv.ParseInt(parts[0], 10, 32)
		if err != nil {
			continue
		}
		filename := parts[1]
		// Parse seq from ckpt-{seq}.tar.gz
		seqStr := strings.TrimPrefix(filename, "ckpt-")
		seqStr = strings.TrimSuffix(seqStr, ".tar.gz")
		s, err := strconv.Atoi(seqStr)
		if err != nil {
			continue
		}
		entries = append(entries, ckptEntry{ordinal: int32(ord), seq: s, key: k})
	}

	if len(entries) == 0 {
		return -1, "", nil
	}

	// Sort by ordinal desc, then seq desc to find the latest.
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].ordinal != entries[j].ordinal {
			return entries[i].ordinal > entries[j].ordinal
		}
		return entries[i].seq > entries[j].seq
	})

	latest := entries[0]
	return latest.ordinal, latest.key, nil
}
