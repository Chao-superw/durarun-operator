package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"durarun-operator/internal/checkpoint"
	"durarun-operator/internal/runner"
)

func main() {
	var (
		mode               string
		workDir            string
		checkpointDir      string
		jobName            string
		attemptNumber      int
		checkpointInterval int
	)

	flag.StringVar(&mode, "mode", "sidecar", "run mode: restore or sidecar")
	flag.StringVar(&workDir, "work-dir", "/workspace", "workspace directory")
	flag.StringVar(&checkpointDir, "checkpoint-dir", "/checkpoints", "checkpoint store base directory")
	flag.StringVar(&jobName, "job-name", "", "AgentJob name (required)")
	flag.IntVar(&attemptNumber, "attempt-number", 1, "attempt number")
	flag.IntVar(&checkpointInterval, "checkpoint-interval", 30, "checkpoint interval in seconds")
	flag.Parse()

	if jobName == "" {
		log.Fatal("--job-name is required")
	}

	switch mode {
	case "restore":
		if err := runRestore(jobName, workDir, checkpointDir); err != nil {
			log.Fatalf("restore failed: %v", err)
		}
	case "sidecar":
		if err := runSidecar(jobName, workDir, checkpointDir, attemptNumber, checkpointInterval); err != nil {
			log.Fatalf("sidecar failed: %v", err)
		}
	default:
		log.Fatalf("unknown mode: %s (must be restore or sidecar)", mode)
	}
}

// runRestore runs as an initContainer to restore the latest checkpoint into the workspace.
func runRestore(jobName, workDir, checkpointDir string) error {
	ctx := context.Background()
	store := checkpoint.NewFSStore(checkpointDir)

	manifest, err := store.RestoreLatest(ctx, jobName, workDir)
	if err != nil {
		return fmt.Errorf("restore latest checkpoint: %w", err)
	}
	if manifest == nil {
		log.Println("no checkpoint found, starting fresh")
		return nil
	}

	log.Printf("restored checkpoint: job=%s attempt=%d seq=%d created=%s completedSteps=%d",
		manifest.JobName, manifest.AttemptNumber, manifest.CheckpointSeq,
		manifest.CreatedAt, len(manifest.CompletedSteps))
	return nil
}

// runSidecar runs alongside the main container, periodically checkpointing the workspace.
func runSidecar(jobName, workDir, checkpointDir string, attemptNum, intervalSec int) error {
	store := checkpoint.NewFSStore(checkpointDir)

	// Detect recovery mode: if the WAL directory exists a checkpoint was restored.
	walDir := filepath.Join(workDir, ".wal")
	recoveryMode := false
	if info, err := os.Stat(walDir); err == nil && info.IsDir() {
		recoveryMode = true
		log.Println("WAL directory found, entering recovery mode")
	}

	r, err := runner.NewRunner(runner.Config{
		WorkDir:      workDir,
		RecoveryMode: recoveryMode,
	})
	if err != nil {
		return fmt.Errorf("create runner: %w", err)
	}

	if err := r.StartHookServer(); err != nil {
		return fmt.Errorf("start hook server: %w", err)
	}
	log.Println("hook server started")

	// Set up signal handling.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	ticker := time.NewTicker(time.Duration(intervalSec) * time.Second)
	defer ticker.Stop()

	seq := 0
	exitCodePath := filepath.Join(workDir, ".exit-code")

	for {
		select {
		case <-ctx.Done():
			log.Println("received shutdown signal, performing final checkpoint")
			seq++
			if err := doCheckpoint(context.Background(), store, workDir, jobName, attemptNum, seq); err != nil {
				log.Printf("final checkpoint failed: %v", err)
			}
			if err := r.Close(); err != nil {
				log.Printf("runner close error: %v", err)
			}
			log.Println("sidecar exiting")
			return nil

		case <-ticker.C:
			seq++
			if err := doCheckpoint(ctx, store, workDir, jobName, attemptNum, seq); err != nil {
				log.Printf("checkpoint seq=%d failed: %v", seq, err)
			} else {
				log.Printf("checkpoint seq=%d completed", seq)
			}

			// Check whether the main container has exited.
			if _, err := os.Stat(exitCodePath); err == nil {
				log.Println("main container exited, performing final checkpoint")
				seq++
				if err := doCheckpoint(context.Background(), store, workDir, jobName, attemptNum, seq); err != nil {
					log.Printf("final checkpoint failed: %v", err)
				}
				if err := r.Close(); err != nil {
					log.Printf("runner close error: %v", err)
				}
				log.Println("sidecar exiting after main container completion")
				return nil
			}
		}
	}
}

// doCheckpoint tars the workspace and persists it through the checkpoint store.
func doCheckpoint(ctx context.Context, store checkpoint.Store, workDir, jobName string, attemptNum, seq int) error {
	tmpFile, err := os.CreateTemp("", "checkpoint-*.tar.gz")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if err := checkpoint.TarWorkspace(workDir, tmpFile); err != nil {
		return fmt.Errorf("tar workspace: %w", err)
	}

	// Seek back to the beginning so the store can read the full archive.
	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek temp file: %w", err)
	}

	manifest := &checkpoint.Manifest{
		SchemaVersion:  1,
		JobName:        jobName,
		AttemptNumber:  attemptNum,
		CheckpointSeq: seq,
		CreatedAt:      time.Now().UTC().Format(time.RFC3339),
	}

	if err := store.Save(ctx, jobName, seq, manifest, tmpFile); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}

	return nil
}
