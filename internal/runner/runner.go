package runner

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"durarun-operator/wal"
)

const runnerVersion = "v2.0.0"

type Runner struct {
	workDir        string
	walWriter      *wal.Writer
	walReader      *wal.Reader
	hookServer     *HookServer
	recoveryMode   bool
	completedSteps map[string]*wal.CompletedStep
	stepOutputs    map[string]string
	mu             sync.Mutex
	cfg            Config
}

type Config struct {
	WorkDir      string
	RecoveryMode bool
	PoolMode     bool
	ActivatePath string
}

type ActivateSpec struct {
	Image   string            `json:"image"`
	Command []string          `json:"command"`
	Args    []string          `json:"args"`
	Env     map[string]string `json:"env"`
	Prompt  string            `json:"prompt"`
}

type StepBeginResponse struct {
	Skipped      bool   `json:"skipped"`
	CachedOutput string `json:"cached_output,omitempty"`
	Seq          int64  `json:"seq,omitempty"`
}

type StepEndResponse struct {
	Seq       int64 `json:"seq"`
	WALSynced bool  `json:"wal_synced"`
}

type CheckpointManifest struct {
	SchemaVersion int           `json:"schemaVersion"`
	JobUID        string        `json:"jobUID"`
	AttemptUID    string        `json:"attemptUID"`
	CheckpointSeq int          `json:"checkpointSeq"`
	StepProgress  *wal.Manifest `json:"stepProgress,omitempty"`
	CreatedAt     string        `json:"createdAt"`
	RunnerVersion string        `json:"runnerVersion"`
}

func NewRunner(cfg Config) (*Runner, error) {
	r := &Runner{
		workDir:        cfg.WorkDir,
		recoveryMode:   cfg.RecoveryMode,
		completedSteps: make(map[string]*wal.CompletedStep),
		stepOutputs:    make(map[string]string),
		cfg:            cfg,
	}

	w, err := wal.NewWriter(cfg.WorkDir)
	if err != nil {
		return nil, fmt.Errorf("runner: create wal writer: %w", err)
	}
	r.walWriter = w

	if cfg.RecoveryMode {
		reader, err := wal.NewReader(cfg.WorkDir)
		if err != nil {
			return nil, fmt.Errorf("runner: create wal reader: %w", err)
		}
		r.walReader = reader

		manifest, err := reader.BuildManifest()
		if err != nil {
			return nil, fmt.Errorf("runner: build manifest: %w", err)
		}

		for i := range manifest.CompletedSteps {
			cs := &manifest.CompletedSteps[i]
			r.completedSteps[cs.StepID] = cs
			r.stepOutputs[cs.StepID] = cs.OutputRef
		}
	}

	return r, nil
}

func (r *Runner) PrepareEnvironment() (map[string]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	env := make(map[string]string)
	env["AF_WAL_SOCKET"] = filepath.Join(r.workDir, ".wal", "af.sock")

	if !r.recoveryMode {
		env["AF_RECOVERY_MODE"] = "false"
		env["AF_LAST_COMPLETED_STEP"] = ""
		env["AF_COMPLETED_STEPS"] = ""
		env["AF_STEP_OUTPUTS"] = "{}"
		return env, nil
	}

	env["AF_RECOVERY_MODE"] = "true"

	var ids []string
	for id := range r.completedSteps {
		ids = append(ids, id)
	}
	env["AF_COMPLETED_STEPS"] = strings.Join(ids, ",")

	last := ""
	var maxSeq int64
	for _, cs := range r.completedSteps {
		if cs.Seq > maxSeq {
			maxSeq = cs.Seq
			last = cs.StepID
		}
	}
	env["AF_LAST_COMPLETED_STEP"] = last

	outputsJSON, err := json.Marshal(r.stepOutputs)
	if err != nil {
		return nil, fmt.Errorf("runner: marshal step outputs: %w", err)
	}
	env["AF_STEP_OUTPUTS"] = string(outputsJSON)

	return env, nil
}

func (r *Runner) StartHookServer() error {
	socketPath := filepath.Join(r.workDir, ".wal", "af.sock")
	r.hookServer = NewHookServer(socketPath, r)
	return r.hookServer.Start()
}

func (r *Runner) StopHookServer() error {
	if r.hookServer == nil {
		return nil
	}
	return r.hookServer.Stop()
}

func (r *Runner) WaitForActivation(ctx context.Context) (*ActivateSpec, error) {
	activatePath := r.cfg.ActivatePath
	if activatePath == "" {
		activatePath = filepath.Join(r.workDir, "activate.json")
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		data, err := os.ReadFile(activatePath)
		if err == nil {
			var spec ActivateSpec
			if err := json.Unmarshal(data, &spec); err != nil {
				return nil, fmt.Errorf("runner: parse activate.json: %w", err)
			}
			return &spec, nil
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (r *Runner) HandleStepBegin(stepID string) (*StepBeginResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.recoveryMode {
		if cs, ok := r.completedSteps[stepID]; ok {
			return &StepBeginResponse{
				Skipped:      true,
				CachedOutput: cs.OutputRef,
			}, nil
		}
	}

	seq, err := r.walWriter.StepBegin(stepID)
	if err != nil {
		return nil, fmt.Errorf("runner: wal step_begin: %w", err)
	}

	return &StepBeginResponse{
		Skipped: false,
		Seq:     seq,
	}, nil
}

func (r *Runner) HandleStepEnd(stepID string, exitCode int, output string) (*StepEndResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	h := sha256.Sum256([]byte(output))
	outputRef := fmt.Sprintf("sha256:%x", h)

	seq, err := r.walWriter.StepEnd(stepID, exitCode, outputRef)
	if err != nil {
		return nil, fmt.Errorf("runner: wal step_end: %w", err)
	}

	r.stepOutputs[stepID] = outputRef

	return &StepEndResponse{
		Seq:       seq,
		WALSynced: true,
	}, nil
}

func (r *Runner) BuildCheckpointManifest(jobUID, attemptUID string, checkpointSeq int) (*CheckpointManifest, error) {
	reader, err := wal.NewReader(r.workDir)
	if err != nil {
		return nil, fmt.Errorf("runner: create reader for checkpoint: %w", err)
	}

	manifest, err := reader.BuildManifest()
	if err != nil {
		return nil, fmt.Errorf("runner: build manifest for checkpoint: %w", err)
	}

	return &CheckpointManifest{
		SchemaVersion: 2,
		JobUID:        jobUID,
		AttemptUID:    attemptUID,
		CheckpointSeq: checkpointSeq,
		StepProgress:  manifest,
		CreatedAt:     time.Now().UTC().Format(time.RFC3339),
		RunnerVersion: runnerVersion,
	}, nil
}

func (r *Runner) Close() error {
	if err := r.StopHookServer(); err != nil {
		return err
	}
	if r.walWriter != nil {
		if err := r.walWriter.Sync(); err != nil {
			return err
		}
		return r.walWriter.Close()
	}
	return nil
}
