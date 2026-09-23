package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"

	"durarun-operator/internal/isolation"
	"durarun-operator/internal/model"
	"durarun-operator/internal/pool"
	"durarun-operator/internal/runner"
	"durarun-operator/internal/store"
)

type WorkerV2 struct {
	id           string
	store        *store.Store
	poolMgr      *pool.Manager
	isolationMgr *isolation.Manager
	dataDir      string
	hostDataDir  string
	interval     time.Duration
}

func NewV2(s *store.Store, poolMgr *pool.Manager, isolationMgr *isolation.Manager, dataDir, hostDataDir string, interval time.Duration) *WorkerV2 {
	return &WorkerV2{
		id:           uuid.New().String(),
		store:        s,
		poolMgr:      poolMgr,
		isolationMgr: isolationMgr,
		dataDir:      dataDir,
		hostDataDir:  hostDataDir,
		interval:     interval,
	}
}

func (w *WorkerV2) Run(ctx context.Context) error {
	log.Printf("worker-v2 %s starting", w.id)
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			attempt, err := w.store.ClaimScheduledAttempt(ctx, w.id)
			if err != nil {
				log.Printf("worker-v2 %s: ClaimScheduledAttempt error: %v", w.id, err)
				continue
			}
			if attempt == nil {
				continue
			}
			log.Printf("worker-v2 %s: claimed attempt %s for job %s", w.id, attempt.ID, attempt.JobID)
			go func(a model.Attempt) {
				if err := w.execute(ctx, &a); err != nil {
					log.Printf("worker-v2 %s: execute attempt %s error: %v", w.id, a.ID, err)
				}
			}(*attempt)
		}
	}
}

// mapIsolationLevel converts model.IsolationLevel to isolation.Level.
func mapIsolationLevel(ml model.IsolationLevel) isolation.Level {
	switch ml {
	case model.L0Process:
		return isolation.L0Process
	case model.L1GVisor:
		return isolation.L1GVisor
	case model.L2Firecracker:
		return isolation.L2Firecracker
	case model.L3Docker:
		return isolation.L3Docker
	default:
		return isolation.L1GVisor
	}
}

func (w *WorkerV2) execute(ctx context.Context, attempt *model.Attempt) error {
	// 1. Retrieve job.
	job, err := w.store.GetJob(ctx, attempt.JobID)
	if err != nil {
		return err
	}

	// 2. Transition attempt and job state.
	if err := w.store.TransitionAttemptState(ctx, attempt.ID, model.AttemptNew, model.AttemptExecuting, "worker-v2-"+w.id); err != nil {
		return err
	}
	_ = w.store.TransitionJobState(ctx, job.ID, model.JobScheduled, model.JobRunning, "worker-v2-"+w.id)

	// 3. Prepare workDir with prompt.txt.
	workDir := filepath.Join(w.dataDir, job.ID)
	artifactsDir := filepath.Join(workDir, "artifacts")
	if err := os.MkdirAll(artifactsDir, 0755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(workDir, "prompt.txt"), []byte(job.Spec.Prompt), 0644); err != nil {
		return err
	}

	// 4. Check latest checkpoint for recovery mode.
	recoveryMode := false
	cp, err := w.store.GetLatestCheckpoint(ctx, job.ID)
	if err == nil && cp != nil {
		// Write checkpoint data to workDir for the runner to pick up.
		data, _ := json.Marshal(cp.Data)
		os.WriteFile(filepath.Join(workDir, "checkpoint.json"), data, 0644)

		// Detect recovery mode: unmarshal checkpoint data and look for WAL manifest
		// information with completed steps.
		var manifest struct {
			StepProgress *struct {
				CompletedSteps []json.RawMessage `json:"completedSteps"`
			} `json:"stepProgress"`
		}
		if json.Unmarshal(cp.Data, &manifest) == nil && manifest.StepProgress != nil && len(manifest.StepProgress.CompletedSteps) > 0 {
			recoveryMode = true
		}
	}

	// 5. Create runner.
	rn, err := runner.NewRunner(runner.Config{
		WorkDir:      workDir,
		RecoveryMode: recoveryMode,
		PoolMode:     true,
	})
	if err != nil {
		w.store.TransitionAttemptState(ctx, attempt.ID, model.AttemptExecuting, model.AttemptFailed, "runner-init-error")
		os.RemoveAll(workDir)
		return fmt.Errorf("worker-v2: create runner: %w", err)
	}
	defer rn.Close()

	// 6. Start runner hook server.
	if err := rn.StartHookServer(); err != nil {
		w.store.TransitionAttemptState(ctx, attempt.ID, model.AttemptExecuting, model.AttemptFailed, "hook-server-error")
		os.RemoveAll(workDir)
		return fmt.Errorf("worker-v2: start hook server: %w", err)
	}

	// 7. Prepare environment vars from runner.
	runnerEnv, err := rn.PrepareEnvironment()
	if err != nil {
		w.store.TransitionAttemptState(ctx, attempt.ID, model.AttemptExecuting, model.AttemptFailed, "env-prepare-error")
		os.RemoveAll(workDir)
		return fmt.Errorf("worker-v2: prepare environment: %w", err)
	}

	// Merge runner env into job env.
	env := make(map[string]string)
	for k, v := range job.Spec.Env {
		env[k] = v
	}
	for k, v := range runnerEnv {
		env[k] = v
	}

	// 8. Try pool claim.
	var claimedPodID string
	claimResult, claimErr := w.poolMgr.Claim(job.ID)
	if claimErr == nil && claimResult != nil {
		// Warm hit — record pool metadata on the attempt.
		claimedPodID = claimResult.Pod.ID
		log.Printf("worker-v2 %s: pool warm hit for job %s, pod %s, latency %v",
			w.id, job.ID, claimedPodID, claimResult.ClaimLatency)
		w.store.UpdateAttemptPoolInfo(ctx, attempt.ID, claimResult.Pod.PoolID,
			claimResult.ClaimLatency.Milliseconds(), claimResult.WarmHit)
	} else {
		log.Printf("worker-v2 %s: pool claim miss for job %s (%v), falling back to isolation manager",
			w.id, job.ID, claimErr)
	}

	// 9. Start sandbox via isolation manager.
	isoLevel := mapIsolationLevel(job.Spec.Isolation.Level)
	resolvedLevel, resolveErr := w.isolationMgr.ResolveLevel(isoLevel, nil)
	if resolveErr != nil {
		w.failAndCleanup(ctx, attempt, job, claimedPodID, workDir, "isolation-resolve-error")
		return fmt.Errorf("worker-v2: resolve isolation level: %w", resolveErr)
	}

	sandboxCtx, sandboxCancel := context.WithTimeout(ctx, job.Spec.TimeoutDuration())
	defer sandboxCancel()

	startResult, startErr := w.isolationMgr.StartSandbox(sandboxCtx, resolvedLevel, isolation.SandboxRunConfig{
		Image:       job.Spec.Image,
		Env:         env,
		CPULimit:    job.Spec.Resources.CPULimit,
		MemoryLimit: job.Spec.Resources.MemoryLimit,
		WorkDir:     workDir,
		Timeout:     job.Spec.TimeoutDuration(),
	})
	if startErr != nil {
		w.failAndCleanup(ctx, attempt, job, claimedPodID, workDir, "sandbox-start-error")
		return fmt.Errorf("worker-v2: start sandbox: %w", startErr)
	}
	log.Printf("worker-v2 %s: sandbox started for job %s at level %s (latency %v)",
		w.id, job.ID, startResult.Level, startResult.StartLatency)

	// 10. Heartbeat loop — also periodically builds checkpoint manifests from runner.
	stopHB := make(chan struct{})
	hbDone := make(chan struct{})
	checkpointSeq := 0
	go func() {
		defer close(hbDone)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stopHB:
				return
			case <-ticker.C:
				if err := w.store.UpdateHeartbeat(ctx, attempt.ID); err != nil {
					log.Printf("worker-v2 %s: UpdateHeartbeat error: %v", w.id, err)
				}

				// Build checkpoint manifest from runner WAL state.
				checkpointSeq++
				cm, err := rn.BuildCheckpointManifest(job.ID, attempt.ID, checkpointSeq)
				if err != nil {
					log.Printf("worker-v2 %s: BuildCheckpointManifest error: %v", w.id, err)
					continue
				}
				cmData, err := json.Marshal(cm)
				if err != nil {
					log.Printf("worker-v2 %s: marshal checkpoint manifest error: %v", w.id, err)
					continue
				}
				stepIdx := 0
				if cm.StepProgress != nil {
					stepIdx = len(cm.StepProgress.CompletedSteps)
				}
				w.store.SaveCheckpoint(ctx, attempt.ID, job.ID, stepIdx, json.RawMessage(cmData))

				// Update step progress on attempt.
				if cm.StepProgress != nil {
					w.store.UpdateAttemptStepProgress(ctx, attempt.ID,
						cm.StepProgress.LastCompleted, len(cm.StepProgress.CompletedSteps))
				}
			}
		}
	}()

	// Wait for sandbox to finish.
	// The isolation manager's StartSandbox is blocking for L3Docker (runs the
	// full container). For simulated levels (L0-L2), it returns after startup
	// latency. We read the result file that the runner produces to determine
	// exit status.
	exitCode := 0
	var runLogs string

	// Read result from workDir once sandbox completes.
	resultPath := filepath.Join(workDir, "result.json")
	logPath := filepath.Join(workDir, "output.log")

	// Wait for the sandbox timeout or context cancellation.
	// For non-Docker levels the sandbox has already started; we poll for
	// completion by checking for a result file.
	waitDone := make(chan struct{})
	var sandboxErr error
	go func() {
		defer close(waitDone)
		deadline := time.After(job.Spec.TimeoutDuration())
		pollTicker := time.NewTicker(500 * time.Millisecond)
		defer pollTicker.Stop()
		for {
			select {
			case <-sandboxCtx.Done():
				sandboxErr = sandboxCtx.Err()
				return
			case <-deadline:
				sandboxErr = fmt.Errorf("worker-v2: sandbox timeout")
				return
			case <-pollTicker.C:
				if _, err := os.Stat(resultPath); err == nil {
					return
				}
			}
		}
	}()
	<-waitDone

	close(stopHB)
	<-hbDone

	// Read result file if available.
	if data, err := os.ReadFile(resultPath); err == nil {
		var result struct {
			ExitCode int    `json:"exitCode"`
			Logs     string `json:"logs"`
		}
		if json.Unmarshal(data, &result) == nil {
			exitCode = result.ExitCode
			runLogs = result.Logs
		}
	}
	if logData, err := os.ReadFile(logPath); err == nil && runLogs == "" {
		runLogs = string(logData)
	}

	// 11. Build final checkpoint manifest and save to store.
	checkpointSeq++
	finalManifest, err := rn.BuildCheckpointManifest(job.ID, attempt.ID, checkpointSeq)
	if err == nil {
		cmData, _ := json.Marshal(finalManifest)
		stepIdx := 0
		if finalManifest.StepProgress != nil {
			stepIdx = len(finalManifest.StepProgress.CompletedSteps)
		}
		w.store.SaveCheckpoint(ctx, attempt.ID, job.ID, stepIdx, json.RawMessage(cmData))
	}

	// Also save raw checkpoint.json if present.
	checkpointPath := filepath.Join(workDir, "checkpoint.json")
	if data, err := os.ReadFile(checkpointPath); err == nil {
		w.store.SaveCheckpoint(ctx, attempt.ID, job.ID, 0, json.RawMessage(data))
	}

	// Collect and save artifacts.
	entries, _ := os.ReadDir(artifactsDir)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(artifactsDir, entry.Name()))
		if err != nil {
			log.Printf("worker-v2 %s: read artifact %s error: %v", w.id, entry.Name(), err)
			continue
		}
		if err := w.store.SaveArtifact(ctx, job.ID, entry.Name(), data); err != nil {
			log.Printf("worker-v2 %s: SaveArtifact %s error: %v", w.id, entry.Name(), err)
		}
	}

	// 12. Release pool pod if claimed.
	if claimedPodID != "" {
		if err := w.poolMgr.Release(claimedPodID); err != nil {
			log.Printf("worker-v2 %s: pool release pod %s error: %v", w.id, claimedPodID, err)
		}
	}

	// 13. Close runner (deferred above) and finalize states.
	if sandboxErr != nil {
		log.Printf("worker-v2 %s: sandbox error: %v", w.id, sandboxErr)
		w.store.SaveLog(ctx, job.ID, attempt.ID, "sandbox error: "+sandboxErr.Error())
		w.store.TransitionAttemptState(ctx, attempt.ID, model.AttemptExecuting, model.AttemptFailed, "sandbox-error")
		os.RemoveAll(workDir)
		return sandboxErr
	}

	w.store.SaveLog(ctx, job.ID, attempt.ID, runLogs)

	if exitCode == 0 {
		w.store.TransitionAttemptState(ctx, attempt.ID, model.AttemptExecuting, model.AttemptCompleted, "exit-0")
		w.store.TransitionJobState(ctx, job.ID, model.JobRunning, model.JobCompleted, "exit-0")
		log.Printf("worker-v2 %s: job %s completed", w.id, job.ID)
	} else {
		w.store.TransitionAttemptState(ctx, attempt.ID, model.AttemptExecuting, model.AttemptFailed, fmt.Sprintf("exit-%d", exitCode))
		log.Printf("worker-v2 %s: job %s attempt failed with exit code %d, leaving job for reconciler", w.id, job.ID, exitCode)
	}

	os.RemoveAll(workDir)
	return nil
}

// failAndCleanup is a helper to mark an attempt failed, release pool pod, and
// remove the workDir.
func (w *WorkerV2) failAndCleanup(ctx context.Context, attempt *model.Attempt, job *model.Job, podID, workDir, trigger string) {
	w.store.TransitionAttemptState(ctx, attempt.ID, model.AttemptExecuting, model.AttemptFailed, trigger)
	if podID != "" {
		if err := w.poolMgr.Release(podID); err != nil {
			log.Printf("worker-v2 %s: pool release pod %s error during cleanup: %v", w.id, podID, err)
		}
	}
	os.RemoveAll(workDir)
}
