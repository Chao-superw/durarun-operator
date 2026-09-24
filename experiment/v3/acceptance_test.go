package v3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"durarun-operator/protect"
	afrt "durarun-operator/runtime"
	"durarun-operator/wal"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const defaultStepDur = 100 * time.Millisecond

var (
	buildOnce     sync.Once
	testAgentPath string
	testAgentErr  error
)

// ensureTestAgent builds the test-agent binary once.
func ensureTestAgent(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "v3-agent-*")
		if err != nil {
			testAgentErr = err
			return
		}
		testAgentPath = filepath.Join(dir, "test-agent")
		cmd := exec.Command("go", "build", "-o", testAgentPath, "./cmd/test-agent/")
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			testAgentErr = fmt.Errorf("build test-agent: %w\n%s", err, stderr.String())
		}
	})
	require.NoError(t, testAgentErr, "test-agent must compile")
	return testAgentPath
}

// moduleRoot returns the project root (parent of go.mod).
func moduleRoot(t *testing.T) string {
	t.Helper()
	cmd := exec.Command("go", "env", "GOMOD")
	out, err := cmd.Output()
	require.NoError(t, err)
	return filepath.Dir(strings.TrimSpace(string(out)))
}

// resultsDir returns the path to store test result artifacts.
// It checks DURARUN_TEST_ARTIFACT_DIR first, falling back to t.TempDir().
func resultsDir(t *testing.T) string {
	t.Helper()
	if dir := os.Getenv("DURARUN_TEST_ARTIFACT_DIR"); dir != "" {
		rd := filepath.Join(dir, "experiment-v3")
		require.NoError(t, os.MkdirAll(rd, 0o755))
		return rd
	}
	return t.TempDir()
}

// agentResult is the JSON produced by test-agent.
type agentResult struct {
	Mode           string `json:"mode"`
	CompletedSteps int    `json:"completed_steps"`
	SkippedSteps   int    `json:"skipped_steps"`
	TotalSteps     int    `json:"total_steps"`
	WallTimeMs     int64  `json:"wall_time_ms"`
}

// runAgent runs the test-agent binary and returns stdout as bytes.
func runAgent(t *testing.T, args ...string) ([]byte, error) {
	t.Helper()
	binary := ensureTestAgent(t)
	cmd := exec.Command(binary, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return stdout.Bytes(), fmt.Errorf("%w: %s", err, stderr.String())
	}
	return stdout.Bytes(), nil
}

// runAgentResult runs the test-agent and decodes the result JSON from a file.
func runAgentResult(t *testing.T, resultPath string, args ...string) (*agentResult, []byte, error) {
	t.Helper()
	binary := ensureTestAgent(t)
	args = append(args, "--result-file", resultPath)
	cmd := exec.Command(binary, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return nil, stdout.Bytes(), fmt.Errorf("%w: %s", err, stderr.String())
	}
	data, readErr := os.ReadFile(resultPath)
	if readErr != nil {
		return nil, stdout.Bytes(), readErr
	}
	var res agentResult
	if jsonErr := json.Unmarshal(data, &res); jsonErr != nil {
		return nil, stdout.Bytes(), jsonErr
	}
	return &res, stdout.Bytes(), nil
}

// crashAgent runs the test-agent expecting a crash (exit code 1).
func crashAgent(t *testing.T, args ...string) {
	t.Helper()
	binary := ensureTestAgent(t)
	cmd := exec.Command(binary, args...)
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	require.Error(t, err)
	var exitErr *exec.ExitError
	require.True(t, errors.As(err, &exitErr), "expected ExitError, got %T", err)
	require.Equal(t, 1, exitErr.ExitCode(), "crash should exit with code 1")
}

// run7StepRuntime runs a 7-step workflow through the AF runtime and returns wall time.
func run7StepRuntime(t *testing.T, dir string) time.Duration {
	t.Helper()
	rt, err := afrt.New(afrt.Config{WorkDir: dir})
	require.NoError(t, err)

	start := time.Now()
	err = rt.Run(context.Background(), func(ac *afrt.Context) error {
		for i := 1; i <= 7; i++ {
			if stepErr := ac.Step(fmt.Sprintf("step-%d", i), func(ctx context.Context) error {
				return SHA256Work(ctx, defaultStepDur)
			}); stepErr != nil {
				return stepErr
			}
		}
		return nil
	})
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.NoError(t, rt.Close())
	return elapsed
}

// ---------------------------------------------------------------------------
// P-series: Performance
// ---------------------------------------------------------------------------

func TestP2_S1_RecoverySkipRate(t *testing.T) {
	const trials = 5
	const crashStep = 4
	var rates []float64
	var trialData []map[string]interface{}

	for i := 0; i < trials; i++ {
		dir := t.TempDir()
		crashAgent(t, "--workdir", dir, "--crash-at", fmt.Sprint(crashStep), "--steps", "7", "--step-duration", defaultStepDur.String())

		out, err := runAgent(t, "--workdir", dir, "--recover", "--steps", "7", "--step-duration", defaultStepDur.String())
		require.NoError(t, err)

		var res agentResult
		require.NoError(t, json.Unmarshal(out, &res))

		completedBefore := crashStep - 1
		rate := float64(res.SkippedSteps) / float64(completedBefore) * 100
		rates = append(rates, rate)
		trialData = append(trialData, map[string]interface{}{
			"trial":            i + 1,
			"completed_before": completedBefore,
			"skipped":          res.SkippedSteps,
			"skip_rate%":       rate,
		})
		t.Logf("trial %d: completedBefore=%d  skipped=%d  rate=%.0f%%", i+1, completedBefore, res.SkippedSteps, rate)
	}

	mean := CalcMean(rates)
	pass := mean >= 60.0
	t.Logf("mean skip rate=%.0f%%  pass=%v", mean, pass)
	assert.True(t, pass, "recovery skip rate should be >= 60%%")

	_ = WriteSingleReport(filepath.Join(resultsDir(t), "P2_recovery_skip_rate.json"), MetricReport{
		MetricID: "P2-S1", Description: "Recovery skip rate",
		Target: ">=60%", Trials: trialData, Mean: mean, Stddev: CalcStddev(rates, mean), Pass: pass,
	})
}

func TestP3_S1_RecoveryTime(t *testing.T) {
	const trials = 5
	const crashStep = 5
	var ratios []float64
	var trialData []map[string]interface{}

	for i := 0; i < trials; i++ {
		dir := t.TempDir()
		crashAgent(t, "--workdir", dir, "--crash-at", fmt.Sprint(crashStep), "--steps", "7", "--step-duration", defaultStepDur.String())

		out, err := runAgent(t, "--workdir", dir, "--recover", "--steps", "7", "--step-duration", defaultStepDur.String())
		require.NoError(t, err)

		var res agentResult
		require.NoError(t, json.Unmarshal(out, &res))

		native := NativeRun7Steps(defaultStepDur)
		ratio := float64(res.WallTimeMs) / float64(native.Milliseconds())
		ratios = append(ratios, ratio)
		trialData = append(trialData, map[string]interface{}{
			"trial":          i + 1,
			"recovery_ms":    res.WallTimeMs,
			"native_ms":      native.Milliseconds(),
			"ratio":          ratio,
		})
		t.Logf("trial %d: recovery=%dms  native=%v  ratio=%.2f", i+1, res.WallTimeMs, native, ratio)
	}

	mean := CalcMean(ratios)
	pass := mean <= 0.50
	t.Logf("mean ratio=%.3f  pass=%v", mean, pass)
	assert.True(t, pass, "recovery time ratio should be <= 0.50x native")

	_ = WriteSingleReport(filepath.Join(resultsDir(t), "P3_recovery_time.json"), MetricReport{
		MetricID: "P3-S1", Description: "Recovery wall-clock time vs native",
		Target: "<=0.50x", Trials: trialData, Mean: mean, Stddev: CalcStddev(ratios, mean), Pass: pass,
	})
}

func TestP7_CodeLineCount(t *testing.T) {
	root := moduleRoot(t)
	cmd := exec.Command("sh", "-c",
		`find runtime/ wal/ observe/ protect/ -name '*.go' ! -name '*_test.go' -print0 | xargs -0 cat | wc -l`)
	cmd.Dir = root
	out, err := cmd.Output()
	require.NoError(t, err)

	var lines int
	_, err = fmt.Sscanf(strings.TrimSpace(string(out)), "%d", &lines)
	require.NoError(t, err)
	t.Logf("total non-test lines: %d", lines)

	pass := lines <= 2000
	assert.True(t, pass, "code line count should be <= 2000, got %d", lines)

	_ = WriteSingleReport(filepath.Join(resultsDir(t), "P7_code_line_count.json"), MetricReport{
		MetricID: "P7", Description: "Source code line count (runtime+wal+observe+protect+sandbox+pool, excl. tests)",
		Target: "<=2000", Trials: []map[string]interface{}{{"lines": lines}}, Mean: float64(lines), Pass: pass,
	})
}

// ---------------------------------------------------------------------------
// R-series: Recovery
// ---------------------------------------------------------------------------

func TestR1_BasicRecovery(t *testing.T) {
	dir := t.TempDir()
	crashAgent(t, "--workdir", dir, "--crash-at", "4", "--steps", "7", "--step-duration", defaultStepDur.String())

	out, err := runAgent(t, "--workdir", dir, "--recover", "--steps", "7", "--step-duration", defaultStepDur.String())
	require.NoError(t, err)

	var res agentResult
	require.NoError(t, json.Unmarshal(out, &res))

	assert.Equal(t, 3, res.SkippedSteps, "should skip 3 already-completed steps")
	assert.Equal(t, 7, res.CompletedSteps, "all 7 steps should complete after recovery")
	assert.Equal(t, 7, res.TotalSteps)

	_ = WriteSingleReport(filepath.Join(resultsDir(t), "R1_basic_recovery.json"), MetricReport{
		MetricID: "R1", Description: "Basic crash-at-4 recovery",
		Target: "skipped=3, completed=7", Pass: res.SkippedSteps == 3 && res.CompletedSteps == 7,
		Trials: []map[string]interface{}{{"skipped": res.SkippedSteps, "completed": res.CompletedSteps}},
	})
}

func TestR2_WALContinuity(t *testing.T) {
	dir := t.TempDir()
	crashAgent(t, "--workdir", dir, "--crash-at", "4", "--steps", "7", "--step-duration", defaultStepDur.String())

	_, err := runAgent(t, "--workdir", dir, "--recover", "--steps", "7", "--step-duration", defaultStepDur.String())
	require.NoError(t, err)

	reader, err := wal.NewReader(dir)
	require.NoError(t, err)

	records, err := reader.ReadAll()
	require.NoError(t, err, "WAL seq should be continuous with no gaps")
	require.True(t, len(records) > 0, "WAL should contain records")

	// Verify seq continuity explicitly.
	for i, r := range records {
		assert.Equal(t, int64(i+1), r.Seq, "seq continuity broken at index %d", i)
	}
	t.Logf("WAL records=%d, all seqs continuous", len(records))
}

func TestR3_RecoveryOTELSkipped(t *testing.T) {
	dir := t.TempDir()
	crashAgent(t, "--workdir", dir, "--crash-at", "4", "--steps", "7", "--step-duration", "50ms")

	resFile := filepath.Join(t.TempDir(), "result.json")
	_, stdout, err := runAgentResult(t, resFile,
		"--workdir", dir, "--recover",
		"--steps", "7", "--step-duration", "50ms",
		"--trace-exporter", "stdout",
	)
	require.NoError(t, err)

	output := string(stdout)
	assert.Contains(t, output, "af.step.skipped",
		"recovery trace output should contain skipped step attribute")
	t.Logf("OTEL stdout output contains af.step.skipped: OK (output length=%d)", len(output))
}

func TestR4_MultiCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	dur := "50ms"

	// Crash 1: at step 2
	crashAgent(t, "--workdir", dir, "--crash-at", "2", "--steps", "7", "--step-duration", dur)
	// Crash 2: recover + crash at step 4
	crashAgent(t, "--workdir", dir, "--recover", "--crash-at", "4", "--steps", "7", "--step-duration", dur)
	// Crash 3: recover + crash at step 6
	crashAgent(t, "--workdir", dir, "--recover", "--crash-at", "6", "--steps", "7", "--step-duration", dur)
	// Final recovery
	out, err := runAgent(t, "--workdir", dir, "--recover", "--steps", "7", "--step-duration", dur)
	require.NoError(t, err)

	var res agentResult
	require.NoError(t, json.Unmarshal(out, &res))
	assert.Equal(t, 7, res.CompletedSteps, "all 7 steps should complete after 3 crashes + final recovery")
	t.Logf("R4: completedSteps=%d  skipped=%d", res.CompletedSteps, res.SkippedSteps)
}

func TestR5_WALCorruptDetection(t *testing.T) {
	dir := t.TempDir()

	// Write a normal WAL via a runtime run.
	rt, err := afrt.New(afrt.Config{WorkDir: dir})
	require.NoError(t, err)
	require.NoError(t, rt.Run(context.Background(), func(ac *afrt.Context) error {
		for i := 1; i <= 3; i++ {
			if e := ac.Step(fmt.Sprintf("step-%d", i), func(ctx context.Context) error {
				return SHA256Work(ctx, 20*time.Millisecond)
			}); e != nil {
				return e
			}
		}
		return nil
	}))
	require.NoError(t, rt.Close())

	// Find the WAL file and truncate it.
	walDir := filepath.Join(dir, ".wal")
	entries, err := os.ReadDir(walDir)
	require.NoError(t, err)

	var walFile string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "wal-") && strings.HasSuffix(e.Name(), ".log") {
			walFile = filepath.Join(walDir, e.Name())
		}
	}
	require.NotEmpty(t, walFile, "WAL log file should exist")

	info, err := os.Stat(walFile)
	require.NoError(t, err)
	require.True(t, info.Size() > 50, "WAL file should be larger than 50 bytes")

	require.NoError(t, os.Truncate(walFile, info.Size()-50))

	// Read should fail with corruption error.
	reader, err := wal.NewReader(dir)
	require.NoError(t, err)
	_, err = reader.ReadAll()
	assert.Error(t, err, "ReadAll should detect truncated/corrupt WAL")
	t.Logf("R5: corruption detected: %v", err)
}

// ---------------------------------------------------------------------------
// F-series: Functional
// ---------------------------------------------------------------------------

func TestF1_ZeroDependencyStartup(t *testing.T) {
	dir := t.TempDir()
	rt, err := afrt.New(afrt.Config{WorkDir: dir})
	require.NoError(t, err)

	err = rt.Run(context.Background(), func(ac *afrt.Context) error {
		for i := 1; i <= 7; i++ {
			if e := ac.Step(fmt.Sprintf("step-%d", i), func(ctx context.Context) error {
				return SHA256Work(ctx, 20*time.Millisecond)
			}); e != nil {
				return e
			}
		}
		return nil
	})
	require.NoError(t, err, "full 7-step pipeline should succeed with zero external dependencies")
	require.NoError(t, rt.Close())
}

func TestF2_WALIntegrity(t *testing.T) {
	dir := t.TempDir()
	rt, err := afrt.New(afrt.Config{WorkDir: dir})
	require.NoError(t, err)

	err = rt.Run(context.Background(), func(ac *afrt.Context) error {
		for i := 1; i <= 7; i++ {
			if e := ac.Step(fmt.Sprintf("step-%d", i), func(ctx context.Context) error {
				return SHA256Work(ctx, 20*time.Millisecond)
			}); e != nil {
				return e
			}
		}
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, rt.Close())

	reader, err := wal.NewReader(dir)
	require.NoError(t, err)
	records, err := reader.ReadAll()
	require.NoError(t, err)

	assert.Equal(t, 14, len(records), "should have 14 records: 7 begin + 7 end")

	for i, r := range records {
		assert.Equal(t, int64(i+1), r.Seq, "seq must be continuous")
	}

	// Verify alternating begin/end per step.
	for s := 0; s < 7; s++ {
		begin := records[s*2]
		end := records[s*2+1]
		assert.Equal(t, wal.TypeStepBegin, begin.Type)
		assert.Equal(t, wal.TypeStepEnd, end.Type)
		assert.Equal(t, begin.StepID, end.StepID)
	}
}

func TestF3_StepTimeout(t *testing.T) {
	dir := t.TempDir()
	rt, err := afrt.New(afrt.Config{WorkDir: dir})
	require.NoError(t, err)
	defer rt.Close()

	start := time.Now()
	err = rt.Run(context.Background(), func(ac *afrt.Context) error {
		return ac.Step("timeout-step", func(ctx context.Context) error {
			return SHA256Work(ctx, 2*time.Second)
		}, afrt.WithTimeout(500*time.Millisecond))
	})
	elapsed := time.Since(start)

	require.Error(t, err, "step should fail with timeout")
	assert.InDelta(t, 500, float64(elapsed.Milliseconds()), 100,
		"step should terminate within 500ms +/- 100ms")
	t.Logf("F3: elapsed=%v", elapsed)
}

func TestF4_JobTimeout(t *testing.T) {
	dir := t.TempDir()
	rt, err := afrt.New(afrt.Config{WorkDir: dir, JobTimeout: 2 * time.Second})
	require.NoError(t, err)
	defer rt.Close()

	completedSteps := 0
	start := time.Now()
	err = rt.Run(context.Background(), func(ac *afrt.Context) error {
		for i := 1; i <= 7; i++ {
			if e := ac.Step(fmt.Sprintf("step-%d", i), func(ctx context.Context) error {
				return SHA256Work(ctx, 500*time.Millisecond)
			}); e != nil {
				return e
			}
			completedSteps++
		}
		return nil
	})
	elapsed := time.Since(start)

	require.Error(t, err, "job should fail with timeout")
	assert.InDelta(t, 2000, float64(elapsed.Milliseconds()), 200,
		"job should terminate at ~2s")
	assert.True(t, completedSteps >= 3 && completedSteps <= 4,
		"expected 3-4 completed steps, got %d", completedSteps)
	t.Logf("F4: elapsed=%v  completedSteps=%d", elapsed, completedSteps)
}

func TestF5_RetrySuccess(t *testing.T) {
	dir := t.TempDir()
	rt, err := afrt.New(afrt.Config{WorkDir: dir})
	require.NoError(t, err)
	defer rt.Close()

	attempt := 0
	err = rt.Run(context.Background(), func(ac *afrt.Context) error {
		return ac.Step("retry-step", func(ctx context.Context) error {
			attempt++
			if attempt < 3 {
				return fmt.Errorf("intentional failure #%d", attempt)
			}
			return SHA256Work(ctx, 20*time.Millisecond)
		}, afrt.WithRetry(protect.RetryConfig{
			MaxAttempts:  3,
			InitialDelay: 10 * time.Millisecond,
		}))
	})

	require.NoError(t, err, "step should succeed on 3rd attempt")
	assert.Equal(t, 3, attempt, "should have executed 3 attempts")
}

func TestF6_RetryWALRecord(t *testing.T) {
	dir := t.TempDir()
	rt, err := afrt.New(afrt.Config{WorkDir: dir})
	require.NoError(t, err)

	attempt := 0
	err = rt.Run(context.Background(), func(ac *afrt.Context) error {
		return ac.Step("retry-step", func(ctx context.Context) error {
			attempt++
			if attempt < 3 {
				return fmt.Errorf("intentional failure #%d", attempt)
			}
			return SHA256Work(ctx, 20*time.Millisecond)
		}, afrt.WithRetry(protect.RetryConfig{
			MaxAttempts:  3,
			InitialDelay: 10 * time.Millisecond,
		}))
	})
	require.NoError(t, err)
	require.NoError(t, rt.Close())

	reader, err := wal.NewReader(dir)
	require.NoError(t, err)
	records, err := reader.ReadAll()
	require.NoError(t, err)

	retryCount := 0
	for _, r := range records {
		if r.Type == wal.TypeStepRetry {
			retryCount++
		}
	}
	assert.Equal(t, 2, retryCount, "WAL should contain 2 step_retry records (before attempts 2 and 3)")
}

func TestF7_CancelTermination(t *testing.T) {
	dir := t.TempDir()
	rt, err := afrt.New(afrt.Config{WorkDir: dir})
	require.NoError(t, err)
	defer rt.Close()

	done := make(chan error, 1)
	go func() {
		done <- rt.Run(context.Background(), func(ac *afrt.Context) error {
			for i := 1; i <= 7; i++ {
				if e := ac.Step(fmt.Sprintf("step-%d", i), func(ctx context.Context) error {
					return SHA256Work(ctx, 5*time.Second)
				}); e != nil {
					return e
				}
			}
			return nil
		})
	}()

	// Let the first step begin, then cancel.
	time.Sleep(1 * time.Second)
	cancelStart := time.Now()
	rt.Cancel()

	err = <-done
	cancelDur := time.Since(cancelStart)

	require.Error(t, err, "Run should return error after cancel")
	assert.Less(t, cancelDur.Milliseconds(), int64(500),
		"Run should return within 500ms of Cancel()")
	t.Logf("F7: cancel-to-return=%v", cancelDur)
}

func TestF8_OTELTraceOutput(t *testing.T) {
	dir := t.TempDir()
	resFile := filepath.Join(t.TempDir(), "result.json")

	_, stdout, err := runAgentResult(t, resFile,
		"--workdir", dir,
		"--steps", "7",
		"--step-duration", "30ms",
		"--trace-exporter", "stdout",
	)
	require.NoError(t, err)

	output := string(stdout)
	// Count span names: 1 root "af.job.run" + 7 "af.step.step-N"
	rootSpans := strings.Count(output, "af.job.run")
	stepSpans := strings.Count(output, "af.step.step-")
	total := rootSpans + stepSpans

	t.Logf("F8: rootSpans=%d  stepSpans=%d  total=%d  outputLen=%d", rootSpans, stepSpans, total, len(output))
	assert.GreaterOrEqual(t, total, 8, "should have >= 8 spans (1 root + 7 steps)")
}

func TestF9_OTELMetricsOutput(t *testing.T) {
	dir := t.TempDir()
	resFile := filepath.Join(t.TempDir(), "result.json")

	_, stdout, err := runAgentResult(t, resFile,
		"--workdir", dir,
		"--steps", "7",
		"--step-duration", "30ms",
		"--metrics-exporter", "stdout",
	)
	require.NoError(t, err)

	output := string(stdout)
	assert.Contains(t, output, "af_step_total",
		"metrics output should contain af_step_total")
	assert.Contains(t, output, "af_step_duration_seconds",
		"metrics output should contain af_step_duration_seconds")
	t.Logf("F9: metrics output length=%d", len(output))
}

// ---------------------------------------------------------------------------
// B-series: Build
// ---------------------------------------------------------------------------

func TestB1_GoBuild(t *testing.T) {
	root := moduleRoot(t)
	cmd := exec.Command("go", "build", "./runtime/...", "./wal/...", "./observe/...", "./protect/...")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "go build should succeed: %s", string(out))
}

func TestB2_NoCGO(t *testing.T) {
	root := moduleRoot(t)
	cmd := exec.Command("go", "build", "./runtime/...", "./wal/...", "./observe/...", "./protect/...")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "CGO_ENABLED=0 go build should succeed: %s", string(out))
}

func TestB3_CrossPlatform(t *testing.T) {
	if goruntime.GOOS == "linux" && goruntime.GOARCH == "amd64" {
		t.Skip("already on linux/amd64")
	}
	root := moduleRoot(t)
	cmd := exec.Command("go", "build", "./runtime/...", "./wal/...", "./observe/...", "./protect/...")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=0")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "cross-platform go build should succeed: %s", string(out))
}
