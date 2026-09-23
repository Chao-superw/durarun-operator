package runtime

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"durarun-operator/protect"
	"durarun-operator/wal"
)

// sha256Work performs real CPU work (SHA-256 hashing) for the given duration,
// checking ctx.Done() periodically so it can respond to cancellation.
func sha256Work(ctx context.Context, duration time.Duration) error {
	deadline := time.Now().Add(duration)
	data := []byte("agent-fabric-v3-benchmark-payload")
	for time.Now().Before(deadline) {
		for i := 0; i < 1000; i++ {
			sha256.Sum256(data)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestNew_CreatesRuntime(t *testing.T) {
	dir := t.TempDir()
	rt, err := New(Config{WorkDir: dir})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	defer rt.Close()
	if rt == nil {
		t.Fatal("expected non-nil Runtime")
	}
	if rt.walWriter == nil {
		t.Fatal("expected non-nil WAL writer")
	}
	if rt.tracer == nil {
		t.Fatal("expected non-nil tracer")
	}
	if rt.metrics == nil {
		t.Fatal("expected non-nil metrics")
	}
}

func TestRun_BasicExecution(t *testing.T) {
	dir := t.TempDir()
	rt, err := New(Config{WorkDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()

	var executed []string
	err = rt.Run(context.Background(), func(c *Context) error {
		for i := 1; i <= 7; i++ {
			id := fmt.Sprintf("sha-%d", i)
			if err := c.Step(id, func(ctx context.Context) error {
				d := []byte(fmt.Sprintf("step-%d-payload", i))
				for j := 0; j < 1000; j++ {
					sha256.Sum256(d)
				}
				return nil
			}); err != nil {
				return err
			}
			executed = append(executed, id)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if len(executed) != 7 {
		t.Fatalf("expected 7 executed steps, got %d", len(executed))
	}

	// Verify WAL state.
	reader, err := wal.NewReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := reader.CompletedStepIDs()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 7 {
		t.Fatalf("expected 7 completed steps in WAL, got %d", len(ids))
	}
}

func TestRun_Recovery(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: run 3 steps and close.
	rt1, err := New(Config{WorkDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	err = rt1.Run(context.Background(), func(c *Context) error {
		for i := 1; i <= 3; i++ {
			id := fmt.Sprintf("step-%d", i)
			if err := c.Step(id, func(ctx context.Context) error {
				d := []byte(fmt.Sprintf("recovery-%d", i))
				for j := 0; j < 500; j++ {
					sha256.Sum256(d)
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	rt1.Close()

	// Phase 2: recovery mode — first 3 steps skipped, steps 4-5 execute.
	rt2, err := New(Config{WorkDir: dir, RecoveryMode: true})
	if err != nil {
		t.Fatal(err)
	}
	defer rt2.Close()

	var actuallyRan []string
	err = rt2.Run(context.Background(), func(c *Context) error {
		for i := 1; i <= 5; i++ {
			id := fmt.Sprintf("step-%d", i)
			if err := c.Step(id, func(ctx context.Context) error {
				actuallyRan = append(actuallyRan, id)
				d := []byte(fmt.Sprintf("recovery-%d", i))
				for j := 0; j < 500; j++ {
					sha256.Sum256(d)
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(actuallyRan) != 2 {
		t.Fatalf("expected 2 executed steps, got %d: %v", len(actuallyRan), actuallyRan)
	}
	if actuallyRan[0] != "step-4" || actuallyRan[1] != "step-5" {
		t.Fatalf("expected [step-4 step-5], got %v", actuallyRan)
	}
}

func TestStep_Timeout(t *testing.T) {
	dir := t.TempDir()
	rt, err := New(Config{
		WorkDir:     dir,
		StepTimeout: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()

	start := time.Now()
	var stepErr error
	_ = rt.Run(context.Background(), func(c *Context) error {
		stepErr = c.Step("slow-step", func(ctx context.Context) error {
			return sha256Work(ctx, 2*time.Second)
		})
		return stepErr
	})
	elapsed := time.Since(start)

	if stepErr == nil {
		t.Fatal("expected timeout error from step")
	}
	if !protect.IsTimeout(stepErr) {
		t.Fatalf("expected DeadlineExceeded, got: %v", stepErr)
	}
	if elapsed > 1*time.Second {
		t.Fatalf("expected completion in ~200ms, took %v", elapsed)
	}
	t.Logf("step timed out correctly in %v", elapsed)
}

func TestRun_JobTimeout(t *testing.T) {
	dir := t.TempDir()
	rt, err := New(Config{
		WorkDir:    dir,
		JobTimeout: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()

	start := time.Now()
	var completed int
	err = rt.Run(context.Background(), func(c *Context) error {
		for i := 1; i <= 10; i++ {
			id := fmt.Sprintf("step-%d", i)
			if err := c.Step(id, func(ctx context.Context) error {
				return sha256Work(ctx, 200*time.Millisecond)
			}); err != nil {
				return err
			}
			completed++
		}
		return nil
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error from job timeout")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("expected job to finish in ~500ms, took %v", elapsed)
	}
	if completed >= 10 {
		t.Fatalf("expected early termination, but all 10 steps completed")
	}
	t.Logf("job timed out after %d steps in %v", completed, elapsed)
}

func TestStep_Retry(t *testing.T) {
	dir := t.TempDir()
	rt, err := New(Config{WorkDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()

	var attempts int
	err = rt.Run(context.Background(), func(c *Context) error {
		return c.Step("flaky-step", func(ctx context.Context) error {
			attempts++
			d := []byte("retry-test-payload")
			for j := 0; j < 1000; j++ {
				sha256.Sum256(d)
			}
			if attempts < 3 {
				return fmt.Errorf("transient error (attempt %d)", attempts)
			}
			return nil
		}, WithRetry(protect.RetryConfig{
			MaxAttempts:  3,
			InitialDelay: 1 * time.Millisecond,
			Backoff:      protect.NoBackoff,
		}))
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}

	// Verify WAL contains exactly 2 retry records.
	reader, err := wal.NewReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatal(err)
	}

	var retryCount int
	for _, r := range records {
		if r.Type == wal.TypeStepRetry {
			retryCount++
		}
	}
	if retryCount != 2 {
		t.Fatalf("expected 2 step_retry WAL records, got %d", retryCount)
	}
}

func TestRun_Cancel(t *testing.T) {
	dir := t.TempDir()
	rt, err := New(Config{WorkDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()

	started := make(chan struct{})
	start := time.Now()

	go func() {
		<-started
		time.Sleep(200 * time.Millisecond)
		rt.Cancel()
	}()

	err = rt.Run(context.Background(), func(c *Context) error {
		close(started)
		return c.Step("long-step", func(ctx context.Context) error {
			return sha256Work(ctx, 10*time.Second)
		})
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error from cancelled run")
	}
	if elapsed > 1*time.Second {
		t.Fatalf("expected cancellation in ~200ms, took %v", elapsed)
	}
	t.Logf("run cancelled in %v with err: %v", elapsed, err)
}

func TestStep_OTELTrace(t *testing.T) {
	// The stdouttrace package captures os.Stdout at package-init time
	// (var defaultWriter = os.Stdout), so changing the Go variable later has
	// no effect.  We redirect at the file-descriptor level instead: fd 1 is
	// what the original *os.File wraps.
	tmpFile, err := os.CreateTemp("", "otel-trace-*.json")
	if err != nil {
		t.Fatal(err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	// Save original fd 1.
	origFd, err := syscall.Dup(syscall.Stdout)
	if err != nil {
		tmpFile.Close()
		t.Fatal(err)
	}

	// Point fd 1 at the temp file.
	if err := syscall.Dup2(int(tmpFile.Fd()), syscall.Stdout); err != nil {
		syscall.Close(origFd)
		tmpFile.Close()
		t.Fatal(err)
	}

	dir := t.TempDir()
	rt, err := New(Config{
		WorkDir: dir,
		Observe: ObserveConfig{TraceExporter: "stdout"},
	})
	if err != nil {
		syscall.Dup2(origFd, syscall.Stdout)
		syscall.Close(origFd)
		tmpFile.Close()
		t.Fatal(err)
	}

	runErr := rt.Run(context.Background(), func(c *Context) error {
		return c.Step("trace-test", func(ctx context.Context) error {
			sha256.Sum256([]byte("otel-trace-payload"))
			return nil
		})
	})

	// Shutdown flushes all pending spans to the exporter.
	rt.Close()

	// Restore fd 1 and close helpers.
	syscall.Dup2(origFd, syscall.Stdout)
	syscall.Close(origFd)
	tmpFile.Close()

	if runErr != nil {
		t.Fatalf("Run returned error: %v", runErr)
	}

	data, err := os.ReadFile(tmpPath)
	if err != nil {
		t.Fatal(err)
	}
	output := string(data)

	if !strings.Contains(output, "af.step.trace-test") {
		t.Fatalf("expected span 'af.step.trace-test' in output, got:\n%s", output)
	}
	if !strings.Contains(output, "af.job.run") {
		t.Fatalf("expected span 'af.job.run' in output, got:\n%s", output)
	}
	t.Logf("trace output length: %d bytes", len(output))
}
