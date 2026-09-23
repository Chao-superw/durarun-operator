package protect

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// --------------- timeout tests ---------------

func TestApplyStepTimeout_Expires(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := ApplyStepTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	select {
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected context to expire before 200ms")
	case <-ctx.Done():
		if !IsTimeout(ctx.Err()) {
			t.Fatalf("expected DeadlineExceeded, got %v", ctx.Err())
		}
	}
}

func TestApplyStepTimeout_NoTimeout(t *testing.T) {
	ctx := context.Background()
	ctx2, cancel := ApplyStepTimeout(ctx, 0)
	defer cancel()

	if _, ok := ctx2.Deadline(); ok {
		t.Fatal("expected no deadline when timeout <= 0")
	}
}

// --------------- retry tests ---------------

func TestRetryExecute_NoRetry(t *testing.T) {
	calls := 0
	attempts, err := Execute(context.Background(), RetryConfig{MaxAttempts: 1}, func(ctx context.Context) error {
		calls++
		return errors.New("fail")
	}, nil)

	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
	if attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", attempts)
	}
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRetryExecute_Success(t *testing.T) {
	calls := 0
	attempts, err := Execute(context.Background(), RetryConfig{
		MaxAttempts:  3,
		InitialDelay: 10 * time.Millisecond,
	}, func(ctx context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("not yet")
		}
		return nil
	}, nil)

	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestRetryExecute_AllFail(t *testing.T) {
	calls := 0
	sentinel := errors.New("persistent")
	attempts, err := Execute(context.Background(), RetryConfig{
		MaxAttempts:  3,
		InitialDelay: 10 * time.Millisecond,
	}, func(ctx context.Context) error {
		calls++
		return sentinel
	}, nil)

	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}
}

func TestRetryExecute_RetryOn(t *testing.T) {
	retryable := errors.New("retryable")
	fatal := errors.New("fatal")

	calls := 0
	attempts, err := Execute(context.Background(), RetryConfig{
		MaxAttempts:  5,
		InitialDelay: 10 * time.Millisecond,
		RetryOn: func(e error) bool {
			return errors.Is(e, retryable)
		},
	}, func(ctx context.Context) error {
		calls++
		if calls == 1 {
			return retryable
		}
		return fatal
	}, nil)

	if calls != 2 {
		t.Fatalf("expected 2 calls, got %d", calls)
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts)
	}
	if !errors.Is(err, fatal) {
		t.Fatalf("expected fatal error, got %v", err)
	}
}

func TestRetryExecute_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var calls int32
	go func() {
		// Cancel after a short while so the retry sleep is interrupted.
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	attempts, err := Execute(ctx, RetryConfig{
		MaxAttempts:  10,
		InitialDelay: 500 * time.Millisecond,
	}, func(ctx context.Context) error {
		atomic.AddInt32(&calls, 1)
		return errors.New("fail")
	}, nil)

	if attempts >= 10 {
		t.Fatal("expected early exit due to context cancellation")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	_ = calls
}

func TestExponentialBackoff(t *testing.T) {
	cfg := RetryConfig{
		MaxAttempts:  4,
		InitialDelay: 20 * time.Millisecond,
		MaxDelay:     100 * time.Millisecond,
		Backoff:      ExponentialBackoff,
	}

	var timestamps []time.Time
	_, _ = Execute(context.Background(), cfg, func(ctx context.Context) error {
		timestamps = append(timestamps, time.Now())
		return errors.New("fail")
	}, nil)

	if len(timestamps) != 4 {
		t.Fatalf("expected 4 timestamps, got %d", len(timestamps))
	}

	// Expected delays: 20ms, 40ms, 80ms (capped at 100ms but 80 < 100 so stays 80).
	// We allow 15ms tolerance for scheduling jitter.
	expectedDelays := []time.Duration{20 * time.Millisecond, 40 * time.Millisecond, 80 * time.Millisecond}
	tolerance := 15 * time.Millisecond

	for i := 0; i < len(expectedDelays); i++ {
		actual := timestamps[i+1].Sub(timestamps[i])
		low := expectedDelays[i] - tolerance
		high := expectedDelays[i] + tolerance
		if actual < low || actual > high {
			t.Errorf("delay[%d]: expected ~%v, got %v", i, expectedDelays[i], actual)
		}
	}
}
