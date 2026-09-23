package pool

import (
	"sync"
	"testing"
	"time"
)

func defaultConfig() PoolConfig {
	return PoolConfig{
		Name:               "test-pool",
		MinSize:            3,
		MaxSize:            10,
		ScaleUpThreshold:   0.3,
		ScaleDownThreshold: 0.8,
		ScaleUpStep:        2,
		CooldownSeconds:    5,
		Image:              "sandbox:latest",
		RuntimeClass:       "gvisor",
	}
}

func TestNewManager(t *testing.T) {
	cfg := defaultConfig()
	m := NewManager(cfg)

	pods := m.ListPods()
	if len(pods) != cfg.MinSize {
		t.Fatalf("expected %d pods, got %d", cfg.MinSize, len(pods))
	}
	for _, pod := range pods {
		if pod.State != PodIdle {
			t.Fatalf("expected idle state, got %s", pod.State)
		}
		if pod.Image != cfg.Image {
			t.Fatalf("expected image %s, got %s", cfg.Image, pod.Image)
		}
		if pod.RuntimeClass != cfg.RuntimeClass {
			t.Fatalf("expected runtimeClass %s, got %s", cfg.RuntimeClass, pod.RuntimeClass)
		}
		if pod.Version != 1 {
			t.Fatalf("expected version 1, got %d", pod.Version)
		}
	}
}

func TestClaim(t *testing.T) {
	m := NewManager(defaultConfig())

	result, err := m.Claim("job-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Pod == nil {
		t.Fatal("expected pod in result")
	}
	if result.Pod.State != PodClaimed {
		t.Fatalf("expected claimed state, got %s", result.Pod.State)
	}
	if result.Pod.ClaimedByJobID != "job-1" {
		t.Fatalf("expected claimedByJobID job-1, got %s", result.Pod.ClaimedByJobID)
	}
	if result.Pod.ClaimedAt == nil {
		t.Fatal("expected claimedAt to be set")
	}
	if !result.WarmHit {
		t.Fatal("expected warm hit")
	}
	if result.Pod.Version != 2 {
		t.Fatalf("expected version 2, got %d", result.Pod.Version)
	}
}

func TestClaimAll(t *testing.T) {
	cfg := defaultConfig()
	cfg.MinSize = 2
	cfg.MaxSize = 2
	m := NewManager(cfg)

	_, err := m.Claim("job-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_, err = m.Claim("job-2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = m.Claim("job-3")
	if err != ErrPoolExhausted {
		t.Fatalf("expected ErrPoolExhausted, got %v", err)
	}

	metrics := m.Metrics()
	if metrics.MissTotal != 1 {
		t.Fatalf("expected 1 miss, got %d", metrics.MissTotal)
	}
}

func TestRelease(t *testing.T) {
	cfg := defaultConfig()
	cfg.MinSize = 2
	m := NewManager(cfg)

	result, err := m.Claim("job-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	podID := result.Pod.ID

	err = m.Release(podID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, found := m.GetPod(podID)
	if found {
		t.Fatal("released pod should be removed")
	}

	pods := m.ListPods()
	if len(pods) != cfg.MinSize {
		t.Fatalf("expected %d pods after release, got %d", cfg.MinSize, len(pods))
	}

	idleCount := 0
	for _, pod := range pods {
		if pod.State == PodIdle {
			idleCount++
		}
	}
	if idleCount != cfg.MinSize {
		t.Fatalf("expected %d idle pods, got %d", cfg.MinSize, idleCount)
	}
}

func TestConcurrentClaim(t *testing.T) {
	cfg := defaultConfig()
	cfg.MinSize = 10
	cfg.MaxSize = 10
	m := NewManager(cfg)

	var wg sync.WaitGroup
	results := make(chan *ClaimResult, 10)
	errs := make(chan error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			r, err := m.Claim("job-concurrent")
			if err != nil {
				errs <- err
				return
			}
			results <- r
		}(i)
	}
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		t.Fatalf("unexpected error: %v", err)
	}

	claimed := make(map[string]bool)
	for r := range results {
		if claimed[r.Pod.ID] {
			t.Fatalf("pod %s claimed twice", r.Pod.ID)
		}
		claimed[r.Pod.ID] = true
	}

	if len(claimed) != 10 {
		t.Fatalf("expected 10 unique claims, got %d", len(claimed))
	}
}

func TestReconcileScaleUp(t *testing.T) {
	cfg := defaultConfig()
	cfg.MinSize = 3
	cfg.MaxSize = 10
	cfg.ScaleUpThreshold = 0.5
	cfg.ScaleUpStep = 2
	cfg.CooldownSeconds = 0
	m := NewManager(cfg)

	_, _ = m.Claim("job-1")
	_, _ = m.Claim("job-2")

	err := m.Reconcile()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	status := m.Status()
	if status.TotalPods != 5 {
		t.Fatalf("expected 5 pods after scale up, got %d", status.TotalPods)
	}

	metrics := m.Metrics()
	if metrics.ScaleUpTotal < 1 {
		t.Fatal("expected at least one scale up event")
	}
}

func TestReconcileScaleDown(t *testing.T) {
	cfg := defaultConfig()
	cfg.MinSize = 2
	cfg.MaxSize = 10
	cfg.ScaleDownThreshold = 0.5
	cfg.CooldownSeconds = 0
	m := NewManager(cfg)

	status := m.Status()
	if status.IdlePods != 2 {
		t.Fatalf("expected 2 idle pods initially, got %d", status.IdlePods)
	}

	err := m.Reconcile()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	status = m.Status()
	if status.TotalPods != cfg.MinSize {
		t.Fatalf("expected %d pods after scale down, got %d", cfg.MinSize, status.TotalPods)
	}
}

func TestCooldown(t *testing.T) {
	cfg := defaultConfig()
	cfg.MinSize = 3
	cfg.MaxSize = 20
	cfg.ScaleUpThreshold = 0.5
	cfg.ScaleUpStep = 2
	cfg.CooldownSeconds = 60
	m := NewManager(cfg)

	_, _ = m.Claim("job-1")
	_, _ = m.Claim("job-2")

	_ = m.Reconcile()
	countAfterFirst := m.Status().TotalPods

	_, _ = m.Claim("job-3")
	_ = m.Reconcile()
	countAfterSecond := m.Status().TotalPods

	if countAfterSecond != countAfterFirst {
		t.Fatalf("expected no scale during cooldown, got %d -> %d", countAfterFirst, countAfterSecond)
	}
}

func TestBurstAdaptation(t *testing.T) {
	cfg := defaultConfig()
	cfg.MinSize = 3
	cfg.MaxSize = 20
	cfg.CooldownSeconds = 60
	m := NewManager(cfg)

	now := time.Now()
	m.mu.Lock()
	m.lastScaleTime = &now
	m.mu.Unlock()

	m.SetPendingDemand(8)

	_ = m.Reconcile()

	status := m.Status()
	if status.IdlePods < 8 {
		t.Fatalf("expected at least 8 idle pods after burst, got %d", status.IdlePods)
	}
}

func TestClaimLatency(t *testing.T) {
	m := NewManager(defaultConfig())

	result, err := m.Claim("job-latency")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ClaimLatency < 0 {
		t.Fatal("expected non-negative latency")
	}
	if result.ClaimLatency > 100*time.Millisecond {
		t.Fatalf("claim latency too high: %v", result.ClaimLatency)
	}
}
