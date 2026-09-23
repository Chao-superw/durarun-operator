package isolation

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestResolveLevel_Default(t *testing.T) {
	m := NewManager([]Level{L0Process, L1GVisor, L2Firecracker, L3Docker})
	level, err := m.ResolveLevel("", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if level != L1GVisor {
		t.Fatalf("expected L1, got %s", level)
	}
}

func TestResolveLevel_Explicit(t *testing.T) {
	m := NewManager([]Level{L0Process, L1GVisor, L2Firecracker})
	policy := &SandboxPolicy{
		AllowedLevels: []Level{L0Process, L1GVisor, L2Firecracker},
		DefaultLevel:  L1GVisor,
	}
	level, err := m.ResolveLevel(L0Process, policy)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if level != L0Process {
		t.Fatalf("expected L0, got %s", level)
	}
}

func TestResolveLevel_ForceLevel(t *testing.T) {
	m := NewManager([]Level{L0Process, L1GVisor, L2Firecracker, L3Docker})
	forced := L2Firecracker
	policy := &SandboxPolicy{
		AllowedLevels: []Level{L0Process, L1GVisor},
		DefaultLevel:  L1GVisor,
		ForceLevel:    &forced,
	}
	level, err := m.ResolveLevel(L0Process, policy)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if level != L2Firecracker {
		t.Fatalf("expected L2 (forced), got %s", level)
	}
}

func TestResolveLevel_NotAllowed(t *testing.T) {
	m := NewManager([]Level{L0Process, L1GVisor, L2Firecracker})
	policy := &SandboxPolicy{
		AllowedLevels: []Level{L1GVisor, L2Firecracker},
	}
	_, err := m.ResolveLevel(L0Process, policy)
	if !errors.Is(err, ErrLevelNotAllowed) {
		t.Fatalf("expected ErrLevelNotAllowed, got %v", err)
	}
}

func TestResolveLevel_Fallback(t *testing.T) {
	m := NewManager([]Level{L0Process, L1GVisor})
	policy := &SandboxPolicy{
		AllowedLevels: []Level{L0Process, L1GVisor, L2Firecracker},
	}
	level, err := m.ResolveLevel(L2Firecracker, policy)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if level != L1GVisor {
		t.Fatalf("expected fallback to L1, got %s", level)
	}
}

func TestResolveLevel_AllUnavailable(t *testing.T) {
	m := NewManager([]Level{})
	_, err := m.ResolveLevel(L1GVisor, nil)
	if !errors.Is(err, ErrIsolationUnavailable) {
		t.Fatalf("expected ErrIsolationUnavailable, got %v", err)
	}
}

func TestGetRuntimeConfig(t *testing.T) {
	m := NewManager(allLevels)

	cases := []struct {
		level        Level
		runtimeClass string
		timeout      time.Duration
		readOnly     bool
	}{
		{L0Process, "runc", 10 * time.Millisecond, false},
		{L1GVisor, "runsc", 80 * time.Millisecond, true},
		{L2Firecracker, "firecracker-containerd", 200 * time.Millisecond, true},
		{L3Docker, "runc", 5000 * time.Millisecond, true},
	}

	for _, tc := range cases {
		cfg := m.GetRuntimeConfig(tc.level)
		if cfg.RuntimeClass != tc.runtimeClass {
			t.Errorf("level %s: expected RuntimeClass %s, got %s", tc.level, tc.runtimeClass, cfg.RuntimeClass)
		}
		if cfg.StartupTimeout != tc.timeout {
			t.Errorf("level %s: expected StartupTimeout %v, got %v", tc.level, tc.timeout, cfg.StartupTimeout)
		}
		if cfg.SecurityProfile.ReadOnlyRootFS != tc.readOnly {
			t.Errorf("level %s: expected ReadOnlyRootFS %v, got %v", tc.level, tc.readOnly, cfg.SecurityProfile.ReadOnlyRootFS)
		}
		if cfg.Level != tc.level {
			t.Errorf("expected Level %s, got %s", tc.level, cfg.Level)
		}
	}

	invalid := m.GetRuntimeConfig("INVALID")
	if invalid.RuntimeClass != "" {
		t.Errorf("expected empty RuntimeConfig for invalid level, got %+v", invalid)
	}
}

func TestStartSandbox_L0(t *testing.T) {
	m := NewManager([]Level{L0Process, L1GVisor, L2Firecracker})
	ctx := context.Background()
	cfg := SandboxRunConfig{Image: "test:latest", Command: []string{"echo", "hello"}}

	start := time.Now()
	result, err := m.StartSandbox(ctx, L0Process, cfg)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Level != L0Process {
		t.Fatalf("expected L0, got %s", result.Level)
	}
	if elapsed > 50*time.Millisecond {
		t.Fatalf("L0 took too long: %v", elapsed)
	}
	if result.RuntimeClass != "runc" {
		t.Fatalf("expected runc, got %s", result.RuntimeClass)
	}
}

func TestStartSandbox_L1(t *testing.T) {
	m := NewManager([]Level{L0Process, L1GVisor, L2Firecracker})
	ctx := context.Background()
	cfg := SandboxRunConfig{Image: "test:latest", Command: []string{"echo", "hello"}}

	start := time.Now()
	result, err := m.StartSandbox(ctx, L1GVisor, cfg)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Level != L1GVisor {
		t.Fatalf("expected L1, got %s", result.Level)
	}
	if elapsed < 30*time.Millisecond || elapsed > 200*time.Millisecond {
		t.Fatalf("L1 latency out of expected range: %v", elapsed)
	}
	if result.RuntimeClass != "runsc" {
		t.Fatalf("expected runsc, got %s", result.RuntimeClass)
	}
}

func TestStartSandbox_Unavailable(t *testing.T) {
	m := NewManager([]Level{L0Process})
	ctx := context.Background()
	cfg := SandboxRunConfig{Image: "test:latest"}

	_, err := m.StartSandbox(ctx, L2Firecracker, cfg)
	if !errors.Is(err, ErrIsolationUnavailable) {
		t.Fatalf("expected ErrIsolationUnavailable, got %v", err)
	}
}

func TestStartSandbox_ContextCancel(t *testing.T) {
	m := NewManager([]Level{L2Firecracker})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cfg := SandboxRunConfig{Image: "test:latest"}

	_, err := m.StartSandbox(ctx, L2Firecracker, cfg)
	if err == nil {
		t.Fatal("expected context canceled error")
	}
}

func TestMetrics(t *testing.T) {
	m := NewManager([]Level{L0Process, L1GVisor})
	ctx := context.Background()
	cfg := SandboxRunConfig{Image: "test:latest", Command: []string{"echo"}}

	for i := 0; i < 5; i++ {
		_, err := m.StartSandbox(ctx, L0Process, cfg)
		if err != nil {
			t.Fatalf("start %d failed: %v", i, err)
		}
	}
	for i := 0; i < 3; i++ {
		_, err := m.StartSandbox(ctx, L1GVisor, cfg)
		if err != nil {
			t.Fatalf("start %d failed: %v", i, err)
		}
	}

	metrics := m.GetMetrics()

	l0m, ok := metrics[L0Process]
	if !ok {
		t.Fatal("missing L0 metrics")
	}
	if l0m.StartCount != 5 {
		t.Fatalf("expected 5 L0 starts, got %d", l0m.StartCount)
	}
	if l0m.AvgLatencyMs < 0 {
		t.Fatalf("invalid avg latency: %f", l0m.AvgLatencyMs)
	}

	l1m, ok := metrics[L1GVisor]
	if !ok {
		t.Fatal("missing L1 metrics")
	}
	if l1m.StartCount != 3 {
		t.Fatalf("expected 3 L1 starts, got %d", l1m.StartCount)
	}
	if l1m.AvgLatencyMs <= l0m.AvgLatencyMs {
		t.Logf("warning: L1 avg (%f ms) not greater than L0 avg (%f ms) - timing can be variable", l1m.AvgLatencyMs, l0m.AvgLatencyMs)
	}

	_, exists := metrics[L2Firecracker]
	if exists {
		t.Fatal("should not have L2 metrics")
	}
}

func TestValidateLevel(t *testing.T) {
	m := NewManager(nil)
	if !m.ValidateLevel(L0Process) {
		t.Error("L0 should be valid")
	}
	if !m.ValidateLevel(L1GVisor) {
		t.Error("L1 should be valid")
	}
	if !m.ValidateLevel(L2Firecracker) {
		t.Error("L2 should be valid")
	}
	if !m.ValidateLevel(L3Docker) {
		t.Error("L3 should be valid")
	}
	if m.ValidateLevel("INVALID") {
		t.Error("INVALID should not be valid")
	}
}

func TestIsAvailable(t *testing.T) {
	m := NewManager([]Level{L0Process, L1GVisor})
	if !m.IsAvailable(L0Process) {
		t.Error("L0 should be available")
	}
	if !m.IsAvailable(L1GVisor) {
		t.Error("L1 should be available")
	}
	if m.IsAvailable(L2Firecracker) {
		t.Error("L2 should not be available")
	}
}
