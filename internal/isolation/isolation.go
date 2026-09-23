package isolation

import (
	"context"
	"errors"
	"math/rand"
	"sort"
	"sync"
	"time"

	"durarun-operator/internal/sandbox"
)

type Level string

const (
	L0Process     Level = "L0"
	L1GVisor      Level = "L1"
	L2Firecracker Level = "L2"
	L3Docker      Level = "L3"
)

var allLevels = []Level{L0Process, L1GVisor, L2Firecracker, L3Docker}

var ErrIsolationUnavailable = errors.New("isolation: requested level not available")
var ErrLevelNotAllowed = errors.New("isolation: level not allowed by policy")

type RuntimeConfig struct {
	Level           Level
	RuntimeClass    string
	StartupTimeout  time.Duration
	SecurityProfile SecurityProfile
}

type SecurityProfile struct {
	ReadOnlyRootFS   bool
	NoNewPrivileges  bool
	SeccompProfile   string
	AppArmorProfile  string
	DropCapabilities []string
}

type StartResult struct {
	Level        Level
	StartLatency time.Duration
	RuntimeClass string
	Fallback     bool
	FallbackFrom Level
}

type SandboxPolicy struct {
	AllowedLevels []Level
	DefaultLevel  Level
	ForceLevel    *Level
}

type SandboxRunConfig struct {
	Image       string
	Command     []string
	Env         map[string]string
	CPULimit    string
	MemoryLimit string
	WorkDir     string
	Timeout     time.Duration
}

type LevelMetrics struct {
	StartCount   int64
	AvgLatencyMs float64
	P95LatencyMs float64
	latencies    []time.Duration
}

type Manager struct {
	mu              sync.Mutex
	availableLevels map[Level]bool
	metrics         map[Level]*LevelMetrics
	dockerSandbox   *sandbox.Sandbox
}

func NewManager(availableLevels []Level) *Manager {
	m := &Manager{
		availableLevels: make(map[Level]bool, len(availableLevels)),
		metrics:         make(map[Level]*LevelMetrics),
	}
	for _, l := range availableLevels {
		m.availableLevels[l] = true
	}
	return m
}

func (m *Manager) ResolveLevel(requested Level, policy *SandboxPolicy) (Level, error) {
	if policy != nil && policy.ForceLevel != nil {
		forced := *policy.ForceLevel
		if !m.IsAvailable(forced) {
			return "", ErrIsolationUnavailable
		}
		return forced, nil
	}

	if requested == "" {
		if policy != nil && policy.DefaultLevel != "" {
			requested = policy.DefaultLevel
		} else {
			requested = L1GVisor
		}
	}

	if policy != nil && len(policy.AllowedLevels) > 0 {
		allowed := false
		for _, l := range policy.AllowedLevels {
			if l == requested {
				allowed = true
				break
			}
		}
		if !allowed {
			return "", ErrLevelNotAllowed
		}
	}

	if m.IsAvailable(requested) {
		return requested, nil
	}

	fallbackOrder := []Level{L2Firecracker, L1GVisor, L0Process, L3Docker}
	for _, l := range fallbackOrder {
		if l == requested {
			continue
		}
		if m.IsAvailable(l) {
			return l, nil
		}
	}

	return "", ErrIsolationUnavailable
}

func (m *Manager) GetRuntimeConfig(level Level) RuntimeConfig {
	switch level {
	case L0Process:
		return RuntimeConfig{
			Level:          L0Process,
			RuntimeClass:   "runc",
			StartupTimeout: 10 * time.Millisecond,
			SecurityProfile: SecurityProfile{
				ReadOnlyRootFS:   false,
				NoNewPrivileges:  false,
				SeccompProfile:   "",
				AppArmorProfile:  "",
				DropCapabilities: nil,
			},
		}
	case L1GVisor:
		return RuntimeConfig{
			Level:          L1GVisor,
			RuntimeClass:   "runsc",
			StartupTimeout: 80 * time.Millisecond,
			SecurityProfile: SecurityProfile{
				ReadOnlyRootFS:   true,
				NoNewPrivileges:  true,
				SeccompProfile:   "",
				AppArmorProfile:  "",
				DropCapabilities: []string{"ALL_EXCEPT_NET_BIND_SERVICE"},
			},
		}
	case L2Firecracker:
		return RuntimeConfig{
			Level:          L2Firecracker,
			RuntimeClass:   "firecracker-containerd",
			StartupTimeout: 200 * time.Millisecond,
			SecurityProfile: SecurityProfile{
				ReadOnlyRootFS:   true,
				NoNewPrivileges:  true,
				SeccompProfile:   "runtime/default",
				AppArmorProfile:  "runtime/default",
				DropCapabilities: []string{"ALL"},
			},
		}
	case L3Docker:
		return RuntimeConfig{
			Level:          L3Docker,
			RuntimeClass:   "runc",
			StartupTimeout: 5000 * time.Millisecond,
			SecurityProfile: SecurityProfile{
				ReadOnlyRootFS:   true,
				NoNewPrivileges:  true,
				SeccompProfile:   "",
				AppArmorProfile:  "",
				DropCapabilities: []string{"ALL_EXCEPT_NET_BIND_SERVICE"},
			},
		}
	default:
		return RuntimeConfig{}
	}
}

func (m *Manager) ValidateLevel(level Level) bool {
	for _, l := range allLevels {
		if l == level {
			return true
		}
	}
	return false
}

func (m *Manager) IsAvailable(level Level) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.availableLevels[level]
}

func (m *Manager) StartSandbox(ctx context.Context, level Level, config SandboxRunConfig) (*StartResult, error) {
	if !m.IsAvailable(level) {
		return nil, ErrIsolationUnavailable
	}

	start := time.Now()

	if level == L3Docker {
		result, err := m.startDockerSandbox(ctx, config)
		if err != nil {
			return nil, err
		}
		result.Level = L3Docker
		result.RuntimeClass = "runc"
		result.StartLatency = time.Since(start)
		m.RecordStartLatency(L3Docker, result.StartLatency)
		return result, nil
	}

	latency := m.simulateStartup(level)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(latency):
	}

	elapsed := time.Since(start)
	m.RecordStartLatency(level, elapsed)

	cfg := m.GetRuntimeConfig(level)
	return &StartResult{
		Level:        level,
		StartLatency: elapsed,
		RuntimeClass: cfg.RuntimeClass,
	}, nil
}

// STUB: L0-L2 are sleep-based simulations; only L3 (Docker) is real.
func (m *Manager) simulateStartup(level Level) time.Duration {
	switch level {
	case L0Process:
		return time.Duration(5+rand.Intn(11)) * time.Millisecond
	case L1GVisor:
		return time.Duration(40+rand.Intn(41)) * time.Millisecond
	case L2Firecracker:
		return time.Duration(100+rand.Intn(101)) * time.Millisecond
	default:
		return 0
	}
}

func (m *Manager) startDockerSandbox(ctx context.Context, config SandboxRunConfig) (*StartResult, error) {
	m.mu.Lock()
	if m.dockerSandbox == nil {
		sb, err := sandbox.New()
		if err != nil {
			m.mu.Unlock()
			return nil, err
		}
		m.dockerSandbox = sb
	}
	m.mu.Unlock()

	prompt := ""
	if len(config.Command) > 0 {
		prompt = config.Command[0]
	}

	_, err := m.dockerSandbox.Run(ctx, sandbox.RunConfig{
		Image:       config.Image,
		Prompt:      prompt,
		Env:         config.Env,
		CPULimit:    config.CPULimit,
		MemoryLimit: config.MemoryLimit,
		Timeout:     config.Timeout,
		WorkDir:     config.WorkDir,
	})
	if err != nil {
		return nil, err
	}

	return &StartResult{}, nil
}

func (m *Manager) RecordStartLatency(level Level, latency time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	lm, ok := m.metrics[level]
	if !ok {
		lm = &LevelMetrics{}
		m.metrics[level] = lm
	}

	lm.latencies = append(lm.latencies, latency)
	lm.StartCount = int64(len(lm.latencies))

	var total float64
	for _, l := range lm.latencies {
		total += float64(l.Milliseconds())
	}
	lm.AvgLatencyMs = total / float64(len(lm.latencies))

	sorted := make([]time.Duration, len(lm.latencies))
	copy(sorted, lm.latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	idx := int(float64(len(sorted)) * 0.95)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	lm.P95LatencyMs = float64(sorted[idx].Milliseconds())
}

func (m *Manager) GetMetrics() map[Level]LevelMetrics {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make(map[Level]LevelMetrics, len(m.metrics))
	for k, v := range m.metrics {
		result[k] = LevelMetrics{
			StartCount:   v.StartCount,
			AvgLatencyMs: v.AvgLatencyMs,
			P95LatencyMs: v.P95LatencyMs,
		}
	}
	return result
}
