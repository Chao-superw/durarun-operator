package pool

import (
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
)

type PodState string

const (
	PodIdle        PodState = "idle"
	PodClaimed     PodState = "claimed"
	PodTerminating PodState = "terminating"
)

type PoolPod struct {
	ID             string     `json:"id"`
	PoolID         string     `json:"poolId"`
	State          PodState   `json:"state"`
	ClaimedByJobID string     `json:"claimedByJobId,omitempty"`
	RuntimeClass   string     `json:"runtimeClass"`
	Image          string     `json:"image"`
	CreatedAt      time.Time  `json:"createdAt"`
	ClaimedAt      *time.Time `json:"claimedAt,omitempty"`
	Version        int64      `json:"version"`
}

type PoolConfig struct {
	Name               string  `json:"name"`
	MinSize            int     `json:"minSize"`
	MaxSize            int     `json:"maxSize"`
	ScaleUpThreshold   float64 `json:"scaleUpThreshold"`
	ScaleDownThreshold float64 `json:"scaleDownThreshold"`
	ScaleUpStep        int     `json:"scaleUpStep"`
	CooldownSeconds    int     `json:"cooldownSeconds"`
	Image              string  `json:"image"`
	RuntimeClass       string  `json:"runtimeClass"`
}

type PoolStatus struct {
	TotalPods     int        `json:"totalPods"`
	IdlePods      int        `json:"idlePods"`
	ClaimedPods   int        `json:"claimedPods"`
	LastScaleTime *time.Time `json:"lastScaleTime,omitempty"`
	Ready         bool       `json:"ready"`
}

type ClaimResult struct {
	Pod          *PoolPod      `json:"pod"`
	WarmHit      bool          `json:"warmHit"`
	ClaimLatency time.Duration `json:"claimLatency"`
}

type PoolMetrics struct {
	ClaimTotal     int64
	MissTotal      int64
	ScaleUpTotal   int64
	ScaleDownTotal int64
}

var (
	ErrPoolExhausted = errors.New("pool: no idle pods available")
	ErrPodNotFound   = errors.New("pool: pod not found")
)

type Manager struct {
	mu            sync.Mutex
	cfg           PoolConfig
	pods          map[string]*PoolPod
	podOrder      []string
	lastScaleTime *time.Time
	pendingDemand int
	metrics       PoolMetrics
}

func NewManager(cfg PoolConfig) *Manager {
	m := &Manager{
		cfg:  cfg,
		pods: make(map[string]*PoolPod),
	}
	for i := 0; i < cfg.MinSize; i++ {
		m.createPod()
	}
	return m
}

func (m *Manager) createPod() *PoolPod {
	pod := &PoolPod{
		ID:           uuid.New().String(),
		PoolID:       m.cfg.Name,
		State:        PodIdle,
		RuntimeClass: m.cfg.RuntimeClass,
		Image:        m.cfg.Image,
		CreatedAt:    time.Now(),
		Version:      1,
	}
	m.pods[pod.ID] = pod
	m.podOrder = append(m.podOrder, pod.ID)
	return pod
}

func (m *Manager) Claim(jobID string) (*ClaimResult, error) {
	start := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()

	m.metrics.ClaimTotal++

	for _, id := range m.podOrder {
		pod, ok := m.pods[id]
		if !ok || pod.State != PodIdle {
			continue
		}

		now := time.Now()
		pod.State = PodClaimed
		pod.ClaimedByJobID = jobID
		pod.ClaimedAt = &now
		pod.Version++

		latency := time.Since(start)
		return &ClaimResult{
			Pod:          pod,
			WarmHit:      true,
			ClaimLatency: latency,
		}, nil
	}

	m.metrics.MissTotal++
	return nil, ErrPoolExhausted
}

func (m *Manager) Release(podID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	pod, ok := m.pods[podID]
	if !ok {
		return ErrPodNotFound
	}

	pod.State = PodTerminating
	pod.Version++

	delete(m.pods, podID)
	newOrder := make([]string, 0, len(m.podOrder))
	for _, id := range m.podOrder {
		if id != podID {
			newOrder = append(newOrder, id)
		}
	}
	m.podOrder = newOrder

	m.createPod()

	return nil
}

func (m *Manager) Status() PoolStatus {
	m.mu.Lock()
	defer m.mu.Unlock()

	total := 0
	idle := 0
	claimed := 0
	for _, pod := range m.pods {
		total++
		switch pod.State {
		case PodIdle:
			idle++
		case PodClaimed:
			claimed++
		}
	}

	return PoolStatus{
		TotalPods:     total,
		IdlePods:      idle,
		ClaimedPods:   claimed,
		LastScaleTime: m.lastScaleTime,
		Ready:         idle > 0,
	}
}

func (m *Manager) Reconcile() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	termIDs := make([]string, 0)
	for _, id := range m.podOrder {
		if pod, ok := m.pods[id]; ok && pod.State == PodTerminating {
			termIDs = append(termIDs, id)
		}
	}
	for _, id := range termIDs {
		delete(m.pods, id)
	}
	if len(termIDs) > 0 {
		newOrder := make([]string, 0, len(m.podOrder))
		for _, id := range m.podOrder {
			keep := true
			for _, tid := range termIDs {
				if id == tid {
					keep = false
					break
				}
			}
			if keep {
				newOrder = append(newOrder, id)
			}
		}
		m.podOrder = newOrder
	}

	total := len(m.pods)
	idle := 0
	for _, pod := range m.pods {
		if pod.State == PodIdle {
			idle++
		}
	}

	burstNeeded := m.pendingDemand > idle
	if burstNeeded {
		needed := m.pendingDemand - idle
		for i := 0; i < needed && len(m.pods) < m.cfg.MaxSize; i++ {
			m.createPod()
		}
		now := time.Now()
		m.lastScaleTime = &now
		m.metrics.ScaleUpTotal++
		return nil
	}

	cooldownOK := m.lastScaleTime == nil ||
		time.Since(*m.lastScaleTime) >= time.Duration(m.cfg.CooldownSeconds)*time.Second

	if total > 0 {
		ratio := float64(idle) / float64(total)

		if ratio < m.cfg.ScaleUpThreshold && cooldownOK {
			added := 0
			for i := 0; i < m.cfg.ScaleUpStep && len(m.pods) < m.cfg.MaxSize; i++ {
				m.createPod()
				added++
			}
			if added > 0 {
				now := time.Now()
				m.lastScaleTime = &now
				m.metrics.ScaleUpTotal++
			}
		}

		if ratio > m.cfg.ScaleDownThreshold && total > m.cfg.MinSize && cooldownOK {
			removed := 0
			newOrder := make([]string, 0, len(m.podOrder))
			for _, id := range m.podOrder {
				pod, ok := m.pods[id]
				if ok && pod.State == PodIdle && len(m.pods)-removed > m.cfg.MinSize {
					delete(m.pods, id)
					removed++
					now := time.Now()
					m.lastScaleTime = &now
					continue
				}
				newOrder = append(newOrder, id)
			}
			m.podOrder = newOrder
			if removed > 0 {
				m.metrics.ScaleDownTotal++
			}
		}
	}

	return nil
}

func (m *Manager) GetPod(podID string) (*PoolPod, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pod, ok := m.pods[podID]
	return pod, ok
}

func (m *Manager) ListPods() []PoolPod {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]PoolPod, 0, len(m.pods))
	for _, id := range m.podOrder {
		if pod, ok := m.pods[id]; ok {
			result = append(result, *pod)
		}
	}
	return result
}

func (m *Manager) SetPendingDemand(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pendingDemand = n
}

func (m *Manager) Metrics() PoolMetrics {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.metrics
}
