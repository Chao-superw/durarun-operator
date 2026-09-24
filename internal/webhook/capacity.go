package webhook

import (
	"fmt"
	"sync"

	v1alpha1 "durarun-operator/api/v1alpha1"
)

// CapacityAdmission is a simple in-memory admission controller that limits the
// number of concurrently active AgentJobs, implementing backpressure.
type CapacityAdmission struct {
	MaxActiveJobs int32
	mu            sync.Mutex
	activeCount   int32
}

// Admit checks whether a new job can be admitted. It returns an error if the
// active job count has reached MaxActiveJobs.
func (c *CapacityAdmission) Admit(_ *v1alpha1.AgentJob) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.activeCount >= c.MaxActiveJobs {
		return fmt.Errorf("capacity backpressure: %d/%d active jobs, cannot admit more",
			c.activeCount, c.MaxActiveJobs)
	}
	c.activeCount++
	return nil
}

// Release decrements the active job counter. It should be called when a job
// reaches a terminal state (succeeded, failed, or deleted).
func (c *CapacityAdmission) Release(_ *v1alpha1.AgentJob) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.activeCount > 0 {
		c.activeCount--
	}
}
