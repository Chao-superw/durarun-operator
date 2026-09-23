package experiment

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const benchTaskQueue = "af-bench"

var activityCounter int32

func benchStepActivity(ctx context.Context, stepWorkMs int64) (string, error) {
	result := doStepWork(time.Duration(stepWorkMs) * time.Millisecond)
	atomic.AddInt32(&activityCounter, 1)
	return result, nil
}

func BenchWorkflow(ctx workflow.Context, steps int, stepWorkMs int64) error {
	opts := workflow.ActivityOptions{
		StartToCloseTimeout: 60 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, opts)

	for i := 0; i < steps; i++ {
		var result string
		err := workflow.ExecuteActivity(ctx, benchStepActivity, stepWorkMs).Get(ctx, &result)
		if err != nil {
			return err
		}
	}
	return nil
}

type TemporalBench struct {
	client client.Client
	addr   string
}

func NewTemporalBench(addr string) (*TemporalBench, error) {
	c, err := client.Dial(client.Options{HostPort: addr})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.CheckHealth(ctx, &client.CheckHealthRequest{}); err != nil {
		c.Close()
		return nil, fmt.Errorf("temporal health check: %w", err)
	}
	return &TemporalBench{client: c, addr: addr}, nil
}

func (tb *TemporalBench) Close() {
	tb.client.Close()
}

func (tb *TemporalBench) newWorker() worker.Worker {
	w := worker.New(tb.client, benchTaskQueue, worker.Options{})
	w.RegisterWorkflow(BenchWorkflow)
	w.RegisterActivity(benchStepActivity)
	return w
}

func (tb *TemporalBench) newWorkerNoSticky() worker.Worker {
	w := worker.New(tb.client, benchTaskQueue, worker.Options{
		StickyScheduleToStartTimeout: time.Millisecond,
	})
	w.RegisterWorkflow(BenchWorkflow)
	w.RegisterActivity(benchStepActivity)
	return w
}

func (tb *TemporalBench) Run(agent AgentDef, scenario, trialID string) PlatformResult {
	switch scenario {
	case "S0-normal":
		return tb.runS0(agent, trialID)
	case "S1-recovery":
		return tb.runS1(agent, trialID)
	case "S3-concurrent-3x":
		return tb.runS3(agent, trialID)
	}
	return PlatformResult{Platform: "temporal", Error: "unknown scenario"}
}

func (tb *TemporalBench) runS0(agent AgentDef, trialID string) PlatformResult {
	w := tb.newWorker()
	if err := w.Start(); err != nil {
		return PlatformResult{Platform: "temporal", Error: fmt.Sprintf("worker.Start: %v", err)}
	}
	defer w.Stop()

	stepWorkMs := int64(agent.StepWork / time.Millisecond)
	wfID := fmt.Sprintf("bench-%s", trialID)

	start := time.Now()
	run, err := tb.client.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        wfID,
		TaskQueue: benchTaskQueue,
	}, BenchWorkflow, agent.Steps, stepWorkMs)
	if err != nil {
		return PlatformResult{Platform: "temporal", Error: fmt.Sprintf("start: %v", err)}
	}
	if err := run.Get(context.Background(), nil); err != nil {
		return PlatformResult{Platform: "temporal", Error: fmt.Sprintf("get: %v", err)}
	}
	elapsed := time.Since(start)

	return PlatformResult{
		Platform:  "temporal",
		Elapsed:   elapsed,
		ElapsedMs: float64(elapsed) / float64(time.Millisecond),
	}
}

func (tb *TemporalBench) runS1(agent AgentDef, trialID string) PlatformResult {
	cp := crashPoint(agent.Steps)
	stepWorkMs := int64(agent.StepWork / time.Millisecond)
	wfID := fmt.Sprintf("bench-%s", trialID)

	// Phase 1 (not timed): execute crashPoint activities then stop worker
	// Wait for cp+1 activities to start, ensuring the workflow task that
	// scheduled activity cp+1 has completed. This prevents a 10s
	// WorkflowTaskTimeout when w1 is stopped mid-workflow-task.
	atomic.StoreInt32(&activityCounter, 0)
	w1 := tb.newWorkerNoSticky()
	if err := w1.Start(); err != nil {
		return PlatformResult{Platform: "temporal", Error: fmt.Sprintf("w1.Start: %v", err)}
	}

	run, err := tb.client.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        wfID,
		TaskQueue: benchTaskQueue,
	}, BenchWorkflow, agent.Steps, stepWorkMs)
	if err != nil {
		w1.Stop()
		return PlatformResult{Platform: "temporal", Error: fmt.Sprintf("start: %v", err)}
	}

	for atomic.LoadInt32(&activityCounter) < int32(cp+1) {
		time.Sleep(5 * time.Millisecond)
	}
	// Guard: let the in-flight workflow task complete before stopping.
	// This prevents a 10s WorkflowTaskTimeout on the server side.
	// The sleep is in the untimed setup phase and does not affect measurements.
	time.Sleep(500 * time.Millisecond)
	w1.Stop()
	skipped := int(atomic.LoadInt32(&activityCounter))

	// Phase 2 (timed): new worker recovers via history replay
	start := time.Now()
	w2 := tb.newWorker()
	if err := w2.Start(); err != nil {
		return PlatformResult{Platform: "temporal", Error: fmt.Sprintf("w2.Start: %v", err)}
	}

	if err := run.Get(context.Background(), nil); err != nil {
		w2.Stop()
		return PlatformResult{Platform: "temporal", Error: fmt.Sprintf("get(recovery): %v", err)}
	}
	elapsed := time.Since(start)
	w2.Stop()

	return PlatformResult{
		Platform:     "temporal",
		Elapsed:      elapsed,
		ElapsedMs:    float64(elapsed) / float64(time.Millisecond),
		StepsSkipped: skipped,
	}
}

func (tb *TemporalBench) runS3(agent AgentDef, trialID string) PlatformResult {
	w := tb.newWorker()
	if err := w.Start(); err != nil {
		return PlatformResult{Platform: "temporal", Error: fmt.Sprintf("worker.Start: %v", err)}
	}
	defer w.Stop()

	stepWorkMs := int64(agent.StepWork / time.Millisecond)
	start := time.Now()

	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			wfID := fmt.Sprintf("bench-%s-%d", trialID, idx)
			run, err := tb.client.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
				ID:        wfID,
				TaskQueue: benchTaskQueue,
			}, BenchWorkflow, agent.Steps, stepWorkMs)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}
			if err := run.Get(context.Background(), nil); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	if firstErr != nil {
		return PlatformResult{Platform: "temporal", Error: firstErr.Error()}
	}
	return PlatformResult{
		Platform:  "temporal",
		Elapsed:   elapsed,
		ElapsedMs: float64(elapsed) / float64(time.Millisecond),
	}
}
