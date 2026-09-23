package runtime

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"durarun-operator/observe"
	"durarun-operator/protect"
)

// StepOption configures a single Step invocation.
type StepOption func(*stepConfig)

type stepConfig struct {
	timeout time.Duration
	retry   protect.RetryConfig
}

// WithTimeout overrides the default step timeout for this step.
func WithTimeout(d time.Duration) StepOption {
	return func(sc *stepConfig) { sc.timeout = d }
}

// WithRetry sets a retry policy for this step.
func WithRetry(cfg protect.RetryConfig) StepOption {
	return func(sc *stepConfig) { sc.retry = cfg }
}

// Step executes fn as a named step with WAL journaling, OTEL tracing,
// metrics recording, optional timeout and retry.
func (c *Context) Step(id string, fn func(ctx context.Context) error, opts ...StepOption) error {
	rt := c.runtime

	// Merge options.
	sc := stepConfig{
		timeout: rt.cfg.StepTimeout,
		retry:   protect.DefaultRetryConfig(),
	}
	for _, o := range opts {
		o(&sc)
	}

	// Recovery: skip already-completed steps.
	if rt.cfg.RecoveryMode {
		if _, ok := rt.completedSteps[id]; ok {
			_, span := rt.tracer.StartSpan(c.ctx, "af.step."+id,
				attribute.String(observe.AttrStepID, id),
				attribute.Bool(observe.AttrStepSkipped, true),
			)
			span.End()
			return nil
		}
	}

	// Normal execution path.
	ctx, span := rt.tracer.StartSpan(c.ctx, "af.step."+id,
		attribute.String(observe.AttrStepID, id),
	)
	start := time.Now()

	rt.walWriter.StepBegin(id)

	// Apply step-level timeout.
	stepCtx, stepCancel := protect.ApplyStepTimeout(ctx, sc.timeout)
	defer stepCancel()

	// Execute with retry.
	onRetry := func(attempt int) {
		rt.walWriter.StepRetry(id, attempt)
	}
	_, err := protect.Execute(stepCtx, sc.retry, fn, onRetry)

	duration := time.Since(start)

	if err == nil {
		rt.walWriter.StepEnd(id, 0, "")
		rt.metrics.RecordWALFsync(ctx, rt.walWriter.LastFsyncLatency())
		span.SetAttributes(attribute.Int(observe.AttrStepExitCode, 0))
	} else {
		if protect.IsTimeout(err) {
			rt.walWriter.StepTimeout(id)
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	rt.metrics.RecordStep(ctx, id, duration, err)
	span.End()

	// Propagate job-level cancellation even when the step itself succeeded.
	if c.ctx.Err() != nil && err == nil {
		return c.ctx.Err()
	}
	return err
}
