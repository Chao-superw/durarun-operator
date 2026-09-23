package runtime

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"durarun-operator/observe"
	"durarun-operator/protect"
	"durarun-operator/wal"
)

// Runtime integrates WAL, observability and protection into a single runner.
type Runtime struct {
	cfg            Config
	walWriter      *wal.Writer
	tracer         *observe.Tracer
	metrics        *observe.Metrics
	completedSteps map[string]*wal.CompletedStep
	stepOutputs    map[string]string
	cancelFunc     context.CancelFunc
}

// New creates a Runtime.  In RecoveryMode it reads the existing WAL to
// discover which steps have already completed.
func New(cfg Config) (*Runtime, error) {
	w, err := wal.NewWriter(cfg.WorkDir)
	if err != nil {
		return nil, fmt.Errorf("runtime: create WAL writer: %w", err)
	}

	rt := &Runtime{
		cfg:            cfg,
		walWriter:      w,
		completedSteps: make(map[string]*wal.CompletedStep),
		stepOutputs:    make(map[string]string),
	}

	if cfg.RecoveryMode {
		reader, err := wal.NewReader(cfg.WorkDir)
		if err != nil {
			return nil, fmt.Errorf("runtime: open WAL reader: %w", err)
		}
		manifest, err := reader.BuildManifest()
		if err != nil {
			return nil, fmt.Errorf("runtime: build manifest: %w", err)
		}
		for i := range manifest.CompletedSteps {
			cs := &manifest.CompletedSteps[i]
			rt.completedSteps[cs.StepID] = cs
			rt.stepOutputs[cs.StepID] = cs.OutputRef
		}
	}

	obsCfg := observe.Config{
		TraceExporter:   cfg.Observe.TraceExporter,
		MetricsExporter: cfg.Observe.MetricsExporter,
		ServiceName:     "agent-fabric",
	}
	rt.tracer, err = observe.NewTracer(obsCfg)
	if err != nil {
		return nil, fmt.Errorf("runtime: create tracer: %w", err)
	}
	rt.metrics, err = observe.NewMetrics(obsCfg)
	if err != nil {
		return nil, fmt.Errorf("runtime: create metrics: %w", err)
	}

	return rt, nil
}

// Run executes fn inside a job-level span, honouring JobTimeout and
// recording a WAL job_cancelled record when the context is cancelled.
func (rt *Runtime) Run(ctx context.Context, fn func(*Context) error) error {
	ctx, jobCancel := protect.ApplyJobTimeout(ctx, rt.cfg.JobTimeout)
	defer jobCancel()

	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	rt.cancelFunc = cancel
	defer cancel()

	ctx, span := rt.tracer.StartSpan(ctx, "af.job.run",
		attribute.Bool(observe.AttrRecoveryMode, rt.cfg.RecoveryMode),
	)

	afCtx := &Context{ctx: ctx, runtime: rt}
	err := fn(afCtx)

	// Capture context state before defers fire.
	ctxErr := ctx.Err()

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()

	if ctxErr != nil {
		rt.walWriter.JobCancelled(ctxErr.Error())
		if err == nil {
			err = ctxErr
		}
	}

	return err
}

// Cancel signals the running job to stop.
func (rt *Runtime) Cancel() {
	if rt.cancelFunc != nil {
		rt.cancelFunc()
	}
}

// Close shuts down observability providers and closes the WAL.
func (rt *Runtime) Close() error {
	ctx := context.Background()
	rt.tracer.Shutdown(ctx)
	rt.metrics.Shutdown(ctx)
	rt.walWriter.Sync()
	return rt.walWriter.Close()
}
