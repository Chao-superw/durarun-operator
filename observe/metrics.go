package observe

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	otelmetric "go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Metrics holds pre-created OpenTelemetry instruments.
type Metrics struct {
	provider otelmetric.MeterProvider
	// shutdown is non-nil only for SDK-based providers.
	shutdown func(context.Context) error

	// Instruments
	StepTotal        otelmetric.Int64Counter
	StepDuration     otelmetric.Float64Histogram
	WALFsyncDuration otelmetric.Float64Histogram
}

// NewMetrics creates a Metrics instance according to cfg.MetricsExporter.
func NewMetrics(cfg Config) (*Metrics, error) {
	name := cfg.serviceName()

	var mp otelmetric.MeterProvider
	var shutdownFn func(context.Context) error

	switch cfg.MetricsExporter {
	case "", "noop":
		mp = metricnoop.NewMeterProvider()

	case "stdout":
		exp, err := stdoutmetric.New()
		if err != nil {
			return nil, fmt.Errorf("observe: create stdout metric exporter: %w", err)
		}
		sdkMP := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp)))
		mp = sdkMP
		shutdownFn = sdkMP.Shutdown

	default:
		return nil, fmt.Errorf("observe: unsupported metrics exporter %q", cfg.MetricsExporter)
	}

	meter := mp.Meter(name)

	stepTotal, err := meter.Int64Counter("af_step_total",
		otelmetric.WithDescription("Total number of steps executed"))
	if err != nil {
		return nil, fmt.Errorf("observe: create af_step_total counter: %w", err)
	}

	stepDur, err := meter.Float64Histogram("af_step_duration_seconds",
		otelmetric.WithDescription("Step execution duration in seconds"))
	if err != nil {
		return nil, fmt.Errorf("observe: create af_step_duration_seconds histogram: %w", err)
	}

	walDur, err := meter.Float64Histogram("af_wal_fsync_seconds",
		otelmetric.WithDescription("WAL fsync duration in seconds"))
	if err != nil {
		return nil, fmt.Errorf("observe: create af_wal_fsync_seconds histogram: %w", err)
	}

	return &Metrics{
		provider:         mp,
		shutdown:         shutdownFn,
		StepTotal:        stepTotal,
		StepDuration:     stepDur,
		WALFsyncDuration: walDur,
	}, nil
}

// RecordStep records one step execution.
func (m *Metrics) RecordStep(ctx context.Context, stepID string, duration time.Duration, err error) {
	attrs := []attribute.KeyValue{
		attribute.String(AttrStepID, stepID),
		attribute.Bool("error", err != nil),
	}
	m.StepTotal.Add(ctx, 1, otelmetric.WithAttributes(attrs...))
	m.StepDuration.Record(ctx, duration.Seconds(), otelmetric.WithAttributes(attrs...))
}

// RecordWALFsync records a WAL fsync duration.
func (m *Metrics) RecordWALFsync(ctx context.Context, duration time.Duration) {
	m.WALFsyncDuration.Record(ctx, duration.Seconds())
}

// Shutdown flushes and shuts down the underlying provider.
func (m *Metrics) Shutdown(ctx context.Context) error {
	if m.shutdown != nil {
		return m.shutdown(ctx)
	}
	return nil
}
