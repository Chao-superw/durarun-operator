package observe

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// Well-known attribute keys.
const (
	AttrStepID       = "af.step.id"
	AttrStepSkipped  = "af.step.skipped"
	AttrStepExitCode = "af.step.exit_code"
	AttrRecoveryMode = "af.recovery_mode"
)

// Tracer wraps an OpenTelemetry TracerProvider and a named Tracer.
type Tracer struct {
	provider trace.TracerProvider
	tracer   trace.Tracer
	// shutdown is non-nil only for SDK-based providers that need cleanup.
	shutdown func(context.Context) error
}

// NewTracer creates a Tracer according to cfg.TraceExporter.
// An empty or "noop" value yields a zero-overhead noop provider.
func NewTracer(cfg Config) (*Tracer, error) {
	name := cfg.serviceName()

	switch cfg.TraceExporter {
	case "", "noop":
		p := tracenoop.NewTracerProvider()
		return &Tracer{provider: p, tracer: p.Tracer(name)}, nil

	case "stdout":
		exp, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
		if err != nil {
			return nil, fmt.Errorf("observe: create stdout trace exporter: %w", err)
		}
		tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp))
		return &Tracer{
			provider: tp,
			tracer:   tp.Tracer(name),
			shutdown:  tp.Shutdown,
		}, nil

	default:
		return nil, fmt.Errorf("observe: unsupported trace exporter %q", cfg.TraceExporter)
	}
}

// StartSpan starts a new span with optional attributes.
func (t *Tracer) StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	opts := []trace.SpanStartOption{}
	if len(attrs) > 0 {
		opts = append(opts, trace.WithAttributes(attrs...))
	}
	return t.tracer.Start(ctx, name, opts...)
}

// Shutdown flushes and shuts down the underlying provider.
func (t *Tracer) Shutdown(ctx context.Context) error {
	if t.shutdown != nil {
		return t.shutdown(ctx)
	}
	return nil
}
