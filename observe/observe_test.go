package observe

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

func TestNewTracer_Stdout(t *testing.T) {
	tr, err := NewTracer(Config{TraceExporter: "stdout"})
	if err != nil {
		t.Fatalf("NewTracer(stdout): %v", err)
	}
	defer tr.Shutdown(context.Background())

	ctx, span := tr.StartSpan(context.Background(), "test-span")
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}
	span.End()
}

func TestNewTracer_Noop(t *testing.T) {
	tr, err := NewTracer(Config{TraceExporter: "noop"})
	if err != nil {
		t.Fatalf("NewTracer(noop): %v", err)
	}
	defer tr.Shutdown(context.Background())

	ctx, span := tr.StartSpan(context.Background(), "noop-span")
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}
	span.End()
}

func TestNewMetrics_Stdout(t *testing.T) {
	m, err := NewMetrics(Config{MetricsExporter: "stdout"})
	if err != nil {
		t.Fatalf("NewMetrics(stdout): %v", err)
	}
	defer m.Shutdown(context.Background())

	ctx := context.Background()
	m.RecordStep(ctx, "step-1", 150*time.Millisecond, nil)
	m.RecordWALFsync(ctx, 5*time.Millisecond)
}

func TestStartSpan_WithAttributes(t *testing.T) {
	tr, err := NewTracer(Config{TraceExporter: "stdout"})
	if err != nil {
		t.Fatalf("NewTracer: %v", err)
	}
	defer tr.Shutdown(context.Background())

	attrs := []attribute.KeyValue{
		attribute.String(AttrStepID, "build-42"),
		attribute.Bool(AttrStepSkipped, false),
		attribute.Int(AttrStepExitCode, 0),
		attribute.Bool(AttrRecoveryMode, true),
	}

	ctx, span := tr.StartSpan(context.Background(), "attr-span", attrs...)
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}
	span.End()
}
