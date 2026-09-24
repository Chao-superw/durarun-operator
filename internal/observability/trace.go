package observability

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

var Tracer = otel.Tracer("durarun-operator")

func StartReconcileSpan(ctx context.Context, controller, jobName, phase string) (context.Context, trace.Span) {
	ctx, span := Tracer.Start(ctx, controller+".Reconcile",
		trace.WithAttributes(
			attribute.String("durarun.job", jobName),
			attribute.String("durarun.phase", phase),
		),
	)
	return ctx, span
}

// InitOTLPExporter sets up an OTLP HTTP trace exporter and configures
// the global TracerProvider. It returns a shutdown function and any error.
func InitOTLPExporter(ctx context.Context, endpoint string) (func(), error) {
	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(endpoint),
	)
	if err != nil {
		return nil, fmt.Errorf("create OTLP exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName("durarun-operator"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	Tracer = tp.Tracer("durarun-operator")

	shutdown := func() {
		_ = tp.Shutdown(context.Background())
	}
	return shutdown, nil
}
