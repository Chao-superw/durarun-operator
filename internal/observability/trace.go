package observability

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
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
