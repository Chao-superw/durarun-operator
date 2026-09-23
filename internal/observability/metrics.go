package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	ReconcileTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "durarun",
			Subsystem: "controller",
			Name:      "reconcile_total",
			Help:      "Total number of reconciliations by result",
		},
		[]string{"controller", "result"},
	)

	ReconcileDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "durarun",
			Subsystem: "controller",
			Name:      "reconcile_duration_seconds",
			Help:      "Duration of reconciliation in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"controller"},
	)

	AttemptTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "durarun",
			Subsystem: "job",
			Name:      "attempt_total",
			Help:      "Total attempts created by outcome",
		},
		[]string{"result"},
	)

	CheckpointTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "durarun",
			Subsystem: "checkpoint",
			Name:      "save_total",
			Help:      "Total checkpoint save operations",
		},
	)

	CheckpointRestoreTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "durarun",
			Subsystem: "checkpoint",
			Name:      "restore_total",
			Help:      "Total checkpoint restore operations",
		},
	)

	FencingRejectTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "durarun",
			Subsystem: "fencing",
			Name:      "reject_total",
			Help:      "Total UID fencing rejections",
		},
	)
)

func init() {
	crmetrics.Registry.MustRegister(
		ReconcileTotal,
		ReconcileDuration,
		AttemptTotal,
		CheckpointTotal,
		CheckpointRestoreTotal,
		FencingRejectTotal,
	)
}
