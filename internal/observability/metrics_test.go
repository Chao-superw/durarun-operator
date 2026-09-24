package observability

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMetricsRegistration(t *testing.T) {
	// Verify that all metric descriptors exist and were registered without panics.
	// The init() function calls MustRegister, so if we reach here the registration succeeded.

	gauges := []prometheus.Gauge{ActiveJobs, ActiveAttempts}
	for _, g := range gauges {
		if g == nil {
			t.Fatal("expected gauge to be non-nil")
		}
	}

	counters := []prometheus.Counter{CheckpointTotal, CheckpointRestoreTotal, FencingRejectTotal}
	for _, c := range counters {
		if c == nil {
			t.Fatal("expected counter to be non-nil")
		}
	}

	counterVecs := []*prometheus.CounterVec{ReconcileTotal, AttemptTotal, AdmissionTotal}
	for _, cv := range counterVecs {
		if cv == nil {
			t.Fatal("expected counter vec to be non-nil")
		}
	}

	histograms := []prometheus.Histogram{RecoveryDuration, CheckpointDuration}
	for _, h := range histograms {
		if h == nil {
			t.Fatal("expected histogram to be non-nil")
		}
	}

	histVecs := []*prometheus.HistogramVec{ReconcileDuration, JobDuration}
	for _, hv := range histVecs {
		if hv == nil {
			t.Fatal("expected histogram vec to be non-nil")
		}
	}
}

func TestCounterOperations(t *testing.T) {
	// Test that counter increment works without panic.
	FencingRejectTotal.Inc()
	CheckpointTotal.Inc()
	CheckpointRestoreTotal.Inc()

	// Test counter vec with labels.
	ReconcileTotal.WithLabelValues("job", "success").Inc()
	AttemptTotal.WithLabelValues("succeeded").Inc()
	AdmissionTotal.WithLabelValues("admitted").Inc()
	AdmissionTotal.WithLabelValues("rejected").Inc()
	AdmissionTotal.WithLabelValues("queued").Inc()
}

func TestGaugeOperations(t *testing.T) {
	ActiveJobs.Set(5)
	ActiveJobs.Inc()
	ActiveJobs.Dec()

	ActiveAttempts.Set(3)
	ActiveAttempts.Inc()
	ActiveAttempts.Dec()
}

func TestHistogramOperations(t *testing.T) {
	RecoveryDuration.Observe(1.5)
	CheckpointDuration.Observe(0.3)

	ReconcileDuration.WithLabelValues("job").Observe(0.1)
	JobDuration.WithLabelValues("succeeded").Observe(120.0)
	JobDuration.WithLabelValues("failed").Observe(30.0)
}
