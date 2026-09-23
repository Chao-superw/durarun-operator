package runtime

import "time"

// Config controls the Runtime behaviour.
type Config struct {
	WorkDir      string        // WAL and working data directory (required)
	RecoveryMode bool          // whether to resume from a previous WAL
	StepTimeout  time.Duration // default per-step timeout (0 = no timeout)
	JobTimeout   time.Duration // job-level timeout (0 = no timeout)
	Observe      ObserveConfig // observability settings
}

// ObserveConfig selects trace and metric exporters.
type ObserveConfig struct {
	TraceExporter   string // "stdout", "noop", ""
	MetricsExporter string // "stdout", "noop", ""
}
