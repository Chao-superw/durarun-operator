package observe

// Config controls the observability backends.
type Config struct {
	TraceExporter   string // "stdout", "noop", "" (default noop)
	MetricsExporter string // "stdout", "noop", "" (default noop)
	ServiceName     string // default "agent-fabric"
}

func (c Config) serviceName() string {
	if c.ServiceName != "" {
		return c.ServiceName
	}
	return "agent-fabric"
}
