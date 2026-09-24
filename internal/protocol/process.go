package protocol

// ProcessResult captures the outcome of a supervised child process.
type ProcessResult struct {
	ExitCode  int    `json:"exitCode"`
	Signal    int    `json:"signal,omitempty"`
	OOMKilled bool   `json:"oomKilled,omitempty"`
	Error     string `json:"error,omitempty"`
}

// Success returns true when the process exited cleanly with code 0,
// no signal was received, and no error was recorded.
func (r *ProcessResult) Success() bool {
	return r.ExitCode == 0 && r.Signal == 0 && r.Error == ""
}
