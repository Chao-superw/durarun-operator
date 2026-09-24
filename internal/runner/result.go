package runner

import (
	"errors"
	"os/exec"
	"syscall"

	"durarun-operator/internal/protocol"
)

// ExitToResult converts the error returned by exec.Cmd.Wait into a
// ProcessResult. A nil error means the process exited with code 0.
func ExitToResult(err error) *protocol.ProcessResult {
	if err == nil {
		return &protocol.ProcessResult{ExitCode: 0}
	}

	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		result := &protocol.ProcessResult{
			ExitCode: exitErr.ExitCode(),
		}
		// Extract the signal that killed the process, if any.
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
			if status.Signaled() {
				result.Signal = int(status.Signal())
			}
		}
		return result
	}

	// Non-ExitError means the process could not be started or waited on.
	return &protocol.ProcessResult{
		ExitCode: -1,
		Error:    err.Error(),
	}
}
