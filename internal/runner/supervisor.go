package runner

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"syscall"

	"durarun-operator/internal/protocol"
)

// Supervisor starts a child process as a direct exec (no shell wrapper),
// places it in its own process group, and waits for it to exit.
type Supervisor struct {
	Argv    []string
	WorkDir string
	Env     []string
	Stdout  io.Writer
	Stderr  io.Writer
}

// Run executes the child process described by Argv, forwarding signals via
// ForwardSignals (on Unix) and waiting for the process to complete.
// The returned ProcessResult captures exit code, signal, and any launch error.
func (s *Supervisor) Run(ctx context.Context) (*protocol.ProcessResult, error) {
	if len(s.Argv) == 0 {
		return nil, fmt.Errorf("supervisor: argv must not be empty")
	}

	cmd := exec.CommandContext(ctx, s.Argv[0], s.Argv[1:]...)
	cmd.Dir = s.WorkDir
	cmd.Env = s.Env
	cmd.Stdout = s.Stdout
	cmd.Stderr = s.Stderr

	// Place child in its own process group so we can signal the whole group.
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	if err := cmd.Start(); err != nil {
		return &protocol.ProcessResult{
			ExitCode: -1,
			Error:    fmt.Sprintf("exec start: %s", err),
		}, nil
	}

	// Forward termination signals to the child process group.
	sigCtx, sigCancel := context.WithCancel(ctx)
	defer sigCancel()
	ForwardSignals(sigCtx, cmd.Process.Pid)

	// Wait for the child to finish.
	waitErr := cmd.Wait()
	sigCancel() // stop signal forwarding

	return ExitToResult(waitErr), nil
}
