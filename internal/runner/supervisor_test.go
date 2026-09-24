package runner

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

func TestSupervisor_EchoHello(t *testing.T) {
	var stdout bytes.Buffer
	s := &Supervisor{
		Argv:   []string{"echo", "hello"},
		Stdout: &stdout,
	}

	result, err := s.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", result.ExitCode)
	}
	if result.Signal != 0 {
		t.Fatalf("expected signal 0, got %d", result.Signal)
	}
	if result.Error != "" {
		t.Fatalf("expected no error, got %q", result.Error)
	}
	if got := strings.TrimSpace(stdout.String()); got != "hello" {
		t.Fatalf("expected stdout %q, got %q", "hello", got)
	}
}

func TestSupervisor_FalseExitCode1(t *testing.T) {
	s := &Supervisor{
		Argv: []string{"false"},
	}

	result, err := s.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ExitCode != 1 {
		t.Fatalf("expected exit code 1, got %d", result.ExitCode)
	}
}

func TestSupervisor_NonExistentCommand(t *testing.T) {
	s := &Supervisor{
		Argv: []string{"/nonexistent/binary/that/does/not/exist"},
	}

	result, err := s.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// The process could not be started, so we expect an error in the result.
	if result.Error == "" {
		t.Fatal("expected error in ProcessResult for non-existent command")
	}
	if result.ExitCode != -1 {
		t.Fatalf("expected exit code -1, got %d", result.ExitCode)
	}
}

func TestSupervisor_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	s := &Supervisor{
		Argv: []string{"sleep", "60"},
	}

	result, err := s.Run(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// The process should have been killed by context cancellation.
	if result.ExitCode == 0 && result.Signal == 0 && result.Error == "" {
		t.Fatal("expected non-zero exit or signal after context cancellation")
	}
}

func TestSupervisor_EmptyArgv(t *testing.T) {
	s := &Supervisor{}

	_, err := s.Run(context.Background())
	if err == nil {
		t.Fatal("expected error for empty argv")
	}
}

func TestSupervisor_WorkDir(t *testing.T) {
	dir := t.TempDir()
	var stdout bytes.Buffer
	s := &Supervisor{
		Argv:    []string{"pwd"},
		WorkDir: dir,
		Stdout:  &stdout,
	}

	result, err := s.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", result.ExitCode)
	}
	// On macOS /tmp is a symlink to /private/tmp, so resolve both.
	got := strings.TrimSpace(stdout.String())
	if got != dir && got != "/private"+dir {
		t.Fatalf("expected working directory %q, got %q", dir, got)
	}
}
