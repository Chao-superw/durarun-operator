package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	v3 "durarun-operator/experiment/v3"
	afrt "durarun-operator/runtime"
	"durarun-operator/wal"
)

type result struct {
	Mode           string `json:"mode"`
	CompletedSteps int    `json:"completed_steps"`
	SkippedSteps   int    `json:"skipped_steps"`
	TotalSteps     int    `json:"total_steps"`
	WallTimeMs     int64  `json:"wall_time_ms"`
}

func main() {
	workDir := flag.String("workdir", "", "WAL working directory (required)")
	crashAt := flag.Int("crash-at", 0, "crash (os.Exit(1)) at step N; 0 means no crash")
	recover := flag.Bool("recover", false, "run in recovery mode")
	steps := flag.Int("steps", 7, "total number of steps")
	stepDur := flag.Duration("step-duration", 100*time.Millisecond, "per-step work duration")
	traceExp := flag.String("trace-exporter", "", "trace exporter: stdout, noop, or empty")
	metricsExp := flag.String("metrics-exporter", "", "metrics exporter: stdout, noop, or empty")
	resultFile := flag.String("result-file", "", "write result JSON to this file instead of stdout")
	flag.Parse()

	if *workDir == "" {
		fmt.Fprintln(os.Stderr, "error: --workdir is required")
		os.Exit(2)
	}

	// In recovery mode, count pre-existing completed steps before creating the runtime.
	preCompleted := 0
	if *recover {
		reader, err := wal.NewReader(*workDir)
		if err == nil {
			manifest, err := reader.BuildManifest()
			if err == nil {
				preCompleted = len(manifest.CompletedSteps)
			}
		}
	}

	cfg := afrt.Config{
		WorkDir:      *workDir,
		RecoveryMode: *recover,
		Observe: afrt.ObserveConfig{
			TraceExporter:   *traceExp,
			MetricsExporter: *metricsExp,
		},
	}

	rt, err := afrt.New(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: create runtime: %v\n", err)
		os.Exit(2)
	}

	start := time.Now()
	_ = rt.Run(context.Background(), func(ac *afrt.Context) error {
		for i := 1; i <= *steps; i++ {
			stepID := fmt.Sprintf("step-%d", i)
			if stepErr := ac.Step(stepID, func(ctx context.Context) error {
				if *crashAt == i {
					// Execute half the work then crash to simulate mid-step failure.
					_ = v3.SHA256Work(ctx, *stepDur/2)
					os.Exit(1)
				}
				return v3.SHA256Work(ctx, *stepDur)
			}); stepErr != nil {
				return stepErr
			}
		}
		return nil
	})
	wallTime := time.Since(start)

	if err := rt.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "warning: close runtime: %v\n", err)
	}

	// Build result from WAL manifest.
	reader, err := wal.NewReader(*workDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: open WAL reader: %v\n", err)
		os.Exit(2)
	}
	manifest, err := reader.BuildManifest()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: build manifest: %v\n", err)
		os.Exit(2)
	}

	mode := "normal"
	if *recover {
		mode = "recovery"
	}
	res := result{
		Mode:           mode,
		CompletedSteps: len(manifest.CompletedSteps),
		SkippedSteps:   preCompleted,
		TotalSteps:     *steps,
		WallTimeMs:     wallTime.Milliseconds(),
	}

	var out *os.File
	if *resultFile != "" {
		out, err = os.Create(*resultFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: create result file: %v\n", err)
			os.Exit(2)
		}
		defer out.Close()
	} else {
		out = os.Stdout
	}

	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	if err := enc.Encode(res); err != nil {
		fmt.Fprintf(os.Stderr, "error: encode result: %v\n", err)
		os.Exit(2)
	}
}
