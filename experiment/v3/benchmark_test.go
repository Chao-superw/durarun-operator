//go:build benchmark

package v3

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	afrt "durarun-operator/runtime"
	"durarun-operator/wal"
)

func TestP1_S0_NormalOverhead(t *testing.T) {
	const trials = 5
	var overheads []float64
	var trialData []map[string]interface{}

	for i := 0; i < trials; i++ {
		native := NativeRun7Steps(defaultStepDur)
		afTime := run7StepRuntime(t, t.TempDir())
		oh := float64(afTime-native) / float64(native) * 100
		overheads = append(overheads, oh)
		trialData = append(trialData, map[string]interface{}{
			"trial":     i + 1,
			"native_ms": native.Milliseconds(),
			"af_ms":     afTime.Milliseconds(),
			"overhead%": oh,
		})
		t.Logf("trial %d: native=%v  af=%v  overhead=%.2f%%", i+1, native, afTime, oh)
	}

	mean := CalcMean(overheads)
	sd := CalcStddev(overheads, mean)
	pass := mean <= 5.0
	t.Logf("mean overhead=%.2f%%  stddev=%.2f%%  pass=%v", mean, sd, pass)
	assert.True(t, pass, "mean overhead should be <= +5%%")

	_ = WriteSingleReport(filepath.Join(resultsDir(t), "P1_normal_overhead.json"), MetricReport{
		MetricID: "P1-S0", Description: "Normal 7-step overhead vs native",
		Target: "<=+5%", Trials: trialData, Mean: mean, Stddev: sd, Pass: pass,
	})
}

func TestP4_S3_ConcurrentOverhead(t *testing.T) {
	const goroutines = 3
	const trials = 5
	var overheads []float64
	var trialData []map[string]interface{}

	for trial := 0; trial < trials; trial++ {
		native := NativeRun7Steps(defaultStepDur)

		var wg sync.WaitGroup
		times := make([]time.Duration, goroutines)
		for g := 0; g < goroutines; g++ {
			g := g
			wg.Add(1)
			go func() {
				defer wg.Done()
				times[g] = run7StepRuntime(t, t.TempDir())
			}()
		}
		wg.Wait()

		var sum float64
		for _, d := range times {
			sum += float64(d.Milliseconds())
		}
		avg := sum / float64(goroutines)
		oh := (avg - float64(native.Milliseconds())) / float64(native.Milliseconds()) * 100
		overheads = append(overheads, oh)
		trialData = append(trialData, map[string]interface{}{
			"trial":     trial + 1,
			"native_ms": native.Milliseconds(),
			"avg_ms":    avg,
			"overhead%": oh,
		})
		t.Logf("trial %d: native=%v  avg_concurrent=%.0fms  overhead=%.2f%%", trial+1, native, avg, oh)
	}

	mean := CalcMean(overheads)
	pass := mean <= 10.0
	t.Logf("mean concurrent overhead=%.2f%%  pass=%v", mean, pass)
	assert.True(t, pass, "concurrent overhead should be <= +10%%")

	_ = WriteSingleReport(filepath.Join(resultsDir(t), "P4_concurrent_overhead.json"), MetricReport{
		MetricID: "P4-S3", Description: "3-goroutine concurrent overhead vs native",
		Target: "<=+10%", Trials: trialData, Mean: mean, Stddev: CalcStddev(overheads, mean), Pass: pass,
	})
}

func TestP6_OTELTraceOverhead(t *testing.T) {
	const trials = 5
	var overheads []float64
	var trialData []map[string]interface{}

	for i := 0; i < trials; i++ {
		// noop run
		dirNoop := t.TempDir()
		rtNoop, err := afrt.New(afrt.Config{WorkDir: dirNoop, Observe: afrt.ObserveConfig{TraceExporter: "noop"}})
		require.NoError(t, err)
		startNoop := time.Now()
		require.NoError(t, rtNoop.Run(context.Background(), func(ac *afrt.Context) error {
			for s := 1; s <= 7; s++ {
				if e := ac.Step(fmt.Sprintf("step-%d", s), func(ctx context.Context) error {
					return SHA256Work(ctx, defaultStepDur)
				}); e != nil {
					return e
				}
			}
			return nil
		}))
		noopMs := float64(time.Since(startNoop).Milliseconds())
		require.NoError(t, rtNoop.Close())

		// stdout run (output goes to pipe via subprocess)
		dirStdout := t.TempDir()
		resFile := filepath.Join(t.TempDir(), "result.json")
		_, _, subErr := runAgentResult(t, resFile,
			"--workdir", dirStdout,
			"--steps", "7",
			"--step-duration", defaultStepDur.String(),
			"--trace-exporter", "stdout",
		)
		require.NoError(t, subErr)
		data, err := os.ReadFile(resFile)
		require.NoError(t, err)
		var res agentResult
		require.NoError(t, json.Unmarshal(data, &res))
		stdoutMs := float64(res.WallTimeMs)

		oh := 0.0
		if noopMs > 0 {
			oh = (stdoutMs - noopMs) / noopMs * 100
		}
		overheads = append(overheads, oh)
		trialData = append(trialData, map[string]interface{}{
			"trial":     i + 1,
			"noop_ms":   noopMs,
			"stdout_ms": stdoutMs,
			"overhead%": oh,
		})
		t.Logf("trial %d: noop=%.0fms  stdout=%.0fms  overhead=%.2f%%", i+1, noopMs, stdoutMs, oh)
	}

	mean := CalcMean(overheads)
	pass := mean <= 2.0
	t.Logf("mean OTEL trace overhead=%.2f%%  pass=%v", mean, pass)
	assert.True(t, pass, "OTEL trace overhead should be <= +2%%")

	_ = WriteSingleReport(filepath.Join(resultsDir(t), "P6_otel_trace_overhead.json"), MetricReport{
		MetricID: "P6", Description: "OTEL stdout trace exporter overhead vs noop",
		Target: "<=+2%", Trials: trialData, Mean: mean, Stddev: CalcStddev(overheads, mean), Pass: pass,
	})
}

func TestP5_WALFsyncLatency(t *testing.T) {
	const warmupSteps = 20
	const measureSteps = 350

	dir := t.TempDir()
	w, err := wal.NewWriter(dir)
	require.NoError(t, err)

	for i := 0; i < warmupSteps; i++ {
		id := fmt.Sprintf("warmup-%d", i)
		_, _ = w.StepBegin(id)
		_, _ = w.StepEnd(id, 0, "")
	}

	var latencies []float64
	var trialData []map[string]interface{}
	const batchSize = 7

	for i := 0; i < measureSteps; i++ {
		id := fmt.Sprintf("step-%d", i)
		_, _ = w.StepBegin(id)
		_, _ = w.StepEnd(id, 0, "")
		lat := w.LastFsyncLatency()
		latencies = append(latencies, float64(lat.Nanoseconds()))

		if (i+1)%batchSize == 0 {
			batchStart := i + 1 - batchSize
			batchLats := latencies[batchStart : i+1]
			avgNs := CalcMean(batchLats)
			trialData = append(trialData, map[string]interface{}{
				"trial":   (i + 1) / batchSize,
				"avg_us":  avgNs / 1e3,
				"samples": batchSize,
			})
		}
	}
	require.NoError(t, w.Close())

	sort.Float64s(latencies)
	p99Idx := int(math.Ceil(0.99*float64(len(latencies)))) - 1
	if p99Idx < 0 {
		p99Idx = 0
	}
	p99Ms := latencies[p99Idx] / 1e6

	mean := CalcMean(latencies) / 1e6
	pass := p99Ms <= 5.0
	t.Logf("total fsyncs=%d  mean=%.3fms  P99=%.3fms  pass=%v", len(latencies), mean, p99Ms, pass)
	assert.True(t, pass, "P99 fsync latency should be <= 5ms")

	_ = WriteSingleReport(filepath.Join(resultsDir(t), "P5_wal_fsync_latency.json"), MetricReport{
		MetricID: "P5", Description: "WAL fsync P99 latency",
		Target: "<=5ms", Trials: trialData, Mean: p99Ms, Pass: pass,
	})
}
