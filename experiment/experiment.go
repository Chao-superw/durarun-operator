package experiment

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"durarun-operator/internal/pool"
	"durarun-operator/internal/runner"
)

type AgentDef struct {
	Name     string
	Steps    int
	StepWork time.Duration
}

type PlatformResult struct {
	Platform     string        `json:"platform"`
	Agent        string        `json:"agent"`
	Scenario     string        `json:"scenario"`
	Trial        int           `json:"trial"`
	Elapsed      time.Duration `json:"elapsedNs"`
	ElapsedMs    float64       `json:"elapsedMs"`
	StepsSkipped int           `json:"stepsSkipped,omitempty"`
	WarmHit      bool          `json:"warmHit,omitempty"`
	Error        string        `json:"error,omitempty"`
}

type ScenarioAggregate struct {
	Platform     string  `json:"platform"`
	Agent        string  `json:"agent"`
	Scenario     string  `json:"scenario"`
	AvgMs        float64 `json:"avgMs"`
	MinMs        float64 `json:"minMs"`
	MaxMs        float64 `json:"maxMs"`
	P95Ms        float64 `json:"p95Ms"`
	Trials       int     `json:"trials"`
	SkippedSteps float64 `json:"avgSkippedSteps,omitempty"`
	WarmHitRate  float64 `json:"warmHitRate,omitempty"`
}

type CrossPlatformSummary struct {
	Platform string  `json:"platform"`
	Scenario string  `json:"scenario"`
	AvgMs    float64 `json:"avgMs"`
	VsNative string  `json:"vsNative"`
}

type ExperimentReport struct {
	RunAt            string                            `json:"runAt"`
	Environment      EnvironmentInfo                   `json:"environment"`
	Config           ExperimentConfig                  `json:"config"`
	RawResults       []PlatformResult                  `json:"rawResults"`
	Aggregates       []ScenarioAggregate               `json:"aggregates"`
	CrossPlatform    map[string][]CrossPlatformSummary `json:"crossPlatform"`
	V1VsV2Comparison []V1V2Row                         `json:"v1VsV2,omitempty"`
	RadarScores      map[string]RadarScore             `json:"radarScores"`
	Verdicts         []Verdict                         `json:"verdicts"`
}

type EnvironmentInfo struct {
	OS       string `json:"os"`
	Arch     string `json:"arch"`
	NumCPU   int    `json:"numCPU"`
	GoMaxP   int    `json:"goMaxProcs"`
	Compiler string `json:"compiler"`
}

type V1V2Row struct {
	Scenario string  `json:"scenario"`
	V1Ms     float64 `json:"v1Ms"`
	V2Ms     float64 `json:"v2Ms"`
	NativeMs float64 `json:"nativeMs"`
	V1Vs     string  `json:"v1VsNative"`
	V2Vs     string  `json:"v2VsNative"`
	Improved string  `json:"improved"`
}

type RadarScore struct {
	ExecOverhead   int `json:"execOverhead"`
	Recovery       int `json:"recovery"`
	Concurrency    int `json:"concurrency"`
	Isolation      int `json:"isolation"`
	Observability  int `json:"observability"`
	Declarative    int `json:"declarative"`
	EaseOfAdoption int `json:"easeOfAdoption"`
}

type Verdict struct {
	ID     string `json:"id"`
	Desc   string `json:"description"`
	Target string `json:"target"`
	Actual string `json:"actual"`
	Pass   bool   `json:"pass"`
}

type ExperimentConfig struct {
	Platforms []string `json:"platforms"`
	Agents    []string `json:"agents"`
	Scenarios []string `json:"scenarios"`
	Trials    int      `json:"trials"`
	TotalRuns int      `json:"totalRuns"`
}

func DefaultAgents() []AgentDef {
	return []AgentDef{
		{Name: "SWE-Agent", Steps: 7, StepWork: 100 * time.Millisecond},
		{Name: "GPT-Researcher", Steps: 5, StepWork: 100 * time.Millisecond},
		{Name: "DeepSeek-Harness", Steps: 10, StepWork: 100 * time.Millisecond},
	}
}

// doStepWork performs real CPU-bound SHA-256 hashing for the target duration.
func doStepWork(target time.Duration) string {
	start := time.Now()
	h := sha256.New()
	payload := make([]byte, 512)
	for i := range payload {
		payload[i] = byte(i & 0xFF)
	}
	var digest [32]byte
	n := 0
	for time.Since(start) < target {
		h.Reset()
		h.Write(payload)
		copy(digest[:], h.Sum(nil))
		payload[0] ^= digest[0]
		n++
	}
	return fmt.Sprintf("sha256:%x:n=%d", digest[:8], n)
}

func crashPoint(steps int) int {
	return int(math.Ceil(float64(steps) * 0.6))
}

// ─── Native Platform ────────────────────────────────────────

func runNative(agent AgentDef, scenario string) PlatformResult {
	var elapsed time.Duration
	var skipped int

	switch scenario {
	case "S0-normal":
		start := time.Now()
		for i := 0; i < agent.Steps; i++ {
			doStepWork(agent.StepWork)
		}
		elapsed = time.Since(start)

	case "S1-recovery":
		// Native has no recovery — must re-execute all steps from scratch
		start := time.Now()
		for i := 0; i < agent.Steps; i++ {
			doStepWork(agent.StepWork)
		}
		elapsed = time.Since(start)
		skipped = 0

	case "S3-concurrent-3x":
		start := time.Now()
		var wg sync.WaitGroup
		for c := 0; c < 3; c++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < agent.Steps; i++ {
					doStepWork(agent.StepWork)
				}
			}()
		}
		wg.Wait()
		elapsed = time.Since(start)
	}

	return PlatformResult{
		Platform:     "native",
		Elapsed:      elapsed,
		ElapsedMs:    float64(elapsed) / float64(time.Millisecond),
		StepsSkipped: skipped,
	}
}

// ─── AF v2 Platform (real components) ───────────────────────

func newV2Pool() *pool.Manager {
	return pool.NewManager(pool.PoolConfig{
		Name:               "af-v2-bench",
		MinSize:            10,
		MaxSize:            20,
		ScaleUpThreshold:   0.3,
		ScaleDownThreshold: 0.8,
		ScaleUpStep:        3,
		CooldownSeconds:    1,
		Image:              "agent-runtime:latest",
		RuntimeClass:       "runsc",
	})
}

func runAFv2(poolMgr *pool.Manager, agent AgentDef, scenario, trialID string) PlatformResult {
	switch scenario {
	case "S0-normal":
		return runAFv2S0(poolMgr, agent, trialID)
	case "S1-recovery":
		return runAFv2S1(poolMgr, agent, trialID)
	case "S3-concurrent-3x":
		return runAFv2S3(poolMgr, agent, trialID)
	}
	return PlatformResult{Platform: "af-v2", Error: "unknown scenario"}
}

func runAFv2S0(poolMgr *pool.Manager, agent AgentDef, trialID string) PlatformResult {
	start := time.Now()

	claim, err := poolMgr.Claim("s0-" + trialID)
	if err != nil {
		return PlatformResult{Platform: "af-v2", Error: fmt.Sprintf("pool.Claim: %v", err)}
	}

	tmpDir, err := os.MkdirTemp("", "afv2-s0-*")
	if err != nil {
		return PlatformResult{Platform: "af-v2", Error: fmt.Sprintf("mkdirTemp: %v", err)}
	}
	defer os.RemoveAll(tmpDir)

	r, err := runner.NewRunner(runner.Config{WorkDir: tmpDir, PoolMode: true})
	if err != nil {
		return PlatformResult{Platform: "af-v2", Error: fmt.Sprintf("runner.New: %v", err)}
	}

	for i := 0; i < agent.Steps; i++ {
		stepID := fmt.Sprintf("step-%d", i)
		if _, err := r.HandleStepBegin(stepID); err != nil {
			r.Close()
			return PlatformResult{Platform: "af-v2", Error: fmt.Sprintf("stepBegin: %v", err)}
		}
		output := doStepWork(agent.StepWork)
		if _, err := r.HandleStepEnd(stepID, 0, output); err != nil {
			r.Close()
			return PlatformResult{Platform: "af-v2", Error: fmt.Sprintf("stepEnd: %v", err)}
		}
	}

	r.Close()
	poolMgr.Release(claim.Pod.ID)

	elapsed := time.Since(start)
	return PlatformResult{
		Platform:  "af-v2",
		Elapsed:   elapsed,
		ElapsedMs: float64(elapsed) / float64(time.Millisecond),
		WarmHit:   claim.WarmHit,
	}
}

func runAFv2S1(poolMgr *pool.Manager, agent AgentDef, trialID string) PlatformResult {
	tmpDir, err := os.MkdirTemp("", "afv2-s1-*")
	if err != nil {
		return PlatformResult{Platform: "af-v2", Error: fmt.Sprintf("mkdirTemp: %v", err)}
	}
	defer os.RemoveAll(tmpDir)

	// Phase 1 (setup, NOT timed): execute steps up to crash point, persisting WAL
	cp := crashPoint(agent.Steps)
	r1, err := runner.NewRunner(runner.Config{WorkDir: tmpDir})
	if err != nil {
		return PlatformResult{Platform: "af-v2", Error: fmt.Sprintf("runner.New(setup): %v", err)}
	}
	for i := 0; i < cp; i++ {
		stepID := fmt.Sprintf("step-%d", i)
		r1.HandleStepBegin(stepID)
		output := doStepWork(agent.StepWork)
		r1.HandleStepEnd(stepID, 0, output)
	}
	r1.Close()

	// Phase 2 (timed): recovery with WAL replay and step skipping
	start := time.Now()

	claim, err := poolMgr.Claim("s1-" + trialID)
	if err != nil {
		return PlatformResult{Platform: "af-v2", Error: fmt.Sprintf("pool.Claim(recovery): %v", err)}
	}

	r2, err := runner.NewRunner(runner.Config{
		WorkDir:      tmpDir,
		RecoveryMode: true,
		PoolMode:     true,
	})
	if err != nil {
		return PlatformResult{Platform: "af-v2", Error: fmt.Sprintf("runner.New(recovery): %v", err)}
	}

	skipped := 0
	for i := 0; i < agent.Steps; i++ {
		stepID := fmt.Sprintf("step-%d", i)
		resp, err := r2.HandleStepBegin(stepID)
		if err != nil {
			r2.Close()
			return PlatformResult{Platform: "af-v2", Error: fmt.Sprintf("stepBegin(recovery): %v", err)}
		}
		if resp.Skipped {
			skipped++
			continue
		}
		output := doStepWork(agent.StepWork)
		if _, err := r2.HandleStepEnd(stepID, 0, output); err != nil {
			r2.Close()
			return PlatformResult{Platform: "af-v2", Error: fmt.Sprintf("stepEnd(recovery): %v", err)}
		}
	}

	r2.Close()
	poolMgr.Release(claim.Pod.ID)

	elapsed := time.Since(start)
	return PlatformResult{
		Platform:     "af-v2",
		Elapsed:      elapsed,
		ElapsedMs:    float64(elapsed) / float64(time.Millisecond),
		StepsSkipped: skipped,
		WarmHit:      claim.WarmHit,
	}
}

func runAFv2S3(poolMgr *pool.Manager, agent AgentDef, trialID string) PlatformResult {
	start := time.Now()

	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for c := 0; c < 3; c++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			claim, err := poolMgr.Claim(fmt.Sprintf("s3-%s-%d", trialID, idx))
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}

			tmpDir, err := os.MkdirTemp("", fmt.Sprintf("afv2-s3-%d-*", idx))
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}
			defer os.RemoveAll(tmpDir)

			r, err := runner.NewRunner(runner.Config{WorkDir: tmpDir, PoolMode: true})
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}

			for i := 0; i < agent.Steps; i++ {
				stepID := fmt.Sprintf("step-%d-%d", idx, i)
				r.HandleStepBegin(stepID)
				output := doStepWork(agent.StepWork)
				r.HandleStepEnd(stepID, 0, output)
			}
			r.Close()
			poolMgr.Release(claim.Pod.ID)
		}(c)
	}
	wg.Wait()

	elapsed := time.Since(start)
	if firstErr != nil {
		return PlatformResult{Platform: "af-v2", Error: firstErr.Error()}
	}
	return PlatformResult{
		Platform:  "af-v2",
		Elapsed:   elapsed,
		ElapsedMs: float64(elapsed) / float64(time.Millisecond),
		WarmHit:   true,
	}
}

// ─── LangGraph Platform (real Python subprocess) ────────────

func checkLangGraph() bool {
	cmd := exec.Command("python3", "-c", "import langgraph; print('ok')")
	out, err := cmd.Output()
	return err == nil && strings.TrimSpace(string(out)) == "ok"
}

func langGraphBenchPath() string {
	exe, _ := os.Executable()
	dir := filepath.Dir(exe)
	candidates := []string{
		filepath.Join(dir, "langgraph_bench.py"),
		filepath.Join(dir, "..", "experiment", "langgraph_bench.py"),
	}
	cwd, _ := os.Getwd()
	candidates = append(candidates,
		filepath.Join(cwd, "experiment", "langgraph_bench.py"),
		filepath.Join(cwd, "langgraph_bench.py"),
	)
	for _, p := range candidates {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}

func runLangGraph(benchScript string, agent AgentDef, scenario string) PlatformResult {
	stepWorkMs := fmt.Sprintf("%.0f", float64(agent.StepWork)/float64(time.Millisecond))
	cmd := exec.Command("python3", benchScript,
		fmt.Sprintf("%d", agent.Steps), stepWorkMs, scenario)
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		return PlatformResult{Platform: "langgraph", Error: fmt.Sprintf("subprocess: %v", err)}
	}

	var result struct {
		ElapsedMs    float64 `json:"elapsed_ms"`
		StepsSkipped int     `json:"steps_skipped"`
		Err          string  `json:"error"`
	}
	if err := json.Unmarshal(out, &result); err != nil {
		return PlatformResult{Platform: "langgraph", Error: fmt.Sprintf("parse: %v (raw: %s)", err, string(out))}
	}
	if result.Err != "" {
		return PlatformResult{Platform: "langgraph", Error: result.Err}
	}

	elapsed := time.Duration(result.ElapsedMs * float64(time.Millisecond))
	return PlatformResult{
		Platform:     "langgraph",
		Elapsed:      elapsed,
		ElapsedMs:    result.ElapsedMs,
		StepsSkipped: result.StepsSkipped,
	}
}

// ─── Orchestration ──────────────────────────────────────────

func RunFullExperiment(trials int, temporalAddr string) *ExperimentReport {
	platforms := []string{"native", "af-v2"}

	// Check LangGraph availability
	lgAvailable := checkLangGraph()
	lgScript := ""
	if lgAvailable {
		lgScript = langGraphBenchPath()
		if lgScript == "" {
			lgAvailable = false
		}
	}
	if lgAvailable {
		platforms = append(platforms, "langgraph")
	}

	var temporalBench *TemporalBench
	if temporalAddr != "" {
		if tb, err := NewTemporalBench(temporalAddr); err == nil {
			temporalBench = tb
			platforms = append(platforms, "temporal")
			defer tb.Close()
		}
	}

	scenarios := []string{"S0-normal", "S1-recovery", "S3-concurrent-3x"}
	agents := DefaultAgents()
	totalRuns := len(platforms) * len(agents) * len(scenarios) * trials

	env := EnvironmentInfo{
		OS:       runtime.GOOS,
		Arch:     runtime.GOARCH,
		NumCPU:   runtime.NumCPU(),
		GoMaxP:   runtime.GOMAXPROCS(0),
		Compiler: runtime.Compiler,
	}

	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║     Agent-Fabric v2 Real Cross-Platform Experiment          ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Printf("Environment: %s/%s, %d CPUs, GOMAXPROCS=%d\n", env.OS, env.Arch, env.NumCPU, env.GoMaxP)
	fmt.Printf("Platforms: %v\n", platforms)
	fmt.Printf("Agents: %d (%v)\n", len(agents), agentNames(agents))
	fmt.Printf("Scenarios: %v\n", scenarios)
	fmt.Printf("Trials: %d | Total runs: %d\n", trials, totalRuns)
	fmt.Printf("Step work: real SHA-256 CPU computation, %v per step\n", agents[0].StepWork)
	if lgAvailable {
		fmt.Printf("LangGraph: available (real Python subprocess via %s)\n", filepath.Base(lgScript))
	} else {
		fmt.Println("LangGraph: not available (pip install langgraph to enable)")
	}
	if temporalBench != nil {
		fmt.Printf("Temporal: available (Go SDK via %s)\n", temporalAddr)
	} else {
		fmt.Println("Temporal: not available (temporal server start-dev)")
	}
	fmt.Println("NOTE: Agent names inspired by real products, not running those products.")
	fmt.Println()

	// Warm up CPU cache with one throw-away call
	doStepWork(10 * time.Millisecond)

	// Create shared AF v2 pool (pre-warmed pods)
	afv2Pool := newV2Pool()
	fmt.Printf("AF-v2 pool ready: %+v\n\n", afv2Pool.Status())

	var results []PlatformResult
	completed := 0
	errors := 0

	for _, platform := range platforms {
		fmt.Printf("── Platform: %s ──\n", platform)
		for _, agent := range agents {
			for _, scenario := range scenarios {
				for trial := 1; trial <= trials; trial++ {
					trialID := fmt.Sprintf("%s-%s-t%d", agent.Name, scenario, trial)

					var result PlatformResult
					switch platform {
					case "native":
						result = runNative(agent, scenario)
					case "af-v2":
						result = runAFv2(afv2Pool, agent, scenario, trialID)
					case "langgraph":
						result = runLangGraph(lgScript, agent, scenario)
					case "temporal":
						if temporalBench != nil {
							result = temporalBench.Run(agent, scenario, trialID)
						}
					}

					result.Platform = platform
					result.Agent = agent.Name
					result.Scenario = scenario
					result.Trial = trial

					results = append(results, result)
					completed++

					if result.Error != "" {
						errors++
						fmt.Printf("  [%d/%d] %s/%s/%s t%d ERROR: %s\n",
							completed, totalRuns, platform, agent.Name, scenario, trial, result.Error)
					} else if completed%9 == 0 || completed == totalRuns {
						fmt.Printf("  [%d/%d] %s/%s/%s t%d = %.1fms",
							completed, totalRuns, platform, agent.Name, scenario, trial, result.ElapsedMs)
						if result.StepsSkipped > 0 {
							fmt.Printf(" (skipped %d steps)", result.StepsSkipped)
						}
						if result.WarmHit {
							fmt.Printf(" [warm]")
						}
						fmt.Println()
					}
				}
			}
		}
		fmt.Println()
	}

	fmt.Printf("All %d runs completed (%d errors).\n\n", totalRuns, errors)

	report := &ExperimentReport{
		RunAt:       time.Now().UTC().Format(time.RFC3339),
		Environment: env,
		Config: ExperimentConfig{
			Platforms: platforms,
			Agents:    agentNames(agents),
			Scenarios: scenarios,
			Trials:    trials,
			TotalRuns: totalRuns,
		},
		RawResults: results,
	}

	report.Aggregates = computeAggregates(results)
	report.CrossPlatform = computeCrossPlatform(report.Aggregates, platforms)
	report.RadarScores = computeRadarScores(report.Aggregates, platforms)
	report.Verdicts = computeVerdicts(report.Aggregates)

	return report
}

func agentNames(agents []AgentDef) []string {
	names := make([]string, len(agents))
	for i, a := range agents {
		names[i] = a.Name
	}
	return names
}

// ─── Aggregation ────────────────────────────────────────────

func computeAggregates(results []PlatformResult) []ScenarioAggregate {
	type key struct{ p, a, s string }
	grouped := make(map[key][]PlatformResult)
	for _, r := range results {
		if r.Error != "" {
			continue
		}
		k := key{r.Platform, r.Agent, r.Scenario}
		grouped[k] = append(grouped[k], r)
	}

	var aggs []ScenarioAggregate
	for k, rs := range grouped {
		var durs []float64
		var totalSkipped int
		warmHits := 0
		for _, r := range rs {
			durs = append(durs, r.ElapsedMs)
			totalSkipped += r.StepsSkipped
			if r.WarmHit {
				warmHits++
			}
		}

		sort.Float64s(durs)
		sum := 0.0
		for _, d := range durs {
			sum += d
		}
		avg := sum / float64(len(durs))
		p95Idx := int(math.Ceil(float64(len(durs))*0.95)) - 1
		if p95Idx < 0 {
			p95Idx = 0
		}
		if p95Idx >= len(durs) {
			p95Idx = len(durs) - 1
		}

		aggs = append(aggs, ScenarioAggregate{
			Platform:     k.p,
			Agent:        k.a,
			Scenario:     k.s,
			AvgMs:        math.Round(avg*100) / 100,
			MinMs:        durs[0],
			MaxMs:        durs[len(durs)-1],
			P95Ms:        durs[p95Idx],
			Trials:       len(rs),
			SkippedSteps: math.Round(float64(totalSkipped)/float64(len(rs))*100) / 100,
			WarmHitRate:  math.Round(float64(warmHits)/float64(len(rs))*100) / 100,
		})
	}

	sort.Slice(aggs, func(i, j int) bool {
		if aggs[i].Scenario != aggs[j].Scenario {
			return aggs[i].Scenario < aggs[j].Scenario
		}
		if aggs[i].Platform != aggs[j].Platform {
			return aggs[i].Platform < aggs[j].Platform
		}
		return aggs[i].Agent < aggs[j].Agent
	})

	return aggs
}

func computeCrossPlatform(aggs []ScenarioAggregate, platforms []string) map[string][]CrossPlatformSummary {
	type key struct{ p, s string }
	grouped := make(map[key][]float64)
	for _, a := range aggs {
		k := key{a.Platform, a.Scenario}
		grouped[k] = append(grouped[k], a.AvgMs)
	}

	scenAvg := make(map[key]float64)
	for k, vals := range grouped {
		sum := 0.0
		for _, v := range vals {
			sum += v
		}
		scenAvg[k] = sum / float64(len(vals))
	}

	scenarios := []string{"S0-normal", "S1-recovery", "S3-concurrent-3x"}
	result := make(map[string][]CrossPlatformSummary)
	for _, s := range scenarios {
		nativeAvg := scenAvg[key{"native", s}]
		for _, p := range platforms {
			avg := scenAvg[key{p, s}]
			var vs string
			if p == "native" {
				vs = "baseline"
			} else if nativeAvg > 0 {
				pct := (avg - nativeAvg) / nativeAvg * 100
				vs = fmt.Sprintf("%+.1f%%", pct)
			}
			result[s] = append(result[s], CrossPlatformSummary{
				Platform: p,
				Scenario: s,
				AvgMs:    math.Round(avg*10) / 10,
				VsNative: vs,
			})
		}
	}
	return result
}

func computeRadarScores(aggs []ScenarioAggregate, platforms []string) map[string]RadarScore {
	type key struct{ p, s string }
	scenAvg := make(map[key]float64)
	scenAvgCount := make(map[key]int)
	for _, a := range aggs {
		k := key{a.Platform, a.Scenario}
		scenAvg[k] += a.AvgMs
		scenAvgCount[k]++
	}
	for k := range scenAvg {
		scenAvg[k] /= float64(scenAvgCount[k])
	}

	nativeS0 := scenAvg[key{"native", "S0-normal"}]

	scores := make(map[string]RadarScore)
	for _, p := range platforms {
		s0 := scenAvg[key{p, "S0-normal"}]
		s3 := scenAvg[key{p, "S3-concurrent-3x"}]

		execScore := int(math.Max(0, math.Min(100, 100-((s0-nativeS0)/nativeS0*100*2))))
		s3Overhead := 0.0
		if s0 > 0 {
			s3Overhead = (s3 - s0) / s0 * 100
		}
		concScore := int(math.Max(0, math.Min(100, 100-s3Overhead*1.5)))

		// Qualitative architectural assessment (not performance-measured)
		iso, decl, obs, ease, rec := 50, 50, 50, 50, 50
		switch p {
		case "native":
			iso, decl, obs, ease, rec = 10, 10, 30, 95, 10
		case "af-v2":
			iso, decl, obs, ease, rec = 95, 80, 90, 70, 95
		case "langgraph":
			iso, decl, obs, ease, rec = 40, 80, 65, 70, 30
		case "temporal":
			iso, decl, obs, ease, rec = 60, 90, 85, 55, 95
		}

		scores[p] = RadarScore{
			ExecOverhead:   execScore,
			Recovery:       rec,
			Concurrency:    concScore,
			Isolation:      iso,
			Observability:  obs,
			Declarative:    decl,
			EaseOfAdoption: ease,
		}
	}
	return scores
}

func computeVerdicts(aggs []ScenarioAggregate) []Verdict {
	type key struct{ p, s string }
	scenAvg := make(map[key]float64)
	scenAvgCount := make(map[key]int)
	scenSkip := make(map[key]float64)
	for _, a := range aggs {
		k := key{a.Platform, a.Scenario}
		scenAvg[k] += a.AvgMs
		scenAvgCount[k]++
		scenSkip[k] += a.SkippedSteps
	}
	for k := range scenAvg {
		scenAvg[k] /= float64(scenAvgCount[k])
		scenSkip[k] /= float64(scenAvgCount[k])
	}

	nativeS0 := scenAvg[key{"native", "S0-normal"}]
	v2S0 := scenAvg[key{"af-v2", "S0-normal"}]
	v2S1 := scenAvg[key{"af-v2", "S1-recovery"}]
	v2S3 := scenAvg[key{"af-v2", "S3-concurrent-3x"}]
	nativeS1 := scenAvg[key{"native", "S1-recovery"}]
	temporalS1 := scenAvg[key{"temporal", "S1-recovery"}]
	v2S1Skip := scenSkip[key{"af-v2", "S1-recovery"}]

	s0Overhead := (v2S0 - nativeS0) / nativeS0 * 100

	agents := DefaultAgents()
	totalSteps := 0
	for _, a := range agents {
		totalSteps += a.Steps
	}
	avgSteps := float64(totalSteps) / float64(len(agents))
	skipRatio := v2S1Skip / avgSteps * 100

	s1VsNative := 0.0
	if nativeS1 > 0 {
		s1VsNative = v2S1 / nativeS1
	}

	s3Overhead := 0.0
	if v2S0 > 0 {
		s3Overhead = (v2S3 - v2S0) / v2S0 * 100
	}

	verdicts := []Verdict{
		{
			ID:     "P-1",
			Desc:   "S0 avg elapsed <= Native +8%",
			Target: "≤ +8%",
			Actual: fmt.Sprintf("+%.2f%%", s0Overhead),
			Pass:   s0Overhead <= 8.0,
		},
		{
			ID:     "P-3",
			Desc:   "S1 skip ratio >= 60%",
			Target: "≥ 60%",
			Actual: fmt.Sprintf("%.1f%%", skipRatio),
			Pass:   skipRatio >= 60.0,
		},
		{
			ID:     "P-4",
			Desc:   "S1 recovery total <= Native S1 * 1.20x",
			Target: "≤ 1.20x",
			Actual: fmt.Sprintf("%.3fx", s1VsNative),
			Pass:   s1VsNative <= 1.20,
		},
		{
			ID:     "P-5",
			Desc:   "S3 concurrent 3x overhead <= +15%",
			Target: "≤ +15%",
			Actual: fmt.Sprintf("+%.2f%%", s3Overhead),
			Pass:   s3Overhead <= 15.0,
		},
	}

	if temporalS1 > 0 {
		v2VsTemporal := v2S1 / temporalS1
		verdicts = append(verdicts, Verdict{
			ID:     "P-6",
			Desc:   "S1 af-v2 recovery <= Temporal recovery x1.50",
			Target: "≤ 1.50x",
			Actual: fmt.Sprintf("%.3fx", v2VsTemporal),
			Pass:   v2VsTemporal <= 1.50,
		})
	}

	return verdicts
}

// ─── Output ─────────────────────────────────────────────────

func (r *ExperimentReport) SaveJSON(path string) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func (r *ExperimentReport) PrintSummary() {
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║        Agent-Fabric v2 — Real Experiment Results            ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println("  Agent names inspired by real products, not running those products.")
	fmt.Printf("Run: %s\n", r.RunAt)
	fmt.Printf("Env: %s/%s %d-CPU GOMAXPROCS=%d\n",
		r.Environment.OS, r.Environment.Arch, r.Environment.NumCPU, r.Environment.GoMaxP)
	fmt.Printf("Platforms: %v | Agents: %v | Trials: %d | Total: %d runs\n\n",
		r.Config.Platforms, r.Config.Agents, r.Config.Trials, r.Config.TotalRuns)

	scenarios := []string{"S0-normal", "S1-recovery", "S3-concurrent-3x"}
	for _, s := range scenarios {
		fmt.Printf("┌─── %s ─────────────────────────────────────────┐\n", s)
		fmt.Printf("│ %-14s │ %10s │ %12s │\n", "Platform", "Avg (ms)", "vs Native")
		fmt.Println("├────────────────┼────────────┼──────────────┤")
		if entries, ok := r.CrossPlatform[s]; ok {
			for _, e := range entries {
				marker := " "
				if e.Platform == "af-v2" {
					marker = "*"
				}
				fmt.Printf("│%s%-13s │ %10.1f │ %12s │\n", marker, e.Platform, e.AvgMs, e.VsNative)
			}
		}
		fmt.Println("└────────────────┴────────────┴──────────────┘")
		fmt.Println()
	}

	fmt.Println("┌─── Per-Agent Detail (S0-normal) ────────────────────────────────┐")
	fmt.Printf("│ %-18s │ %-10s │ %10s │ %10s │ %8s │\n",
		"Agent", "Platform", "Avg(ms)", "P95(ms)", "WAL hit")
	fmt.Println("├────────────────────┼────────────┼────────────┼────────────┼──────────┤")
	for _, a := range r.Aggregates {
		if a.Scenario == "S0-normal" {
			warmStr := ""
			if a.WarmHitRate > 0 {
				warmStr = fmt.Sprintf("%.0f%%", a.WarmHitRate*100)
			}
			fmt.Printf("│ %-18s │ %-10s │ %10.2f │ %10.2f │ %8s │\n",
				a.Agent, a.Platform, a.AvgMs, a.P95Ms, warmStr)
		}
	}
	fmt.Println("└────────────────────┴────────────┴────────────┴────────────┴──────────┘")
	fmt.Println()

	fmt.Println("┌─── S1 Recovery Detail ──────────────────────────────────────────┐")
	fmt.Printf("│ %-18s │ %-10s │ %10s │ %12s │\n",
		"Agent", "Platform", "Avg(ms)", "Skipped")
	fmt.Println("├────────────────────┼────────────┼────────────┼──────────────┤")
	for _, a := range r.Aggregates {
		if a.Scenario == "S1-recovery" {
			skipStr := ""
			if a.SkippedSteps > 0 {
				skipStr = fmt.Sprintf("%.1f steps", a.SkippedSteps)
			} else {
				skipStr = "0 (no WAL)"
			}
			fmt.Printf("│ %-18s │ %-10s │ %10.2f │ %12s │\n",
				a.Agent, a.Platform, a.AvgMs, skipStr)
		}
	}
	fmt.Println("└────────────────────┴────────────┴────────────┴──────────────┘")
	fmt.Println()

	fmt.Println("┌─── Radar Scores (/100) ─────────────────────────────────────────────────────┐")
	fmt.Printf("│ %-12s │ %6s │ %8s │ %6s │ %5s │ %5s │ %5s │ %5s │\n",
		"Platform", "Exec", "Recovery", "Conc", "Isol", "Obs", "Decl", "Ease")
	fmt.Println("├──────────────┼────────┼──────────┼────────┼───────┼───────┼───────┼───────┤")
	for _, p := range r.Config.Platforms {
		s := r.RadarScores[p]
		marker := " "
		if p == "af-v2" {
			marker = "*"
		}
		fmt.Printf("│%s%-11s │ %6d │ %8d │ %6d │ %5d │ %5d │ %5d │ %5d │\n",
			marker, p, s.ExecOverhead, s.Recovery, s.Concurrency,
			s.Isolation, s.Observability, s.Declarative, s.EaseOfAdoption)
	}
	fmt.Println("└──────────────┴────────┴──────────┴────────┴───────┴───────┴───────┴───────┘")
	fmt.Printf("  (Exec/Conc = measured; Isol/Obs/Decl/Ease/Recovery = architectural assessment)\n\n")

	fmt.Println("┌─── v2 Acceptance Verdicts ──────────────────────────────────────┐")
	allPass := true
	for _, v := range r.Verdicts {
		status := "PASS"
		if !v.Pass {
			status = "FAIL"
			allPass = false
		}
		fmt.Printf("│ [%s] %s: %s (target: %s, actual: %s)\n",
			status, v.ID, v.Desc, v.Target, v.Actual)
	}
	fmt.Println("└──────────────────────────────────────────────────────────────────┘")
	fmt.Println()

	if allPass {
		fmt.Println("ALL TARGETS MET — v2 achieves design goals with real measurements.")
	} else {
		fmt.Println("Some targets not met — review results.")
	}
}

func (r *ExperimentReport) SaveMarkdown(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	fmt.Fprintf(f, "# Agent-Fabric v2 Experiment Report\n\n")
	fmt.Fprintf(f, "**Date**: %s\n\n", r.RunAt)
	fmt.Fprintf(f, "**Environment**: %s/%s, %d CPUs, GOMAXPROCS=%d, %s\n\n",
		r.Environment.OS, r.Environment.Arch, r.Environment.NumCPU, r.Environment.GoMaxP, r.Environment.Compiler)
	fmt.Fprintf(f, "> **Note**: Agent names (SWE-Agent, GPT-Researcher, DeepSeek-Harness) are inspired by\n")
	fmt.Fprintf(f, "> real products' typical pipeline lengths. This benchmark does not run those products.\n\n")
	fmt.Fprintf(f, "**Methodology**: All measurements are wall-clock times from real code execution.\n")
	fmt.Fprintf(f, "- **Step work**: SHA-256 CPU hashing for %s per step (not sleep/jitter)\n", DefaultAgents()[0].StepWork)
	fmt.Fprintf(f, "- **Native**: Direct Go function calls, no framework\n")
	fmt.Fprintf(f, "- **AF v2**: Real pool.Manager.Claim + runner.NewRunner + WAL writes (fsync) + pool.Release\n")
	fmt.Fprintf(f, "- **LangGraph**: Real Python subprocess, StateGraph with sequential nodes\n")
	fmt.Fprintf(f, "- **Temporal**: Go SDK workflow + activities, connected to real dev server (gRPC)\n\n")

	fmt.Fprintf(f, "## Cross-Platform Summary\n\n")
	for _, s := range []string{"S0-normal", "S1-recovery", "S3-concurrent-3x"} {
		fmt.Fprintf(f, "### %s\n\n", s)
		fmt.Fprintf(f, "| Platform | Avg (ms) | vs Native |\n")
		fmt.Fprintf(f, "|----------|----------|----------|\n")
		if entries, ok := r.CrossPlatform[s]; ok {
			for _, e := range entries {
				fmt.Fprintf(f, "| %s | %.1f | %s |\n", e.Platform, e.AvgMs, e.VsNative)
			}
		}
		fmt.Fprintln(f)
	}

	fmt.Fprintf(f, "## Acceptance Verdicts\n\n")
	fmt.Fprintf(f, "| ID | Description | Target | Actual | Status |\n")
	fmt.Fprintf(f, "|----|-------------|--------|--------|--------|\n")
	for _, v := range r.Verdicts {
		status := "PASS"
		if !v.Pass {
			status = "FAIL"
		}
		fmt.Fprintf(f, "| %s | %s | %s | %s | %s |\n", v.ID, v.Desc, v.Target, v.Actual, status)
	}
	fmt.Fprintln(f)

	fmt.Fprintf(f, "## Raw Data\n\n")
	fmt.Fprintf(f, "See `experiment-report.json` for full raw results (%d data points).\n", len(r.RawResults))

	return nil
}

func SaveAll(report *ExperimentReport, outDir string) error {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	if err := report.SaveJSON(filepath.Join(outDir, "experiment-report.json")); err != nil {
		return err
	}
	if err := report.SaveMarkdown(filepath.Join(outDir, "experiment-report.md")); err != nil {
		return err
	}
	return nil
}
