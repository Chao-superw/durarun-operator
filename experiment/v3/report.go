package v3

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	goruntime "runtime"
	"time"
)

// MetricReport represents the result of a single acceptance metric.
type MetricReport struct {
	MetricID    string                   `json:"metric_id"`
	Description string                   `json:"description"`
	Target      string                   `json:"target"`
	Trials      []map[string]interface{} `json:"trials"`
	Mean        float64                  `json:"mean"`
	Stddev      float64                  `json:"stddev"`
	Pass        bool                     `json:"pass"`
	Timestamp   string                   `json:"timestamp"`
	Environment map[string]string        `json:"environment"`
}

// GetEnvironment returns system information for report context.
func GetEnvironment() map[string]string {
	return map[string]string{
		"os":   goruntime.GOOS,
		"arch": goruntime.GOARCH,
		"cpus": fmt.Sprintf("%d", goruntime.NumCPU()),
		"go":   goruntime.Version(),
	}
}

// CalcMean returns the arithmetic mean of vals.
func CalcMean(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	var sum float64
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}

// CalcStddev returns the population standard deviation given the mean.
func CalcStddev(vals []float64, mean float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	var sumSq float64
	for _, v := range vals {
		d := v - mean
		sumSq += d * d
	}
	return math.Sqrt(sumSq / float64(len(vals)))
}

// WriteReport writes a list of MetricReport to the given path as JSON.
func WriteReport(path string, reports []MetricReport) error {
	data, err := json.MarshalIndent(reports, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal report: %w", err)
	}
	return os.WriteFile(path, data, 0o644)
}

// WriteSingleReport writes a single MetricReport with auto-populated
// timestamp and environment information.
func WriteSingleReport(path string, report MetricReport) error {
	report.Timestamp = time.Now().UTC().Format(time.RFC3339)
	report.Environment = GetEnvironment()
	return WriteReport(path, []MetricReport{report})
}
