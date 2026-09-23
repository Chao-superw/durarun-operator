package wal

import "errors"

// ErrCorrupt is returned when a WAL file contains invalid or inconsistent data.
var ErrCorrupt = errors.New("wal: corrupt WAL file")

const (
	walDir       = ".wal"
	maxFileSize  = 10 * 1024 * 1024 // 10MB
	manifestFile = "manifest.json"

	TypeStepBegin    = "step_begin"
	TypeStepEnd      = "step_end"
	TypeStepRetry    = "step_retry"
	TypeStepTimeout  = "step_timeout"
	TypeJobCancelled = "job_cancelled"
)

// Record is a single entry in the WAL, encoded as one JSON line.
type Record struct {
	Seq       int64  `json:"seq"`
	Type      string `json:"type"`
	StepID    string `json:"step_id,omitempty"`
	Timestamp int64  `json:"ts"`
	Exit      *int   `json:"exit,omitempty"`
	OutputRef string `json:"output_ref,omitempty"`
	Attempt   int    `json:"attempt,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

// Manifest summarises the WAL state on disk.
type Manifest struct {
	SchemaVersion   int             `json:"schemaVersion"`
	LastCompleted   string          `json:"lastCompletedStep"`
	CompletedSteps  []CompletedStep `json:"completedSteps"`
	WALFileCount    int             `json:"walFileCount"`
	WALTotalRecords int64           `json:"walTotalRecords"`
}

// CompletedStep records one successfully finished step.
type CompletedStep struct {
	StepID    string `json:"stepId"`
	Seq       int64  `json:"seq"`
	OutputRef string `json:"outputRef"`
}
