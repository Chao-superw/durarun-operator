package wal

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteAndRead(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir)
	if err != nil {
		t.Fatal(err)
	}

	seq1, err := w.StepBegin("s1")
	if err != nil {
		t.Fatal(err)
	}
	if seq1 != 1 {
		t.Fatalf("expected seq 1, got %d", seq1)
	}

	seq2, err := w.StepEnd("s1", 0, "out/s1.tar.gz")
	if err != nil {
		t.Fatal(err)
	}
	if seq2 != 2 {
		t.Fatalf("expected seq 2, got %d", seq2)
	}

	seq3, err := w.StepBegin("s2")
	if err != nil {
		t.Fatal(err)
	}
	if seq3 != 3 {
		t.Fatalf("expected seq 3, got %d", seq3)
	}

	seq4, err := w.StepEnd("s2", 1, "out/s2.tar.gz")
	if err != nil {
		t.Fatal(err)
	}
	if seq4 != 4 {
		t.Fatalf("expected seq 4, got %d", seq4)
	}
	w.Close()

	r, err := NewReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	records, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 4 {
		t.Fatalf("expected 4 records, got %d", len(records))
	}
	if records[0].Type != TypeStepBegin || records[0].StepID != "s1" {
		t.Fatalf("unexpected record[0]: %+v", records[0])
	}
	if records[1].Type != TypeStepEnd || *records[1].Exit != 0 || records[1].OutputRef != "out/s1.tar.gz" {
		t.Fatalf("unexpected record[1]: %+v", records[1])
	}
	if records[3].Type != TypeStepEnd || *records[3].Exit != 1 {
		t.Fatalf("unexpected record[3]: %+v", records[3])
	}
}

func TestBuildManifest(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.StepBegin("s1")
	w.StepEnd("s1", 0, "ref-1")
	w.StepBegin("s2")
	w.StepEnd("s2", 1, "ref-2") // exit=1, not completed
	w.StepBegin("s3")
	w.StepEnd("s3", 0, "ref-3")
	w.Close()

	r, err := NewReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	m, err := r.BuildManifest()
	if err != nil {
		t.Fatal(err)
	}
	if m.SchemaVersion != 1 {
		t.Fatalf("expected schema version 1, got %d", m.SchemaVersion)
	}
	if len(m.CompletedSteps) != 2 {
		t.Fatalf("expected 2 completed steps, got %d", len(m.CompletedSteps))
	}
	if m.CompletedSteps[0].StepID != "s1" || m.CompletedSteps[1].StepID != "s3" {
		t.Fatalf("unexpected completed steps: %+v", m.CompletedSteps)
	}
	if m.LastCompleted != "s3" {
		t.Fatalf("expected lastCompleted s3, got %s", m.LastCompleted)
	}
	if m.WALTotalRecords != 6 {
		t.Fatalf("expected 6 total records, got %d", m.WALTotalRecords)
	}
}

func TestCorruptDetection(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.StepBegin("s1")
	w.StepEnd("s1", 0, "ref-1")
	w.Close()

	walFile := filepath.Join(walDirPath(dir), "wal-0001.log")

	// Tamper: create a sequence gap
	line1, _ := json.Marshal(Record{Seq: 1, Type: TypeStepBegin, StepID: "s1", Timestamp: 1})
	line2, _ := json.Marshal(Record{Seq: 5, Type: TypeStepEnd, StepID: "s1", Timestamp: 2})
	tampered := append(line1, '\n')
	tampered = append(tampered, line2...)
	tampered = append(tampered, '\n')
	os.WriteFile(walFile, tampered, 0o644)

	r, err := NewReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.ReadAll()
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt on seq gap, got %v", err)
	}

	// Malformed JSON
	os.WriteFile(walFile, []byte("not json\n"), 0o644)
	_, err = r.ReadAll()
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt on bad JSON, got %v", err)
	}

	// Truncated file (incomplete last line)
	good, _ := json.Marshal(Record{Seq: 1, Type: TypeStepBegin, StepID: "s1", Timestamp: 1})
	truncated := append(good, '\n')
	truncated = append(truncated, []byte(`{"seq":2,"type":"step_en`)...) // no closing brace or newline
	os.WriteFile(walFile, truncated, 0o644)
	_, err = r.ReadAll()
	// The scanner may or may not deliver the partial line; either a corrupt
	// error or a sequence validation failure is acceptable.
	if err == nil {
		t.Fatal("expected error on truncated file, got nil")
	}
}

func TestNewRecordTypes(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir)
	if err != nil {
		t.Fatal(err)
	}

	// step_retry
	w.StepBegin("s1")
	retrySeq, err := w.StepRetry("s1", 2)
	if err != nil {
		t.Fatal(err)
	}
	if retrySeq != 2 {
		t.Fatalf("expected retry seq 2, got %d", retrySeq)
	}

	// step_timeout
	timeoutSeq, err := w.StepTimeout("s1")
	if err != nil {
		t.Fatal(err)
	}
	if timeoutSeq != 3 {
		t.Fatalf("expected timeout seq 3, got %d", timeoutSeq)
	}

	// job_cancelled
	cancelSeq, err := w.JobCancelled("user requested")
	if err != nil {
		t.Fatal(err)
	}
	if cancelSeq != 4 {
		t.Fatalf("expected cancel seq 4, got %d", cancelSeq)
	}
	w.Close()

	// Read back and verify fields
	r, err := NewReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	records, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 4 {
		t.Fatalf("expected 4 records, got %d", len(records))
	}

	retry := records[1]
	if retry.Type != TypeStepRetry || retry.StepID != "s1" || retry.Attempt != 2 {
		t.Fatalf("unexpected retry record: %+v", retry)
	}

	timeout := records[2]
	if timeout.Type != TypeStepTimeout || timeout.StepID != "s1" || timeout.Reason != "timeout" {
		t.Fatalf("unexpected timeout record: %+v", timeout)
	}

	cancel := records[3]
	if cancel.Type != TypeJobCancelled || cancel.Reason != "user requested" {
		t.Fatalf("unexpected cancel record: %+v", cancel)
	}
	if cancel.StepID != "" {
		t.Fatalf("job_cancelled should have empty step_id, got %q", cancel.StepID)
	}
}

func TestFsyncLatency(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if w.LastFsyncLatency() != 0 {
		t.Fatal("expected zero latency before any writes")
	}

	w.StepBegin("s1")
	w.StepEnd("s1", 0, "ref")

	lat := w.LastFsyncLatency()
	if lat <= 0 {
		t.Fatalf("expected positive fsync latency after StepEnd, got %v", lat)
	}

	// Also verify that StepRetry updates latency
	w.StepBegin("s2")
	w.StepRetry("s2", 1)
	lat2 := w.LastFsyncLatency()
	if lat2 <= 0 {
		t.Fatalf("expected positive fsync latency after StepRetry, got %v", lat2)
	}
}

func TestFileRotation(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Write records with large OutputRef to exceed maxFileSize quickly.
	bigPayload := strings.Repeat("x", 512*1024) // 512KB per record
	count := 0
	for w.fileNum == 1 {
		count++
		stepID := "rot-step"
		w.StepBegin(stepID)
		w.StepEnd(stepID, 0, bigPayload)
		if count > 100 {
			t.Fatal("expected file rotation within 100 iterations")
		}
	}

	w.Close()

	// Verify multiple WAL files exist.
	wdir := walDirPath(dir)
	files, err := listWALFiles(wdir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) < 2 {
		t.Fatalf("expected at least 2 WAL files after rotation, got %d", len(files))
	}

	// All records must be readable and contiguous.
	r, err := NewReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	records, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != count*2 {
		t.Fatalf("expected %d records, got %d", count*2, len(records))
	}
}
