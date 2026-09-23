package runner

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"durarun-operator/wal"
)

func TestNewRunner(t *testing.T) {
	dir := t.TempDir()
	r, err := NewRunner(Config{WorkDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if r.workDir != dir {
		t.Fatalf("expected workDir %s, got %s", dir, r.workDir)
	}
	if r.walWriter == nil {
		t.Fatal("expected walWriter to be non-nil")
	}
	if r.recoveryMode {
		t.Fatal("expected recoveryMode false")
	}
	if len(r.completedSteps) != 0 {
		t.Fatalf("expected empty completedSteps, got %d", len(r.completedSteps))
	}
}

func TestPrepareEnvironment_NoRecovery(t *testing.T) {
	dir := t.TempDir()
	r, err := NewRunner(Config{WorkDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	env, err := r.PrepareEnvironment()
	if err != nil {
		t.Fatal(err)
	}

	if env["AF_RECOVERY_MODE"] != "false" {
		t.Fatalf("expected AF_RECOVERY_MODE=false, got %s", env["AF_RECOVERY_MODE"])
	}
	if env["AF_LAST_COMPLETED_STEP"] != "" {
		t.Fatalf("expected empty AF_LAST_COMPLETED_STEP, got %s", env["AF_LAST_COMPLETED_STEP"])
	}
	if env["AF_COMPLETED_STEPS"] != "" {
		t.Fatalf("expected empty AF_COMPLETED_STEPS, got %s", env["AF_COMPLETED_STEPS"])
	}
	if env["AF_STEP_OUTPUTS"] != "{}" {
		t.Fatalf("expected AF_STEP_OUTPUTS={}, got %s", env["AF_STEP_OUTPUTS"])
	}
	expectedSocket := filepath.Join(dir, ".wal", "af.sock")
	if env["AF_WAL_SOCKET"] != expectedSocket {
		t.Fatalf("expected AF_WAL_SOCKET=%s, got %s", expectedSocket, env["AF_WAL_SOCKET"])
	}
}

func TestPrepareEnvironment_Recovery(t *testing.T) {
	dir := t.TempDir()

	w, err := wal.NewWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.StepBegin("step-a")
	w.StepEnd("step-a", 0, "ref-a")
	w.StepBegin("step-b")
	w.StepEnd("step-b", 0, "ref-b")
	w.Close()

	r, err := NewRunner(Config{WorkDir: dir, RecoveryMode: true})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	env, err := r.PrepareEnvironment()
	if err != nil {
		t.Fatal(err)
	}

	if env["AF_RECOVERY_MODE"] != "true" {
		t.Fatalf("expected AF_RECOVERY_MODE=true, got %s", env["AF_RECOVERY_MODE"])
	}

	completedSteps := env["AF_COMPLETED_STEPS"]
	if !strings.Contains(completedSteps, "step-a") || !strings.Contains(completedSteps, "step-b") {
		t.Fatalf("expected AF_COMPLETED_STEPS to contain step-a and step-b, got %s", completedSteps)
	}

	last := env["AF_LAST_COMPLETED_STEP"]
	if last != "step-b" {
		t.Fatalf("expected AF_LAST_COMPLETED_STEP=step-b, got %s", last)
	}

	var outputs map[string]string
	if err := json.Unmarshal([]byte(env["AF_STEP_OUTPUTS"]), &outputs); err != nil {
		t.Fatal(err)
	}
	if outputs["step-a"] != "ref-a" || outputs["step-b"] != "ref-b" {
		t.Fatalf("unexpected AF_STEP_OUTPUTS: %v", outputs)
	}
}

func TestHandleStepBegin_Normal(t *testing.T) {
	dir := t.TempDir()
	r, err := NewRunner(Config{WorkDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	resp, err := r.HandleStepBegin("step-x")
	if err != nil {
		t.Fatal(err)
	}
	if resp.Skipped {
		t.Fatal("expected skipped=false")
	}
	if resp.Seq != 1 {
		t.Fatalf("expected seq 1, got %d", resp.Seq)
	}

	reader, err := wal.NewReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 WAL record, got %d", len(records))
	}
	if records[0].StepID != "step-x" {
		t.Fatalf("expected step-x, got %s", records[0].StepID)
	}
}

func TestHandleStepBegin_Recovery_Skipped(t *testing.T) {
	dir := t.TempDir()

	w, err := wal.NewWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.StepBegin("step-done")
	w.StepEnd("step-done", 0, "ref-done")
	w.Close()

	r, err := NewRunner(Config{WorkDir: dir, RecoveryMode: true})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	resp, err := r.HandleStepBegin("step-done")
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Skipped {
		t.Fatal("expected skipped=true for completed step in recovery mode")
	}
	if resp.CachedOutput != "ref-done" {
		t.Fatalf("expected cached output ref-done, got %s", resp.CachedOutput)
	}
}

func TestHandleStepBegin_Recovery_NotCompleted(t *testing.T) {
	dir := t.TempDir()

	w, err := wal.NewWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.StepBegin("step-done")
	w.StepEnd("step-done", 0, "ref-done")
	w.Close()

	r, err := NewRunner(Config{WorkDir: dir, RecoveryMode: true})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	resp, err := r.HandleStepBegin("step-new")
	if err != nil {
		t.Fatal(err)
	}
	if resp.Skipped {
		t.Fatal("expected skipped=false for uncompleted step in recovery mode")
	}
	if resp.Seq == 0 {
		t.Fatal("expected non-zero seq")
	}
}

func TestHandleStepEnd(t *testing.T) {
	dir := t.TempDir()
	r, err := NewRunner(Config{WorkDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	_, err = r.HandleStepBegin("step-1")
	if err != nil {
		t.Fatal(err)
	}

	resp, err := r.HandleStepEnd("step-1", 0, "hello world")
	if err != nil {
		t.Fatal(err)
	}
	if resp.Seq != 2 {
		t.Fatalf("expected seq 2, got %d", resp.Seq)
	}
	if !resp.WALSynced {
		t.Fatal("expected WALSynced=true")
	}

	r.mu.Lock()
	ref, ok := r.stepOutputs["step-1"]
	r.mu.Unlock()
	if !ok {
		t.Fatal("expected step-1 in stepOutputs")
	}
	if !strings.HasPrefix(ref, "sha256:") {
		t.Fatalf("expected sha256 prefix in output ref, got %s", ref)
	}

	reader, err := wal.NewReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 WAL records, got %d", len(records))
	}
}

func TestHookServer(t *testing.T) {
	dir := t.TempDir()
	r, err := NewRunner(Config{WorkDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if err := r.StartHookServer(); err != nil {
		t.Fatal(err)
	}
	defer r.StopHookServer()

	socketPath := filepath.Join(dir, ".wal", "af.sock")

	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	beginReq := jsonRPCRequest{
		JSONRPC: "2.0",
		Method:  "step_begin",
		Params:  json.RawMessage(`{"step_id":"hook-step-1"}`),
		ID:      1,
	}
	if err := json.NewEncoder(conn).Encode(beginReq); err != nil {
		t.Fatal(err)
	}
	var beginResp jsonRPCResponse
	if err := json.NewDecoder(conn).Decode(&beginResp); err != nil {
		t.Fatal(err)
	}
	conn.Close()

	if beginResp.Error != nil {
		t.Fatalf("unexpected error: %s", beginResp.Error.Message)
	}
	resultBytes, _ := json.Marshal(beginResp.Result)
	var sbr StepBeginResponse
	json.Unmarshal(resultBytes, &sbr)
	if sbr.Skipped {
		t.Fatal("expected skipped=false")
	}
	if sbr.Seq != 1 {
		t.Fatalf("expected seq 1, got %d", sbr.Seq)
	}

	conn2, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	endReq := jsonRPCRequest{
		JSONRPC: "2.0",
		Method:  "step_end",
		Params:  json.RawMessage(`{"step_id":"hook-step-1","exit":0,"output":"result data"}`),
		ID:      2,
	}
	if err := json.NewEncoder(conn2).Encode(endReq); err != nil {
		t.Fatal(err)
	}
	var endResp jsonRPCResponse
	if err := json.NewDecoder(conn2).Decode(&endResp); err != nil {
		t.Fatal(err)
	}
	conn2.Close()

	if endResp.Error != nil {
		t.Fatalf("unexpected error: %s", endResp.Error.Message)
	}
	resultBytes2, _ := json.Marshal(endResp.Result)
	var ser StepEndResponse
	json.Unmarshal(resultBytes2, &ser)
	if ser.Seq != 2 {
		t.Fatalf("expected seq 2, got %d", ser.Seq)
	}
	if !ser.WALSynced {
		t.Fatal("expected WALSynced=true")
	}
}

func TestWaitForActivation(t *testing.T) {
	dir := t.TempDir()
	activatePath := filepath.Join(dir, "activate.json")

	r, err := NewRunner(Config{WorkDir: dir, PoolMode: true, ActivatePath: activatePath})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	spec := ActivateSpec{
		Image:   "test-image:latest",
		Command: []string{"/bin/sh"},
		Args:    []string{"-c", "echo hello"},
		Env:     map[string]string{"FOO": "bar"},
		Prompt:  "test prompt",
	}
	specData, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		time.Sleep(200 * time.Millisecond)
		os.WriteFile(activatePath, specData, 0o644)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	got, err := r.WaitForActivation(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Image != spec.Image {
		t.Fatalf("expected image %s, got %s", spec.Image, got.Image)
	}
	if got.Prompt != spec.Prompt {
		t.Fatalf("expected prompt %s, got %s", spec.Prompt, got.Prompt)
	}
	if len(got.Command) != 1 || got.Command[0] != "/bin/sh" {
		t.Fatalf("unexpected command: %v", got.Command)
	}
	if got.Env["FOO"] != "bar" {
		t.Fatalf("expected FOO=bar, got %s", got.Env["FOO"])
	}
}

func TestBuildCheckpointManifest(t *testing.T) {
	dir := t.TempDir()

	w, err := wal.NewWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.StepBegin("step-1")
	w.StepEnd("step-1", 0, "ref-1")
	w.StepBegin("step-2")
	w.StepEnd("step-2", 0, "ref-2")
	w.Close()

	r, err := NewRunner(Config{WorkDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	m, err := r.BuildCheckpointManifest("job-123", "attempt-456", 1)
	if err != nil {
		t.Fatal(err)
	}

	if m.SchemaVersion != 2 {
		t.Fatalf("expected schema version 2, got %d", m.SchemaVersion)
	}
	if m.JobUID != "job-123" {
		t.Fatalf("expected jobUID job-123, got %s", m.JobUID)
	}
	if m.AttemptUID != "attempt-456" {
		t.Fatalf("expected attemptUID attempt-456, got %s", m.AttemptUID)
	}
	if m.CheckpointSeq != 1 {
		t.Fatalf("expected checkpointSeq 1, got %d", m.CheckpointSeq)
	}
	if m.RunnerVersion != runnerVersion {
		t.Fatalf("expected runner version %s, got %s", runnerVersion, m.RunnerVersion)
	}
	if m.StepProgress == nil {
		t.Fatal("expected non-nil StepProgress")
	}
	if len(m.StepProgress.CompletedSteps) != 2 {
		t.Fatalf("expected 2 completed steps in progress, got %d", len(m.StepProgress.CompletedSteps))
	}
	if m.StepProgress.LastCompleted != "step-2" {
		t.Fatalf("expected lastCompleted step-2, got %s", m.StepProgress.LastCompleted)
	}
	if m.CreatedAt == "" {
		t.Fatal("expected non-empty CreatedAt")
	}
}
