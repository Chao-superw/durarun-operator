package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"

	"durarun-operator/internal/model"
)

const initSQL = `
CREATE TABLE IF NOT EXISTS jobs (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    spec       JSONB NOT NULL,
    state      TEXT NOT NULL DEFAULT 'Pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS attempts (
    id             TEXT PRIMARY KEY,
    job_id         TEXT NOT NULL REFERENCES jobs(id),
    number         INT NOT NULL,
    worker_id      TEXT NOT NULL DEFAULT '',
    state          TEXT NOT NULL DEFAULT 'New',
    started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ,
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT now(),
    checkpoint_id  TEXT
);

CREATE INDEX IF NOT EXISTS idx_attempts_job_id ON attempts(job_id);
CREATE INDEX IF NOT EXISTS idx_attempts_state ON attempts(state);

CREATE TABLE IF NOT EXISTS checkpoints (
    id         TEXT PRIMARY KEY,
    attempt_id TEXT NOT NULL REFERENCES attempts(id),
    job_id     TEXT NOT NULL REFERENCES jobs(id),
    step_index INT NOT NULL,
    data       JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_checkpoints_job_id ON checkpoints(job_id);

CREATE TABLE IF NOT EXISTS state_transitions (
    id          TEXT PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    from_state  TEXT NOT NULL,
    to_state    TEXT NOT NULL,
    trigger     TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_state_transitions_entity ON state_transitions(entity_type, entity_id);

CREATE TABLE IF NOT EXISTS artifacts (
    id         TEXT PRIMARY KEY,
    job_id     TEXT NOT NULL REFERENCES jobs(id),
    name       TEXT NOT NULL,
    data       BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(job_id, name)
);

CREATE TABLE IF NOT EXISTS logs (
    id         TEXT PRIMARY KEY,
    job_id     TEXT NOT NULL REFERENCES jobs(id),
    attempt_id TEXT NOT NULL REFERENCES attempts(id),
    content    TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS isolation_level TEXT NOT NULL DEFAULT 'L1';
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS step_tracking_enabled BOOLEAN NOT NULL DEFAULT true;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS step_tracking_protocol TEXT NOT NULL DEFAULT 'env';
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS pool_ref TEXT NOT NULL DEFAULT 'default';

ALTER TABLE attempts ADD COLUMN IF NOT EXISTS pool_name TEXT;
ALTER TABLE attempts ADD COLUMN IF NOT EXISTS claim_latency_ms BIGINT;
ALTER TABLE attempts ADD COLUMN IF NOT EXISTS warm_hit BOOLEAN;
ALTER TABLE attempts ADD COLUMN IF NOT EXISTS last_completed_step TEXT;
ALTER TABLE attempts ADD COLUMN IF NOT EXISTS completed_step_count INT NOT NULL DEFAULT 0;
ALTER TABLE attempts ADD COLUMN IF NOT EXISTS isolation_level TEXT;
ALTER TABLE attempts ADD COLUMN IF NOT EXISTS recovered_from_step TEXT;

CREATE TABLE IF NOT EXISTS sandbox_pools (
    id                  TEXT PRIMARY KEY,
    name                TEXT NOT NULL UNIQUE,
    min_size            INT NOT NULL DEFAULT 3,
    max_size            INT NOT NULL DEFAULT 20,
    scale_up_threshold  REAL NOT NULL DEFAULT 0.7,
    scale_down_threshold REAL NOT NULL DEFAULT 0.3,
    scale_up_step       INT NOT NULL DEFAULT 3,
    cooldown_seconds    INT NOT NULL DEFAULT 60,
    template_image      TEXT NOT NULL DEFAULT '',
    template_cpu        TEXT NOT NULL DEFAULT '1',
    template_memory     TEXT NOT NULL DEFAULT '2Gi',
    runtime_class       TEXT NOT NULL DEFAULT 'af-l1-gvisor',
    total_pods          INT NOT NULL DEFAULT 0,
    idle_pods           INT NOT NULL DEFAULT 0,
    claimed_pods        INT NOT NULL DEFAULT 0,
    last_scale_time     TIMESTAMPTZ,
    ready               BOOLEAN NOT NULL DEFAULT false,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pool_pods (
    id               TEXT PRIMARY KEY,
    pool_id          TEXT NOT NULL REFERENCES sandbox_pools(id),
    state            TEXT NOT NULL DEFAULT 'idle',
    claimed_by_job   TEXT,
    runtime_class    TEXT NOT NULL DEFAULT '',
    image            TEXT NOT NULL DEFAULT '',
    version          BIGINT NOT NULL DEFAULT 1,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at       TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_pool_pods_pool_id ON pool_pods(pool_id);
CREATE INDEX IF NOT EXISTS idx_pool_pods_state ON pool_pods(state);

CREATE TABLE IF NOT EXISTS sandbox_policies (
    id                  TEXT PRIMARY KEY,
    name                TEXT NOT NULL,
    namespace           TEXT NOT NULL,
    allowed_levels      TEXT[] NOT NULL DEFAULT '{L1}',
    default_level       TEXT NOT NULL DEFAULT 'L1',
    force_level         TEXT,
    max_concurrent_jobs INT NOT NULL DEFAULT 10,
    max_pool_size       INT NOT NULL DEFAULT 20,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(name, namespace)
);

CREATE TABLE IF NOT EXISTS step_progress (
    id                  TEXT PRIMARY KEY,
    job_id              TEXT NOT NULL REFERENCES jobs(id),
    attempt_id          TEXT NOT NULL REFERENCES attempts(id),
    step_id             TEXT NOT NULL,
    seq                 BIGINT NOT NULL,
    type                TEXT NOT NULL,
    exit_code           INT,
    output_ref          TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_step_progress_job_id ON step_progress(job_id);
CREATE INDEX IF NOT EXISTS idx_step_progress_attempt_id ON step_progress(attempt_id);
`

type Store struct {
	db *sql.DB
}

func New(dsn string) (*Store, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) InitSchema(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, initSQL)
	return err
}

func (s *Store) CreateJob(ctx context.Context, spec model.JobSpec) (*model.Job, error) {
	id := uuid.New().String()
	specData, err := json.Marshal(spec)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	_, err = s.db.ExecContext(ctx,
		"INSERT INTO jobs (id, name, spec, state, created_at, updated_at) VALUES ($1, $2, $3, 'Pending', $4, $4)",
		id, spec.Name, specData, now)
	if err != nil {
		return nil, err
	}
	return &model.Job{
		ID:        id,
		Spec:      spec,
		State:     model.JobPending,
		CreatedAt: now.Format(time.RFC3339),
		UpdatedAt: now.Format(time.RFC3339),
	}, nil
}

func (s *Store) GetJob(ctx context.Context, id string) (*model.Job, error) {
	var job model.Job
	var specData []byte
	var createdAt, updatedAt time.Time
	err := s.db.QueryRowContext(ctx,
		"SELECT id, spec, state, created_at, updated_at FROM jobs WHERE id = $1", id).
		Scan(&job.ID, &specData, &job.State, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(specData, &job.Spec); err != nil {
		return nil, err
	}
	job.CreatedAt = createdAt.Format(time.RFC3339)
	job.UpdatedAt = updatedAt.Format(time.RFC3339)
	return &job, nil
}

func (s *Store) ListJobs(ctx context.Context) ([]model.Job, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT id, spec, state, created_at, updated_at FROM jobs ORDER BY created_at DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []model.Job
	for rows.Next() {
		var job model.Job
		var specData []byte
		var createdAt, updatedAt time.Time
		if err := rows.Scan(&job.ID, &specData, &job.State, &createdAt, &updatedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(specData, &job.Spec); err != nil {
			return nil, err
		}
		job.CreatedAt = createdAt.Format(time.RFC3339)
		job.UpdatedAt = updatedAt.Format(time.RFC3339)
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (s *Store) TransitionJobState(ctx context.Context, jobID string, from, to model.JobState, trigger string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var cur model.JobState
	if err := tx.QueryRowContext(ctx, "SELECT state FROM jobs WHERE id = $1 FOR UPDATE", jobID).Scan(&cur); err != nil {
		return err
	}
	if cur != from {
		return fmt.Errorf("job %s: expected state %s but got %s", jobID, from, cur)
	}

	if _, err := tx.ExecContext(ctx, "UPDATE jobs SET state = $1, updated_at = now() WHERE id = $2", to, jobID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		"INSERT INTO state_transitions (id, entity_type, entity_id, from_state, to_state, trigger) VALUES ($1, 'job', $2, $3, $4, $5)",
		uuid.New().String(), jobID, string(from), string(to), trigger); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) CreateAttempt(ctx context.Context, jobID, workerID string) (*model.Attempt, error) {
	var nextNum int
	err := s.db.QueryRowContext(ctx,
		"SELECT COALESCE(MAX(number), 0) + 1 FROM attempts WHERE job_id = $1", jobID).Scan(&nextNum)
	if err != nil {
		return nil, err
	}

	id := uuid.New().String()
	now := time.Now()
	_, err = s.db.ExecContext(ctx,
		"INSERT INTO attempts (id, job_id, number, worker_id, state, started_at, last_heartbeat) VALUES ($1, $2, $3, $4, 'New', $5, $5)",
		id, jobID, nextNum, workerID, now)
	if err != nil {
		return nil, err
	}
	return &model.Attempt{
		ID:            id,
		JobID:         jobID,
		Number:        nextNum,
		WorkerID:      workerID,
		State:         model.AttemptNew,
		StartedAt:     now.Format(time.RFC3339),
		LastHeartbeat: now.Format(time.RFC3339),
	}, nil
}

func (s *Store) ClaimScheduledAttempt(ctx context.Context, workerID string) (*model.Attempt, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var a model.Attempt
	var startedAt, lastHeartbeat time.Time
	var finishedAt *time.Time
	err = tx.QueryRowContext(ctx,
		`SELECT id, job_id, number, worker_id, state, started_at, finished_at, last_heartbeat, checkpoint_id
		 FROM attempts WHERE state = 'New' ORDER BY started_at LIMIT 1 FOR UPDATE SKIP LOCKED`).
		Scan(&a.ID, &a.JobID, &a.Number, &a.WorkerID, &a.State,
			&startedAt, &finishedAt, &lastHeartbeat, &a.CheckpointID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if _, err := tx.ExecContext(ctx, "UPDATE attempts SET worker_id = $1 WHERE id = $2", workerID, a.ID); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	a.WorkerID = workerID
	a.StartedAt = startedAt.Format(time.RFC3339)
	a.LastHeartbeat = lastHeartbeat.Format(time.RFC3339)
	if finishedAt != nil {
		s := finishedAt.Format(time.RFC3339)
		a.FinishedAt = &s
	}
	return &a, nil
}

func (s *Store) TransitionAttemptState(ctx context.Context, attemptID string, from, to model.AttemptState, trigger string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var cur model.AttemptState
	if err := tx.QueryRowContext(ctx, "SELECT state FROM attempts WHERE id = $1 FOR UPDATE", attemptID).Scan(&cur); err != nil {
		return err
	}
	if cur != from {
		return fmt.Errorf("attempt %s: expected state %s but got %s", attemptID, from, cur)
	}

	isTerminal := to == model.AttemptCompleted || to == model.AttemptFailed || to == model.AttemptTimedOut
	if isTerminal {
		_, err = tx.ExecContext(ctx, "UPDATE attempts SET state = $1, finished_at = now() WHERE id = $2", to, attemptID)
	} else {
		_, err = tx.ExecContext(ctx, "UPDATE attempts SET state = $1 WHERE id = $2", to, attemptID)
	}
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		"INSERT INTO state_transitions (id, entity_type, entity_id, from_state, to_state, trigger) VALUES ($1, 'attempt', $2, $3, $4, $5)",
		uuid.New().String(), attemptID, string(from), string(to), trigger); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) UpdateHeartbeat(ctx context.Context, attemptID string) error {
	_, err := s.db.ExecContext(ctx, "UPDATE attempts SET last_heartbeat = now() WHERE id = $1", attemptID)
	return err
}

func (s *Store) FindStaleAttempts(ctx context.Context, threshold time.Duration) ([]model.Attempt, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, job_id, number, worker_id, state, started_at, finished_at, last_heartbeat, checkpoint_id
		 FROM attempts WHERE state = 'Executing' AND extract(epoch FROM (now() - last_heartbeat)) > $1`,
		threshold.Seconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanAttempts(rows)
}

func (s *Store) SaveCheckpoint(ctx context.Context, attemptID, jobID string, stepIndex int, data json.RawMessage) error {
	id := uuid.New().String()
	_, err := s.db.ExecContext(ctx,
		"INSERT INTO checkpoints (id, attempt_id, job_id, step_index, data) VALUES ($1, $2, $3, $4, $5)",
		id, attemptID, jobID, stepIndex, []byte(data))
	return err
}

func (s *Store) GetLatestCheckpoint(ctx context.Context, jobID string) (*model.Checkpoint, error) {
	var cp model.Checkpoint
	var data []byte
	var createdAt time.Time
	err := s.db.QueryRowContext(ctx,
		`SELECT id, attempt_id, job_id, step_index, data, created_at
		 FROM checkpoints WHERE job_id = $1 ORDER BY step_index DESC, created_at DESC LIMIT 1`, jobID).
		Scan(&cp.ID, &cp.AttemptID, &cp.JobID, &cp.StepIndex, &data, &createdAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	cp.Data = json.RawMessage(data)
	cp.CreatedAt = createdAt.Format(time.RFC3339)
	return &cp, nil
}

func (s *Store) SaveArtifact(ctx context.Context, jobID, name string, data []byte) error {
	id := uuid.New().String()
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO artifacts (id, job_id, name, data) VALUES ($1, $2, $3, $4)
		 ON CONFLICT (job_id, name) DO UPDATE SET data = EXCLUDED.data`,
		id, jobID, name, data)
	return err
}

func (s *Store) GetArtifact(ctx context.Context, jobID, name string) ([]byte, error) {
	var data []byte
	err := s.db.QueryRowContext(ctx,
		"SELECT data FROM artifacts WHERE job_id = $1 AND name = $2", jobID, name).Scan(&data)
	return data, err
}

func (s *Store) ListArtifacts(ctx context.Context, jobID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT name FROM artifacts WHERE job_id = $1", jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

func (s *Store) SaveLog(ctx context.Context, jobID, attemptID, content string) error {
	id := uuid.New().String()
	_, err := s.db.ExecContext(ctx,
		"INSERT INTO logs (id, job_id, attempt_id, content) VALUES ($1, $2, $3, $4)",
		id, jobID, attemptID, content)
	return err
}

func (s *Store) GetLogs(ctx context.Context, jobID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT content FROM logs WHERE job_id = $1 ORDER BY created_at", jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var logs []string
	for rows.Next() {
		var content string
		if err := rows.Scan(&content); err != nil {
			return nil, err
		}
		logs = append(logs, content)
	}
	return logs, rows.Err()
}

func (s *Store) GetAttempts(ctx context.Context, jobID string) ([]model.Attempt, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, job_id, number, worker_id, state, started_at, finished_at, last_heartbeat, checkpoint_id
		 FROM attempts WHERE job_id = $1 ORDER BY number`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanAttempts(rows)
}

func (s *Store) GetTransitions(ctx context.Context, entityType, entityID string) ([]model.StateTransition, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, entity_type, entity_id, from_state, to_state, trigger, created_at
		 FROM state_transitions WHERE entity_type = $1 AND entity_id = $2 ORDER BY created_at`,
		entityType, entityID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var transitions []model.StateTransition
	for rows.Next() {
		var t model.StateTransition
		var createdAt time.Time
		if err := rows.Scan(&t.ID, &t.EntityType, &t.EntityID, &t.FromState, &t.ToState, &t.Trigger, &createdAt); err != nil {
			return nil, err
		}
		t.CreatedAt = createdAt.Format(time.RFC3339)
		transitions = append(transitions, t)
	}
	return transitions, rows.Err()
}

func (s *Store) CountAttempts(ctx context.Context, jobID string) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM attempts WHERE job_id = $1", jobID).Scan(&count)
	return count, err
}

func scanAttempts(rows *sql.Rows) ([]model.Attempt, error) {
	var attempts []model.Attempt
	for rows.Next() {
		var a model.Attempt
		var startedAt, lastHeartbeat time.Time
		var finishedAt *time.Time
		if err := rows.Scan(&a.ID, &a.JobID, &a.Number, &a.WorkerID, &a.State,
			&startedAt, &finishedAt, &lastHeartbeat, &a.CheckpointID); err != nil {
			return nil, err
		}
		a.StartedAt = startedAt.Format(time.RFC3339)
		a.LastHeartbeat = lastHeartbeat.Format(time.RFC3339)
		if finishedAt != nil {
			s := finishedAt.Format(time.RFC3339)
			a.FinishedAt = &s
		}
		attempts = append(attempts, a)
	}
	return attempts, rows.Err()
}

type SandboxPool struct {
	ID                 string
	Name               string
	MinSize            int
	MaxSize            int
	ScaleUpThreshold   float64
	ScaleDownThreshold float64
	ScaleUpStep        int
	CooldownSeconds    int
	TemplateImage      string
	TemplateCPU        string
	TemplateMemory     string
	RuntimeClass       string
	TotalPods          int
	IdlePods           int
	ClaimedPods        int
	LastScaleTime      *time.Time
	Ready              bool
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

type PoolPod struct {
	ID           string
	PoolID       string
	State        string
	ClaimedByJob string
	RuntimeClass string
	Image        string
	Version      int64
	CreatedAt    time.Time
	ClaimedAt    *time.Time
}

type SandboxPolicy struct {
	ID                string
	Name              string
	Namespace         string
	AllowedLevels     []string
	DefaultLevel      string
	ForceLevel        *string
	MaxConcurrentJobs int
	MaxPoolSize       int
	CreatedAt         time.Time
}

type StepProgressRecord struct {
	ID        string
	JobID     string
	AttemptID string
	StepID    string
	Seq       int64
	Type      string
	ExitCode  *int
	OutputRef string
	CreatedAt time.Time
}

type SandboxPoolSpec struct {
	MinSize            int
	MaxSize            int
	ScaleUpThreshold   float64
	ScaleDownThreshold float64
	ScaleUpStep        int
	CooldownSeconds    int
	TemplateImage      string
	TemplateCPU        string
	TemplateMemory     string
	RuntimeClass       string
}

type SandboxPolicySpec struct {
	AllowedLevels     []string
	DefaultLevel      string
	ForceLevel        *string
	MaxConcurrentJobs int
	MaxPoolSize       int
}

func (s *Store) CreateSandboxPool(ctx context.Context, name string, spec SandboxPoolSpec) (*SandboxPool, error) {
	id := uuid.New().String()
	now := time.Now()
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO sandbox_pools (id, name, min_size, max_size, scale_up_threshold, scale_down_threshold,
		 scale_up_step, cooldown_seconds, template_image, template_cpu, template_memory, runtime_class,
		 created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$13)`,
		id, name, spec.MinSize, spec.MaxSize, spec.ScaleUpThreshold, spec.ScaleDownThreshold,
		spec.ScaleUpStep, spec.CooldownSeconds, spec.TemplateImage, spec.TemplateCPU,
		spec.TemplateMemory, spec.RuntimeClass, now)
	if err != nil {
		return nil, err
	}
	return &SandboxPool{
		ID:                 id,
		Name:               name,
		MinSize:            spec.MinSize,
		MaxSize:            spec.MaxSize,
		ScaleUpThreshold:   spec.ScaleUpThreshold,
		ScaleDownThreshold: spec.ScaleDownThreshold,
		ScaleUpStep:        spec.ScaleUpStep,
		CooldownSeconds:    spec.CooldownSeconds,
		TemplateImage:      spec.TemplateImage,
		TemplateCPU:        spec.TemplateCPU,
		TemplateMemory:     spec.TemplateMemory,
		RuntimeClass:       spec.RuntimeClass,
		CreatedAt:          now,
		UpdatedAt:          now,
	}, nil
}

func (s *Store) GetSandboxPool(ctx context.Context, name string) (*SandboxPool, error) {
	var p SandboxPool
	err := s.db.QueryRowContext(ctx,
		`SELECT id, name, min_size, max_size, scale_up_threshold, scale_down_threshold,
		 scale_up_step, cooldown_seconds, template_image, template_cpu, template_memory,
		 runtime_class, total_pods, idle_pods, claimed_pods, last_scale_time, ready,
		 created_at, updated_at FROM sandbox_pools WHERE name = $1`, name).
		Scan(&p.ID, &p.Name, &p.MinSize, &p.MaxSize, &p.ScaleUpThreshold, &p.ScaleDownThreshold,
			&p.ScaleUpStep, &p.CooldownSeconds, &p.TemplateImage, &p.TemplateCPU, &p.TemplateMemory,
			&p.RuntimeClass, &p.TotalPods, &p.IdlePods, &p.ClaimedPods, &p.LastScaleTime, &p.Ready,
			&p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func (s *Store) ListSandboxPools(ctx context.Context) ([]SandboxPool, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, name, min_size, max_size, scale_up_threshold, scale_down_threshold,
		 scale_up_step, cooldown_seconds, template_image, template_cpu, template_memory,
		 runtime_class, total_pods, idle_pods, claimed_pods, last_scale_time, ready,
		 created_at, updated_at FROM sandbox_pools ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pools []SandboxPool
	for rows.Next() {
		var p SandboxPool
		if err := rows.Scan(&p.ID, &p.Name, &p.MinSize, &p.MaxSize, &p.ScaleUpThreshold, &p.ScaleDownThreshold,
			&p.ScaleUpStep, &p.CooldownSeconds, &p.TemplateImage, &p.TemplateCPU, &p.TemplateMemory,
			&p.RuntimeClass, &p.TotalPods, &p.IdlePods, &p.ClaimedPods, &p.LastScaleTime, &p.Ready,
			&p.CreatedAt, &p.UpdatedAt); err != nil {
			return nil, err
		}
		pools = append(pools, p)
	}
	return pools, rows.Err()
}

func (s *Store) UpdateSandboxPoolStatus(ctx context.Context, name string, total, idle, claimed int, ready bool) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE sandbox_pools SET total_pods = $1, idle_pods = $2, claimed_pods = $3,
		 ready = $4, updated_at = now() WHERE name = $5`,
		total, idle, claimed, ready, name)
	return err
}

func (s *Store) CreatePoolPod(ctx context.Context, poolID, runtimeClass, image string) (*PoolPod, error) {
	id := uuid.New().String()
	now := time.Now()
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO pool_pods (id, pool_id, state, runtime_class, image, created_at)
		 VALUES ($1, $2, 'idle', $3, $4, $5)`,
		id, poolID, runtimeClass, image, now)
	if err != nil {
		return nil, err
	}
	return &PoolPod{
		ID:           id,
		PoolID:       poolID,
		State:        "idle",
		RuntimeClass: runtimeClass,
		Image:        image,
		Version:      1,
		CreatedAt:    now,
	}, nil
}

func (s *Store) ClaimPoolPod(ctx context.Context, poolID, jobID string) (*PoolPod, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var p PoolPod
	err = tx.QueryRowContext(ctx,
		`SELECT id, pool_id, state, runtime_class, image, version, created_at
		 FROM pool_pods WHERE state = 'idle' AND pool_id = $1
		 FOR UPDATE SKIP LOCKED LIMIT 1`, poolID).
		Scan(&p.ID, &p.PoolID, &p.State, &p.RuntimeClass, &p.Image, &p.Version, &p.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	now := time.Now()
	_, err = tx.ExecContext(ctx,
		`UPDATE pool_pods SET state = 'claimed', claimed_by_job = $1, version = version + 1,
		 claimed_at = $2 WHERE id = $3`, jobID, now, p.ID)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	p.State = "claimed"
	p.ClaimedByJob = jobID
	p.Version = p.Version + 1
	p.ClaimedAt = &now
	return &p, nil
}

func (s *Store) ReleasePoolPod(ctx context.Context, podID string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE pool_pods SET state = 'terminating', version = version + 1 WHERE id = $1`, podID)
	return err
}

func (s *Store) ListPoolPods(ctx context.Context, poolID string) ([]PoolPod, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, pool_id, state, claimed_by_job, runtime_class, image, version, created_at, claimed_at
		 FROM pool_pods WHERE pool_id = $1 ORDER BY created_at`, poolID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pods []PoolPod
	for rows.Next() {
		var p PoolPod
		var claimedByJob sql.NullString
		if err := rows.Scan(&p.ID, &p.PoolID, &p.State, &claimedByJob, &p.RuntimeClass,
			&p.Image, &p.Version, &p.CreatedAt, &p.ClaimedAt); err != nil {
			return nil, err
		}
		p.ClaimedByJob = claimedByJob.String
		pods = append(pods, p)
	}
	return pods, rows.Err()
}

func (s *Store) DeletePoolPod(ctx context.Context, podID string) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM pool_pods WHERE id = $1", podID)
	return err
}

func (s *Store) CountPoolPods(ctx context.Context, poolID string) (total int, idle int, claimed int, err error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT state, COUNT(*) FROM pool_pods WHERE pool_id = $1 GROUP BY state`, poolID)
	if err != nil {
		return 0, 0, 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var state string
		var count int
		if err := rows.Scan(&state, &count); err != nil {
			return 0, 0, 0, err
		}
		total += count
		switch state {
		case "idle":
			idle = count
		case "claimed":
			claimed = count
		}
	}
	return total, idle, claimed, rows.Err()
}

func (s *Store) CreateSandboxPolicy(ctx context.Context, name, namespace string, spec SandboxPolicySpec) error {
	id := uuid.New().String()
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO sandbox_policies (id, name, namespace, allowed_levels, default_level,
		 force_level, max_concurrent_jobs, max_pool_size) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		id, name, namespace, pq.Array(spec.AllowedLevels), spec.DefaultLevel,
		spec.ForceLevel, spec.MaxConcurrentJobs, spec.MaxPoolSize)
	return err
}

func (s *Store) GetSandboxPolicy(ctx context.Context, namespace string) (*SandboxPolicy, error) {
	var p SandboxPolicy
	err := s.db.QueryRowContext(ctx,
		`SELECT id, name, namespace, allowed_levels, default_level, force_level,
		 max_concurrent_jobs, max_pool_size, created_at
		 FROM sandbox_policies WHERE namespace = $1 LIMIT 1`, namespace).
		Scan(&p.ID, &p.Name, &p.Namespace, pq.Array(&p.AllowedLevels), &p.DefaultLevel,
			&p.ForceLevel, &p.MaxConcurrentJobs, &p.MaxPoolSize, &p.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func (s *Store) SaveStepProgress(ctx context.Context, jobID, attemptID, stepID string, seq int64, stepType string, exitCode *int, outputRef string) error {
	id := uuid.New().String()
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO step_progress (id, job_id, attempt_id, step_id, seq, type, exit_code, output_ref)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		id, jobID, attemptID, stepID, seq, stepType, exitCode, outputRef)
	return err
}

func (s *Store) GetStepProgress(ctx context.Context, attemptID string) ([]StepProgressRecord, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, job_id, attempt_id, step_id, seq, type, exit_code, output_ref, created_at
		 FROM step_progress WHERE attempt_id = $1 ORDER BY seq`, attemptID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []StepProgressRecord
	for rows.Next() {
		var r StepProgressRecord
		if err := rows.Scan(&r.ID, &r.JobID, &r.AttemptID, &r.StepID, &r.Seq,
			&r.Type, &r.ExitCode, &r.OutputRef, &r.CreatedAt); err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

func (s *Store) GetCompletedSteps(ctx context.Context, attemptID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT DISTINCT step_id FROM step_progress
		 WHERE attempt_id = $1 AND type = 'completed' ORDER BY step_id`, attemptID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var steps []string
	for rows.Next() {
		var stepID string
		if err := rows.Scan(&stepID); err != nil {
			return nil, err
		}
		steps = append(steps, stepID)
	}
	return steps, rows.Err()
}

func (s *Store) UpdateAttemptPoolInfo(ctx context.Context, attemptID, poolName string, claimLatencyMs int64, warmHit bool) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE attempts SET pool_name = $1, claim_latency_ms = $2, warm_hit = $3 WHERE id = $4`,
		poolName, claimLatencyMs, warmHit, attemptID)
	return err
}

func (s *Store) UpdateAttemptStepProgress(ctx context.Context, attemptID, lastStep string, completedCount int) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE attempts SET last_completed_step = $1, completed_step_count = $2 WHERE id = $3`,
		lastStep, completedCount, attemptID)
	return err
}
