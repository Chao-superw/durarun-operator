package api

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"durarun-operator/internal/isolation"
	"durarun-operator/internal/model"
	"durarun-operator/internal/pool"
	"durarun-operator/internal/store"
)

type Server struct {
	store        *store.Store
	addr         string
	poolMgr      *pool.Manager
	isolationMgr *isolation.Manager
}

func New(s *store.Store, addr string) *Server {
	return &Server{store: s, addr: addr}
}

func NewV2(s *store.Store, addr string, poolMgr *pool.Manager, isolationMgr *isolation.Manager) *Server {
	return &Server{store: s, addr: addr, poolMgr: poolMgr, isolationMgr: isolationMgr}
}

func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/jobs", s.handleCreateJob)
	mux.HandleFunc("GET /api/v1/jobs", s.handleListJobs)
	mux.HandleFunc("GET /api/v1/jobs/{id}", s.handleGetJob)
	mux.HandleFunc("GET /api/v1/jobs/{id}/logs", s.handleGetLogs)
	mux.HandleFunc("GET /api/v1/jobs/{id}/artifacts", s.handleListArtifacts)
	mux.HandleFunc("GET /api/v1/jobs/{id}/artifacts/{name}", s.handleGetArtifact)
	mux.HandleFunc("GET /api/v1/jobs/{id}/transitions", s.handleGetTransitions)
	mux.HandleFunc("DELETE /api/v1/jobs/{id}", s.handleTerminateJob)

	mux.HandleFunc("GET /api/v2/pools", s.handleListPools)
	mux.HandleFunc("GET /api/v2/pools/{name}", s.handleGetPool)
	mux.HandleFunc("POST /api/v2/pools", s.handleCreatePool)

	mux.HandleFunc("GET /api/v2/policies/{namespace}", s.handleGetPolicy)
	mux.HandleFunc("POST /api/v2/policies", s.handleCreatePolicy)

	mux.HandleFunc("POST /api/v2/jobs", s.handleCreateJobV2)
	mux.HandleFunc("GET /api/v2/jobs/{id}/steps", s.handleGetJobSteps)
	mux.HandleFunc("GET /api/v2/jobs/{id}/recovery", s.handleGetRecoveryInfo)

	mux.HandleFunc("GET /api/v2/metrics", s.handleMetrics)

	log.Printf("API server listening on %s", s.addr)
	return http.ListenAndServe(s.addr, corsMiddleware(mux))
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func (s *Server) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	var spec model.JobSpec
	if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
		writeJSON(w, http.StatusBadRequest, model.ErrorResponse{Error: "invalid JSON: " + err.Error()})
		return
	}
	if err := model.ValidateJobSpec(&spec); err != nil {
		writeJSON(w, http.StatusBadRequest, model.ErrorResponse{Error: err.Error()})
		return
	}
	job, err := s.store.CreateJob(r.Context(), spec)
	if err != nil {
		log.Printf("CreateJob error: %v", err)
		writeJSON(w, http.StatusInternalServerError, model.ErrorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, model.SubmitResponse{JobID: job.ID})
}

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := s.store.ListJobs(r.Context())
	if err != nil {
		log.Printf("ListJobs error: %v", err)
		writeJSON(w, http.StatusInternalServerError, model.ErrorResponse{Error: err.Error()})
		return
	}
	if jobs == nil {
		jobs = []model.Job{}
	}
	writeJSON(w, http.StatusOK, jobs)
}

type jobDetail struct {
	model.Job
	Attempts []model.Attempt `json:"attempts"`
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, err := s.store.GetJob(r.Context(), id)
	if err == sql.ErrNoRows {
		writeJSON(w, http.StatusNotFound, model.ErrorResponse{Error: "job not found"})
		return
	}
	if err != nil {
		log.Printf("GetJob error: %v", err)
		writeJSON(w, http.StatusInternalServerError, model.ErrorResponse{Error: err.Error()})
		return
	}
	attempts, err := s.store.GetAttempts(r.Context(), id)
	if err != nil {
		log.Printf("GetAttempts error: %v", err)
		writeJSON(w, http.StatusInternalServerError, model.ErrorResponse{Error: err.Error()})
		return
	}
	if attempts == nil {
		attempts = []model.Attempt{}
	}
	writeJSON(w, http.StatusOK, jobDetail{Job: *job, Attempts: attempts})
}

func (s *Server) handleGetLogs(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	logs, err := s.store.GetLogs(r.Context(), id)
	if err != nil {
		log.Printf("GetLogs error: %v", err)
		writeJSON(w, http.StatusInternalServerError, model.ErrorResponse{Error: err.Error()})
		return
	}
	if logs == nil {
		logs = []string{}
	}
	writeJSON(w, http.StatusOK, logs)
}

func (s *Server) handleListArtifacts(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	names, err := s.store.ListArtifacts(r.Context(), id)
	if err != nil {
		log.Printf("ListArtifacts error: %v", err)
		writeJSON(w, http.StatusInternalServerError, model.ErrorResponse{Error: err.Error()})
		return
	}
	if names == nil {
		names = []string{}
	}
	writeJSON(w, http.StatusOK, names)
}

func (s *Server) handleGetArtifact(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	name := r.PathValue("name")
	data, err := s.store.GetArtifact(r.Context(), jobID, name)
	if err == sql.ErrNoRows {
		writeJSON(w, http.StatusNotFound, model.ErrorResponse{Error: "artifact not found"})
		return
	}
	if err != nil {
		log.Printf("GetArtifact error: %v", err)
		writeJSON(w, http.StatusInternalServerError, model.ErrorResponse{Error: err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

func (s *Server) handleGetTransitions(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	transitions, err := s.store.GetTransitions(r.Context(), "job", id)
	if err != nil {
		log.Printf("GetTransitions error: %v", err)
		writeJSON(w, http.StatusInternalServerError, model.ErrorResponse{Error: err.Error()})
		return
	}
	if transitions == nil {
		transitions = []model.StateTransition{}
	}
	writeJSON(w, http.StatusOK, transitions)
}

func (s *Server) handleTerminateJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, err := s.store.GetJob(r.Context(), id)
	if err == sql.ErrNoRows {
		writeJSON(w, http.StatusNotFound, model.ErrorResponse{Error: "job not found"})
		return
	}
	if err != nil {
		log.Printf("GetJob error: %v", err)
		writeJSON(w, http.StatusInternalServerError, model.ErrorResponse{Error: err.Error()})
		return
	}
	if job.State == model.JobCompleted || job.State == model.JobFailed || job.State == model.JobTerminated {
		writeJSON(w, http.StatusConflict, model.ErrorResponse{Error: "job is already in terminal state: " + string(job.State)})
		return
	}
	if err := s.store.TransitionJobState(r.Context(), id, job.State, model.JobTerminated, "api-delete"); err != nil {
		log.Printf("TransitionJobState error: %v", err)
		writeJSON(w, http.StatusConflict, model.ErrorResponse{Error: err.Error()})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

var (
	poolsMu sync.RWMutex
	pools   = make(map[string]*poolEntry)
)

type poolEntry struct {
	mgr     *pool.Manager
	cfg     pool.PoolConfig
	created time.Time
}

func (s *Server) handleListPools(w http.ResponseWriter, r *http.Request) {
	poolsMu.RLock()
	defer poolsMu.RUnlock()

	type poolInfo struct {
		Name      string          `json:"name"`
		Config    pool.PoolConfig `json:"config"`
		Status    pool.PoolStatus `json:"status"`
		CreatedAt string          `json:"createdAt"`
	}

	result := make([]poolInfo, 0, len(pools))
	for _, entry := range pools {
		result = append(result, poolInfo{
			Name:      entry.cfg.Name,
			Config:    entry.cfg,
			Status:    entry.mgr.Status(),
			CreatedAt: entry.created.Format(time.RFC3339),
		})
	}

	if s.poolMgr != nil {
		found := false
		for _, entry := range pools {
			if entry.mgr == s.poolMgr {
				found = true
				break
			}
		}
		if !found {
			result = append(result, poolInfo{
				Name:   "default",
				Status: s.poolMgr.Status(),
			})
		}
	}

	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleGetPool(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	poolsMu.RLock()
	entry, ok := pools[name]
	poolsMu.RUnlock()

	if ok {
		status := entry.mgr.Status()
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"name":      entry.cfg.Name,
			"config":    entry.cfg,
			"status":    status,
			"createdAt": entry.created.Format(time.RFC3339),
		})
		return
	}

	if name == "default" && s.poolMgr != nil {
		status := s.poolMgr.Status()
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"name":   "default",
			"status": status,
		})
		return
	}

	writeJSON(w, http.StatusNotFound, model.ErrorResponse{Error: "pool not found: " + name})
}

type createPoolRequest struct {
	Name               string  `json:"name"`
	MinSize            int     `json:"minSize"`
	MaxSize            int     `json:"maxSize"`
	ScaleUpThreshold   float64 `json:"scaleUpThreshold"`
	ScaleDownThreshold float64 `json:"scaleDownThreshold"`
	ScaleUpStep        int     `json:"scaleUpStep"`
	CooldownSeconds    int     `json:"cooldownSeconds"`
	Template           struct {
		Image            string `json:"image"`
		RuntimeClassName string `json:"runtimeClassName"`
	} `json:"template"`
}

func (s *Server) handleCreatePool(w http.ResponseWriter, r *http.Request) {
	var req createPoolRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, model.ErrorResponse{Error: "invalid JSON: " + err.Error()})
		return
	}
	if req.Name == "" {
		writeJSON(w, http.StatusBadRequest, model.ErrorResponse{Error: "name is required"})
		return
	}
	if req.MinSize < 0 {
		writeJSON(w, http.StatusBadRequest, model.ErrorResponse{Error: "minSize must be >= 0"})
		return
	}
	if req.MaxSize < req.MinSize {
		writeJSON(w, http.StatusBadRequest, model.ErrorResponse{Error: "maxSize must be >= minSize"})
		return
	}

	poolsMu.Lock()
	if _, exists := pools[req.Name]; exists {
		poolsMu.Unlock()
		writeJSON(w, http.StatusConflict, model.ErrorResponse{Error: "pool already exists: " + req.Name})
		return
	}

	cfg := pool.PoolConfig{
		Name:               req.Name,
		MinSize:            req.MinSize,
		MaxSize:            req.MaxSize,
		ScaleUpThreshold:   req.ScaleUpThreshold,
		ScaleDownThreshold: req.ScaleDownThreshold,
		ScaleUpStep:        req.ScaleUpStep,
		CooldownSeconds:    req.CooldownSeconds,
		Image:              req.Template.Image,
		RuntimeClass:       req.Template.RuntimeClassName,
	}
	mgr := pool.NewManager(cfg)
	pools[req.Name] = &poolEntry{mgr: mgr, cfg: cfg, created: time.Now()}
	poolsMu.Unlock()

	status := mgr.Status()
	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"name":   req.Name,
		"config": cfg,
		"status": status,
	})
}

var (
	policiesMu sync.RWMutex
	policies   = make(map[string]*policyEntry)
)

type policyEntry struct {
	Name              string                 `json:"name"`
	Namespace         string                 `json:"namespace"`
	AllowedLevels     []model.IsolationLevel `json:"allowedLevels"`
	DefaultLevel      model.IsolationLevel   `json:"defaultLevel"`
	MaxConcurrentJobs int                    `json:"maxConcurrentJobs"`
	CreatedAt         time.Time              `json:"createdAt"`
}

func (s *Server) handleGetPolicy(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")

	policiesMu.RLock()
	entry, ok := policies[namespace]
	policiesMu.RUnlock()

	if !ok {
		writeJSON(w, http.StatusNotFound, model.ErrorResponse{Error: "policy not found for namespace: " + namespace})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"name":              entry.Name,
		"namespace":         entry.Namespace,
		"allowedLevels":     entry.AllowedLevels,
		"defaultLevel":      entry.DefaultLevel,
		"maxConcurrentJobs": entry.MaxConcurrentJobs,
		"createdAt":         entry.CreatedAt.Format(time.RFC3339),
	})
}

type createPolicyRequest struct {
	Name              string                 `json:"name"`
	Namespace         string                 `json:"namespace"`
	AllowedLevels     []model.IsolationLevel `json:"allowedLevels"`
	DefaultLevel      model.IsolationLevel   `json:"defaultLevel"`
	MaxConcurrentJobs int                    `json:"maxConcurrentJobs"`
}

func (s *Server) handleCreatePolicy(w http.ResponseWriter, r *http.Request) {
	var req createPolicyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, model.ErrorResponse{Error: "invalid JSON: " + err.Error()})
		return
	}
	if req.Name == "" {
		writeJSON(w, http.StatusBadRequest, model.ErrorResponse{Error: "name is required"})
		return
	}
	if req.Namespace == "" {
		writeJSON(w, http.StatusBadRequest, model.ErrorResponse{Error: "namespace is required"})
		return
	}

	policiesMu.Lock()
	if _, exists := policies[req.Namespace]; exists {
		policiesMu.Unlock()
		writeJSON(w, http.StatusConflict, model.ErrorResponse{Error: "policy already exists for namespace: " + req.Namespace})
		return
	}
	entry := &policyEntry{
		Name:              req.Name,
		Namespace:         req.Namespace,
		AllowedLevels:     req.AllowedLevels,
		DefaultLevel:      req.DefaultLevel,
		MaxConcurrentJobs: req.MaxConcurrentJobs,
		CreatedAt:         time.Now(),
	}
	policies[req.Namespace] = entry
	policiesMu.Unlock()

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"name":              entry.Name,
		"namespace":         entry.Namespace,
		"allowedLevels":     entry.AllowedLevels,
		"defaultLevel":      entry.DefaultLevel,
		"maxConcurrentJobs": entry.MaxConcurrentJobs,
		"createdAt":         entry.CreatedAt.Format(time.RFC3339),
	})
}

func (s *Server) handleCreateJobV2(w http.ResponseWriter, r *http.Request) {
	var spec model.JobSpec
	if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
		writeJSON(w, http.StatusBadRequest, model.ErrorResponse{Error: "invalid JSON: " + err.Error()})
		return
	}
	if err := model.ValidateJobSpec(&spec); err != nil {
		writeJSON(w, http.StatusBadRequest, model.ErrorResponse{Error: err.Error()})
		return
	}

	if s.isolationMgr != nil {
		level := isolation.Level(spec.Isolation.Level)
		if !s.isolationMgr.ValidateLevel(level) {
			writeJSON(w, http.StatusBadRequest, model.ErrorResponse{Error: "invalid isolation level: " + string(spec.Isolation.Level)})
			return
		}
	}

	job, err := s.store.CreateJob(r.Context(), spec)
	if err != nil {
		log.Printf("CreateJobV2 error: %v", err)
		writeJSON(w, http.StatusInternalServerError, model.ErrorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, model.SubmitResponse{JobID: job.ID})
}

type stepEntry struct {
	StepID    string `json:"stepId"`
	Type      string `json:"type"`
	Seq       int64  `json:"seq"`
	ExitCode  *int   `json:"exitCode,omitempty"`
	OutputRef string `json:"outputRef,omitempty"`
}

type jobStepsResponse struct {
	JobID             string      `json:"jobId"`
	Steps             []stepEntry `json:"steps"`
	LastCompletedStep string      `json:"lastCompletedStep"`
	CompletedCount    int         `json:"completedCount"`
}

func (s *Server) handleGetJobSteps(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	_, err := s.store.GetJob(r.Context(), id)
	if err == sql.ErrNoRows {
		writeJSON(w, http.StatusNotFound, model.ErrorResponse{Error: "job not found"})
		return
	}
	if err != nil {
		log.Printf("GetJob error: %v", err)
		writeJSON(w, http.StatusInternalServerError, model.ErrorResponse{Error: err.Error()})
		return
	}

	cp, err := s.store.GetLatestCheckpoint(r.Context(), id)
	if err != nil {
		log.Printf("GetLatestCheckpoint error: %v", err)
		writeJSON(w, http.StatusInternalServerError, model.ErrorResponse{Error: err.Error()})
		return
	}

	resp := jobStepsResponse{
		JobID: id,
		Steps: []stepEntry{},
	}

	if cp != nil && cp.Data != nil {
		var manifest model.WALManifest
		if err := json.Unmarshal(cp.Data, &manifest); err == nil {
			for _, cs := range manifest.CompletedSteps {
				exitCode := 0
				resp.Steps = append(resp.Steps, stepEntry{
					StepID:    cs.StepID,
					Type:      "step_end",
					Seq:       cs.Seq,
					ExitCode:  &exitCode,
					OutputRef: cs.OutputRef,
				})
			}
			resp.LastCompletedStep = manifest.LastCompletedStep
			resp.CompletedCount = len(manifest.CompletedSteps)
		}
	}

	writeJSON(w, http.StatusOK, resp)
}

type recoveryInfoResponse struct {
	JobID             string   `json:"jobId"`
	RecoveryMode      bool     `json:"recoveryMode"`
	CompletedSteps    []string `json:"completedSteps"`
	LastCompletedStep string   `json:"lastCompletedStep"`
	RecoveredFromStep string   `json:"recoveredFromStep"`
}

func (s *Server) handleGetRecoveryInfo(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, err := s.store.GetJob(r.Context(), id)
	if err == sql.ErrNoRows {
		writeJSON(w, http.StatusNotFound, model.ErrorResponse{Error: "job not found"})
		return
	}
	if err != nil {
		log.Printf("GetJob error: %v", err)
		writeJSON(w, http.StatusInternalServerError, model.ErrorResponse{Error: err.Error()})
		return
	}

	resp := recoveryInfoResponse{
		JobID:          id,
		RecoveryMode:   job.State == model.JobRecovering,
		CompletedSteps: []string{},
	}

	cp, err := s.store.GetLatestCheckpoint(r.Context(), id)
	if err != nil {
		log.Printf("GetLatestCheckpoint error: %v", err)
		writeJSON(w, http.StatusInternalServerError, model.ErrorResponse{Error: err.Error()})
		return
	}

	if cp != nil && cp.Data != nil {
		var manifest model.WALManifest
		if err := json.Unmarshal(cp.Data, &manifest); err == nil {
			for _, cs := range manifest.CompletedSteps {
				resp.CompletedSteps = append(resp.CompletedSteps, cs.StepID)
			}
			resp.LastCompletedStep = manifest.LastCompletedStep
			resp.RecoveredFromStep = manifest.LastCompletedStep
		}
	}

	writeJSON(w, http.StatusOK, resp)
}

type metricsPoolSection struct {
	TotalPods  int   `json:"totalPods"`
	IdlePods   int   `json:"idlePods"`
	ClaimedPods int  `json:"claimedPods"`
	ClaimTotal int64 `json:"claimTotal"`
	MissTotal  int64 `json:"missTotal"`
}

type metricsWALSection struct {
	TotalSteps     int64 `json:"totalSteps"`
	CompletedSteps int64 `json:"completedSteps"`
	SkippedSteps   int64 `json:"skippedSteps"`
}

type metricsIsolationSection struct {
	L0 int64 `json:"L0"`
	L1 int64 `json:"L1"`
	L2 int64 `json:"L2"`
	L3 int64 `json:"L3"`
}

type metricsResponse struct {
	Pool      metricsPoolSection      `json:"pool"`
	WAL       metricsWALSection       `json:"wal"`
	Isolation metricsIsolationSection `json:"isolation"`
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	resp := metricsResponse{}

	if s.poolMgr != nil {
		status := s.poolMgr.Status()
		pm := s.poolMgr.Metrics()
		resp.Pool = metricsPoolSection{
			TotalPods:   status.TotalPods,
			IdlePods:    status.IdlePods,
			ClaimedPods: status.ClaimedPods,
			ClaimTotal:  pm.ClaimTotal,
			MissTotal:   pm.MissTotal,
		}
	}

	if s.isolationMgr != nil {
		im := s.isolationMgr.GetMetrics()
		for level, lm := range im {
			switch level {
			case isolation.L0Process:
				resp.Isolation.L0 = lm.StartCount
			case isolation.L1GVisor:
				resp.Isolation.L1 = lm.StartCount
			case isolation.L2Firecracker:
				resp.Isolation.L2 = lm.StartCount
			case isolation.L3Docker:
				resp.Isolation.L3 = lm.StartCount
			}
		}
	}

	writeJSON(w, http.StatusOK, resp)
}
