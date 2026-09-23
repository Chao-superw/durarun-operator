package scheduler

import (
	"context"
	"log"
	"time"

	"durarun-operator/internal/isolation"
	"durarun-operator/internal/model"
	"durarun-operator/internal/pool"
	"durarun-operator/internal/store"
)

type Reconciler struct {
	store            *store.Store
	poolMgr          *pool.Manager
	isolationMgr     *isolation.Manager
	interval         time.Duration
	heartbeatTimeout time.Duration
}

type Option func(*Reconciler)

func WithPoolManager(p *pool.Manager) Option {
	return func(r *Reconciler) { r.poolMgr = p }
}

func WithIsolationManager(m *isolation.Manager) Option {
	return func(r *Reconciler) { r.isolationMgr = m }
}

func New(s *store.Store, interval, heartbeatTimeout time.Duration, opts ...Option) *Reconciler {
	r := &Reconciler{
		store:            s,
		interval:         interval,
		heartbeatTimeout: heartbeatTimeout,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

func (r *Reconciler) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			r.reconcile(ctx)
		}
	}
}

func (r *Reconciler) reconcile(ctx context.Context) {
	r.reconcilePool(ctx)
	r.schedulePendingJobs(ctx)
	r.handleStaleAttempts(ctx)
	r.handleFailedAttempts(ctx)
}

func (r *Reconciler) reconcilePool(ctx context.Context) {
	if r.poolMgr == nil {
		return
	}

	jobs, err := r.store.ListJobs(ctx)
	if err != nil {
		log.Printf("reconciler: reconcilePool ListJobs error: %v", err)
		return
	}

	pending := 0
	for _, job := range jobs {
		if job.State == model.JobPending {
			pending++
		}
	}

	r.poolMgr.SetPendingDemand(pending)

	if err := r.poolMgr.Reconcile(); err != nil {
		log.Printf("reconciler: poolMgr.Reconcile error: %v", err)
	}
}

func (r *Reconciler) schedulePendingJobs(ctx context.Context) {
	jobs, err := r.store.ListJobs(ctx)
	if err != nil {
		log.Printf("reconciler: ListJobs error: %v", err)
		return
	}
	for _, job := range jobs {
		if job.State != model.JobPending {
			continue
		}

		if r.poolMgr != nil {
			claimStart := time.Now()
			result, claimErr := r.poolMgr.Claim(job.ID)
			if claimErr == nil && result != nil && result.WarmHit {
				log.Printf("reconciler: warm pool hit for job %s pod %s latency=%v", job.ID, result.Pod.ID, time.Since(claimStart))

				attempt, err := r.store.CreateAttempt(ctx, job.ID, "")
				if err != nil {
					log.Printf("reconciler: CreateAttempt for job %s error: %v", job.ID, err)
					continue
				}
				if err := r.store.TransitionJobState(ctx, job.ID, model.JobPending, model.JobScheduled, "pool-claim"); err != nil {
					log.Printf("reconciler: TransitionJobState for job %s error: %v", job.ID, err)
					continue
				}
				log.Printf("reconciler: scheduled job %s attempt %s via pool", job.ID, attempt.ID)
				continue
			}
			if claimErr != nil {
				log.Printf("reconciler: pool miss for job %s: %v, falling back to direct scheduling", job.ID, claimErr)
			}
		}

		attempt, err := r.store.CreateAttempt(ctx, job.ID, "")
		if err != nil {
			log.Printf("reconciler: CreateAttempt for job %s error: %v", job.ID, err)
			continue
		}
		if err := r.store.TransitionJobState(ctx, job.ID, model.JobPending, model.JobScheduled, "reconciler"); err != nil {
			log.Printf("reconciler: TransitionJobState for job %s error: %v", job.ID, err)
			continue
		}
		log.Printf("reconciler: scheduled job %s attempt %s", job.ID, attempt.ID)
	}
}

func (r *Reconciler) handleFailedAttempts(ctx context.Context) {
	jobs, err := r.store.ListJobs(ctx)
	if err != nil {
		log.Printf("reconciler: ListJobs error: %v", err)
		return
	}
	for _, job := range jobs {
		if job.State != model.JobRunning {
			continue
		}
		attempts, err := r.store.GetAttempts(ctx, job.ID)
		if err != nil || len(attempts) == 0 {
			continue
		}
		latest := attempts[len(attempts)-1]
		if latest.State != model.AttemptFailed {
			continue
		}

		count, err := r.store.CountAttempts(ctx, job.ID)
		if err != nil {
			continue
		}

		if count < job.Spec.MaxRetries+1 {
			newAttempt, err := r.store.CreateAttempt(ctx, job.ID, "")
			if err != nil {
				log.Printf("reconciler: CreateAttempt for retry job %s error: %v", job.ID, err)
				continue
			}
			if err := r.store.TransitionJobState(ctx, job.ID, model.JobRunning, model.JobScheduled, "retry-after-failure"); err != nil {
				log.Printf("reconciler: retry TransitionJobState for job %s error: %v", job.ID, err)
				continue
			}
			log.Printf("reconciler: retrying failed job %s new attempt %s (count=%d max=%d)", job.ID, newAttempt.ID, count, job.Spec.MaxRetries+1)
		} else {
			if err := r.store.TransitionJobState(ctx, job.ID, model.JobRunning, model.JobFailed, "max-retries-exceeded"); err != nil {
				log.Printf("reconciler: fail TransitionJobState for job %s error: %v", job.ID, err)
				continue
			}
			log.Printf("reconciler: job %s failed after %d attempts (max retries exhausted)", job.ID, count)
		}
	}
}

func (r *Reconciler) handleStaleAttempts(ctx context.Context) {
	stale, err := r.store.FindStaleAttempts(ctx, r.heartbeatTimeout)
	if err != nil {
		log.Printf("reconciler: FindStaleAttempts error: %v", err)
		return
	}
	for _, attempt := range stale {
		if err := r.store.TransitionAttemptState(ctx, attempt.ID, model.AttemptExecuting, model.AttemptTimedOut, "heartbeat-timeout"); err != nil {
			log.Printf("reconciler: TransitionAttemptState for attempt %s error: %v", attempt.ID, err)
			continue
		}

		job, err := r.store.GetJob(ctx, attempt.JobID)
		if err != nil {
			log.Printf("reconciler: GetJob %s error: %v", attempt.JobID, err)
			continue
		}

		count, err := r.store.CountAttempts(ctx, attempt.JobID)
		if err != nil {
			log.Printf("reconciler: CountAttempts for job %s error: %v", attempt.JobID, err)
			continue
		}

		if count < job.Spec.MaxRetries+1 {
			newAttempt, err := r.store.CreateAttempt(ctx, attempt.JobID, "")
			if err != nil {
				log.Printf("reconciler: CreateAttempt for retry job %s error: %v", attempt.JobID, err)
				continue
			}
			if err := r.store.TransitionJobState(ctx, attempt.JobID, model.JobRunning, model.JobScheduled, "retry"); err != nil {
				log.Printf("reconciler: retry TransitionJobState for job %s error: %v", attempt.JobID, err)
				continue
			}
			log.Printf("reconciler: retrying job %s new attempt %s (count=%d max=%d)", attempt.JobID, newAttempt.ID, count, job.Spec.MaxRetries+1)
		} else {
			if err := r.store.TransitionJobState(ctx, attempt.JobID, model.JobRunning, model.JobFailed, "max-retries-exceeded"); err != nil {
				log.Printf("reconciler: fail TransitionJobState for job %s error: %v", attempt.JobID, err)
				continue
			}
			log.Printf("reconciler: job %s failed after %d attempts", attempt.JobID, count)
		}
	}
}
