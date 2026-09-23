package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"durarun-operator/internal/isolation"
	"durarun-operator/internal/pool"
	"durarun-operator/internal/scheduler"
	"durarun-operator/internal/store"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	dsn := envOr("DATABASE_URL", "postgres://agentfabric:agentfabric@localhost:5432/agentfabric?sslmode=disable")
	intervalStr := envOr("RECONCILE_INTERVAL", "3s")
	heartbeatTimeoutStr := envOr("HEARTBEAT_TIMEOUT", "15s")

	interval, err := time.ParseDuration(intervalStr)
	if err != nil {
		log.Fatalf("invalid RECONCILE_INTERVAL: %v", err)
	}
	heartbeatTimeout, err := time.ParseDuration(heartbeatTimeoutStr)
	if err != nil {
		log.Fatalf("invalid HEARTBEAT_TIMEOUT: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	s, err := store.New(dsn)
	if err != nil {
		log.Fatalf("store.New: %v", err)
	}
	if err := s.InitSchema(ctx); err != nil {
		log.Fatalf("InitSchema: %v", err)
	}

	poolCfg := pool.PoolConfig{
		Name:               "default",
		MinSize:            3,
		MaxSize:            20,
		ScaleUpThreshold:   0.7,
		ScaleDownThreshold: 0.3,
		ScaleUpStep:        3,
		CooldownSeconds:    60,
		Image:              envOr("POOL_IMAGE", "agent-base:latest"),
		RuntimeClass:       envOr("POOL_RUNTIME_CLASS", "runsc"),
	}
	poolMgr := pool.NewManager(poolCfg)
	isolationMgr := isolation.NewManager([]isolation.Level{
		isolation.L0Process, isolation.L1GVisor, isolation.L2Firecracker, isolation.L3Docker,
	})

	r := scheduler.New(s, interval, heartbeatTimeout,
		scheduler.WithPoolManager(poolMgr),
		scheduler.WithIsolationManager(isolationMgr),
	)
	log.Printf("scheduler starting (interval=%s heartbeatTimeout=%s)", interval, heartbeatTimeout)
	if err := r.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("reconciler error: %v", err)
	}
}
