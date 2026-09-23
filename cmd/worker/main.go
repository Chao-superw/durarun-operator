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
	"durarun-operator/internal/store"
	"durarun-operator/internal/worker"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	dsn := envOr("DATABASE_URL", "postgres://agentfabric:agentfabric@localhost:5432/agentfabric?sslmode=disable")
	dataDir := envOr("DATA_DIR", "/tmp/agentfabric")
	hostDataDir := envOr("HOST_DATA_DIR", "")
	pollIntervalStr := envOr("POLL_INTERVAL", "2s")

	pollInterval, err := time.ParseDuration(pollIntervalStr)
	if err != nil {
		log.Fatalf("invalid POLL_INTERVAL: %v", err)
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

	w := worker.NewV2(s, poolMgr, isolationMgr, dataDir, hostDataDir, pollInterval)
	if err := w.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("worker error: %v", err)
	}
}
