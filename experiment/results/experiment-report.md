# Agent-Fabric v2 Experiment Report

**Date**: 2026-09-17T13:28:04Z

**Environment**: darwin/arm64, 18 CPUs, GOMAXPROCS=18, gc

> **Note**: Agent names (SWE-Agent, GPT-Researcher, DeepSeek-Harness) are inspired by
> real products' typical pipeline lengths. This benchmark does not run those products.

**Methodology**: All measurements are wall-clock times from real code execution.
- **Step work**: SHA-256 CPU hashing for 100ms per step (not sleep/jitter)
- **Native**: Direct Go function calls, no framework
- **AF v2**: Real pool.Manager.Claim + runner.NewRunner + WAL writes (fsync) + pool.Release
- **LangGraph**: Real Python subprocess, StateGraph with sequential nodes
- **Temporal**: Go SDK workflow + activities, connected to real dev server (gRPC)

## Cross-Platform Summary

### S0-normal

| Platform | Avg (ms) | vs Native |
|----------|----------|----------|
| native | 733.4 | baseline |
| af-v2 | 765.0 | +4.3% |
| langgraph | 737.2 | +0.5% |
| temporal | 826.6 | +12.7% |

### S1-recovery

| Platform | Avg (ms) | vs Native |
|----------|----------|----------|
| native | 733.4 | baseline |
| af-v2 | 282.6 | -61.5% |
| langgraph | 737.0 | +0.5% |
| temporal | 2.3 | -99.7% |

### S3-concurrent-3x

| Platform | Avg (ms) | vs Native |
|----------|----------|----------|
| native | 733.4 | baseline |
| af-v2 | 782.0 | +6.6% |
| langgraph | 807.2 | +10.1% |
| temporal | 1050.9 | +43.3% |

## Acceptance Verdicts

| ID | Description | Target | Actual | Status |
|----|-------------|--------|--------|--------|
| P-1 | S0 avg elapsed <= Native +8% | ≤ +8% | +4.31% | PASS |
| P-3 | S1 skip ratio >= 60% | ≥ 60% | 63.6% | PASS |
| P-4 | S1 recovery total <= Native S1 * 1.20x | ≤ 1.20x | 0.385x | PASS |
| P-5 | S3 concurrent 3x overhead <= +15% | ≤ +15% | +2.22% | PASS |
| P-6 | S1 af-v2 recovery <= Temporal recovery x1.50 | ≤ 1.50x | 123.593x | FAIL |

## Raw Data

See `experiment-report.json` for full raw results (180 data points).
