# durarun v4 横向对比实验报告

## 1. 实验概述

| 项目 | 值 |
|---|---|
| 实验日期 | 2026-09-20 |
| 实验环境 | devbox-boe (Linux 5.15, 8 核, 15G, Python 3.11.2) |
| 总运行数 | 144 |
| 成功率 | 66/144 (46%) |
| LLM | 智谱 GLM-5.3-Flash (MAX_TOKENS=256, TEMP=0.7) |

### 被测平台

| 平台 | 部署方式 |
|---|---|
| Native | 裸 Python 函数（零框架基线）|
| durarun | pip install + 本地进程 |
| LangGraph | pip install + SqliteSaver |
| Temporal | Python SDK + docker-compose server |

---

## 2. S0 正常执行延迟

| 平台 | Avg (ms) | Min | Max | Stddev | N | vs Native |
|---|---|---|---|---|---|---|
| Native | 28731.4 | 22335.4 | 33756.1 | 3457.9 | 9 | 基线 |
| durarun | 29829.7 | 23158.8 | 34159.1 | 3468.2 | 9 | +3.8% |
| LangGraph | 28739.6 | 24645.5 | 33877.2 | 2606.6 | 9 | +0.0% |
| Temporal | 29638.1 | 25722.3 | 32979.6 | 2723.7 | 9 | +3.2% |

---

## 3. S1 崩溃恢复

| 平台 | 恢复机制 | Avg (ms) | vs S0 节省 | 跳步率 |
|---|---|---|---|---|
| Native | Checkpoint 文件跳步 | 12403.4 | 56.8% | 60% |
| durarun | DurableRunner(run_id=...) WAL 恢复 | 12044.0 | 59.6% | 60% |
| LangGraph | SqliteSaver + 同一 thread_id | 11321.3 | 60.6% | 60% |
| Temporal | Workflow History Replay | 25983.0 | 12.3% | 0% |

---

## 4. S2 失败重试

| 平台 | 成功率 | Avg (ms) | vs S0 额外开销 |
|---|---|---|---|
| Native | 0/9 (0%) | — |  |
| durarun | 0/9 (0%) | — |  |
| LangGraph | 0/9 (0%) | — |  |
| Temporal | 0/9 (0%) | — |  |

---

## 5. S3 并发 (3x)

| 平台 | Avg Max Elapsed (ms) | vs S0 增量 |
|---|---|---|
| Native | — | — |
| durarun | — | — |
| LangGraph | — | — |
| Temporal | — | — |

---

## 6. S4 可观测性成本

| 平台 | OTEL On Avg (ms) | OTEL Off Avg (ms) | 退化 % |
|---|---|---|---|
| durarun | — | — |  |
| Temporal | — | — |  |

---

## 7. S5 冷启动 + 接入成本

| 平台 | 冷启动 (ms) | 接入 LOC | 依赖数 | 需要基础设施 |
|---|---|---|---|---|
| Native | -1.0 | 12 | 1 | 否 |
| durarun | -1.0 | 15 | 4 | 否 |
| LangGraph | -1.0 | 25 | 12 | 否 |
| Temporal | -1.0 | 40 | 8 | 是 |

---

## 8. 五维雷达评分 (0-100)

| 平台 | 执行开销 | 恢复能力 | 重试能力 | 并发扩展 | 接入成本 |
|---|---|---|---|---|---|
| Native | 100 | 80 | 0.0 | 50 | 98.0 |
| durarun | 80.9 | 80 | 0.0 | 50 | 92.0 |
| LangGraph | 99.9 | 80 | 0.0 | 50 | 70.0 |
| Temporal | 84.2 | 30 | 0.0 | 50 | 31.0 |

---

## 9. 验收裁决 (durarun)

| ID | 指标 | 阈值 | 实际值 | 结果 |
|---|---|---|---|---|
| B-1 | S0 overhead vs Native | ≤ +10% | +3.8% | **PASS** |
| B-2 | S1 恢复耗时 vs S0 全量 | ≤ 50% | 40.4% | **PASS** |
| B-3 | S1 跳步率 | ≥ 60% | 60% | **PASS** |
| B-4 | S2 自动重试成功率 | 100% | 0% | **FAIL** |
| B-5 | S3 并发 overhead vs S0 | ≤ +15% | N/A | **N/A** |
| B-6 | S4 OTEL 性能退化 | ≤ 2% | N/A | **N/A** |
| B-7 | S5 接入 LOC | ≤ 20 | 15 | **PASS** |
| B-8 | S5 依赖包数 | ≤ 10 | 4 | **PASS** |
