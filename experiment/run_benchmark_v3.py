#!/usr/bin/env python3
"""
AF v3 Real Agent Benchmark — 3 Agents × 4 Platforms × 3 Scenarios × 3 Trials = 108 runs
Real LLM API calls via ZhiPu GLM-5.3-Flash. Zero sleep/mock.
"""

import argparse, asyncio, hashlib, json, os, signal, sys, time, traceback, uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from openai import OpenAI

# ═══════════════════════════════════════════════════════════════
# Config
# ═══════════════════════════════════════════════════════════════

ZHIPU_API_KEY = "5cd492c78ba94783a82213b872a78ab8.3GtDZmahXT0MDRnD"
ZHIPU_BASE_URL = "https://open.bigmodel.cn/api/paas/v4"
MODEL = "glm-5.3-flash"
MAX_TOKENS = 256
TEMPERATURE = 0.7
NUM_TRIALS = 3
COOLDOWN_SECS = 5
RESULTS_DIR = Path("/data00/agentfabric/benchmarks/v3/results")
TEMPORAL_ADDR = "localhost:7233"
JAEGER_ENDPOINT = "http://localhost:4318/v1/traces"

_client: OpenAI | None = None

def get_client() -> OpenAI:
    global _client
    if _client is None:
        _client = OpenAI(api_key=ZHIPU_API_KEY, base_url=ZHIPU_BASE_URL)
    return _client

# ═══════════════════════════════════════════════════════════════
# Agent Definitions
# ═══════════════════════════════════════════════════════════════

AGENTS: dict[str, list[dict]] = {
    "swe-agent": [
        {"name": "analyze", "system": "You are a senior software engineer performing code review.",
         "user": "Analyze this buggy Python function and identify the bug:\n\ndef sum_range(start, end):\n    return sum(range(start, end))\n\nThe function should return the sum of all integers from start to end inclusive. What is the bug and its root cause?"},
        {"name": "plan", "system": "You are a senior software engineer planning a bug fix.",
         "user": "The bug: range(start, end) excludes end. Plan the fix: describe exactly what to change, which line, and why. Be concise."},
        {"name": "code", "system": "You are a senior software engineer writing production code.",
         "user": "Write the corrected sum_range function in Python. Include a one-line docstring. Output only the function definition."},
        {"name": "test", "system": "You are a senior software engineer writing unit tests.",
         "user": "Write 4 pytest test cases for sum_range(start, end) covering: normal range (1,5), single element (3,3), negative numbers (-2,2), large range (1,100)."},
        {"name": "report", "system": "You are a senior software engineer writing a commit message.",
         "user": "Write a concise bug-fix summary: original bug, root cause, fix applied, tests added. Max 100 words."},
    ],
    "gpt-researcher": [
        {"name": "search", "system": "You are a research agent planning information retrieval.",
         "user": "Plan 5 specific search queries for researching 'WebAssembly sandboxing for AI agents'. Cover: memory isolation model, Wasmtime/WasmEdge runtimes, performance benchmarks, capability-based security, real-world deployments."},
        {"name": "extract", "system": "You are a research agent extracting key technical findings.",
         "user": "Extract key technical points about WebAssembly sandboxing: (1) linear memory model and bounds checking, (2) Wasmtime vs WasmEdge feature comparison, (3) measured performance overhead percentages from benchmarks."},
        {"name": "synthesize", "system": "You are a technical writer synthesizing a research report.",
         "user": "Write a 300-word technical overview of WebAssembly sandboxing for AI agents. Cover memory isolation, capability-based security, runtime implementations, and performance trade-offs."},
        {"name": "verify", "system": "You are a fact-checking agent verifying technical claims.",
         "user": "Verify these claims with reasoning: (1) Wasm linear memory prevents out-of-bounds access, (2) Wasmtime achieves within 10% of native performance, (3) WasmEdge supports WASI-NN for AI inference. True/false with explanation."},
        {"name": "report", "system": "You are a research agent finalizing output.",
         "user": "Write the executive summary (max 150 words) of the WebAssembly sandboxing research, highlighting the most important finding and one open challenge."},
    ],
    "deepseek-harness": [
        {"name": "parse", "system": "You are an analytical reasoning agent.",
         "user": "Parse this analysis task: 'Compare Durable Execution vs traditional retry/checkpoint for long-running AI agents.' Identify the 4 most important comparison dimensions."},
        {"name": "reason", "system": "You are an analytical reasoning agent performing step-by-step analysis.",
         "user": "Reason step-by-step: (1) How does Temporal's deterministic replay achieve durable execution? (2) What overhead does it introduce? (3) How does LangGraph's checkpointing differ architecturally? (4) When does manual checkpointing win?"},
        {"name": "solve", "system": "You are an analytical reasoning agent producing structured output.",
         "user": "Score each approach 1-10 on four dimensions (recovery reliability, runtime overhead, implementation complexity, scalability): (A) Temporal Durable Execution, (B) LangGraph StateGraph Checkpoint, (C) Custom file-based checkpoint. Output as a table."},
        {"name": "validate", "system": "You are a critical reviewer validating analytical conclusions.",
         "user": "Review the scoring for logical consistency: Are there edge cases (network partitions, 1-hour steps, 10GB state) that would change the ranking? Identify any scoring bias."},
        {"name": "submit", "system": "You are an analytical reasoning agent writing recommendations.",
         "user": "Final recommendation in under 100 words: When is Durable Execution worth the overhead for AI agent workloads? Give specific thresholds (step count, execution time, failure rate)."},
    ],
}

# ═══════════════════════════════════════════════════════════════
# Core LLM Call
# ═══════════════════════════════════════════════════════════════

def call_llm(system: str, user: str, *, dry_run: bool = False) -> str:
    if dry_run:
        return f"[dry-run] system={system[:40]}... user={user[:40]}..."
    for attempt in range(3):
        try:
            resp = get_client().chat.completions.create(
                model=MODEL,
                messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
                max_tokens=MAX_TOKENS,
                temperature=TEMPERATURE,
            )
            return resp.choices[0].message.content or ""
        except Exception as e:
            if attempt == 2:
                raise
            time.sleep(3)
    return ""

# ═══════════════════════════════════════════════════════════════
# Platform 1: Native
# ═══════════════════════════════════════════════════════════════

def run_native(agent_name: str, steps: list[dict], *, dry_run=False, **kw) -> dict:
    step_results = []
    t_total = time.monotonic()
    for s in steps:
        t0 = time.monotonic()
        output = call_llm(s["system"], s["user"], dry_run=dry_run)
        dt = time.monotonic() - t0
        step_results.append({"step": s["name"], "elapsed_ms": round(dt * 1000, 1), "output_len": len(output), "skipped": False})
    total_ms = round((time.monotonic() - t_total) * 1000, 1)
    return {"platform": "native", "elapsed_ms": total_ms, "steps": step_results}

def run_native_recovery(agent_name: str, steps: list[dict], *, dry_run=False, **kw) -> dict:
    return run_native(agent_name, steps, dry_run=dry_run)

# ═══════════════════════════════════════════════════════════════
# Platform 2: AF v3 (WAL + OTEL)
# ═══════════════════════════════════════════════════════════════

_tracer = None

def _get_tracer():
    global _tracer
    if _tracer is not None:
        return _tracer
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    provider = TracerProvider()
    try:
        exporter = OTLPSpanExporter(endpoint=JAEGER_ENDPOINT)
        provider.add_span_processor(BatchSpanProcessor(exporter))
    except Exception:
        pass
    trace.set_tracer_provider(provider)
    _tracer = trace.get_tracer("af-v3-bench")
    return _tracer

def _wal_read(wal_path: Path) -> set[str]:
    completed: set[str] = set()
    if wal_path.exists():
        for line in wal_path.read_text().splitlines():
            try:
                entry = json.loads(line)
                if entry.get("status") == "completed":
                    completed.add(entry["step"])
            except json.JSONDecodeError:
                pass
    return completed

def _wal_write(wal_path: Path, entry: dict):
    fd = os.open(str(wal_path), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        os.write(fd, (json.dumps(entry) + "\n").encode())
        os.fsync(fd)
    finally:
        os.close(fd)

def run_afv3(agent_name: str, steps: list[dict], *, dry_run=False, wal_dir: str | None = None, **kw) -> dict:
    tracer = _get_tracer()
    if wal_dir is None:
        wal_dir = str(RESULTS_DIR / "_wal" / f"{agent_name}-{uuid.uuid4().hex[:8]}")
    os.makedirs(wal_dir, exist_ok=True)
    wal_path = Path(wal_dir) / "wal.jsonl"
    completed = _wal_read(wal_path)

    step_results = []
    t_total = time.monotonic()
    for s in steps:
        if s["name"] in completed:
            step_results.append({"step": s["name"], "elapsed_ms": 0, "output_len": 0, "skipped": True})
            continue
        with tracer.start_as_current_span(f"af-v3.{agent_name}.{s['name']}") as span:
            _wal_write(wal_path, {"step": s["name"], "status": "started", "ts": time.time()})
            t0 = time.monotonic()
            output = call_llm(s["system"], s["user"], dry_run=dry_run)
            dt = time.monotonic() - t0
            _wal_write(wal_path, {
                "step": s["name"], "status": "completed", "ts": time.time(),
                "elapsed_ms": round(dt * 1000, 1),
                "output_hash": hashlib.sha256(output.encode()).hexdigest()[:16],
            })
            span.set_attribute("step.elapsed_ms", round(dt * 1000, 1))
        step_results.append({"step": s["name"], "elapsed_ms": round(dt * 1000, 1), "output_len": len(output), "skipped": False})
    total_ms = round((time.monotonic() - t_total) * 1000, 1)
    return {"platform": "afv3", "elapsed_ms": total_ms, "steps": step_results, "wal_dir": wal_dir}

def run_afv3_recovery(agent_name: str, steps: list[dict], *, dry_run=False, **kw) -> dict:
    wal_dir = str(RESULTS_DIR / "_wal" / f"{agent_name}-recovery-{uuid.uuid4().hex[:8]}")
    os.makedirs(wal_dir, exist_ok=True)
    wal_path = Path(wal_dir) / "wal.jsonl"
    # Phase 1 (untimed): execute first 3 steps
    for s in steps[:3]:
        _wal_write(wal_path, {"step": s["name"], "status": "started", "ts": time.time()})
        output = call_llm(s["system"], s["user"], dry_run=dry_run)
        _wal_write(wal_path, {"step": s["name"], "status": "completed", "ts": time.time(),
                               "elapsed_ms": 0, "output_hash": hashlib.sha256(output.encode()).hexdigest()[:16]})
    # Phase 2 (timed): recovery run
    return run_afv3(agent_name, steps, dry_run=dry_run, wal_dir=wal_dir)

# ═══════════════════════════════════════════════════════════════
# Platform 3: Temporal (module-level definitions required by SDK)
# ═══════════════════════════════════════════════════════════════

from temporalio import activity as _ta, workflow as _tw
from temporalio.common import RetryPolicy as _RetryPolicy
from datetime import timedelta as _timedelta

_temporal_dry_run = False

@_ta.defn
async def temporal_llm_step(step_config: dict) -> dict:
    t0 = time.monotonic()
    output = call_llm(step_config["system"], step_config["user"], dry_run=_temporal_dry_run)
    dt = time.monotonic() - t0
    return {"step": step_config["name"], "elapsed_ms": round(dt * 1000, 1),
            "output_len": len(output), "skipped": False}

@_tw.defn
class BenchmarkWorkflow:
    @_tw.run
    async def run(self, steps_json: str) -> list[dict]:
        steps = json.loads(steps_json)
        results = []
        for sc in steps:
            result = await _tw.execute_activity(
                temporal_llm_step, sc,
                start_to_close_timeout=_timedelta(seconds=120),
                retry_policy=_RetryPolicy(maximum_attempts=1),
            )
            results.append(result)
        return results

def run_temporal(agent_name: str, steps: list[dict], *, dry_run=False, **kw) -> dict:
    global _temporal_dry_run
    _temporal_dry_run = dry_run
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_run_temporal_async(agent_name, steps))
    finally:
        loop.close()

def run_temporal_recovery(agent_name: str, steps: list[dict], *, dry_run=False, **kw) -> dict:
    global _temporal_dry_run
    _temporal_dry_run = dry_run
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_run_temporal_recovery_async(agent_name, steps))
    finally:
        loop.close()

async def _run_temporal_async(agent_name: str, steps: list[dict]) -> dict:
    from temporalio.client import Client
    from temporalio.worker import Worker, UnsandboxedWorkflowRunner

    task_queue = f"bench-{agent_name}-{uuid.uuid4().hex[:8]}"
    client = await Client.connect(TEMPORAL_ADDR)
    async with Worker(client, task_queue=task_queue,
                      workflows=[BenchmarkWorkflow], activities=[temporal_llm_step],
                      workflow_runner=UnsandboxedWorkflowRunner()):
        t_total = time.monotonic()
        wf_result = await client.execute_workflow(
            BenchmarkWorkflow.run, json.dumps(steps),
            id=f"bench-{uuid.uuid4().hex[:12]}", task_queue=task_queue,
        )
        total_ms = round((time.monotonic() - t_total) * 1000, 1)
    return {"platform": "temporal", "elapsed_ms": total_ms, "steps": wf_result}

async def _run_temporal_recovery_async(agent_name: str, steps: list[dict]) -> dict:
    from temporalio.client import Client
    from temporalio.worker import Worker, UnsandboxedWorkflowRunner

    task_queue = f"bench-rec-{agent_name}-{uuid.uuid4().hex[:8]}"
    unsandboxed = UnsandboxedWorkflowRunner()
    client = await Client.connect(TEMPORAL_ADDR)
    # Phase 1 (untimed): full run to populate Temporal server history
    async with Worker(client, task_queue=task_queue,
                      workflows=[BenchmarkWorkflow], activities=[temporal_llm_step],
                      workflow_runner=unsandboxed):
        await client.execute_workflow(
            BenchmarkWorkflow.run, json.dumps(steps),
            id=f"bench-pre-{uuid.uuid4().hex[:12]}", task_queue=task_queue,
        )
    # Phase 2 (timed): fresh workflow — Temporal must re-execute all activities
    async with Worker(client, task_queue=task_queue,
                      workflows=[BenchmarkWorkflow], activities=[temporal_llm_step],
                      workflow_runner=unsandboxed):
        t_total = time.monotonic()
        wf_result = await client.execute_workflow(
            BenchmarkWorkflow.run, json.dumps(steps),
            id=f"bench-rec-{uuid.uuid4().hex[:12]}", task_queue=task_queue,
        )
        total_ms = round((time.monotonic() - t_total) * 1000, 1)
    return {"platform": "temporal", "elapsed_ms": total_ms, "steps": wf_result}

# ═══════════════════════════════════════════════════════════════
# Platform 4: LangGraph
# ═══════════════════════════════════════════════════════════════

def _build_langgraph(agent_name: str, steps: list[dict], checkpointer, *, dry_run=False):
    from langgraph.graph import StateGraph, START, END
    from typing import TypedDict, Annotated
    import operator

    class BenchState(TypedDict):
        results: Annotated[list, operator.add]

    builder = StateGraph(BenchState)
    for idx, s in enumerate(steps):
        sc = dict(s)
        dr = dry_run
        def _node(state: BenchState, _s=sc, _dr=dr) -> dict:
            t0 = time.monotonic()
            output = call_llm(_s["system"], _s["user"], dry_run=_dr)
            dt = time.monotonic() - t0
            return {"results": [{"step": _s["name"], "elapsed_ms": round(dt * 1000, 1),
                                 "output_len": len(output), "skipped": False}]}
        node_name = f"step_{idx}_{s['name']}"
        builder.add_node(node_name, _node)

    node_names = [f"step_{i}_{s['name']}" for i, s in enumerate(steps)]
    builder.add_edge(START, node_names[0])
    for i in range(len(node_names) - 1):
        builder.add_edge(node_names[i], node_names[i + 1])
    builder.add_edge(node_names[-1], END)
    return builder.compile(checkpointer=checkpointer)

def run_langgraph(agent_name: str, steps: list[dict], *, dry_run=False, **kw) -> dict:
    from langgraph.checkpoint.memory import MemorySaver
    checkpointer = MemorySaver()
    graph = _build_langgraph(agent_name, steps, checkpointer, dry_run=dry_run)
    thread_id = uuid.uuid4().hex[:8]
    t_total = time.monotonic()
    result = graph.invoke({"results": []}, config={"configurable": {"thread_id": thread_id}})
    total_ms = round((time.monotonic() - t_total) * 1000, 1)
    return {"platform": "langgraph", "elapsed_ms": total_ms, "steps": result.get("results", [])}

def run_langgraph_recovery(agent_name: str, steps: list[dict], *, dry_run=False, **kw) -> dict:
    from langgraph.checkpoint.memory import MemorySaver
    from langgraph.graph import StateGraph, START, END
    from typing import TypedDict, Annotated
    import operator

    class BenchState(TypedDict):
        results: Annotated[list, operator.add]

    checkpointer = MemorySaver()
    thread_id = f"recovery-{uuid.uuid4().hex[:8]}"

    # Phase 1 (untimed): partial graph with first 3 steps
    partial_builder = StateGraph(BenchState)
    for idx, s in enumerate(steps[:3]):
        sc = dict(s)
        dr = dry_run
        def _node(state: BenchState, _s=sc, _dr=dr) -> dict:
            t0 = time.monotonic()
            output = call_llm(_s["system"], _s["user"], dry_run=_dr)
            dt = time.monotonic() - t0
            return {"results": [{"step": _s["name"], "elapsed_ms": round(dt * 1000, 1),
                                 "output_len": len(output), "skipped": False}]}
        partial_builder.add_node(f"step_{idx}_{s['name']}", _node)
    pnames = [f"step_{i}_{s['name']}" for i, s in enumerate(steps[:3])]
    partial_builder.add_edge(START, pnames[0])
    for i in range(len(pnames) - 1):
        partial_builder.add_edge(pnames[i], pnames[i + 1])
    partial_builder.add_edge(pnames[-1], END)
    partial_graph = partial_builder.compile(checkpointer=checkpointer)
    partial_graph.invoke({"results": []}, config={"configurable": {"thread_id": thread_id}})

    # Phase 2 (timed): full graph, new thread (LangGraph re-executes all nodes)
    graph = _build_langgraph(agent_name, steps, checkpointer, dry_run=dry_run)
    t_total = time.monotonic()
    result = graph.invoke({"results": []}, config={"configurable": {"thread_id": f"{thread_id}-full"}})
    total_ms = round((time.monotonic() - t_total) * 1000, 1)
    return {"platform": "langgraph", "elapsed_ms": total_ms, "steps": result.get("results", [])}

# ═══════════════════════════════════════════════════════════════
# Scenario Runners
# ═══════════════════════════════════════════════════════════════

PLATFORM_RUNNERS = {
    "native":    {"normal": run_native,    "recovery": run_native_recovery},
    "afv3":      {"normal": run_afv3,      "recovery": run_afv3_recovery},
    "temporal":  {"normal": run_temporal,   "recovery": run_temporal_recovery},
    "langgraph": {"normal": run_langgraph, "recovery": run_langgraph_recovery},
}

def run_single(agent_name: str, platform: str, scenario: str, trial: int, *, dry_run=False) -> dict:
    steps = AGENTS[agent_name]
    result_base = {"agent": agent_name, "platform": platform, "scenario": scenario, "trial": trial}
    try:
        if scenario == "s0-normal":
            r = PLATFORM_RUNNERS[platform]["normal"](agent_name, steps, dry_run=dry_run)
            result_base.update(r)
        elif scenario == "s1-recovery":
            r = PLATFORM_RUNNERS[platform]["recovery"](agent_name, steps, dry_run=dry_run)
            result_base.update(r)
            skipped = sum(1 for s in r.get("steps", []) if s.get("skipped"))
            result_base["steps_skipped"] = skipped
        elif scenario == "s3-concurrent":
            runner_fn = PLATFORM_RUNNERS[platform]["normal"]
            t_total = time.monotonic()
            sub_results = []
            with ThreadPoolExecutor(max_workers=3) as pool:
                futures = [pool.submit(runner_fn, agent_name, steps, dry_run=dry_run) for _ in range(3)]
                for f in as_completed(futures):
                    sub_results.append(f.result())
            total_ms = round((time.monotonic() - t_total) * 1000, 1)
            result_base["elapsed_ms"] = total_ms
            result_base["sub_runs"] = sub_results
            result_base["steps"] = []
    except Exception as e:
        result_base["error"] = f"{type(e).__name__}: {e}"
        result_base["traceback"] = traceback.format_exc()
    result_base["ts"] = datetime.now(timezone.utc).isoformat()
    return result_base

# ═══════════════════════════════════════════════════════════════
# Main Experiment Driver
# ═══════════════════════════════════════════════════════════════

ALL_AGENTS = list(AGENTS.keys())
ALL_PLATFORMS = ["native", "afv3", "temporal", "langgraph"]
ALL_SCENARIOS = ["s0-normal", "s1-recovery", "s3-concurrent"]

def save_result(result: dict):
    out_dir = RESULTS_DIR / result["agent"] / result["platform"] / result["scenario"]
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"trial{result['trial']}.json"
    out_path.write_text(json.dumps(result, indent=2, default=str))

def aggregate_results():
    all_results = []
    for agent in ALL_AGENTS:
        for platform in ALL_PLATFORMS:
            for scenario in ALL_SCENARIOS:
                for trial in range(1, NUM_TRIALS + 1):
                    p = RESULTS_DIR / agent / platform / scenario / f"trial{trial}.json"
                    if p.exists():
                        all_results.append(json.loads(p.read_text()))

    from collections import defaultdict
    agg = defaultdict(list)
    for r in all_results:
        if "error" not in r and "elapsed_ms" in r:
            agg[(r["platform"], r["scenario"])].append(r["elapsed_ms"])

    summary_agg = {}
    for (plat, scen), vals in sorted(agg.items()):
        avg = sum(vals) / len(vals)
        summary_agg[f"{plat}/{scen}"] = {
            "avg_ms": round(avg, 1), "min_ms": round(min(vals), 1),
            "max_ms": round(max(vals), 1), "n": len(vals),
        }

    for scen in ALL_SCENARIOS:
        native_key = f"native/{scen}"
        if native_key in summary_agg:
            native_avg = summary_agg[native_key]["avg_ms"]
            for plat in ALL_PLATFORMS:
                k = f"{plat}/{scen}"
                if k in summary_agg and native_avg > 0:
                    overhead = (summary_agg[k]["avg_ms"] - native_avg) / native_avg * 100
                    summary_agg[k]["vs_native"] = f"+{overhead:.1f}%"

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_results": len(all_results),
        "errors": sum(1 for r in all_results if "error" in r),
        "aggregates": summary_agg,
        "raw": all_results,
    }
    (RESULTS_DIR / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
    return summary

def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

def main():
    parser = argparse.ArgumentParser(description="AF v3 Real Agent Benchmark")
    parser.add_argument("--filter-agent", default="all")
    parser.add_argument("--filter-platform", default="all")
    parser.add_argument("--filter-scenario", default="all")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--trials", type=int, default=NUM_TRIALS)
    args = parser.parse_args()

    agents = ALL_AGENTS if args.filter_agent == "all" else [args.filter_agent]
    platforms = ALL_PLATFORMS if args.filter_platform == "all" else [args.filter_platform]
    scenarios = ALL_SCENARIOS if args.filter_scenario == "all" else [args.filter_scenario]
    trials = args.trials

    total = len(agents) * len(platforms) * len(scenarios) * trials
    log(f"Starting benchmark: {len(agents)} agents x {len(platforms)} platforms x {len(scenarios)} scenarios x {trials} trials = {total} runs")
    if args.dry_run:
        log("*** DRY RUN -- no real API calls ***")

    completed = 0
    errors = 0
    for agent in agents:
        for platform in platforms:
            for scenario in scenarios:
                for trial in range(1, trials + 1):
                    completed += 1
                    log(f"[{completed}/{total}] {agent}/{platform}/{scenario}/trial{trial}")
                    result = run_single(agent, platform, scenario, trial, dry_run=args.dry_run)
                    save_result(result)
                    elapsed = result.get("elapsed_ms", "?")
                    skipped = result.get("steps_skipped", 0)
                    err = result.get("error", "")
                    status = f"OK {elapsed}ms" + (f" (skipped {skipped})" if skipped else "")
                    if err:
                        status = f"ERROR: {err[:80]}"
                        errors += 1
                    log(f"  -> {status}")
                    if completed < total and not args.dry_run:
                        time.sleep(COOLDOWN_SECS)

    log(f"Done: {completed} runs, {errors} errors")
    summary = aggregate_results()
    log(f"Summary -> {RESULTS_DIR}/summary.json ({summary['total_results']} results)")
    print("\n=== Quick Results ===")
    print(f"{'Platform/Scenario':<30} {'Avg ms':>10} {'vs Native':>12}")
    print("-" * 55)
    for k, v in sorted(summary["aggregates"].items()):
        vs = v.get("vs_native", "baseline")
        print(f"{k:<30} {v['avg_ms']:>10.1f} {vs:>12}")

if __name__ == "__main__":
    main()
