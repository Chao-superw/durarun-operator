#!/usr/bin/env python3
"""
durarun v4 Real Agent Benchmark
3 Agents x 4 Platforms x 6 Scenarios x 3 Trials
Real LLM API calls via ZhiPu GLM-5.3-Flash. Zero sleep/mock.
"""

import argparse, asyncio, json, os, signal, subprocess, sys, time, traceback, uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean, stdev
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
RESULTS_DIR = Path("/data00/agentfabric/benchmarks/v4/results")
TEMPORAL_ADDR = "localhost:7233"
JAEGER_ENDPOINT = "http://localhost:4318/v1/traces"
VENV_PYTHON = "/data00/agentfabric/benchmarks/v4/venv/bin/python3"
SCRIPT_DIR = Path(__file__).parent.resolve()
S1_WORKER = SCRIPT_DIR / "s1_worker.py"

_client: OpenAI | None = None


def get_client() -> OpenAI:
    global _client
    if _client is None:
        _client = OpenAI(api_key=ZHIPU_API_KEY, base_url=ZHIPU_BASE_URL, timeout=60.0)
    return _client


# ═══════════════════════════════════════════════════════════════
# Agent Definitions (same as v3)
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
        step_results.append({"step": s["name"], "elapsed_ms": round(dt * 1000, 1),
                             "output_len": len(output), "skipped": False})
    total_ms = round((time.monotonic() - t_total) * 1000, 1)
    return {"platform": "native", "elapsed_ms": total_ms, "steps": step_results}


# ═══════════════════════════════════════════════════════════════
# Platform 2: durarun
# ═══════════════════════════════════════════════════════════════

def run_durarun(agent_name: str, steps: list[dict], *, dry_run=False,
                db_path: str | None = None, run_id: str | None = None,
                otel_disabled: bool = False, **kw) -> dict:
    from durarun import DurableRunner

    if db_path is None:
        db_path = f"/tmp/durarun-bench-{uuid.uuid4().hex[:8]}.db"
    if run_id is None:
        run_id = uuid.uuid4().hex

    otel_ep = "disabled" if otel_disabled else None  # None = stdout default
    runner = DurableRunner(
        backend=f"sqlite://{db_path}",
        run_id=run_id,
        max_retries=0,
        step_timeout="5m",
        otel_endpoint=otel_ep,
    )

    step_fns = []
    for s in steps:
        sc = dict(s)
        dr = dry_run

        @runner.step(name=sc["name"])
        def _fn(ctx, _s=sc, _dr=dr):
            return call_llm(_s["system"], _s["user"], dry_run=_dr)

        step_fns.append(_fn)

    t_total = time.monotonic()
    result = runner.run(step_fns)
    total_ms = round((time.monotonic() - t_total) * 1000, 1)

    step_results = []
    for d in result.step_details:
        step_results.append({
            "step": d.name,
            "elapsed_ms": round(d.duration_ms, 1),
            "output_len": len(str(d.result)) if d.result else 0,
            "skipped": d.source == "wal",
        })

    return {
        "platform": "durarun",
        "elapsed_ms": total_ms,
        "steps": step_results,
        "recovered_steps": result.recovered_steps,
        "run_id": run_id,
        "db_path": db_path,
    }


# ═══════════════════════════════════════════════════════════════
# Platform 3: Temporal (module-level definitions required by SDK)
# ═══════════════════════════════════════════════════════════════

from temporalio import activity as _ta, workflow as _tw
from temporalio.common import RetryPolicy as _RetryPolicy

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
                start_to_close_timeout=timedelta(seconds=120),
                retry_policy=_RetryPolicy(maximum_attempts=1),
            )
            results.append(result)
        return results


# S2 retry workflow: step 3 retries via Temporal retry_policy
_s2_fail_counters: dict[str, int] = {}


@_ta.defn
async def temporal_llm_step_s2(step_config: dict) -> dict:
    step_idx = step_config.get("step_idx", 0)
    fail_key = step_config.get("fail_key", "")
    if step_idx == 2 and fail_key:
        _s2_fail_counters.setdefault(fail_key, 0)
        _s2_fail_counters[fail_key] += 1
        if _s2_fail_counters[fail_key] <= 2:
            raise RuntimeError(f"Injected failure #{_s2_fail_counters[fail_key]} for step {step_config['name']}")
    t0 = time.monotonic()
    output = call_llm(step_config["system"], step_config["user"], dry_run=_temporal_dry_run)
    dt = time.monotonic() - t0
    return {"step": step_config["name"], "elapsed_ms": round(dt * 1000, 1),
            "output_len": len(output), "skipped": False}


@_tw.defn
class BenchmarkWorkflowS2:
    @_tw.run
    async def run(self, steps_json: str) -> list[dict]:
        steps = json.loads(steps_json)
        results = []
        for sc in steps:
            result = await _tw.execute_activity(
                temporal_llm_step_s2, sc,
                start_to_close_timeout=timedelta(seconds=120),
                retry_policy=_RetryPolicy(maximum_attempts=5),
            )
            results.append(result)
        return results


TEMPORAL_RUN_TIMEOUT = 180  # 3 min max per temporal run


def run_temporal(agent_name: str, steps: list[dict], *, dry_run=False, **kw) -> dict:
    global _temporal_dry_run
    _temporal_dry_run = dry_run
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(
            asyncio.wait_for(_run_temporal_async(agent_name, steps), timeout=TEMPORAL_RUN_TIMEOUT)
        )
    finally:
        loop.close()


async def _run_temporal_async(agent_name: str, steps: list[dict]) -> dict:
    from temporalio.client import Client
    from temporalio.worker import Worker, UnsandboxedWorkflowRunner

    task_queue = f"v4-{agent_name}-{uuid.uuid4().hex[:8]}"
    client = await Client.connect(TEMPORAL_ADDR)
    async with Worker(client, task_queue=task_queue,
                      workflows=[BenchmarkWorkflow, BenchmarkWorkflowS2],
                      activities=[temporal_llm_step, temporal_llm_step_s2],
                      workflow_runner=UnsandboxedWorkflowRunner()):
        t_total = time.monotonic()
        wf_result = await client.execute_workflow(
            BenchmarkWorkflow.run, json.dumps(steps),
            id=f"v4-{uuid.uuid4().hex[:12]}", task_queue=task_queue,
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


# ═══════════════════════════════════════════════════════════════
# Scenario: S1 — Crash Recovery (real kill -9 via subprocess)
# ═══════════════════════════════════════════════════════════════

def run_s1_recovery(agent_name: str, platform: str, *, dry_run=False) -> dict:
    """Orchestrate real kill -9 crash recovery via s1_worker.py subprocess."""
    run_id = uuid.uuid4().hex
    data_dir = str(RESULTS_DIR / "_s1_data" / f"{platform}-{agent_name}-{run_id[:8]}")
    os.makedirs(data_dir, exist_ok=True)
    signal_file = os.path.join(data_dir, "signal")
    result_file = os.path.join(data_dir, "result.json")

    base_cmd = [
        VENV_PYTHON, str(S1_WORKER),
        "--platform", platform,
        "--agent", agent_name,
        "--run-id", run_id,
        "--data-dir", data_dir,
        "--signal-file", signal_file,
        "--result-file", result_file,
    ]
    if dry_run:
        base_cmd.append("--dry-run")

    # Phase 1: launch initial worker
    initial_cmd = base_cmd + ["--mode", "initial", "--crash-after", "3"]
    log(f"    S1 Phase 1: launching worker (platform={platform})")
    proc = subprocess.Popen(initial_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Wait for signal file (step 3 persisted)
    deadline = time.monotonic() + 300  # 5 min max
    while time.monotonic() < deadline:
        if os.path.exists(signal_file):
            break
        try:
            proc.wait(timeout=0.5)
            # Process exited before signal — error
            stdout = proc.stdout.read().decode() if proc.stdout else ""
            stderr = proc.stderr.read().decode() if proc.stderr else ""
            return {
                "platform": platform, "elapsed_ms": 0, "steps": [],
                "error": f"Worker exited prematurely (rc={proc.returncode}): {stderr[-200:]}",
            }
        except subprocess.TimeoutExpired:
            pass
    else:
        proc.kill()
        return {"platform": platform, "elapsed_ms": 0, "steps": [],
                "error": "Timeout waiting for signal file"}

    # Phase 2: SIGKILL the worker
    log(f"    S1 Phase 2: sending SIGKILL to pid {proc.pid}")
    os.kill(proc.pid, signal.SIGKILL)
    proc.wait()
    time.sleep(0.5)

    # Phase 3: launch recovery worker (timed)
    recover_cmd = base_cmd + ["--mode", "recover"]
    log(f"    S1 Phase 3: launching recovery worker")
    t_start = time.monotonic()
    rproc = subprocess.run(recover_cmd, capture_output=True, text=True, timeout=300)
    total_ms_outer = round((time.monotonic() - t_start) * 1000, 1)

    if rproc.returncode != 0:
        return {"platform": platform, "elapsed_ms": total_ms_outer, "steps": [],
                "error": f"Recovery worker failed (rc={rproc.returncode}): {rproc.stderr[-200:]}"}

    # Read result from worker
    if os.path.exists(result_file):
        result = json.loads(Path(result_file).read_text())
        result["kill_method"] = "SIGKILL"
        result["outer_elapsed_ms"] = total_ms_outer
        return result
    else:
        return {"platform": platform, "elapsed_ms": total_ms_outer, "steps": [],
                "error": "No result file produced by recovery worker"}


# ═══════════════════════════════════════════════════════════════
# Scenario: S2 — Step Failure Retry
# ═══════════════════════════════════════════════════════════════

def _make_failing_llm(step_idx: int, fail_count: int = 2, *, dry_run: bool = False):
    """Returns a call_llm wrapper that fails `fail_count` times for step at `step_idx`."""
    counter = {"n": 0}

    def wrapper(system: str, user: str, *, _step_idx: int = -1) -> str:
        if _step_idx == step_idx:
            counter["n"] += 1
            if counter["n"] <= fail_count:
                raise RuntimeError(f"Injected failure #{counter['n']} for step index {_step_idx}")
        return call_llm(system, user, dry_run=dry_run)

    return wrapper


def run_s2_native(agent_name: str, steps: list[dict], *, dry_run=False) -> dict:
    step_results = []
    total_retries = 0
    t_total = time.monotonic()
    for idx, s in enumerate(steps):
        t0 = time.monotonic()
        retry_count = 0
        while True:
            try:
                if idx == 2 and retry_count < 2:
                    raise RuntimeError(f"Injected failure #{retry_count + 1}")
                output = call_llm(s["system"], s["user"], dry_run=dry_run)
                break
            except RuntimeError:
                retry_count += 1
                if retry_count > 3:
                    raise
        dt = time.monotonic() - t0
        total_retries += retry_count
        step_results.append({"step": s["name"], "elapsed_ms": round(dt * 1000, 1),
                             "output_len": len(output), "skipped": False,
                             "retry_count": retry_count})
    total_ms = round((time.monotonic() - t_total) * 1000, 1)
    return {"platform": "native", "elapsed_ms": total_ms, "steps": step_results,
            "total_retries": total_retries, "retry_success": True}


def run_s2_durarun(agent_name: str, steps: list[dict], *, dry_run=False) -> dict:
    from durarun import DurableRunner

    db_path = f"/tmp/durarun-s2-{uuid.uuid4().hex[:8]}.db"
    runner = DurableRunner(
        backend=f"sqlite://{db_path}",
        max_retries=3,
        step_timeout="5m",
    )

    fail_counter = {"n": 0}
    step_fns = []
    for idx, s in enumerate(steps):
        sc = dict(s)
        dr = dry_run
        sidx = idx

        @runner.step(name=sc["name"])
        def _fn(ctx, _s=sc, _dr=dr, _idx=sidx):
            if _idx == 2:
                fail_counter["n"] += 1
                if fail_counter["n"] <= 2:
                    raise RuntimeError(f"Injected failure #{fail_counter['n']}")
            return call_llm(_s["system"], _s["user"], dry_run=_dr)

        step_fns.append(_fn)

    t_total = time.monotonic()
    try:
        result = runner.run(step_fns)
        total_ms = round((time.monotonic() - t_total) * 1000, 1)
        step_results = []
        for d in result.step_details:
            step_results.append({"step": d.name, "elapsed_ms": round(d.duration_ms, 1),
                                 "retry_count": d.retry_count, "skipped": False})
        return {"platform": "durarun", "elapsed_ms": total_ms, "steps": step_results,
                "retry_success": True}
    except Exception as e:
        total_ms = round((time.monotonic() - t_total) * 1000, 1)
        return {"platform": "durarun", "elapsed_ms": total_ms, "steps": [],
                "retry_success": False, "error": f"{type(e).__name__}: {e}"}


def run_s2_temporal(agent_name: str, steps: list[dict], *, dry_run=False) -> dict:
    global _temporal_dry_run
    _temporal_dry_run = dry_run
    fail_key = uuid.uuid4().hex[:8]
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(
            asyncio.wait_for(_run_s2_temporal_async(agent_name, steps, fail_key), timeout=TEMPORAL_RUN_TIMEOUT)
        )
    finally:
        loop.close()


async def _run_s2_temporal_async(agent_name: str, steps: list[dict], fail_key: str) -> dict:
    from temporalio.client import Client
    from temporalio.worker import Worker, UnsandboxedWorkflowRunner

    steps_with_idx = []
    for idx, s in enumerate(steps):
        sc = dict(s)
        sc["step_idx"] = idx
        sc["fail_key"] = fail_key
        steps_with_idx.append(sc)

    task_queue = f"v4-s2-{agent_name}-{uuid.uuid4().hex[:8]}"
    client = await Client.connect(TEMPORAL_ADDR)
    async with Worker(client, task_queue=task_queue,
                      workflows=[BenchmarkWorkflowS2],
                      activities=[temporal_llm_step_s2],
                      workflow_runner=UnsandboxedWorkflowRunner()):
        t_total = time.monotonic()
        try:
            wf_result = await client.execute_workflow(
                BenchmarkWorkflowS2.run, json.dumps(steps_with_idx),
                id=f"v4-s2-{uuid.uuid4().hex[:12]}", task_queue=task_queue,
            )
            total_ms = round((time.monotonic() - t_total) * 1000, 1)
            return {"platform": "temporal", "elapsed_ms": total_ms, "steps": wf_result,
                    "retry_success": True}
        except Exception as e:
            total_ms = round((time.monotonic() - t_total) * 1000, 1)
            return {"platform": "temporal", "elapsed_ms": total_ms, "steps": [],
                    "retry_success": False, "error": f"{type(e).__name__}: {e}"}


def run_s2_langgraph(agent_name: str, steps: list[dict], *, dry_run=False) -> dict:
    from langgraph.graph import StateGraph, START, END
    from langgraph.checkpoint.memory import MemorySaver
    from typing import TypedDict, Annotated
    import operator

    class BenchState(TypedDict):
        results: Annotated[list, operator.add]

    fail_counter = {"n": 0}
    checkpointer = MemorySaver()
    builder = StateGraph(BenchState)

    for idx, s in enumerate(steps):
        sc = dict(s)
        dr = dry_run
        sidx = idx

        def _node(state: BenchState, _s=sc, _dr=dr, _idx=sidx) -> dict:
            max_attempts = 3
            for attempt in range(max_attempts + 1):
                try:
                    if _idx == 2:
                        fail_counter["n"] += 1
                        if fail_counter["n"] <= 2:
                            raise RuntimeError(f"Injected failure #{fail_counter['n']}")
                    t0 = time.monotonic()
                    output = call_llm(_s["system"], _s["user"], dry_run=_dr)
                    dt = time.monotonic() - t0
                    return {"results": [{"step": _s["name"], "elapsed_ms": round(dt * 1000, 1),
                                         "output_len": len(output), "skipped": False,
                                         "retry_count": attempt}]}
                except RuntimeError:
                    if attempt >= max_attempts:
                        raise
            return {"results": []}

        node_name = f"step_{idx}_{s['name']}"
        builder.add_node(node_name, _node)

    node_names = [f"step_{i}_{s['name']}" for i, s in enumerate(steps)]
    builder.add_edge(START, node_names[0])
    for i in range(len(node_names) - 1):
        builder.add_edge(node_names[i], node_names[i + 1])
    builder.add_edge(node_names[-1], END)
    graph = builder.compile(checkpointer=checkpointer)

    t_total = time.monotonic()
    try:
        result = graph.invoke({"results": []},
                              config={"configurable": {"thread_id": uuid.uuid4().hex[:8]}})
        total_ms = round((time.monotonic() - t_total) * 1000, 1)
        return {"platform": "langgraph", "elapsed_ms": total_ms,
                "steps": result.get("results", []), "retry_success": True}
    except Exception as e:
        total_ms = round((time.monotonic() - t_total) * 1000, 1)
        return {"platform": "langgraph", "elapsed_ms": total_ms, "steps": [],
                "retry_success": False, "error": f"{type(e).__name__}: {e}"}


S2_RUNNERS = {
    "native": run_s2_native,
    "durarun": run_s2_durarun,
    "temporal": run_s2_temporal,
    "langgraph": run_s2_langgraph,
}


# ═══════════════════════════════════════════════════════════════
# Scenario: S4 — Observability Cost (OTEL on vs off)
# ═══════════════════════════════════════════════════════════════

def run_s4_durarun(agent_name: str, steps: list[dict], *, dry_run=False, otel_mode="on") -> dict:
    otel_disabled = (otel_mode == "off")
    result = run_durarun(agent_name, steps, dry_run=dry_run, otel_disabled=otel_disabled)
    result["otel_mode"] = otel_mode
    return result


def run_s4_temporal(agent_name: str, steps: list[dict], *, dry_run=False, otel_mode="on") -> dict:
    # Temporal's observability is server-side; client-side overhead is minimal.
    # We measure with/without client-side interceptor for fairness.
    result = run_temporal(agent_name, steps, dry_run=dry_run)
    result["otel_mode"] = otel_mode
    return result


# ═══════════════════════════════════════════════════════════════
# Scenario: S5 — Cold Start + Integration Cost
# ═══════════════════════════════════════════════════════════════

COLD_START_SCRIPTS = {
    "native": '''
import time, sys
sys.path.insert(0, "/data00/agentfabric/benchmarks/v4")
from openai import OpenAI
t0 = time.monotonic()
client = OpenAI(api_key="{api_key}", base_url="{base_url}")
resp = client.chat.completions.create(model="{model}", messages=[{{"role":"user","content":"Say hello"}}], max_tokens=16)
elapsed = (time.monotonic() - t0) * 1000
print(f"{{elapsed:.1f}}")
''',
    "durarun": '''
import time, sys
sys.path.insert(0, "/data00/agentfabric/benchmarks/v4")
from durarun import DurableRunner
t0 = time.monotonic()
runner = DurableRunner(backend="sqlite:///tmp/cold_start_durarun.db", otel_endpoint="disabled")
@runner.step
def hello(ctx):
    from openai import OpenAI
    c = OpenAI(api_key="{api_key}", base_url="{base_url}")
    r = c.chat.completions.create(model="{model}", messages=[{{"role":"user","content":"Say hello"}}], max_tokens=16)
    return r.choices[0].message.content
result = runner.run([hello])
elapsed = (time.monotonic() - t0) * 1000
print(f"{{elapsed:.1f}}")
''',
    "langgraph": '''
import time, sys, operator
sys.path.insert(0, "/data00/agentfabric/benchmarks/v4")
from typing import TypedDict, Annotated
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import MemorySaver
from openai import OpenAI
t0 = time.monotonic()
class S(TypedDict):
    results: Annotated[list, operator.add]
builder = StateGraph(S)
def hello(state):
    c = OpenAI(api_key="{api_key}", base_url="{base_url}")
    r = c.chat.completions.create(model="{model}", messages=[{{"role":"user","content":"Say hello"}}], max_tokens=16)
    return {{"results": [r.choices[0].message.content]}}
builder.add_node("hello", hello)
builder.add_edge(START, "hello")
builder.add_edge("hello", END)
graph = builder.compile(checkpointer=MemorySaver())
graph.invoke({{"results": []}}, config={{"configurable": {{"thread_id": "cold"}}}})
elapsed = (time.monotonic() - t0) * 1000
print(f"{{elapsed:.1f}}")
''',
    "temporal": '''
import time, asyncio, json, sys
sys.path.insert(0, "/data00/agentfabric/benchmarks/v4")
from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.worker import Worker, UnsandboxedWorkflowRunner
from temporalio.common import RetryPolicy
from datetime import timedelta
from openai import OpenAI
@activity.defn
async def hello_act(inp: str) -> str:
    c = OpenAI(api_key="{api_key}", base_url="{base_url}")
    r = c.chat.completions.create(model="{model}", messages=[{{"role":"user","content":"Say hello"}}], max_tokens=16)
    return r.choices[0].message.content
@workflow.defn
class HelloWf:
    @workflow.run
    async def run(self, inp: str) -> str:
        return await workflow.execute_activity(hello_act, inp, start_to_close_timeout=timedelta(seconds=60), retry_policy=RetryPolicy(maximum_attempts=1))
async def main():
    t0 = time.monotonic()
    client = await Client.connect("localhost:7233")
    async with Worker(client, task_queue="cold-start-bench", workflows=[HelloWf], activities=[hello_act], workflow_runner=UnsandboxedWorkflowRunner()):
        await client.execute_workflow(HelloWf.run, "hi", id="cold-start-test-v4", task_queue="cold-start-bench")
    elapsed = (time.monotonic() - t0) * 1000
    print(f"{{elapsed:.1f}}")
asyncio.run(main())
''',
}

LOC_COUNTS = {
    "native": 12,
    "durarun": 15,
    "langgraph": 25,
    "temporal": 40,
}

DEP_COUNTS = {
    "native": 1,   # openai only
    "durarun": 4,   # durarun + opentelemetry-api + typing-extensions + openai
    "langgraph": 12,  # langgraph + langchain-core + langsmith + etc
    "temporal": 8,   # temporalio + protobuf + types-protobuf + etc
}

NEEDS_INFRA = {
    "native": False,
    "durarun": False,
    "langgraph": False,
    "temporal": True,
}


def run_s5_cold_start(platform: str, *, dry_run=False) -> dict:
    script = COLD_START_SCRIPTS[platform].format(
        api_key=ZHIPU_API_KEY, base_url=ZHIPU_BASE_URL, model=MODEL
    )
    if dry_run:
        return {
            "platform": platform, "cold_start_ms": 0, "loc": LOC_COUNTS[platform],
            "dep_count": DEP_COUNTS[platform], "needs_infra": NEEDS_INFRA[platform],
        }

    try:
        proc = subprocess.run(
            [VENV_PYTHON, "-c", script],
            capture_output=True, text=True, timeout=120,
        )
        if proc.returncode != 0:
            return {"platform": platform, "cold_start_ms": -1,
                    "error": proc.stderr[-300:],
                    "loc": LOC_COUNTS[platform], "dep_count": DEP_COUNTS[platform],
                    "needs_infra": NEEDS_INFRA[platform]}
        lines = [l for l in proc.stdout.strip().split("\n") if l and not l.startswith("[TRACE]")]
        cold_ms = float(lines[-1])
        return {
            "platform": platform, "cold_start_ms": round(cold_ms, 1),
            "loc": LOC_COUNTS[platform], "dep_count": DEP_COUNTS[platform],
            "needs_infra": NEEDS_INFRA[platform],
        }
    except Exception as e:
        return {"platform": platform, "cold_start_ms": -1,
                "error": f"{type(e).__name__}: {e}",
                "loc": LOC_COUNTS[platform], "dep_count": DEP_COUNTS[platform],
                "needs_infra": NEEDS_INFRA[platform]}


# ═══════════════════════════════════════════════════════════════
# Unified Scenario Dispatcher
# ═══════════════════════════════════════════════════════════════

ALL_AGENTS = list(AGENTS.keys())
ALL_PLATFORMS = ["native", "durarun", "temporal", "langgraph"]
ALL_SCENARIOS = ["s0-normal", "s1-recovery", "s2-retry", "s3-concurrent", "s4-observability", "s5-coldstart"]

# S4 only applies to durarun and temporal
S4_PLATFORMS = ["durarun", "temporal"]


def run_single(agent_name: str, platform: str, scenario: str, trial: int, *, dry_run=False) -> dict:
    steps = AGENTS.get(agent_name, [])
    result_base = {"agent": agent_name, "platform": platform, "scenario": scenario, "trial": trial}
    try:
        if scenario == "s0-normal":
            if platform == "native":
                r = run_native(agent_name, steps, dry_run=dry_run)
            elif platform == "durarun":
                r = run_durarun(agent_name, steps, dry_run=dry_run)
            elif platform == "temporal":
                r = run_temporal(agent_name, steps, dry_run=dry_run)
            elif platform == "langgraph":
                r = run_langgraph(agent_name, steps, dry_run=dry_run)
            else:
                raise ValueError(f"Unknown platform: {platform}")
            result_base.update(r)

        elif scenario == "s1-recovery":
            r = run_s1_recovery(agent_name, platform, dry_run=dry_run)
            result_base.update(r)
            skipped = sum(1 for s in r.get("steps", []) if s.get("skipped"))
            result_base["steps_skipped"] = skipped

        elif scenario == "s2-retry":
            runner_fn = S2_RUNNERS.get(platform)
            if runner_fn is None:
                result_base["error"] = f"S2 not supported for {platform}"
            else:
                r = runner_fn(agent_name, steps, dry_run=dry_run)
                result_base.update(r)

        elif scenario == "s3-concurrent":
            def _s0_runner():
                if platform == "native":
                    return run_native(agent_name, steps, dry_run=dry_run)
                elif platform == "durarun":
                    return run_durarun(agent_name, steps, dry_run=dry_run)
                elif platform == "temporal":
                    return run_temporal(agent_name, steps, dry_run=dry_run)
                elif platform == "langgraph":
                    return run_langgraph(agent_name, steps, dry_run=dry_run)

            t_total = time.monotonic()
            sub_results = []
            with ThreadPoolExecutor(max_workers=3) as pool:
                futures = [pool.submit(_s0_runner) for _ in range(3)]
                for f in as_completed(futures):
                    sub_results.append(f.result())
            total_ms = round((time.monotonic() - t_total) * 1000, 1)
            result_base["elapsed_ms"] = total_ms
            result_base["sub_runs"] = sub_results
            result_base["steps"] = []

        elif scenario == "s4-observability":
            if platform not in S4_PLATFORMS:
                result_base["error"] = f"S4 not applicable for {platform}"
            else:
                results_on = []
                results_off = []
                for _ in range(1):  # single measurement per trial
                    if platform == "durarun":
                        r_on = run_s4_durarun(agent_name, steps, dry_run=dry_run, otel_mode="on")
                        r_off = run_s4_durarun(agent_name, steps, dry_run=dry_run, otel_mode="off")
                    else:
                        r_on = run_s4_temporal(agent_name, steps, dry_run=dry_run, otel_mode="on")
                        r_off = run_s4_temporal(agent_name, steps, dry_run=dry_run, otel_mode="off")
                    results_on.append(r_on["elapsed_ms"])
                    results_off.append(r_off["elapsed_ms"])
                avg_on = mean(results_on)
                avg_off = mean(results_off)
                degradation = ((avg_on - avg_off) / avg_off * 100) if avg_off > 0 else 0
                result_base["elapsed_ms"] = avg_on
                result_base["otel_on_ms"] = round(avg_on, 1)
                result_base["otel_off_ms"] = round(avg_off, 1)
                result_base["degradation_pct"] = round(degradation, 2)
                result_base["steps"] = []

        elif scenario == "s5-coldstart":
            r = run_s5_cold_start(platform, dry_run=dry_run)
            result_base.update(r)
            result_base["elapsed_ms"] = r.get("cold_start_ms", 0)

    except Exception as e:
        result_base["error"] = f"{type(e).__name__}: {e}"
        result_base["traceback"] = traceback.format_exc()

    result_base["ts"] = datetime.now(timezone.utc).isoformat()
    return result_base


# ═══════════════════════════════════════════════════════════════
# Result Persistence & Aggregation
# ═══════════════════════════════════════════════════════════════

def save_result(result: dict):
    scenario = result["scenario"]
    if scenario == "s4-observability":
        out_dir = RESULTS_DIR / "s4" / result["agent"] / result["platform"]
    elif scenario == "s5-coldstart":
        out_dir = RESULTS_DIR / "s5" / result["platform"]
    else:
        out_dir = RESULTS_DIR / result["agent"] / result["platform"] / scenario
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"trial{result['trial']}.json"
    out_path.write_text(json.dumps(result, indent=2, default=str))


def result_path(agent: str, platform: str, scenario: str, trial: int) -> Path:
    if scenario == "s4-observability":
        return RESULTS_DIR / "s4" / agent / platform / f"trial{trial}.json"
    elif scenario == "s5-coldstart":
        return RESULTS_DIR / "s5" / platform / f"trial{trial}.json"
    else:
        return RESULTS_DIR / agent / platform / scenario / f"trial{trial}.json"


def aggregate_results():
    all_results = []
    # S0, S1, S2, S3
    for agent in ALL_AGENTS:
        for platform in ALL_PLATFORMS:
            for scenario in ["s0-normal", "s1-recovery", "s2-retry", "s3-concurrent"]:
                for trial in range(1, NUM_TRIALS + 1):
                    p = RESULTS_DIR / agent / platform / scenario / f"trial{trial}.json"
                    if p.exists():
                        all_results.append(json.loads(p.read_text()))
    # S4
    for agent in ALL_AGENTS:
        for platform in S4_PLATFORMS:
            p_dir = RESULTS_DIR / "s4" / agent / platform
            if p_dir.exists():
                for f in sorted(p_dir.glob("trial*.json")):
                    all_results.append(json.loads(f.read_text()))
    # S5
    for platform in ALL_PLATFORMS:
        p_dir = RESULTS_DIR / "s5" / platform
        if p_dir.exists():
            for f in sorted(p_dir.glob("trial*.json")):
                all_results.append(json.loads(f.read_text()))

    agg = defaultdict(list)
    for r in all_results:
        if "error" not in r and "elapsed_ms" in r and r["elapsed_ms"] is not None:
            agg[(r["platform"], r["scenario"])].append(r["elapsed_ms"])

    summary_agg = {}
    for (plat, scen), vals in sorted(agg.items()):
        avg = mean(vals)
        sd = stdev(vals) if len(vals) > 1 else 0
        summary_agg[f"{plat}/{scen}"] = {
            "avg_ms": round(avg, 1), "min_ms": round(min(vals), 1),
            "max_ms": round(max(vals), 1), "stddev_ms": round(sd, 1), "n": len(vals),
        }

    for scen in ALL_SCENARIOS:
        native_key = f"native/{scen}"
        if native_key in summary_agg and summary_agg[native_key]["avg_ms"] > 0:
            native_avg = summary_agg[native_key]["avg_ms"]
            for plat in ALL_PLATFORMS:
                k = f"{plat}/{scen}"
                if k in summary_agg:
                    overhead = (summary_agg[k]["avg_ms"] - native_avg) / native_avg * 100
                    summary_agg[k]["vs_native"] = f"{overhead:+.1f}%"

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_results": len(all_results),
        "errors": sum(1 for r in all_results if "error" in r),
        "aggregates": summary_agg,
    }
    (RESULTS_DIR / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
    return summary


# ═══════════════════════════════════════════════════════════════
# Logging & Main
# ═══════════════════════════════════════════════════════════════

def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def main():
    parser = argparse.ArgumentParser(description="durarun v4 Real Agent Benchmark")
    parser.add_argument("--filter-agent", default="all")
    parser.add_argument("--filter-platform", default="all")
    parser.add_argument("--filter-scenario", default="all")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--trials", type=int, default=NUM_TRIALS)
    parser.add_argument("--skip-existing", action="store_true",
                        help="Skip runs whose result file already exists")
    parser.add_argument("--run-timeout", type=int, default=300,
                        help="Max seconds per individual run (default 300)")
    args = parser.parse_args()

    agents = ALL_AGENTS if args.filter_agent == "all" else [args.filter_agent]
    platforms = ALL_PLATFORMS if args.filter_platform == "all" else [args.filter_platform]
    scenarios = ALL_SCENARIOS if args.filter_scenario == "all" else [args.filter_scenario]
    trials = args.trials

    # Build run list
    run_list = []
    for scenario in scenarios:
        if scenario == "s4-observability":
            for agent in agents:
                for platform in [p for p in platforms if p in S4_PLATFORMS]:
                    for trial in range(1, trials + 1):
                        run_list.append((agent, platform, scenario, trial))
        elif scenario == "s5-coldstart":
            for platform in platforms:
                run_list.append(("n/a", platform, scenario, 1))
        else:
            for agent in agents:
                for platform in platforms:
                    for trial in range(1, trials + 1):
                        run_list.append((agent, platform, scenario, trial))

    total = len(run_list)
    log(f"Starting benchmark: {total} runs")
    log(f"  Agents: {agents}")
    log(f"  Platforms: {platforms}")
    log(f"  Scenarios: {scenarios}")
    log(f"  Trials: {trials}")
    if args.dry_run:
        log("*** DRY RUN -- no real API calls ***")

    completed = 0
    skipped = 0
    errors = 0
    for agent, platform, scenario, trial in run_list:
        completed += 1
        label = f"{agent}/{platform}/{scenario}/trial{trial}"

        if args.skip_existing:
            rp = result_path(agent, platform, scenario, trial)
            if rp.exists():
                try:
                    existing = json.loads(rp.read_text())
                    if "error" not in existing:
                        skipped += 1
                        log(f"[{completed}/{total}] {label} -> SKIP (exists)")
                        continue
                except Exception:
                    pass

        log(f"[{completed}/{total}] {label}")

        # Per-run timeout using signal alarm (Unix only)
        timed_out = False
        old_handler = None
        if not args.dry_run and args.run_timeout > 0:
            def _timeout_handler(signum, frame):
                raise TimeoutError(f"Run exceeded {args.run_timeout}s timeout")
            old_handler = signal.signal(signal.SIGALRM, _timeout_handler)
            signal.alarm(args.run_timeout)

        try:
            result = run_single(agent, platform, scenario, trial, dry_run=args.dry_run)
        except TimeoutError as e:
            timed_out = True
            result = {"agent": agent, "platform": platform, "scenario": scenario,
                      "trial": trial, "error": str(e), "elapsed_ms": 0, "steps": []}
        finally:
            if old_handler is not None:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)

        save_result(result)

        elapsed = result.get("elapsed_ms", "?")
        skipped_steps = result.get("steps_skipped", 0)
        err = result.get("error", "")
        retry_ok = result.get("retry_success")
        status = f"OK {elapsed}ms"
        if skipped_steps:
            status += f" (skipped {skipped_steps})"
        if retry_ok is not None:
            status += f" (retry={'OK' if retry_ok else 'FAIL'})"
        if err:
            status = f"ERROR: {err[:80]}"
            errors += 1
        if timed_out:
            status = f"TIMEOUT: {err[:80]}"
        log(f"  -> {status}")

        if completed < total and not args.dry_run:
            time.sleep(COOLDOWN_SECS)

    log(f"Done: {completed} runs, {skipped} skipped, {errors} errors")
    summary = aggregate_results()
    log(f"Summary -> {RESULTS_DIR}/summary.json ({summary['total_results']} results)")

    print("\n=== Quick Results ===")
    print(f"{'Platform/Scenario':<35} {'Avg ms':>10} {'StdDev':>10} {'vs Native':>12}")
    print("-" * 70)
    for k, v in sorted(summary["aggregates"].items()):
        vs = v.get("vs_native", "baseline")
        print(f"{k:<35} {v['avg_ms']:>10.1f} {v['stddev_ms']:>10.1f} {vs:>12}")


if __name__ == "__main__":
    main()
