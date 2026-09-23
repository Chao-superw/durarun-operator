#!/usr/bin/env python3
"""
S1 crash-recovery worker — spawned as subprocess by run_benchmark_v4.py.

Modes:
  initial  — execute steps, persist via platform mechanism, signal after crash_after, sleep forever
  recover  — resume from same run_id using platform-native recovery, write result JSON
"""

import argparse
import asyncio
import json
import os
import sys
import time
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
TEMPORAL_ADDR = "localhost:7233"

_client: OpenAI | None = None


def get_client() -> OpenAI:
    global _client
    if _client is None:
        _client = OpenAI(api_key=ZHIPU_API_KEY, base_url=ZHIPU_BASE_URL, timeout=60.0)
    return _client


# ═══════════════════════════════════════════════════════════════
# Agent Definitions (identical to v3)
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
# LLM Call
# ═══════════════════════════════════════════════════════════════

def call_llm(system: str, user: str) -> str:
    for attempt in range(3):
        try:
            resp = get_client().chat.completions.create(
                model=MODEL,
                messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
                max_tokens=MAX_TOKENS,
                temperature=TEMPERATURE,
            )
            return resp.choices[0].message.content or ""
        except Exception:
            if attempt == 2:
                raise
            time.sleep(3)
    return ""


# ═══════════════════════════════════════════════════════════════
# Helper: write signal file (fsync to guarantee visibility)
# ═══════════════════════════════════════════════════════════════

def write_signal(signal_file: str):
    fd = os.open(signal_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        os.write(fd, b"done\n")
        os.fsync(fd)
    finally:
        os.close(fd)


def write_result(result_file: str, data: dict):
    Path(result_file).parent.mkdir(parents=True, exist_ok=True)
    Path(result_file).write_text(json.dumps(data, indent=2, default=str))


# ═══════════════════════════════════════════════════════════════
# Platform: Native (file-based checkpoint)
# ═══════════════════════════════════════════════════════════════

def run_native_initial(steps: list[dict], crash_after: int, data_dir: str, signal_file: str):
    ckpt_path = Path(data_dir) / "checkpoint.json"
    completed = []
    for idx, s in enumerate(steps):
        output = call_llm(s["system"], s["user"])
        completed.append({"step": s["name"], "output_len": len(output)})
        # Persist checkpoint atomically
        tmp = str(ckpt_path) + ".tmp"
        Path(tmp).write_text(json.dumps(completed))
        os.replace(tmp, str(ckpt_path))
        if idx + 1 >= crash_after:
            write_signal(signal_file)
            time.sleep(3600)


def run_native_recover(steps: list[dict], data_dir: str, result_file: str):
    ckpt_path = Path(data_dir) / "checkpoint.json"
    completed_names: set[str] = set()
    if ckpt_path.exists():
        for entry in json.loads(ckpt_path.read_text()):
            completed_names.add(entry["step"])

    step_results = []
    t_total = time.monotonic()
    recovered = 0
    for s in steps:
        if s["name"] in completed_names:
            step_results.append({"step": s["name"], "elapsed_ms": 0, "skipped": True, "output_len": 0})
            recovered += 1
            continue
        t0 = time.monotonic()
        output = call_llm(s["system"], s["user"])
        dt = time.monotonic() - t0
        step_results.append({"step": s["name"], "elapsed_ms": round(dt * 1000, 1), "skipped": False, "output_len": len(output)})
    total_ms = round((time.monotonic() - t_total) * 1000, 1)
    write_result(result_file, {
        "platform": "native", "elapsed_ms": total_ms,
        "steps": step_results, "recovered_steps": recovered,
    })


# ═══════════════════════════════════════════════════════════════
# Platform: durarun (WAL-backed DurableRunner)
# ═══════════════════════════════════════════════════════════════

def run_durarun_initial(steps: list[dict], crash_after: int, run_id: str, data_dir: str, signal_file: str):
    from durarun import DurableRunner
    db_path = str(Path(data_dir) / "durarun.db")
    runner = DurableRunner(backend=f"sqlite://{db_path}", run_id=run_id, step_timeout="5m")

    step_fns = []
    for idx, s in enumerate(steps):
        sc = dict(s)
        ca = crash_after
        sf = signal_file
        step_idx = idx

        @runner.step(name=sc["name"])
        def _step(ctx, _s=sc, _idx=step_idx, _ca=ca, _sf=sf):
            if _idx >= _ca:
                write_signal(_sf)
                time.sleep(3600)
            output = call_llm(_s["system"], _s["user"])
            return output

        step_fns.append(_step)

    runner.run(step_fns)


def run_durarun_recover(steps: list[dict], run_id: str, data_dir: str, result_file: str):
    from durarun import DurableRunner
    db_path = str(Path(data_dir) / "durarun.db")
    runner = DurableRunner(backend=f"sqlite://{db_path}", run_id=run_id, step_timeout="5m")

    step_fns = []
    for s in steps:
        sc = dict(s)

        @runner.step(name=sc["name"])
        def _step(ctx, _s=sc):
            return call_llm(_s["system"], _s["user"])

        step_fns.append(_step)

    t_total = time.monotonic()
    result = runner.run(step_fns)
    total_ms = round((time.monotonic() - t_total) * 1000, 1)

    step_results = []
    for sd in result.step_details:
        step_results.append({
            "step": sd.name, "elapsed_ms": round(sd.duration_ms, 1),
            "skipped": sd.source == "wal", "output_len": len(str(sd.result)) if sd.result else 0,
        })
    write_result(result_file, {
        "platform": "durarun", "elapsed_ms": total_ms,
        "steps": step_results, "recovered_steps": result.recovered_steps,
    })


# ═══════════════════════════════════════════════════════════════
# Platform: LangGraph (SqliteSaver checkpoint)
# ═══════════════════════════════════════════════════════════════

def _build_lg_graph(steps: list[dict], checkpointer, crash_after: int | None = None, signal_file: str | None = None):
    from langgraph.graph import StateGraph, START, END
    from typing import TypedDict, Annotated
    import operator

    class BenchState(TypedDict):
        results: Annotated[list, operator.add]
        completed_count: int

    builder = StateGraph(BenchState)
    for idx, s in enumerate(steps):
        sc = dict(s)
        step_idx = idx
        ca = crash_after
        sf = signal_file

        def _node(state: BenchState, _s=sc, _idx=step_idx, _ca=ca, _sf=sf) -> dict:
            if _ca is not None and _sf is not None and _idx >= _ca:
                write_signal(_sf)
                time.sleep(3600)
            t0 = time.monotonic()
            output = call_llm(_s["system"], _s["user"])
            dt = time.monotonic() - t0
            new_count = state.get("completed_count", 0) + 1
            result = {
                "results": [{"step": _s["name"], "elapsed_ms": round(dt * 1000, 1),
                              "output_len": len(output), "skipped": False}],
                "completed_count": new_count,
            }
            return result

        node_name = f"step_{idx}_{s['name']}"
        builder.add_node(node_name, _node)

    node_names = [f"step_{i}_{s['name']}" for i, s in enumerate(steps)]
    builder.add_edge(START, node_names[0])
    for i in range(len(node_names) - 1):
        builder.add_edge(node_names[i], node_names[i + 1])
    builder.add_edge(node_names[-1], END)
    return builder.compile(checkpointer=checkpointer)


def run_langgraph_initial(steps: list[dict], crash_after: int, run_id: str, data_dir: str, signal_file: str):
    from langgraph.checkpoint.sqlite import SqliteSaver
    import sqlite3
    db_path = str(Path(data_dir) / "langgraph.db")
    conn = sqlite3.connect(db_path, check_same_thread=False)
    checkpointer = SqliteSaver(conn)
    checkpointer.setup()
    graph = _build_lg_graph(steps, checkpointer, crash_after=crash_after, signal_file=signal_file)
    graph.invoke(
        {"results": [], "completed_count": 0},
        config={"configurable": {"thread_id": run_id}},
    )


def run_langgraph_recover(steps: list[dict], run_id: str, data_dir: str, result_file: str):
    from langgraph.checkpoint.sqlite import SqliteSaver
    from langgraph.graph import StateGraph, START, END
    from typing import TypedDict, Annotated
    import operator
    import sqlite3

    class BenchState(TypedDict):
        results: Annotated[list, operator.add]
        completed_count: int

    db_path = str(Path(data_dir) / "langgraph.db")
    conn = sqlite3.connect(db_path, check_same_thread=False)
    checkpointer = SqliteSaver(conn)
    checkpointer.setup()

    config = {"configurable": {"thread_id": run_id}}
    checkpoint_tuple = checkpointer.get_tuple(config)
    already_done = set()
    if checkpoint_tuple and checkpoint_tuple.checkpoint:
        channel = checkpoint_tuple.checkpoint.get("channel_values", {})
        for r in channel.get("results", []):
            if isinstance(r, dict) and "step" in r:
                already_done.add(r["step"])

    step_results = []
    recovered = len(already_done)

    builder = StateGraph(BenchState)
    remaining = []
    for idx, s in enumerate(steps):
        sc = dict(s)
        if sc["name"] in already_done:
            step_results.append({"step": sc["name"], "elapsed_ms": 0, "skipped": True, "output_len": 0})
            continue
        remaining.append((idx, sc))

        def _node(state: BenchState, _s=sc) -> dict:
            t0 = time.monotonic()
            output = call_llm(_s["system"], _s["user"])
            dt = time.monotonic() - t0
            return {
                "results": [{"step": _s["name"], "elapsed_ms": round(dt * 1000, 1),
                              "output_len": len(output), "skipped": False}],
                "completed_count": state.get("completed_count", 0) + 1,
            }

        builder.add_node(f"step_{idx}_{sc['name']}", _node)

    if remaining:
        rnames = [f"step_{idx}_{sc['name']}" for idx, sc in remaining]
        builder.add_edge(START, rnames[0])
        for i in range(len(rnames) - 1):
            builder.add_edge(rnames[i], rnames[i + 1])
        builder.add_edge(rnames[-1], END)
        graph = builder.compile(checkpointer=checkpointer)

        t_total = time.monotonic()
        result = graph.invoke(
            {"results": [], "completed_count": recovered},
            config={"configurable": {"thread_id": f"{run_id}-recover"}},
        )
        total_ms = round((time.monotonic() - t_total) * 1000, 1)
        for r in result.get("results", []):
            step_results.append(r)
    else:
        t_total = time.monotonic()
        total_ms = 0

    write_result(result_file, {
        "platform": "langgraph", "elapsed_ms": total_ms,
        "steps": step_results, "recovered_steps": recovered,
    })


# ═══════════════════════════════════════════════════════════════
# Platform: Temporal (workflow history replay)
# ═══════════════════════════════════════════════════════════════

from temporalio import activity as _ta, workflow as _tw
from temporalio.common import RetryPolicy as _RetryPolicy
from datetime import timedelta as _timedelta

_crash_after_global: int = 0
_signal_file_global: str = ""
_mode_global: str = "initial"


@_ta.defn
def temporal_s1_step(step_config: dict) -> dict:
    _ta.heartbeat("starting")
    if _mode_global == "initial" and step_config["index"] >= _crash_after_global:
        write_signal(_signal_file_global)
        time.sleep(3600)
    t0 = time.monotonic()
    output = call_llm(step_config["system"], step_config["user"])
    _ta.heartbeat("llm_done")
    dt = time.monotonic() - t0
    result = {
        "step": step_config["name"], "elapsed_ms": round(dt * 1000, 1),
        "output_len": len(output), "skipped": False,
    }
    return result


@_tw.defn
class S1BenchWorkflow:
    @_tw.run
    async def run(self, steps_json: str) -> list[dict]:
        steps = json.loads(steps_json)
        results = []
        for sc in steps:
            result = await _tw.execute_activity(
                temporal_s1_step, sc,
                start_to_close_timeout=_timedelta(seconds=300),
                retry_policy=_RetryPolicy(maximum_attempts=3),
                heartbeat_timeout=_timedelta(seconds=15),
            )
            results.append(result)
        return results


def run_temporal_initial(steps: list[dict], crash_after: int, run_id: str, signal_file: str):
    global _crash_after_global, _signal_file_global, _mode_global
    _crash_after_global = crash_after
    _signal_file_global = signal_file
    _mode_global = "initial"

    indexed_steps = [dict(s, index=i) for i, s in enumerate(steps)]
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(_temporal_initial_async(run_id, indexed_steps))
    finally:
        loop.close()


async def _temporal_initial_async(run_id: str, steps: list[dict]):
    from concurrent.futures import ThreadPoolExecutor
    from temporalio.client import Client
    from temporalio.worker import Worker, UnsandboxedWorkflowRunner

    task_queue = f"s1-{run_id}"
    client = await Client.connect(TEMPORAL_ADDR)
    async with Worker(
        client, task_queue=task_queue,
        workflows=[S1BenchWorkflow], activities=[temporal_s1_step],
        workflow_runner=UnsandboxedWorkflowRunner(),
        activity_executor=ThreadPoolExecutor(),
    ):
        await client.execute_workflow(
            S1BenchWorkflow.run, json.dumps(steps),
            id=run_id, task_queue=task_queue,
        )


def run_temporal_recover(steps: list[dict], run_id: str, result_file: str):
    global _mode_global
    _mode_global = "recover"

    indexed_steps = [dict(s, index=i) for i, s in enumerate(steps)]
    loop = asyncio.new_event_loop()
    try:
        result = loop.run_until_complete(_temporal_recover_async(run_id, indexed_steps))
    finally:
        loop.close()
    write_result(result_file, result)


async def _temporal_recover_async(run_id: str, steps: list[dict]) -> dict:
    from concurrent.futures import ThreadPoolExecutor
    from temporalio.client import Client
    from temporalio.worker import Worker, UnsandboxedWorkflowRunner

    task_queue = f"s1-{run_id}"
    client = await Client.connect(TEMPORAL_ADDR)

    # The original workflow is still registered on the server with the same ID.
    # When we start a new worker on the same task queue, Temporal will replay
    # completed activities from history (no re-execution) and continue
    # executing remaining activities.
    handle = client.get_workflow_handle(run_id)

    t_total = time.monotonic()
    async with Worker(
        client, task_queue=task_queue,
        workflows=[S1BenchWorkflow], activities=[temporal_s1_step],
        workflow_runner=UnsandboxedWorkflowRunner(),
        activity_executor=ThreadPoolExecutor(),
    ):
        wf_result = await handle.result()
    total_ms = round((time.monotonic() - t_total) * 1000, 1)

    recovered = 0
    step_results = []
    for r in wf_result:
        is_skipped = r.get("elapsed_ms", 0) < 10
        if is_skipped:
            recovered += 1
        step_results.append({
            "step": r["step"], "elapsed_ms": r.get("elapsed_ms", 0),
            "skipped": is_skipped, "output_len": r.get("output_len", 0),
        })

    return {
        "platform": "temporal", "elapsed_ms": total_ms,
        "steps": step_results, "recovered_steps": recovered,
    }


# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="S1 crash-recovery worker")
    parser.add_argument("--platform", required=True, choices=["native", "durarun", "langgraph", "temporal"])
    parser.add_argument("--agent", required=True, choices=list(AGENTS.keys()))
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--mode", required=True, choices=["initial", "recover"])
    parser.add_argument("--crash-after", type=int, default=3)
    parser.add_argument("--signal-file", required=True)
    parser.add_argument("--result-file", default="")
    parser.add_argument("--data-dir", required=True)
    args = parser.parse_args()

    Path(args.data_dir).mkdir(parents=True, exist_ok=True)
    steps = AGENTS[args.agent]

    if args.mode == "initial":
        if args.platform == "native":
            run_native_initial(steps, args.crash_after, args.data_dir, args.signal_file)
        elif args.platform == "durarun":
            run_durarun_initial(steps, args.crash_after, args.run_id, args.data_dir, args.signal_file)
        elif args.platform == "langgraph":
            run_langgraph_initial(steps, args.crash_after, args.run_id, args.data_dir, args.signal_file)
        elif args.platform == "temporal":
            run_temporal_initial(steps, args.crash_after, args.run_id, args.signal_file)

    elif args.mode == "recover":
        if not args.result_file:
            print("ERROR: --result-file required for recover mode", file=sys.stderr)
            sys.exit(1)
        if args.platform == "native":
            run_native_recover(steps, args.data_dir, args.result_file)
        elif args.platform == "durarun":
            run_durarun_recover(steps, args.run_id, args.data_dir, args.result_file)
        elif args.platform == "langgraph":
            run_langgraph_recover(steps, args.run_id, args.data_dir, args.result_file)
        elif args.platform == "temporal":
            run_temporal_recover(steps, args.run_id, args.result_file)


if __name__ == "__main__":
    main()
