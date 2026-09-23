#!/usr/bin/env python3
"""LangGraph real benchmark runner.
Executes a StateGraph with real CPU work (SHA-256 hashing) per node.
Called from Go experiment via subprocess with JSON output.
"""
import hashlib
import json
import sys
import time
from typing import TypedDict

from langgraph.graph import StateGraph, END


class BenchState(TypedDict):
    step_outputs: list[str]
    steps_done: int


def make_step_work(target_ms: float):
    """Return a function that does real SHA-256 hashing for target_ms."""
    def do_work(state: BenchState) -> BenchState:
        start = time.monotonic()
        payload = bytearray(range(256)) * 2
        n = 0
        target_s = target_ms / 1000.0
        while time.monotonic() - start < target_s:
            digest = hashlib.sha256(payload).digest()
            payload[0] = digest[0]
            n += 1
        out = f"sha256:{digest[:8].hex()}:n={n}"
        return {
            "step_outputs": state["step_outputs"] + [out],
            "steps_done": state["steps_done"] + 1,
        }
    return do_work


def build_graph(num_steps: int, step_work_ms: float) -> StateGraph:
    builder = StateGraph(BenchState)
    for i in range(num_steps):
        builder.add_node(f"step_{i}", make_step_work(step_work_ms))
    builder.set_entry_point("step_0")
    for i in range(num_steps - 1):
        builder.add_edge(f"step_{i}", f"step_{i+1}")
    builder.add_edge(f"step_{num_steps-1}", END)
    return builder.compile()


def run_scenario(num_steps: int, step_work_ms: float, scenario: str) -> dict:
    if scenario == "S0-normal":
        graph = build_graph(num_steps, step_work_ms)
        start = time.monotonic()
        result = graph.invoke({"step_outputs": [], "steps_done": 0})
        elapsed_ms = (time.monotonic() - start) * 1000
        return {"elapsed_ms": elapsed_ms, "steps_skipped": 0}

    elif scenario == "S1-recovery":
        # LangGraph has no built-in WAL recovery; must re-execute all steps
        graph = build_graph(num_steps, step_work_ms)
        start = time.monotonic()
        result = graph.invoke({"step_outputs": [], "steps_done": 0})
        elapsed_ms = (time.monotonic() - start) * 1000
        return {"elapsed_ms": elapsed_ms, "steps_skipped": 0}

    elif scenario == "S3-concurrent-3x":
        import concurrent.futures
        graph = build_graph(num_steps, step_work_ms)
        start = time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            futures = [
                pool.submit(graph.invoke, {"step_outputs": [], "steps_done": 0})
                for _ in range(3)
            ]
            concurrent.futures.wait(futures)
        elapsed_ms = (time.monotonic() - start) * 1000
        return {"elapsed_ms": elapsed_ms, "steps_skipped": 0}

    return {"error": f"unknown scenario: {scenario}"}


def main():
    if len(sys.argv) != 4:
        print(json.dumps({"error": "usage: langgraph_bench.py <steps> <step_work_ms> <scenario>"}))
        sys.exit(1)

    num_steps = int(sys.argv[1])
    step_work_ms = float(sys.argv[2])
    scenario = sys.argv[3]

    result = run_scenario(num_steps, step_work_ms, scenario)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
