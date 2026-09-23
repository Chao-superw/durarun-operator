#!/usr/bin/env python3
"""
durarun v4 Benchmark Report Generator
Reads experiment results and generates Markdown + HTML reports.
"""

import argparse
import json
import math
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ALL_AGENTS = ["swe-agent", "gpt-researcher", "deepseek-harness"]
ALL_PLATFORMS = ["native", "durarun", "langgraph", "temporal"]
ALL_SCENARIOS = ["s0-normal", "s1-recovery", "s2-retry", "s3-concurrent"]

PLATFORM_COLORS = {
    "native": "#9ca3af",
    "durarun": "#8b5cf6",
    "langgraph": "#22c55e",
    "temporal": "#3b82f6",
}

PLATFORM_LABELS = {
    "native": "Native",
    "durarun": "durarun",
    "langgraph": "LangGraph",
    "temporal": "Temporal",
}

RECOVERY_MECHANISMS = {
    "native": "Checkpoint 文件跳步",
    "durarun": "DurableRunner(run_id=...) WAL 恢复",
    "langgraph": "SqliteSaver + 同一 thread_id",
    "temporal": "Workflow History Replay",
}

RADAR_DIMS = ["执行开销", "恢复能力", "重试能力", "并发扩展", "接入成本"]

ACCEPTANCE_CRITERIA = [
    ("B-1", "S0 overhead vs Native", "≤ +10%"),
    ("B-2", "S1 恢复耗时 vs S0 全量", "≤ 50%"),
    ("B-3", "S1 跳步率", "≥ 60%"),
    ("B-4", "S2 自动重试成功率", "100%"),
    ("B-5", "S3 并发 overhead vs S0", "≤ +15%"),
    ("B-6", "S4 OTEL 性能退化", "≤ 2%"),
    ("B-7", "S5 接入 LOC", "≤ 20"),
    ("B-8", "S5 依赖包数", "≤ 10"),
]


def safe_stdev(vals):
    if len(vals) < 2:
        return 0.0
    return statistics.stdev(vals)


def load_results(results_dir: Path):
    all_results = []
    for agent in ALL_AGENTS:
        for platform in ALL_PLATFORMS:
            for scenario in ALL_SCENARIOS:
                d = results_dir / agent / platform / scenario
                if not d.exists():
                    continue
                for f in sorted(d.glob("trial*.json")):
                    try:
                        data = json.loads(f.read_text())
                        all_results.append(data)
                    except (json.JSONDecodeError, OSError):
                        pass
    return all_results


def load_s4_results(results_dir: Path):
    s4_data = defaultdict(lambda: defaultdict(list))
    s4_dir = results_dir / "s4"
    if not s4_dir.exists():
        return s4_data
    for agent_dir in sorted(s4_dir.iterdir()):
        if not agent_dir.is_dir():
            continue
        agent = agent_dir.name
        for plat_dir in sorted(agent_dir.iterdir()):
            if not plat_dir.is_dir():
                continue
            platform = plat_dir.name
            for f in sorted(plat_dir.glob("trial*.json")):
                try:
                    data = json.loads(f.read_text())
                    if "otel_on_ms" in data and "otel_off_ms" in data:
                        s4_data[(platform, "otel-on")][agent].append(
                            {**data, "elapsed_ms": data["otel_on_ms"]}
                        )
                        s4_data[(platform, "otel-off")][agent].append(
                            {**data, "elapsed_ms": data["otel_off_ms"]}
                        )
                    else:
                        mode = data.get("otel_mode", "unknown")
                        s4_data[(platform, mode)][agent].append(data)
                except (json.JSONDecodeError, OSError):
                    pass
    return s4_data


def load_s5_results(results_dir: Path):
    s5_data = {}
    s5_dir = results_dir / "s5"
    if not s5_dir.exists():
        return s5_data
    for plat_dir in sorted(s5_dir.iterdir()):
        if not plat_dir.is_dir():
            continue
        for name in ["cold_start.json", "trial1.json"]:
            f = plat_dir / name
            if f.exists():
                try:
                    s5_data[plat_dir.name] = json.loads(f.read_text())
                except (json.JSONDecodeError, OSError):
                    pass
                break
    return s5_data


def aggregate(results, key_fn):
    groups = defaultdict(list)
    for r in results:
        if "error" in r and r["error"]:
            continue
        if "elapsed_ms" not in r:
            continue
        k = key_fn(r)
        groups[k].append(r)
    return groups


def stats_from_group(group):
    vals = [r["elapsed_ms"] for r in group if "elapsed_ms" in r]
    if not vals:
        return {"avg": 0, "min": 0, "max": 0, "stddev": 0, "n": 0}
    return {
        "avg": statistics.mean(vals),
        "min": min(vals),
        "max": max(vals),
        "stddev": safe_stdev(vals),
        "n": len(vals),
    }


def compute_radar_scores(s0_agg, s1_agg, s2_results, s3_agg, s5_data):
    scores = {}
    native_s0 = s0_agg.get("native", {}).get("avg", 1)

    for plat in ALL_PLATFORMS:
        s = [0.0] * 5

        plat_s0 = s0_agg.get(plat, {}).get("avg", native_s0)
        overhead_pct = (plat_s0 - native_s0) / max(native_s0, 1) * 100
        s[0] = max(0, min(100, 100 - overhead_pct * 5))

        plat_s1 = s1_agg.get(plat, {})
        if plat_s1 and plat_s0 > 0:
            recovery_ratio = plat_s1.get("avg", plat_s0) / plat_s0
            if recovery_ratio < 0.3:
                s[1] = 95
            elif recovery_ratio < 0.5:
                s[1] = 80
            elif recovery_ratio < 0.8:
                s[1] = 60
            else:
                s[1] = 30
        else:
            s[1] = 0

        plat_s2 = [r for r in s2_results if r.get("platform") == plat and not r.get("error")]
        plat_s2_total = [r for r in s2_results if r.get("platform") == plat]
        if plat_s2_total:
            s[2] = len(plat_s2) / len(plat_s2_total) * 100
        else:
            s[2] = 0

        plat_s3 = s3_agg.get(plat, {})
        if plat_s3 and plat_s0 > 0:
            conc_overhead = (plat_s3.get("avg", plat_s0) - plat_s0) / max(plat_s0, 1) * 100
            s[3] = max(0, min(100, 100 - conc_overhead * 3))
        else:
            s[3] = 50

        s5 = s5_data.get(plat, {})
        loc = s5.get("loc", 200)
        deps = s5.get("dep_count", 50)
        infra = s5.get("needs_infra", True)
        loc_score = max(0, min(100, 100 - (loc - 10) * 2))
        dep_score = max(0, min(100, 100 - (deps - 2) * 3))
        infra_penalty = 30 if infra else 0
        s[4] = max(0, (loc_score + dep_score) / 2 - infra_penalty)

        scores[plat] = [round(v, 1) for v in s]

    return scores


def generate_markdown(results, s4_data, s5_data, output_dir: Path):
    total = len(results)
    errors = sum(1 for r in results if r.get("error"))
    success = total - errors

    by_scenario = defaultdict(list)
    for r in results:
        by_scenario[r.get("scenario", "unknown")].append(r)

    s0_groups = aggregate(by_scenario.get("s0-normal", []), lambda r: r["platform"])
    s0_agg = {k: stats_from_group(v) for k, v in s0_groups.items()}
    native_s0_avg = s0_agg.get("native", {}).get("avg", 1)

    s1_groups = aggregate(by_scenario.get("s1-recovery", []), lambda r: r["platform"])
    s1_agg = {k: stats_from_group(v) for k, v in s1_groups.items()}

    s2_results_all = by_scenario.get("s2-retry", [])
    s2_groups = aggregate(s2_results_all, lambda r: r["platform"])
    s2_agg = {k: stats_from_group(v) for k, v in s2_groups.items()}

    s3_groups = aggregate(by_scenario.get("s3-concurrent", []), lambda r: r["platform"])
    s3_agg = {k: stats_from_group(v) for k, v in s3_groups.items()}

    lines = []
    lines.append("# durarun v4 横向对比实验报告\n")

    lines.append("## 1. 实验概述\n")
    lines.append("| 项目 | 值 |")
    lines.append("|---|---|")
    lines.append(f"| 实验日期 | {datetime.now(timezone.utc).strftime('%Y-%m-%d')} |")
    lines.append("| 实验环境 | devbox-boe (Linux 5.15, 8 核, 15G, Python 3.11.2) |")
    lines.append(f"| 总运行数 | {total} |")
    lines.append(f"| 成功率 | {success}/{total} ({success/max(total,1)*100:.0f}%) |")
    lines.append("| LLM | 智谱 GLM-5.3-Flash (MAX_TOKENS=256, TEMP=0.7) |")
    lines.append("")

    lines.append("### 被测平台\n")
    lines.append("| 平台 | 部署方式 |")
    lines.append("|---|---|")
    lines.append("| Native | 裸 Python 函数（零框架基线）|")
    lines.append("| durarun | pip install + 本地进程 |")
    lines.append("| LangGraph | pip install + SqliteSaver |")
    lines.append("| Temporal | Python SDK + docker-compose server |")
    lines.append("")

    lines.append("---\n")
    lines.append("## 2. S0 正常执行延迟\n")
    lines.append("| 平台 | Avg (ms) | Min | Max | Stddev | N | vs Native |")
    lines.append("|---|---|---|---|---|---|---|")
    for plat in ALL_PLATFORMS:
        st = s0_agg.get(plat, {})
        if not st or st.get("n", 0) == 0:
            lines.append(f"| {PLATFORM_LABELS.get(plat, plat)} | — | — | — | — | 0 | — |")
            continue
        vs = ""
        if plat == "native":
            vs = "基线"
        elif native_s0_avg > 0:
            pct = (st["avg"] - native_s0_avg) / native_s0_avg * 100
            vs = f"+{pct:.1f}%" if pct >= 0 else f"{pct:.1f}%"
        lines.append(
            f"| {PLATFORM_LABELS.get(plat, plat)} "
            f"| {st['avg']:.1f} | {st['min']:.1f} | {st['max']:.1f} "
            f"| {st['stddev']:.1f} | {st['n']} | {vs} |"
        )
    lines.append("")

    lines.append("---\n")
    lines.append("## 3. S1 崩溃恢复\n")
    lines.append("| 平台 | 恢复机制 | Avg (ms) | vs S0 节省 | 跳步率 |")
    lines.append("|---|---|---|---|---|")
    for plat in ALL_PLATFORMS:
        st = s1_agg.get(plat, {})
        s0_st = s0_agg.get(plat, {})
        mech = RECOVERY_MECHANISMS.get(plat, "—")
        if not st or st.get("n", 0) == 0:
            lines.append(f"| {PLATFORM_LABELS.get(plat, plat)} | {mech} | — | — | — |")
            continue
        saving = ""
        if s0_st and s0_st.get("avg", 0) > 0:
            pct = (1 - st["avg"] / s0_st["avg"]) * 100
            saving = f"{pct:.1f}%"

        s1_runs = s1_groups.get(plat, [])
        total_steps = 0
        skipped_steps = 0
        for r in s1_runs:
            steps = r.get("steps", [])
            total_steps += len(steps)
            skipped_steps += sum(1 for s in steps if s.get("skipped"))
        skip_rate = f"{skipped_steps/max(total_steps,1)*100:.0f}%" if total_steps else "—"

        lines.append(
            f"| {PLATFORM_LABELS.get(plat, plat)} | {mech} "
            f"| {st['avg']:.1f} | {saving} | {skip_rate} |"
        )
    lines.append("")

    lines.append("---\n")
    lines.append("## 4. S2 失败重试\n")
    lines.append("| 平台 | 成功率 | Avg (ms) | vs S0 额外开销 |")
    lines.append("|---|---|---|---|")
    for plat in ALL_PLATFORMS:
        plat_all = [r for r in s2_results_all if r.get("platform") == plat]
        plat_ok = [r for r in plat_all if not r.get("error")]
        st = s2_agg.get(plat, {})
        s0_st = s0_agg.get(plat, {})
        if not plat_all:
            lines.append(f"| {PLATFORM_LABELS.get(plat, plat)} | N/A | — | — |")
            continue
        rate = f"{len(plat_ok)}/{len(plat_all)} ({len(plat_ok)/len(plat_all)*100:.0f}%)"
        overhead = ""
        if st.get("avg") and s0_st.get("avg") and s0_st["avg"] > 0:
            pct = (st["avg"] - s0_st["avg"]) / s0_st["avg"] * 100
            overhead = f"+{pct:.1f}%" if pct >= 0 else f"{pct:.1f}%"
        avg_str = f"{st['avg']:.1f}" if st.get("avg") else "—"
        lines.append(f"| {PLATFORM_LABELS.get(plat, plat)} | {rate} | {avg_str} | {overhead} |")
    lines.append("")

    lines.append("---\n")
    lines.append("## 5. S3 并发 (3x)\n")
    lines.append("| 平台 | Avg Max Elapsed (ms) | vs S0 增量 |")
    lines.append("|---|---|---|")
    for plat in ALL_PLATFORMS:
        st = s3_agg.get(plat, {})
        s0_st = s0_agg.get(plat, {})
        if not st or st.get("n", 0) == 0:
            lines.append(f"| {PLATFORM_LABELS.get(plat, plat)} | — | — |")
            continue
        overhead = ""
        if s0_st and s0_st.get("avg", 0) > 0:
            pct = (st["avg"] - s0_st["avg"]) / s0_st["avg"] * 100
            overhead = f"+{pct:.1f}%" if pct >= 0 else f"{pct:.1f}%"
        lines.append(f"| {PLATFORM_LABELS.get(plat, plat)} | {st['avg']:.1f} | {overhead} |")
    lines.append("")

    lines.append("---\n")
    lines.append("## 6. S4 可观测性成本\n")
    lines.append("| 平台 | OTEL On Avg (ms) | OTEL Off Avg (ms) | 退化 % |")
    lines.append("|---|---|---|---|")
    s4_platforms = set()
    for (plat, mode) in s4_data:
        s4_platforms.add(plat)
    for plat in sorted(s4_platforms):
        on_vals = []
        off_vals = []
        for agent in ALL_AGENTS:
            on_vals.extend([r["elapsed_ms"] for r in s4_data.get((plat, "otel-on"), {}).get(agent, []) if "elapsed_ms" in r])
            off_vals.extend([r["elapsed_ms"] for r in s4_data.get((plat, "otel-off"), {}).get(agent, []) if "elapsed_ms" in r])
        on_avg = statistics.mean(on_vals) if on_vals else 0
        off_avg = statistics.mean(off_vals) if off_vals else 0
        deg = ""
        if off_avg > 0:
            deg_pct = (on_avg - off_avg) / off_avg * 100
            deg = f"+{deg_pct:.2f}%" if deg_pct >= 0 else f"{deg_pct:.2f}%"
        on_str = f"{on_avg:.1f}" if on_vals else "—"
        off_str = f"{off_avg:.1f}" if off_vals else "—"
        lines.append(f"| {PLATFORM_LABELS.get(plat, plat)} | {on_str} | {off_str} | {deg} |")
    if not s4_platforms:
        lines.append("| (无数据) | — | — | — |")
    lines.append("")

    lines.append("---\n")
    lines.append("## 7. S5 冷启动 + 接入成本\n")
    lines.append("| 平台 | 冷启动 (ms) | 接入 LOC | 依赖数 | 需要基础设施 |")
    lines.append("|---|---|---|---|---|")
    for plat in ALL_PLATFORMS:
        s5 = s5_data.get(plat, {})
        if not s5:
            lines.append(f"| {PLATFORM_LABELS.get(plat, plat)} | — | — | — | — |")
            continue
        cs = s5.get("cold_start_ms", "—")
        cs_str = f"{cs:.1f}" if isinstance(cs, (int, float)) else str(cs)
        loc = s5.get("loc", "—")
        deps = s5.get("dep_count", "—")
        infra = "是" if s5.get("needs_infra") else "否"
        lines.append(f"| {PLATFORM_LABELS.get(plat, plat)} | {cs_str} | {loc} | {deps} | {infra} |")
    lines.append("")

    radar_scores = compute_radar_scores(s0_agg, s1_agg, s2_results_all, s3_agg, s5_data)
    lines.append("---\n")
    lines.append("## 8. 五维雷达评分 (0-100)\n")
    header = "| 平台 | " + " | ".join(RADAR_DIMS) + " |"
    sep = "|---|" + "|".join(["---"] * len(RADAR_DIMS)) + "|"
    lines.append(header)
    lines.append(sep)
    for plat in ALL_PLATFORMS:
        sc = radar_scores.get(plat, [0] * 5)
        row = f"| {PLATFORM_LABELS.get(plat, plat)} | " + " | ".join(str(v) for v in sc) + " |"
        lines.append(row)
    lines.append("")

    lines.append("---\n")
    lines.append("## 9. 验收裁决 (durarun)\n")
    lines.append("| ID | 指标 | 阈值 | 实际值 | 结果 |")
    lines.append("|---|---|---|---|---|")

    verdicts = compute_verdicts(s0_agg, s1_agg, s1_groups, s2_results_all, s3_agg, s4_data, s5_data, native_s0_avg)
    for v in verdicts:
        lines.append(f"| {v['id']} | {v['desc']} | {v['target']} | {v['actual']} | **{v['status']}** |")
    lines.append("")

    md = "\n".join(lines)
    (output_dir / "REPORT.md").write_text(md, encoding="utf-8")
    print(f"Markdown report -> {output_dir / 'REPORT.md'}")
    return {
        "s0_agg": s0_agg,
        "s1_agg": s1_agg,
        "s2_agg": s2_agg,
        "s3_agg": s3_agg,
        "radar_scores": radar_scores,
        "verdicts": verdicts,
        "total": total,
        "errors": errors,
    }


def compute_verdicts(s0_agg, s1_agg, s1_groups, s2_results_all, s3_agg, s4_data, s5_data, native_s0_avg):
    verdicts = []

    dr_s0 = s0_agg.get("durarun", {}).get("avg", 0)
    if native_s0_avg > 0 and dr_s0 > 0:
        b1_pct = (dr_s0 - native_s0_avg) / native_s0_avg * 100
        verdicts.append({"id": "B-1", "desc": "S0 overhead vs Native", "target": "≤ +10%",
                         "actual": f"+{b1_pct:.1f}%", "status": "PASS" if b1_pct <= 10 else "FAIL"})
    else:
        verdicts.append({"id": "B-1", "desc": "S0 overhead vs Native", "target": "≤ +10%",
                         "actual": "N/A", "status": "N/A"})

    dr_s1 = s1_agg.get("durarun", {}).get("avg", 0)
    if dr_s0 > 0 and dr_s1 > 0:
        b2_ratio = dr_s1 / dr_s0 * 100
        verdicts.append({"id": "B-2", "desc": "S1 恢复耗时 vs S0 全量", "target": "≤ 50%",
                         "actual": f"{b2_ratio:.1f}%", "status": "PASS" if b2_ratio <= 50 else "FAIL"})
    else:
        verdicts.append({"id": "B-2", "desc": "S1 恢复耗时 vs S0 全量", "target": "≤ 50%",
                         "actual": "N/A", "status": "N/A"})

    dr_s1_runs = s1_groups.get("durarun", [])
    total_steps = sum(len(r.get("steps", [])) for r in dr_s1_runs)
    skipped = sum(sum(1 for s in r.get("steps", []) if s.get("skipped")) for r in dr_s1_runs)
    if total_steps > 0:
        b3_rate = skipped / total_steps * 100
        verdicts.append({"id": "B-3", "desc": "S1 跳步率", "target": "≥ 60%",
                         "actual": f"{b3_rate:.0f}%", "status": "PASS" if b3_rate >= 60 else "FAIL"})
    else:
        verdicts.append({"id": "B-3", "desc": "S1 跳步率", "target": "≥ 60%",
                         "actual": "N/A", "status": "N/A"})

    dr_s2_all = [r for r in s2_results_all if r.get("platform") == "durarun"]
    dr_s2_ok = [r for r in dr_s2_all if not r.get("error")]
    if dr_s2_all:
        rate = len(dr_s2_ok) / len(dr_s2_all) * 100
        verdicts.append({"id": "B-4", "desc": "S2 自动重试成功率", "target": "100%",
                         "actual": f"{rate:.0f}%", "status": "PASS" if rate == 100 else "FAIL"})
    else:
        verdicts.append({"id": "B-4", "desc": "S2 自动重试成功率", "target": "100%",
                         "actual": "N/A", "status": "N/A"})

    dr_s3 = s3_agg.get("durarun", {}).get("avg", 0)
    if dr_s0 > 0 and dr_s3 > 0:
        b5_pct = (dr_s3 - dr_s0) / dr_s0 * 100
        verdicts.append({"id": "B-5", "desc": "S3 并发 overhead vs S0", "target": "≤ +15%",
                         "actual": f"+{b5_pct:.1f}%", "status": "PASS" if b5_pct <= 15 else "FAIL"})
    else:
        verdicts.append({"id": "B-5", "desc": "S3 并发 overhead vs S0", "target": "≤ +15%",
                         "actual": "N/A", "status": "N/A"})

    dr_on_vals = []
    dr_off_vals = []
    for agent in ALL_AGENTS:
        dr_on_vals.extend([r["elapsed_ms"] for r in s4_data.get(("durarun", "otel-on"), {}).get(agent, []) if "elapsed_ms" in r])
        dr_off_vals.extend([r["elapsed_ms"] for r in s4_data.get(("durarun", "otel-off"), {}).get(agent, []) if "elapsed_ms" in r])
    if dr_on_vals and dr_off_vals:
        on_avg = statistics.mean(dr_on_vals)
        off_avg = statistics.mean(dr_off_vals)
        if off_avg > 0:
            b6_pct = (on_avg - off_avg) / off_avg * 100
            verdicts.append({"id": "B-6", "desc": "S4 OTEL 性能退化", "target": "≤ 2%",
                             "actual": f"+{b6_pct:.2f}%", "status": "PASS" if b6_pct <= 2 else "FAIL"})
        else:
            verdicts.append({"id": "B-6", "desc": "S4 OTEL 性能退化", "target": "≤ 2%",
                             "actual": "N/A", "status": "N/A"})
    else:
        verdicts.append({"id": "B-6", "desc": "S4 OTEL 性能退化", "target": "≤ 2%",
                         "actual": "N/A", "status": "N/A"})

    dr_s5 = s5_data.get("durarun", {})
    loc = dr_s5.get("loc", None)
    if loc is not None:
        verdicts.append({"id": "B-7", "desc": "S5 接入 LOC", "target": "≤ 20",
                         "actual": str(loc), "status": "PASS" if loc <= 20 else "FAIL"})
    else:
        verdicts.append({"id": "B-7", "desc": "S5 接入 LOC", "target": "≤ 20",
                         "actual": "N/A", "status": "N/A"})

    deps = dr_s5.get("dep_count", None)
    if deps is not None:
        verdicts.append({"id": "B-8", "desc": "S5 依赖包数", "target": "≤ 10",
                         "actual": str(deps), "status": "PASS" if deps <= 10 else "FAIL"})
    else:
        verdicts.append({"id": "B-8", "desc": "S5 依赖包数", "target": "≤ 10",
                         "actual": "N/A", "status": "N/A"})

    return verdicts


def generate_html(report_data, output_dir: Path):
    s0 = report_data["s0_agg"]
    radar = report_data["radar_scores"]
    verdicts = report_data["verdicts"]

    bar_svg = _bar_chart_svg(s0, "S0 正常执行延迟 (ms)")
    radar_svg = _radar_chart_svg(radar)

    verdict_rows = ""
    for v in verdicts:
        color = "#22c55e" if v["status"] == "PASS" else ("#ef4444" if v["status"] == "FAIL" else "#9ca3af")
        verdict_rows += f'<tr><td>{v["id"]}</td><td>{v["desc"]}</td><td>{v["target"]}</td><td>{v["actual"]}</td><td style="color:{color};font-weight:700">{v["status"]}</td></tr>\n'

    s0_rows = ""
    native_avg = s0.get("native", {}).get("avg", 1)
    for plat in ALL_PLATFORMS:
        st = s0.get(plat, {})
        if not st or st.get("n", 0) == 0:
            continue
        vs = "基线" if plat == "native" else (f'+{(st["avg"]-native_avg)/max(native_avg,1)*100:.1f}%')
        c = PLATFORM_COLORS.get(plat, "#666")
        s0_rows += f'<tr><td style="border-left:4px solid {c}">{PLATFORM_LABELS.get(plat,plat)}</td><td>{st["avg"]:.1f}</td><td>{st["min"]:.1f}</td><td>{st["max"]:.1f}</td><td>{st["stddev"]:.1f}</td><td>{st["n"]}</td><td>{vs}</td></tr>\n'

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<title>durarun v4 Benchmark Report</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0f0f13;color:#e4e4e7;padding:2rem;max-width:1200px;margin:0 auto}}
h1{{font-size:1.8rem;color:#a78bfa;margin-bottom:.5rem}}
h2{{font-size:1.3rem;color:#c4b5fd;margin:2rem 0 1rem;border-bottom:1px solid #27272a;padding-bottom:.5rem}}
.subtitle{{color:#71717a;margin-bottom:2rem}}
.grid{{display:grid;grid-template-columns:1fr 1fr;gap:2rem;margin:2rem 0}}
.card{{background:#18181b;border:1px solid #27272a;border-radius:12px;padding:1.5rem}}
.card h3{{font-size:1rem;color:#a1a1aa;margin-bottom:1rem}}
table{{width:100%;border-collapse:collapse;font-size:.85rem}}
th{{text-align:left;padding:.5rem .75rem;color:#71717a;border-bottom:1px solid #27272a;font-weight:500}}
td{{padding:.5rem .75rem;border-bottom:1px solid #1e1e22}}
tr:hover td{{background:#1e1e24}}
.verdict-table td:last-child{{font-weight:700}}
.pass{{color:#22c55e}} .fail{{color:#ef4444}} .na{{color:#9ca3af}}
svg text{{font-family:inherit}}
.legend{{display:flex;gap:1.5rem;flex-wrap:wrap;margin:.75rem 0}}
.legend-item{{display:flex;align-items:center;gap:.4rem;font-size:.8rem;color:#a1a1aa}}
.legend-dot{{width:10px;height:10px;border-radius:50%;display:inline-block}}
</style>
</head>
<body>
<h1>durarun v4 横向对比实验报告</h1>
<p class="subtitle">Generated {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} · {report_data['total']} runs · {report_data['errors']} errors</p>

<div class="legend">
  <span class="legend-item"><span class="legend-dot" style="background:#9ca3af"></span>Native</span>
  <span class="legend-item"><span class="legend-dot" style="background:#8b5cf6"></span>durarun</span>
  <span class="legend-item"><span class="legend-dot" style="background:#22c55e"></span>LangGraph</span>
  <span class="legend-item"><span class="legend-dot" style="background:#3b82f6"></span>Temporal</span>
</div>

<div class="grid">
<div class="card">
<h3>S0 正常执行延迟</h3>
{bar_svg}
</div>
<div class="card">
<h3>五维雷达评分</h3>
{radar_svg}
</div>
</div>

<h2>S0 详细数据</h2>
<table>
<tr><th>平台</th><th>Avg (ms)</th><th>Min</th><th>Max</th><th>Stddev</th><th>N</th><th>vs Native</th></tr>
{s0_rows}
</table>

<h2>验收裁决 (durarun)</h2>
<table class="verdict-table">
<tr><th>ID</th><th>指标</th><th>阈值</th><th>实际值</th><th>结果</th></tr>
{verdict_rows}
</table>

</body>
</html>"""
    (output_dir / "report.html").write_text(html, encoding="utf-8")
    print(f"HTML report -> {output_dir / 'report.html'}")


def _bar_chart_svg(s0_agg, title):
    w, h = 440, 220
    pad_l, pad_b, pad_t = 60, 30, 10
    chart_w = w - pad_l - 20
    chart_h = h - pad_b - pad_t

    vals = []
    for plat in ALL_PLATFORMS:
        avg = s0_agg.get(plat, {}).get("avg", 0)
        vals.append((plat, avg))

    max_val = max((v for _, v in vals), default=1) * 1.15
    if max_val == 0:
        max_val = 1

    bars = ""
    bar_w = chart_w / len(vals) * 0.6
    gap = chart_w / len(vals)

    for i, (plat, avg) in enumerate(vals):
        bh = avg / max_val * chart_h
        x = pad_l + i * gap + (gap - bar_w) / 2
        y = pad_t + chart_h - bh
        color = PLATFORM_COLORS.get(plat, "#666")
        label = PLATFORM_LABELS.get(plat, plat)
        bars += f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{bh:.1f}" fill="{color}" rx="4"/>\n'
        bars += f'<text x="{x + bar_w/2:.1f}" y="{y - 6:.1f}" text-anchor="middle" fill="#a1a1aa" font-size="11">{avg:.0f}</text>\n'
        bars += f'<text x="{x + bar_w/2:.1f}" y="{pad_t + chart_h + 18:.1f}" text-anchor="middle" fill="#71717a" font-size="10">{label}</text>\n'

    grid = ""
    for i in range(5):
        yy = pad_t + chart_h - i * chart_h / 4
        v = max_val * i / 4
        grid += f'<line x1="{pad_l}" y1="{yy:.1f}" x2="{w-20}" y2="{yy:.1f}" stroke="#27272a" stroke-width="1"/>\n'
        grid += f'<text x="{pad_l-8}" y="{yy+4:.1f}" text-anchor="end" fill="#52525b" font-size="10">{v:.0f}</text>\n'

    return f'<svg viewBox="0 0 {w} {h}" xmlns="http://www.w3.org/2000/svg">{grid}{bars}</svg>'


def _radar_chart_svg(radar_scores):
    w, h = 440, 360
    cx, cy = w / 2, h / 2 - 10
    r = 130
    n = len(RADAR_DIMS)
    angles = [i * 2 * math.pi / n - math.pi / 2 for i in range(n)]

    grid_lines = ""
    for level in [0.25, 0.5, 0.75, 1.0]:
        pts = " ".join(f"{cx + r*level*math.cos(a):.1f},{cy + r*level*math.sin(a):.1f}" for a in angles)
        grid_lines += f'<polygon points="{pts}" fill="none" stroke="#27272a" stroke-width="1"/>\n'

    axis_lines = ""
    labels = ""
    for i, dim in enumerate(RADAR_DIMS):
        ex = cx + r * 1.18 * math.cos(angles[i])
        ey = cy + r * 1.18 * math.sin(angles[i])
        axis_lines += f'<line x1="{cx}" y1="{cy}" x2="{cx+r*math.cos(angles[i]):.1f}" y2="{cy+r*math.sin(angles[i]):.1f}" stroke="#27272a" stroke-width="1"/>\n'
        anchor = "middle"
        if math.cos(angles[i]) > 0.3:
            anchor = "start"
        elif math.cos(angles[i]) < -0.3:
            anchor = "end"
        labels += f'<text x="{ex:.1f}" y="{ey:.1f}" text-anchor="{anchor}" fill="#a1a1aa" font-size="11" dominant-baseline="central">{dim}</text>\n'

    polys = ""
    for plat in ALL_PLATFORMS:
        sc = radar_scores.get(plat, [0] * n)
        color = PLATFORM_COLORS.get(plat, "#666")
        pts = " ".join(
            f"{cx + r*(sc[i]/100)*math.cos(angles[i]):.1f},{cy + r*(sc[i]/100)*math.sin(angles[i]):.1f}"
            for i in range(n)
        )
        polys += f'<polygon points="{pts}" fill="{color}" fill-opacity="0.15" stroke="{color}" stroke-width="2"/>\n'
        for i in range(n):
            px = cx + r * (sc[i] / 100) * math.cos(angles[i])
            py = cy + r * (sc[i] / 100) * math.sin(angles[i])
            polys += f'<circle cx="{px:.1f}" cy="{py:.1f}" r="3" fill="{color}"/>\n'

    legend_y = h - 20
    legend = ""
    lx = 40
    for plat in ALL_PLATFORMS:
        color = PLATFORM_COLORS.get(plat, "#666")
        label = PLATFORM_LABELS.get(plat, plat)
        legend += f'<rect x="{lx}" y="{legend_y-6}" width="10" height="10" rx="2" fill="{color}"/>'
        legend += f'<text x="{lx+14}" y="{legend_y+3}" fill="#a1a1aa" font-size="10">{label}</text>'
        lx += len(label) * 7 + 30

    return f'<svg viewBox="0 0 {w} {h}" xmlns="http://www.w3.org/2000/svg">{grid_lines}{axis_lines}{labels}{polys}{legend}</svg>'


def main():
    parser = argparse.ArgumentParser(description="durarun v4 Benchmark Report Generator")
    parser.add_argument("--results-dir", default="/data00/agentfabric/benchmarks/v4/results")
    parser.add_argument("--output-dir", default="/data00/agentfabric/benchmarks/v4")
    args = parser.parse_args()

    results_dir = Path(args.results_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading results from {results_dir} ...")
    results = load_results(results_dir)
    s4_data = load_s4_results(results_dir)
    s5_data = load_s5_results(results_dir)
    print(f"Loaded {len(results)} trial results (S0-S3), S4: {sum(len(v) for vv in s4_data.values() for v in vv.values())} trials, S5: {len(s5_data)} platforms")

    if not results:
        print("ERROR: No results found. Aborting.", file=sys.stderr)
        sys.exit(1)

    report_data = generate_markdown(results, s4_data, s5_data, output_dir)
    generate_html(report_data, output_dir)

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_results": len(results),
        "errors": report_data["errors"],
        "s0_aggregates": {k: v for k, v in report_data["s0_agg"].items()},
        "verdicts": report_data["verdicts"],
        "radar_scores": report_data["radar_scores"],
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print(f"Summary JSON -> {output_dir / 'summary.json'}")

    passed = sum(1 for v in report_data["verdicts"] if v["status"] == "PASS")
    failed = sum(1 for v in report_data["verdicts"] if v["status"] == "FAIL")
    na = sum(1 for v in report_data["verdicts"] if v["status"] == "N/A")
    print(f"\nAcceptance: {passed} PASS, {failed} FAIL, {na} N/A")


if __name__ == "__main__":
    main()
