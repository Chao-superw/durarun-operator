#!/usr/bin/env python3
"""
AF v3 Benchmark Report Generator
Reads trial JSON results, computes aggregates, and generates a self-contained HTML report.
Usage: python3 generate_report.py  (from the v3/ directory)
"""

import json, math, os, statistics, sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
# Config
# ═══════════════════════════════════════════════════════════════

SCRIPT_DIR = Path(__file__).resolve().parent
RESULTS_DIR = SCRIPT_DIR / "results"
SUMMARY_PATH = SCRIPT_DIR / "summary.json"
REPORT_PATH = SCRIPT_DIR / "report.html"

ALL_PLATFORMS = ["native", "afv3", "temporal", "langgraph"]
ALL_SCENARIOS = ["s0-normal", "s1-recovery", "s3-concurrent"]
ALL_AGENTS = ["swe-agent", "gpt-researcher", "deepseek-harness"]

PLATFORM_LABELS = {
    "native": "Native",
    "afv3": "AF v3",
    "temporal": "Temporal",
    "langgraph": "LangGraph",
}
PLATFORM_COLORS = {
    "native": "#4ade80",
    "afv3": "#a78bfa",
    "temporal": "#60a5fa",
    "langgraph": "#f97316",
}
SCENARIO_LABELS = {
    "s0-normal": "S0 Normal Execution",
    "s1-recovery": "S1 Recovery",
    "s3-concurrent": "S3 Concurrent 3x",
}

# Fixed qualitative scores (0-100)
FIXED_SCORES = {
    "ease_of_adoption": {"native": 95, "afv3": 80, "temporal": 55, "langgraph": 70},
    "observability":    {"native": 20, "afv3": 90, "temporal": 85, "langgraph": 60},
}

MODEL = "glm-5.3-flash"

# ═══════════════════════════════════════════════════════════════
# Data Loading
# ═══════════════════════════════════════════════════════════════

def load_results() -> list[dict]:
    """Recursively load all trial JSON files from results/ directory."""
    results = []
    if not RESULTS_DIR.exists():
        print(f"[WARN] Results directory not found: {RESULTS_DIR}", file=sys.stderr)
        return results
    for json_path in sorted(RESULTS_DIR.rglob("trial*.json")):
        try:
            data = json.loads(json_path.read_text())
            results.append(data)
        except (json.JSONDecodeError, OSError) as e:
            print(f"[WARN] Failed to read {json_path}: {e}", file=sys.stderr)
    return results


# ═══════════════════════════════════════════════════════════════
# Statistics Helpers
# ═══════════════════════════════════════════════════════════════

def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = (len(s) - 1) * pct / 100.0
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return s[int(k)]
    return s[f] * (c - k) + s[c] * (k - f)

def safe_stdev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    return statistics.stdev(values)

def safe_mean(values: list[float]) -> float:
    return statistics.mean(values) if values else 0.0


# ═══════════════════════════════════════════════════════════════
# Aggregation
# ═══════════════════════════════════════════════════════════════

def compute_aggregates(results: list[dict]) -> dict:
    """Compute all aggregates from raw result records."""

    # Filter valid (no error) results
    valid = [r for r in results if not r.get("error")]
    errored = [r for r in results if r.get("error")]

    # ---- Per (agent, platform, scenario) aggregation ----
    per_aps = defaultdict(list)  # (agent, platform, scenario) -> [elapsed_ms]
    for r in valid:
        key = (r["agent"], r["platform"], r["scenario"])
        if "elapsed_ms" in r:
            per_aps[key].append(r["elapsed_ms"])

    aps_agg = {}
    for (agent, plat, scen), vals in sorted(per_aps.items()):
        aps_agg[f"{agent}/{plat}/{scen}"] = {
            "agent": agent, "platform": plat, "scenario": scen,
            "avg_ms": round(safe_mean(vals), 2),
            "min_ms": round(min(vals), 2),
            "max_ms": round(max(vals), 2),
            "p95_ms": round(percentile(vals, 95), 2),
            "stddev_ms": round(safe_stdev(vals), 2),
            "n": len(vals),
        }

    # ---- Cross-agent average per (platform, scenario) ----
    per_ps = defaultdict(list)
    for r in valid:
        if "elapsed_ms" in r:
            per_ps[(r["platform"], r["scenario"])].append(r["elapsed_ms"])

    cross_agent = {}
    for (plat, scen), vals in sorted(per_ps.items()):
        cross_agent[f"{plat}/{scen}"] = {
            "platform": plat, "scenario": scen,
            "avg_ms": round(safe_mean(vals), 2),
            "min_ms": round(min(vals), 2),
            "max_ms": round(max(vals), 2),
            "p95_ms": round(percentile(vals, 95), 2),
            "stddev_ms": round(safe_stdev(vals), 2),
            "n": len(vals),
        }

    # ---- vs Native overhead ----
    vs_native = {}
    for scen in ALL_SCENARIOS:
        native_key = f"native/{scen}"
        if native_key in cross_agent:
            native_avg = cross_agent[native_key]["avg_ms"]
            for plat in ALL_PLATFORMS:
                k = f"{plat}/{scen}"
                if k in cross_agent and native_avg > 0:
                    overhead_pct = (cross_agent[k]["avg_ms"] - native_avg) / native_avg * 100
                    vs_native[k] = round(overhead_pct, 2)
                    cross_agent[k]["vs_native_pct"] = round(overhead_pct, 2)

    # ---- Per-step timing breakdown per (agent, platform) ----
    step_timings = defaultdict(lambda: defaultdict(list))  # (agent, plat) -> step_name -> [ms]
    for r in valid:
        if r.get("scenario") != "s0-normal":
            continue
        for s in r.get("steps", []):
            if not s.get("skipped", False):
                step_timings[(r["agent"], r["platform"])][s["step"]].append(s["elapsed_ms"])

    step_breakdown = {}
    for (agent, plat), steps_map in sorted(step_timings.items()):
        breakdown = {}
        for step_name, vals in steps_map.items():
            breakdown[step_name] = round(safe_mean(vals), 2)
        step_breakdown[f"{agent}/{plat}"] = breakdown

    # ---- Recovery (S1) analysis ----
    recovery_data = {}
    for r in valid:
        if r.get("scenario") != "s1-recovery":
            continue
        plat = r["platform"]
        agent = r["agent"]
        key = f"{agent}/{plat}"
        if key not in recovery_data:
            recovery_data[key] = {"elapsed_list": [], "skipped_list": [], "platform": plat, "agent": agent}
        recovery_data[key]["elapsed_list"].append(r.get("elapsed_ms", 0))
        skipped = r.get("steps_skipped", sum(1 for s in r.get("steps", []) if s.get("skipped", False)))
        recovery_data[key]["skipped_list"].append(skipped)

    recovery_summary = {}
    for key, rd in recovery_data.items():
        native_s1_key = f"{rd['agent']}/native"
        native_avg = safe_mean(recovery_data.get(native_s1_key, {}).get("elapsed_list", [0]))
        avg_elapsed = safe_mean(rd["elapsed_list"])
        avg_skipped = safe_mean(rd["skipped_list"]) if rd["skipped_list"] else 0
        time_saved = native_avg - avg_elapsed if native_avg > 0 else 0
        recovery_summary[key] = {
            "avg_elapsed_ms": round(avg_elapsed, 2),
            "avg_steps_skipped": round(avg_skipped, 1),
            "time_saved_ms": round(time_saved, 2),
            "time_saved_pct": round(time_saved / native_avg * 100, 1) if native_avg > 0 else 0,
        }

    # ---- Concurrent (S3) overhead analysis ----
    concurrent_data = {}
    for plat in ALL_PLATFORMS:
        s0_key = f"{plat}/s0-normal"
        s3_key = f"{plat}/s3-concurrent"
        if s0_key in cross_agent and s3_key in cross_agent:
            s0_avg = cross_agent[s0_key]["avg_ms"]
            s3_avg = cross_agent[s3_key]["avg_ms"]
            overhead = (s3_avg - s0_avg) / s0_avg * 100 if s0_avg > 0 else 0
            concurrent_data[plat] = {
                "s0_avg_ms": round(s0_avg, 2),
                "s3_avg_ms": round(s3_avg, 2),
                "s3_max_ms": round(cross_agent[s3_key]["max_ms"], 2),
                "overhead_pct": round(overhead, 2),
            }

    # ---- Radar scores ----
    radar_scores = {}
    for plat in ALL_PLATFORMS:
        # Exec Overhead: 100 - overhead% * 5 (clamped 0-100)
        s0_overhead = vs_native.get(f"{plat}/s0-normal", 0)
        exec_score = max(0, min(100, round(100 - abs(s0_overhead) * 5)))

        # Recovery: based on S1 time saved
        s1_key = f"{plat}/s1-recovery"
        if s1_key in cross_agent:
            native_s1_avg = cross_agent.get("native/s1-recovery", {}).get("avg_ms", 1)
            plat_s1_avg = cross_agent[s1_key]["avg_ms"]
            if native_s1_avg > 0:
                recovery_ratio = 1 - (plat_s1_avg / native_s1_avg)
                recovery_score = max(0, min(100, round(recovery_ratio * 100)))
            else:
                recovery_score = 0
        else:
            recovery_score = 0
        # Native has no recovery capability
        if plat == "native":
            recovery_score = 5

        # Concurrency: based on S3 overhead vs S0
        conc_overhead = concurrent_data.get(plat, {}).get("overhead_pct", 0)
        conc_score = max(0, min(100, round(100 - abs(conc_overhead) * 2)))

        # Fixed qualitative scores
        ease_score = FIXED_SCORES["ease_of_adoption"].get(plat, 50)
        obs_score = FIXED_SCORES["observability"].get(plat, 50)

        radar_scores[plat] = {
            "exec_overhead": exec_score,
            "recovery": recovery_score,
            "concurrency": conc_score,
            "ease_of_adoption": ease_score,
            "observability": obs_score,
        }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "model": MODEL,
        "total_results": len(results),
        "valid_results": len(valid),
        "errors": len(errored),
        "error_details": [{"agent": r.get("agent"), "platform": r.get("platform"),
                           "scenario": r.get("scenario"), "error": r.get("error")} for r in errored],
        "per_agent_platform_scenario": aps_agg,
        "cross_agent": cross_agent,
        "vs_native": vs_native,
        "step_breakdown": step_breakdown,
        "recovery_summary": recovery_summary,
        "concurrent_overhead": concurrent_data,
        "radar_scores": radar_scores,
    }


# ═══════════════════════════════════════════════════════════════
# HTML Report Generation
# ═══════════════════════════════════════════════════════════════

def _svg_bar_chart(title: str, bars: list[dict], width=700, height=320, show_labels=True) -> str:
    """
    Generate an SVG bar chart.
    bars: [{"label": str, "value": float, "color": str, "annotation": str|None}]
    """
    if not bars:
        return f'<p style="color:#888">No data for {title}</p>'

    margin_left = 80
    margin_right = 30
    margin_top = 50
    margin_bottom = 70
    chart_w = width - margin_left - margin_right
    chart_h = height - margin_top - margin_bottom

    max_val = max(b["value"] for b in bars) * 1.15
    if max_val == 0:
        max_val = 1

    n = len(bars)
    bar_gap = 8
    bar_w = max(20, min(60, (chart_w - bar_gap * (n + 1)) / n))
    total_bars_w = n * bar_w + (n + 1) * bar_gap
    offset_x = margin_left + (chart_w - total_bars_w) / 2

    lines = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
             f'style="background:transparent;font-family:\'SF Mono\',Consolas,monospace">']
    # Title
    lines.append(f'<text x="{width/2}" y="28" text-anchor="middle" fill="#e2e8f0" font-size="14" font-weight="600">{title}</text>')

    # Y-axis gridlines
    num_gridlines = 5
    for i in range(num_gridlines + 1):
        y = margin_top + chart_h - (i / num_gridlines) * chart_h
        val = (i / num_gridlines) * max_val
        lines.append(f'<line x1="{margin_left}" y1="{y}" x2="{margin_left + chart_w}" y2="{y}" stroke="#334155" stroke-width="0.5"/>')
        lines.append(f'<text x="{margin_left - 8}" y="{y + 4}" text-anchor="end" fill="#94a3b8" font-size="10">{val:.0f}</text>')

    # Bars
    for i, b in enumerate(bars):
        x = offset_x + bar_gap + i * (bar_w + bar_gap)
        bar_h = (b["value"] / max_val) * chart_h if max_val > 0 else 0
        y = margin_top + chart_h - bar_h

        lines.append(f'<rect x="{x}" y="{y}" width="{bar_w}" height="{bar_h}" fill="{b["color"]}" rx="3" opacity="0.9"/>')
        # Value on top
        lines.append(f'<text x="{x + bar_w/2}" y="{y - 6}" text-anchor="middle" fill="#e2e8f0" font-size="10" font-weight="500">{b["value"]:.1f}</text>')
        # Label below
        if show_labels:
            label = b.get("label", "")
            lines.append(f'<text x="{x + bar_w/2}" y="{margin_top + chart_h + 18}" text-anchor="middle" fill="#94a3b8" font-size="10">{label}</text>')
        # Annotation
        if b.get("annotation"):
            lines.append(f'<text x="{x + bar_w/2}" y="{margin_top + chart_h + 34}" text-anchor="middle" fill="#64748b" font-size="9">{b["annotation"]}</text>')

    # Y-axis label
    lines.append(f'<text x="14" y="{margin_top + chart_h/2}" text-anchor="middle" fill="#94a3b8" font-size="10" transform="rotate(-90, 14, {margin_top + chart_h/2})">ms</text>')

    lines.append('</svg>')
    return '\n'.join(lines)


def _svg_grouped_bar_chart(title: str, groups: list[dict], width=800, height=360) -> str:
    """
    Grouped bar chart.
    groups: [{"label": str, "bars": [{"sub_label": str, "value": float, "color": str}]}]
    """
    if not groups:
        return f'<p style="color:#888">No data for {title}</p>'

    margin_left = 80
    margin_right = 30
    margin_top = 50
    margin_bottom = 90
    chart_w = width - margin_left - margin_right
    chart_h = height - margin_top - margin_bottom

    all_vals = [b["value"] for g in groups for b in g["bars"]]
    max_val = max(all_vals) * 1.15 if all_vals else 1

    n_groups = len(groups)
    n_bars = max(len(g["bars"]) for g in groups) if groups else 1
    group_gap = 20
    bar_gap = 3
    group_w = max(40, (chart_w - group_gap * (n_groups + 1)) / n_groups)
    bar_w = max(12, (group_w - bar_gap * (n_bars + 1)) / n_bars)
    total_w = n_groups * group_w + (n_groups + 1) * group_gap
    offset_x = margin_left + (chart_w - total_w) / 2

    lines = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
             f'style="background:transparent;font-family:\'SF Mono\',Consolas,monospace">']
    lines.append(f'<text x="{width/2}" y="28" text-anchor="middle" fill="#e2e8f0" font-size="14" font-weight="600">{title}</text>')

    # Gridlines
    for i in range(6):
        y = margin_top + chart_h - (i / 5) * chart_h
        val = (i / 5) * max_val
        lines.append(f'<line x1="{margin_left}" y1="{y}" x2="{margin_left + chart_w}" y2="{y}" stroke="#334155" stroke-width="0.5"/>')
        lines.append(f'<text x="{margin_left - 8}" y="{y + 4}" text-anchor="end" fill="#94a3b8" font-size="10">{val:.0f}</text>')

    for gi, g in enumerate(groups):
        gx = offset_x + group_gap + gi * (group_w + group_gap)
        for bi, b in enumerate(g["bars"]):
            x = gx + bar_gap + bi * (bar_w + bar_gap)
            bar_h = (b["value"] / max_val) * chart_h if max_val > 0 else 0
            y = margin_top + chart_h - bar_h
            lines.append(f'<rect x="{x}" y="{y}" width="{bar_w}" height="{bar_h}" fill="{b["color"]}" rx="2" opacity="0.85"/>')
            lines.append(f'<text x="{x + bar_w/2}" y="{y - 4}" text-anchor="middle" fill="#e2e8f0" font-size="8">{b["value"]:.0f}</text>')
        # Group label
        lines.append(f'<text x="{gx + group_w/2}" y="{margin_top + chart_h + 18}" text-anchor="middle" fill="#94a3b8" font-size="10">{g["label"]}</text>')

    # Legend
    legend_y = height - 20
    seen_labels = []
    for g in groups:
        for b in g["bars"]:
            if b["sub_label"] not in [s[0] for s in seen_labels]:
                seen_labels.append((b["sub_label"], b["color"]))
    lx = margin_left
    for label, color in seen_labels:
        lines.append(f'<rect x="{lx}" y="{legend_y - 8}" width="10" height="10" fill="{color}" rx="2"/>')
        lines.append(f'<text x="{lx + 14}" y="{legend_y}" fill="#94a3b8" font-size="10">{label}</text>')
        lx += len(label) * 7 + 30

    lines.append(f'<text x="14" y="{margin_top + chart_h/2}" text-anchor="middle" fill="#94a3b8" font-size="10" transform="rotate(-90, 14, {margin_top + chart_h/2})">ms</text>')
    lines.append('</svg>')
    return '\n'.join(lines)


def _svg_radar_chart(title: str, platforms_scores: dict, dimensions: list[str], dim_labels: list[str],
                     width=520, height=520) -> str:
    """
    Radar/spider chart using SVG polygons.
    platforms_scores: {platform: {dim: score_0_100}}
    """
    cx, cy = width / 2, height / 2 + 10
    radius = min(width, height) / 2 - 70
    n = len(dimensions)
    if n == 0:
        return ''

    def polar(angle_deg, r):
        a = math.radians(angle_deg - 90)  # Start from top
        return cx + r * math.cos(a), cy + r * math.sin(a)

    angles = [i * 360 / n for i in range(n)]

    lines = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
             f'style="background:transparent;font-family:\'SF Mono\',Consolas,monospace">']
    lines.append(f'<text x="{width/2}" y="28" text-anchor="middle" fill="#e2e8f0" font-size="14" font-weight="600">{title}</text>')

    # Grid rings
    for ring in [20, 40, 60, 80, 100]:
        r = radius * ring / 100
        points = ' '.join(f'{polar(a, r)[0]},{polar(a, r)[1]}' for a in angles)
        lines.append(f'<polygon points="{points}" fill="none" stroke="#334155" stroke-width="0.5"/>')
        # Ring label
        px, py = polar(0, r)
        lines.append(f'<text x="{px + 4}" y="{py - 2}" fill="#475569" font-size="8">{ring}</text>')

    # Axis lines
    for a in angles:
        px, py = polar(a, radius)
        lines.append(f'<line x1="{cx}" y1="{cy}" x2="{px}" y2="{py}" stroke="#334155" stroke-width="0.5"/>')

    # Dimension labels
    for i, a in enumerate(angles):
        px, py = polar(a, radius + 28)
        anchor = "middle"
        if a > 10 and a < 170:
            anchor = "start"
        elif a > 190 and a < 350:
            anchor = "end"
        lines.append(f'<text x="{px}" y="{py + 4}" text-anchor="{anchor}" fill="#cbd5e1" font-size="11">{dim_labels[i]}</text>')

    # Platform polygons
    for plat, scores in platforms_scores.items():
        color = PLATFORM_COLORS.get(plat, "#888")
        label = PLATFORM_LABELS.get(plat, plat)
        pts = []
        for i, dim in enumerate(dimensions):
            v = scores.get(dim, 0)
            r = radius * v / 100
            pts.append(polar(angles[i], r))
        points_str = ' '.join(f'{x},{y}' for x, y in pts)
        lines.append(f'<polygon points="{points_str}" fill="{color}" fill-opacity="0.12" stroke="{color}" stroke-width="2"/>')
        # Dots
        for x, y in pts:
            lines.append(f'<circle cx="{x}" cy="{y}" r="3" fill="{color}"/>')

    # Legend
    ly = height - 30
    lx = 40
    for plat in platforms_scores:
        color = PLATFORM_COLORS.get(plat, "#888")
        label = PLATFORM_LABELS.get(plat, plat)
        lines.append(f'<rect x="{lx}" y="{ly - 8}" width="12" height="12" fill="{color}" rx="2"/>')
        lines.append(f'<text x="{lx + 16}" y="{ly + 2}" fill="#94a3b8" font-size="11">{label}</text>')
        lx += len(label) * 8 + 36

    lines.append('</svg>')
    return '\n'.join(lines)


def generate_html(agg: dict) -> str:
    """Generate the full self-contained HTML report."""

    cross = agg["cross_agent"]
    vs_native = agg["vs_native"]
    radar = agg["radar_scores"]
    recovery = agg["recovery_summary"]
    concurrent = agg["concurrent_overhead"]
    step_bd = agg["step_breakdown"]
    per_aps = agg["per_agent_platform_scenario"]

    # ---- Chart 1: S0 Normal Execution - cross-agent avg per platform ----
    s0_bars = []
    for plat in ALL_PLATFORMS:
        k = f"{plat}/s0-normal"
        if k in cross:
            overhead = vs_native.get(k, 0)
            ann = "baseline" if plat == "native" else f'{overhead:+.1f}%'
            s0_bars.append({
                "label": PLATFORM_LABELS[plat],
                "value": cross[k]["avg_ms"],
                "color": PLATFORM_COLORS[plat],
                "annotation": ann,
            })
    chart_s0 = _svg_bar_chart("S0 Normal Execution - Cross-Agent Average (ms)", s0_bars)

    # ---- Chart 2: Grouped bar - per-agent S0 timing ----
    agent_groups = []
    for agent in ALL_AGENTS:
        agent_label = agent.replace("-", " ").title()
        bars = []
        for plat in ALL_PLATFORMS:
            k = f"{agent}/{plat}/s0-normal"
            if k in per_aps:
                bars.append({
                    "sub_label": PLATFORM_LABELS[plat],
                    "value": per_aps[k]["avg_ms"],
                    "color": PLATFORM_COLORS[plat],
                })
        if bars:
            agent_groups.append({"label": agent_label, "bars": bars})
    chart_s0_agents = _svg_grouped_bar_chart("S0 Per-Agent Timing Across Platforms (ms)", agent_groups)

    # ---- Chart 3: S1 Recovery - elapsed per platform ----
    s1_bars = []
    for plat in ALL_PLATFORMS:
        k = f"{plat}/s1-recovery"
        if k in cross:
            # count skipped steps
            total_skipped = 0
            n_entries = 0
            for rk, rv in recovery.items():
                if rk.endswith(f"/{plat}"):
                    total_skipped += rv["avg_steps_skipped"]
                    n_entries += 1
            avg_skip = total_skipped / n_entries if n_entries > 0 else 0
            ann = f'skip: {avg_skip:.0f} steps' if avg_skip > 0 else 'no skip'
            s1_bars.append({
                "label": PLATFORM_LABELS[plat],
                "value": cross[k]["avg_ms"],
                "color": PLATFORM_COLORS[plat],
                "annotation": ann,
            })
    chart_s1 = _svg_bar_chart("S1 Recovery - Cross-Agent Average (ms)", s1_bars)

    # ---- Chart 4: S3 Concurrent ----
    s3_bars = []
    for plat in ALL_PLATFORMS:
        k = f"{plat}/s3-concurrent"
        if k in cross:
            overhead = concurrent.get(plat, {}).get("overhead_pct", 0)
            ann = f'vs S0: {overhead:+.1f}%'
            s3_bars.append({
                "label": PLATFORM_LABELS[plat],
                "value": cross[k]["avg_ms"],
                "color": PLATFORM_COLORS[plat],
                "annotation": ann,
            })
    chart_s3 = _svg_bar_chart("S3 Concurrent 3x - Cross-Agent Average (ms)", s3_bars)

    # ---- Chart 5: Radar ----
    dims = ["exec_overhead", "recovery", "concurrency", "ease_of_adoption", "observability"]
    dim_labels = ["Exec Overhead", "Recovery", "Concurrency", "Ease of Adoption", "Observability"]
    chart_radar = _svg_radar_chart("Platform Capability Radar", radar, dims, dim_labels)

    # ---- Build HTML tables ----

    # vs Native overhead table
    overhead_rows = ""
    for scen in ALL_SCENARIOS:
        for plat in ALL_PLATFORMS:
            k = f"{plat}/{scen}"
            if k not in cross:
                continue
            avg = cross[k]["avg_ms"]
            overhead = vs_native.get(k, None)
            if plat == "native":
                oh_str = "baseline"
                cell_class = "cell-neutral"
            elif overhead is not None:
                oh_str = f"{overhead:+.2f}%"
                if overhead <= 5:
                    cell_class = "cell-good"
                elif overhead <= 15:
                    cell_class = "cell-warn"
                else:
                    cell_class = "cell-bad"
                # Negative means faster (recovery)
                if overhead < 0:
                    cell_class = "cell-great"
            else:
                oh_str = "N/A"
                cell_class = "cell-neutral"
            overhead_rows += f'<tr><td>{SCENARIO_LABELS.get(scen, scen)}</td><td>{PLATFORM_LABELS.get(plat, plat)}</td><td>{avg:.2f}</td><td class="{cell_class}">{oh_str}</td></tr>\n'

    # Full results matrix table
    matrix_rows = ""
    for key in sorted(per_aps.keys()):
        d = per_aps[key]
        plat = d["platform"]
        color = PLATFORM_COLORS.get(plat, "#888")
        matrix_rows += (f'<tr><td>{d["agent"]}</td>'
                        f'<td style="color:{color}">{PLATFORM_LABELS.get(plat, plat)}</td>'
                        f'<td>{d["scenario"]}</td>'
                        f'<td>{d["avg_ms"]:.2f}</td>'
                        f'<td>{d["min_ms"]:.2f}</td>'
                        f'<td>{d["max_ms"]:.2f}</td>'
                        f'<td>{d["p95_ms"]:.2f}</td>'
                        f'<td>{d["stddev_ms"]:.2f}</td>'
                        f'<td>{d["n"]}</td></tr>\n')

    # Step breakdown table
    step_rows = ""
    for key in sorted(step_bd.keys()):
        parts = key.split("/")
        agent, plat = parts[0], parts[1]
        for step_name, avg_ms in step_bd[key].items():
            color = PLATFORM_COLORS.get(plat, "#888")
            step_rows += f'<tr><td>{agent}</td><td style="color:{color}">{PLATFORM_LABELS.get(plat, plat)}</td><td>{step_name}</td><td>{avg_ms:.2f}</td></tr>\n'

    # Recovery detail table
    recovery_rows = ""
    for key in sorted(recovery.items(), key=lambda x: x[0]):
        rk, rv = key
        parts = rk.split("/")
        agent, plat = parts[0], parts[1]
        color = PLATFORM_COLORS.get(plat, "#888")
        recovery_rows += (f'<tr><td>{agent}</td>'
                         f'<td style="color:{color}">{PLATFORM_LABELS.get(plat, plat)}</td>'
                         f'<td>{rv["avg_elapsed_ms"]:.2f}</td>'
                         f'<td>{rv["avg_steps_skipped"]:.0f}</td>'
                         f'<td>{rv["time_saved_ms"]:.2f}</td>'
                         f'<td>{rv["time_saved_pct"]:.1f}%</td></tr>\n')

    # Radar scores table
    radar_rows = ""
    for plat in ALL_PLATFORMS:
        scores = radar.get(plat, {})
        color = PLATFORM_COLORS.get(plat, "#888")
        radar_rows += (f'<tr><td style="color:{color}">{PLATFORM_LABELS.get(plat, plat)}</td>'
                       f'<td>{scores.get("exec_overhead", 0)}</td>'
                       f'<td>{scores.get("recovery", 0)}</td>'
                       f'<td>{scores.get("concurrency", 0)}</td>'
                       f'<td>{scores.get("ease_of_adoption", 0)}</td>'
                       f'<td>{scores.get("observability", 0)}</td></tr>\n')

    # Executive summary
    s0_native_avg = cross.get("native/s0-normal", {}).get("avg_ms", 0)
    s0_afv3_avg = cross.get("afv3/s0-normal", {}).get("avg_ms", 0)
    s0_temporal_avg = cross.get("temporal/s0-normal", {}).get("avg_ms", 0)
    s0_lg_avg = cross.get("langgraph/s0-normal", {}).get("avg_ms", 0)

    afv3_overhead = vs_native.get("afv3/s0-normal", 0)
    temporal_overhead = vs_native.get("temporal/s0-normal", 0)
    lg_overhead = vs_native.get("langgraph/s0-normal", 0)

    s1_afv3_avg = cross.get("afv3/s1-recovery", {}).get("avg_ms", 0)
    s1_temporal_avg = cross.get("temporal/s1-recovery", {}).get("avg_ms", 0)
    s1_native_avg = cross.get("native/s1-recovery", {}).get("avg_ms", 0)

    s3_temporal_overhead = concurrent.get("temporal", {}).get("overhead_pct", 0)
    s3_afv3_overhead = concurrent.get("afv3", {}).get("overhead_pct", 0)

    gen_time = agg.get("generated_at", "N/A")
    total_runs = agg.get("total_results", 0)
    valid_runs = agg.get("valid_results", 0)
    error_count = agg.get("errors", 0)
    success_rate = valid_runs / total_runs * 100 if total_runs > 0 else 0

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>AF v3 Benchmark Report</title>
<style>
:root {{
    --bg-primary: #0f172a;
    --bg-secondary: #1e293b;
    --bg-card: #1e293b;
    --border: #334155;
    --text-primary: #e2e8f0;
    --text-secondary: #94a3b8;
    --text-muted: #64748b;
    --green: #4ade80;
    --purple: #a78bfa;
    --blue: #60a5fa;
    --orange: #f97316;
    --red: #f87171;
    --yellow: #fbbf24;
}}
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{
    background: var(--bg-primary);
    color: var(--text-primary);
    font-family: 'SF Mono', 'Fira Code', 'JetBrains Mono', Consolas, 'Courier New', monospace;
    font-size: 14px;
    line-height: 1.6;
    max-width: 1200px;
    margin: 0 auto;
    padding: 40px 24px;
}}
h1 {{
    font-size: 28px;
    font-weight: 700;
    color: var(--text-primary);
    border-bottom: 2px solid var(--purple);
    padding-bottom: 12px;
    margin-bottom: 8px;
}}
h1 span {{ color: var(--purple); }}
.subtitle {{
    color: var(--text-muted);
    font-size: 13px;
    margin-bottom: 32px;
}}
h2 {{
    font-size: 20px;
    font-weight: 600;
    color: var(--text-primary);
    margin: 40px 0 16px 0;
    padding-left: 12px;
    border-left: 3px solid var(--purple);
}}
h2 .en {{ color: var(--text-secondary); font-size: 14px; font-weight: 400; margin-left: 8px; }}
h3 {{
    font-size: 16px;
    font-weight: 500;
    color: var(--text-secondary);
    margin: 24px 0 12px 0;
}}
.card {{
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 24px;
    margin: 16px 0;
}}
.card-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(360px, 1fr));
    gap: 16px;
}}
.stat-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 12px;
    margin: 16px 0;
}}
.stat-box {{
    background: var(--bg-secondary);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 16px;
    text-align: center;
}}
.stat-box .value {{
    font-size: 28px;
    font-weight: 700;
    color: var(--purple);
}}
.stat-box .label {{
    font-size: 11px;
    color: var(--text-muted);
    margin-top: 4px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}}
table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
    margin: 12px 0;
}}
th {{
    background: #0f172a;
    color: var(--text-secondary);
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    font-size: 11px;
    padding: 10px 12px;
    text-align: left;
    border-bottom: 2px solid var(--border);
    position: sticky;
    top: 0;
}}
td {{
    padding: 8px 12px;
    border-bottom: 1px solid #1e293b;
    color: var(--text-primary);
}}
tr:hover td {{ background: rgba(148, 163, 184, 0.05); }}
.cell-great {{ color: #22d3ee; font-weight: 600; }}
.cell-good {{ color: var(--green); font-weight: 600; }}
.cell-warn {{ color: var(--yellow); font-weight: 600; }}
.cell-bad {{ color: var(--red); font-weight: 600; }}
.cell-neutral {{ color: var(--text-muted); }}
.executive {{
    background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
    border: 1px solid var(--purple);
    border-radius: 8px;
    padding: 24px;
    margin: 16px 0;
}}
.executive ul {{
    list-style: none;
    padding: 0;
}}
.executive li {{
    padding: 8px 0;
    border-bottom: 1px solid rgba(148, 163, 184, 0.1);
    color: var(--text-secondary);
}}
.executive li:last-child {{ border-bottom: none; }}
.executive li strong {{ color: var(--text-primary); }}
.tag {{
    display: inline-block;
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 11px;
    font-weight: 600;
}}
.tag-green {{ background: rgba(74, 222, 128, 0.15); color: var(--green); }}
.tag-red {{ background: rgba(248, 113, 113, 0.15); color: var(--red); }}
.tag-blue {{ background: rgba(96, 165, 250, 0.15); color: var(--blue); }}
.tag-purple {{ background: rgba(167, 139, 250, 0.15); color: var(--purple); }}
.chart-container {{
    display: flex;
    justify-content: center;
    overflow-x: auto;
    padding: 8px 0;
}}
details {{
    margin: 16px 0;
}}
summary {{
    cursor: pointer;
    color: var(--purple);
    font-weight: 500;
    padding: 8px 0;
}}
summary:hover {{ color: var(--blue); }}
.footer {{
    margin-top: 60px;
    padding-top: 20px;
    border-top: 1px solid var(--border);
    text-align: center;
    color: var(--text-muted);
    font-size: 12px;
}}
@media (max-width: 768px) {{
    body {{ padding: 16px 12px; }}
    h1 {{ font-size: 22px; }}
    .card {{ padding: 16px; }}
    .stat-grid {{ grid-template-columns: repeat(2, 1fr); }}
}}
</style>
</head>
<body>

<h1>AF v3 <span>Benchmark Report</span></h1>
<div class="subtitle">
    Agent Fabric v3 Platform Comparison &mdash; Real LLM Workload Benchmark
    &bull; Model: {MODEL}
    &bull; Generated: {gen_time[:19]}Z
</div>

<!-- ============ Experiment Metadata ============ -->
<h2>实验概览 <span class="en">Experiment Overview</span></h2>
<div class="stat-grid">
    <div class="stat-box"><div class="value">{total_runs}</div><div class="label">Total Runs</div></div>
    <div class="stat-box"><div class="value">{valid_runs}</div><div class="label">Valid Results</div></div>
    <div class="stat-box"><div class="value">{error_count}</div><div class="label">Errors</div></div>
    <div class="stat-box"><div class="value">{success_rate:.1f}%</div><div class="label">Success Rate</div></div>
    <div class="stat-box"><div class="value">{len(ALL_PLATFORMS)}</div><div class="label">Platforms</div></div>
    <div class="stat-box"><div class="value">{len(ALL_AGENTS)}</div><div class="label">Agents</div></div>
    <div class="stat-box"><div class="value">{len(ALL_SCENARIOS)}</div><div class="label">Scenarios</div></div>
    <div class="stat-box"><div class="value">{MODEL}</div><div class="label">LLM Model</div></div>
</div>

<!-- ============ Executive Summary ============ -->
<h2>核心发现 <span class="en">Executive Summary</span></h2>
<div class="executive">
<ul>
    <li><strong>S0 Normal Execution:</strong>
        AF v3 overhead vs Native = <span class="tag tag-{'green' if abs(afv3_overhead) < 8 else 'red'}">{afv3_overhead:+.2f}%</span>,
        Temporal = <span class="tag tag-{'green' if abs(temporal_overhead) < 8 else 'red'}">{temporal_overhead:+.2f}%</span>,
        LangGraph = <span class="tag tag-{'green' if abs(lg_overhead) < 8 else 'red'}">{lg_overhead:+.2f}%</span>
    </li>
    <li><strong>S1 Recovery:</strong>
        AF v3 recovery avg = {s1_afv3_avg:.1f}ms (vs Native {s1_native_avg:.1f}ms),
        Temporal replay = {s1_temporal_avg:.1f}ms <span class="tag tag-blue">deterministic replay</span>
    </li>
    <li><strong>S3 Concurrency:</strong>
        AF v3 3x overhead = <span class="tag tag-{'green' if abs(s3_afv3_overhead) < 15 else 'red'}">{s3_afv3_overhead:+.1f}%</span>,
        Temporal = <span class="tag tag-{'green' if abs(s3_temporal_overhead) < 15 else 'red'}">{s3_temporal_overhead:+.1f}%</span>
    </li>
    <li><strong>Best Balance:</strong>
        AF v3 offers <span class="tag tag-purple">low overhead + WAL recovery + OTEL observability</span>.
        Temporal excels at deterministic replay but adds ~{temporal_overhead:+.1f}% runtime overhead.
        LangGraph is lightweight but lacks true recovery capability.
    </li>
</ul>
</div>

<!-- ============ S0 Normal Charts ============ -->
<h2>S0 正常执行 <span class="en">Normal Execution</span></h2>
<div class="card">
<div class="chart-container">{chart_s0}</div>
</div>
<div class="card">
<div class="chart-container">{chart_s0_agents}</div>
</div>

<!-- ============ S1 Recovery ============ -->
<h2>S1 故障恢复 <span class="en">Recovery</span></h2>
<div class="card">
<div class="chart-container">{chart_s1}</div>
</div>
<h3>Recovery Detail</h3>
<div class="card">
<table>
<thead><tr><th>Agent</th><th>Platform</th><th>Avg Elapsed (ms)</th><th>Steps Skipped</th><th>Time Saved (ms)</th><th>Time Saved %</th></tr></thead>
<tbody>{recovery_rows}</tbody>
</table>
</div>

<!-- ============ S3 Concurrent ============ -->
<h2>S3 并发执行 <span class="en">Concurrent Execution</span></h2>
<div class="card">
<div class="chart-container">{chart_s3}</div>
</div>

<!-- ============ vs Native Overhead ============ -->
<h2>对比原生开销 <span class="en">vs Native Overhead</span></h2>
<div class="card">
<table>
<thead><tr><th>Scenario</th><th>Platform</th><th>Avg (ms)</th><th>vs Native</th></tr></thead>
<tbody>{overhead_rows}</tbody>
</table>
</div>

<!-- ============ Radar Chart ============ -->
<h2>平台能力雷达 <span class="en">Platform Capability Radar</span></h2>
<div class="card-grid">
<div class="card">
<div class="chart-container">{chart_radar}</div>
</div>
<div class="card">
<h3>Radar Scores (0-100)</h3>
<table>
<thead><tr><th>Platform</th><th>Exec Overhead</th><th>Recovery</th><th>Concurrency</th><th>Ease of Adopt</th><th>Observability</th></tr></thead>
<tbody>{radar_rows}</tbody>
</table>
</div>
</div>

<!-- ============ Per-Step Breakdown ============ -->
<h2>步骤级时间分析 <span class="en">Per-Step Timing Breakdown</span></h2>
<div class="card">
<table>
<thead><tr><th>Agent</th><th>Platform</th><th>Step</th><th>Avg (ms)</th></tr></thead>
<tbody>{step_rows}</tbody>
</table>
</div>

<!-- ============ Full Results Matrix ============ -->
<h2>完整结果矩阵 <span class="en">Full Results Matrix</span></h2>
<details>
<summary>展开查看完整数据 (Click to expand raw data)</summary>
<div class="card" style="overflow-x:auto">
<table>
<thead><tr><th>Agent</th><th>Platform</th><th>Scenario</th><th>Avg (ms)</th><th>Min</th><th>Max</th><th>P95</th><th>StdDev</th><th>N</th></tr></thead>
<tbody>{matrix_rows}</tbody>
</table>
</div>
</details>

<!-- ============ Footer ============ -->
<div class="footer">
    AF v3 Benchmark Report &bull; Generated {gen_time[:19]}Z &bull;
    {total_runs} runs &bull; {valid_runs} valid &bull; Model: {MODEL}
    <br>Agent Fabric v3 &mdash; Durable Execution for LLM Agents
</div>

</body>
</html>"""
    return html


# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════

def main():
    print(f"[generate_report] Loading results from {RESULTS_DIR}")
    results = load_results()
    if not results:
        print("[generate_report] No results found. Generating empty report.")

    print(f"[generate_report] Loaded {len(results)} result files")
    agg = compute_aggregates(results)

    # Write summary.json
    print(f"[generate_report] Writing summary -> {SUMMARY_PATH}")
    SUMMARY_PATH.write_text(json.dumps(agg, indent=2, ensure_ascii=False, default=str))

    # Generate HTML
    print(f"[generate_report] Generating HTML report -> {REPORT_PATH}")
    html = generate_html(agg)
    REPORT_PATH.write_text(html, encoding="utf-8")

    print(f"[generate_report] Done!")
    print(f"  summary.json : {SUMMARY_PATH}")
    print(f"  report.html  : {REPORT_PATH}")
    print(f"  Total: {agg['total_results']} results, {agg['valid_results']} valid, {agg['errors']} errors")

    # Quick console summary
    cross = agg["cross_agent"]
    vs_native = agg["vs_native"]
    print(f"\n{'Platform/Scenario':<30} {'Avg ms':>10} {'vs Native':>12}")
    print("-" * 55)
    for scen in ALL_SCENARIOS:
        for plat in ALL_PLATFORMS:
            k = f"{plat}/{scen}"
            if k in cross:
                avg = cross[k]["avg_ms"]
                overhead = vs_native.get(k, None)
                oh_str = "baseline" if plat == "native" else (f"{overhead:+.2f}%" if overhead is not None else "N/A")
                label = f"{PLATFORM_LABELS[plat]}/{SCENARIO_LABELS[scen][:6]}"
                print(f"  {label:<28} {avg:>10.2f} {oh_str:>12}")
        print()


if __name__ == "__main__":
    main()
