#!/usr/bin/env python3
"""
Generate all benchmark plots from results/benchmark_results.json.

Produces (saved to results/):
  plot1_cache_warmup.png         — latency & hit-rate over query sequence
  plot2_insert_throughput.png    — INSERT avg latency vs network delay
  plot3_latency_sensitivity.png  — throughput speedup vs network delay
  plot4_type_breakdown.png       — per-query-type avg latency (grouped bars)
  plot5_rw_sweep.png             — throughput speedup vs read fraction
  plot6_p95_comparison.png       — p95 latency comparison at 50 ms
  plot7_overall_heatmap.png      — speedup heatmap (delay × query type)
"""

import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

# ── Style ────────────────────────────────────────────────────────────────────
TRAD_COLOR  = "#E74C3C"   # red  — traditional
PROXY_COLOR = "#2ECC71"   # green — proxy
HIT_COLOR   = "#3498DB"   # blue  — cache hits
MISS_COLOR  = "#F39C12"   # amber — cache misses

plt.rcParams.update({
    "figure.dpi":        150,
    "font.family":       "DejaVu Sans",
    "font.size":         10,
    "axes.spines.top":   False,
    "axes.spines.right": False,
    "axes.grid":         True,
    "grid.alpha":        0.3,
    "grid.linestyle":    "--",
})

RESULTS_DIR = Path("results")


def load() -> dict:
    with open(RESULTS_DIR / "benchmark_results.json") as f:
        return json.load(f)


# ── Plot 1: Cache Warmup ──────────────────────────────────────────────────────

def plot_cache_warmup(data: dict):
    sc   = data["cache_warmup"]
    t_seq = sc["traditional"]["seq"]      # [[idx, lat, type], ...]
    p_seq = sc["proxy"]["seq"]

    t_lats = [s[1] for s in t_seq]
    p_lats = [s[1] for s in p_seq]

    # Rolling average (window = 30)
    W = 30
    def roll(arr, w):
        cs = np.cumsum(arr, dtype=float)
        cs[w:] = cs[w:] - cs[:-w]
        return cs[w-1:] / w

    t_roll = roll(t_lats, W)
    p_roll = roll(p_lats, W)
    xs     = np.arange(W - 1, len(t_lats))

    # Cumulative cache hit rate
    hits_cum  = []
    total_cum = 0
    hit_cum   = 0
    cache_hits_by_idx = {s[0]: True for s in sc["proxy"]["seq"]}
    # Recompute from seq: a Type B query that arrived with cache_hit
    # We tag by checking: latency < 2ms means local (hit)
    # This is a heuristic — works because miss latency >> hit latency at 50ms
    THRESHOLD = 5.0   # ms; below = cache hit
    for i, (idx, lat, qt) in enumerate(p_seq):
        if qt == "B":
            total_cum += 1
            if lat < THRESHOLD:
                hit_cum += 1
        hits_cum.append(hit_cum / total_cum if total_cum else 0)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(9, 7), sharex=True)
    fig.suptitle("Cache Warmup Effect\n"
                 "(Type-B point-lookup workload, 50 ms network delay, 200 unique keys)",
                 fontsize=12, fontweight="bold")

    ax1.plot(range(len(t_lats)), t_lats, alpha=0.12, color=TRAD_COLOR, lw=0.7)
    ax1.plot(range(len(p_lats)), p_lats, alpha=0.12, color=PROXY_COLOR, lw=0.7)
    ax1.plot(xs, t_roll, color=TRAD_COLOR,  lw=2.0, label="Traditional (rolling avg)")
    ax1.plot(xs, p_roll, color=PROXY_COLOR, lw=2.0, label="Proxy (rolling avg)")
    ax1.set_ylabel("Query Latency (ms)")
    ax1.legend(loc="upper right")
    ax1.set_ylim(bottom=0)

    ax2.plot(range(len(hits_cum)), [h * 100 for h in hits_cum],
             color=HIT_COLOR, lw=2.0)
    ax2.axhline(hits_cum[-1] * 100, color=HIT_COLOR, ls="--", lw=1.0, alpha=0.6,
                label=f"Final hit rate: {hits_cum[-1]*100:.1f}%")
    ax2.set_ylabel("Cumulative Cache Hit Rate (%)")
    ax2.set_xlabel("Query Sequence Number")
    ax2.set_ylim(0, 100)
    ax2.legend(loc="lower right")

    plt.tight_layout()
    plt.savefig(RESULTS_DIR / "plot1_cache_warmup.png", bbox_inches="tight")
    plt.close()
    print("  → plot1_cache_warmup.png")


# ── Plot 2: INSERT Throughput ─────────────────────────────────────────────────

def plot_insert_throughput(data: dict):
    ins    = data["insert_throughput"]
    delays = [0, 25, 50, 100]
    t_avg  = [ins[str(d)]["traditional"]["avg_ms"]    for d in delays]
    p_avg  = [ins[str(d)]["proxy"]["avg_ms"]          for d in delays]
    t_qps  = [ins[str(d)]["traditional"]["throughput_qps"] for d in delays]
    p_qps  = [ins[str(d)]["proxy"]["throughput_qps"]       for d in delays]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    fig.suptitle("INSERT Performance — Lazy Write (Proxy) vs Eager Write (Traditional)",
                 fontsize=12, fontweight="bold")

    x  = np.arange(len(delays))
    bw = 0.35
    ax1.bar(x - bw/2, t_avg, bw, color=TRAD_COLOR,  label="Traditional")
    ax1.bar(x + bw/2, p_avg, bw, color=PROXY_COLOR, label="Proxy (lazy)")
    ax1.set_xticks(x)
    ax1.set_xticklabels([f"{d} ms" for d in delays])
    ax1.set_xlabel("Network Delay (one-way)")
    ax1.set_ylabel("Avg Latency (ms)")
    ax1.set_title("Average INSERT Latency")
    ax1.legend()
    for xi, (tv, pv) in enumerate(zip(t_avg, p_avg)):
        spd = tv / pv if pv else 0
        ax1.annotate(f"{spd:.0f}x", xy=(xi + bw/2, pv + max(t_avg)*0.02),
                     ha="center", fontsize=9, color=PROXY_COLOR, fontweight="bold")

    ax2.bar(x - bw/2, t_qps, bw, color=TRAD_COLOR,  label="Traditional")
    ax2.bar(x + bw/2, p_qps, bw, color=PROXY_COLOR, label="Proxy (lazy)")
    ax2.set_xticks(x)
    ax2.set_xticklabels([f"{d} ms" for d in delays])
    ax2.set_xlabel("Network Delay (one-way)")
    ax2.set_ylabel("Throughput (QPS)")
    ax2.set_title("INSERT Throughput")
    ax2.legend()
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f}"))

    plt.tight_layout()
    plt.savefig(RESULTS_DIR / "plot2_insert_throughput.png", bbox_inches="tight")
    plt.close()
    print("  → plot2_insert_throughput.png")


# ── Plot 3: Latency Sensitivity ───────────────────────────────────────────────

def plot_latency_sensitivity(data: dict):
    ls     = data["latency_sensitivity"]
    delays = [0, 10, 25, 50, 100, 200]
    t_qps  = [ls[str(d)]["traditional"]["throughput_qps"] for d in delays]
    p_qps  = [ls[str(d)]["proxy"]["throughput_qps"]       for d in delays]
    t_avg  = [ls[str(d)]["traditional"]["avg_ms"]         for d in delays]
    p_avg  = [ls[str(d)]["proxy"]["avg_ms"]               for d in delays]
    hits   = [ls[str(d)]["proxy"]["cache_hit_rate"] * 100 for d in delays]
    spds   = [p/t if t else 0 for p, t in zip(p_qps, t_qps)]

    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    fig.suptitle("Mixed Workload — Impact of Network Latency on Proxy Benefit",
                 fontsize=12, fontweight="bold")

    # Throughput
    ax = axes[0]
    ax.plot(delays, t_qps, "o-", color=TRAD_COLOR,  lw=2, label="Traditional")
    ax.plot(delays, p_qps, "s-", color=PROXY_COLOR, lw=2, label="Proxy")
    ax.set_xlabel("Network Delay (ms)")
    ax.set_ylabel("Throughput (QPS)")
    ax.set_title("Throughput vs Network Delay")
    ax.legend()

    # Avg latency
    ax = axes[1]
    ax.plot(delays, t_avg, "o-", color=TRAD_COLOR,  lw=2, label="Traditional")
    ax.plot(delays, p_avg, "s-", color=PROXY_COLOR, lw=2, label="Proxy")
    ax.set_xlabel("Network Delay (ms)")
    ax.set_ylabel("Avg Latency (ms)")
    ax.set_title("Avg Latency vs Network Delay")
    ax.legend()
    ax.fill_between(delays, p_avg, t_avg, alpha=0.12, color=PROXY_COLOR)

    # Speedup
    ax = axes[2]
    ax2b = ax.twinx()
    ax.bar(delays, spds, width=6, color=PROXY_COLOR, alpha=0.7, label="Speedup")
    ax2b.plot(delays, hits, "D--", color=HIT_COLOR, lw=2, label="Cache Hit %")
    ax.set_xlabel("Network Delay (ms)")
    ax.set_ylabel("Throughput Speedup  (proxy / traditional)")
    ax2b.set_ylabel("Cache Hit Rate (%)")
    ax.set_title("Speedup & Cache Hit Rate")
    ax.set_ylim(bottom=0)
    ax2b.set_ylim(0, 100)
    lines1, labs1 = ax.get_legend_handles_labels()
    lines2, labs2 = ax2b.get_legend_handles_labels()
    ax.legend(lines1 + lines2, labs1 + labs2, loc="upper left")

    plt.tight_layout()
    plt.savefig(RESULTS_DIR / "plot3_latency_sensitivity.png", bbox_inches="tight")
    plt.close()
    print("  → plot3_latency_sensitivity.png")


# ── Plot 4: Per-Type Breakdown ────────────────────────────────────────────────

def plot_type_breakdown(data: dict):
    bd     = data["type_breakdown"]
    qtypes = ["A", "B", "INSERT", "C"]
    labels = ["Type A\n(Aggregate)", "Type B\n(Point Lookup)", "INSERT\n(Lazy Write)",
              "Type C\n(UPDATE)"]
    t_avgs = [bd["traditional"]["type_stats"][qt]["avg_ms"] for qt in qtypes]
    p_avgs = [bd["proxy"]["type_stats"][qt]["avg_ms"]       for qt in qtypes]
    counts = [bd["traditional"]["type_stats"][qt]["count"]  for qt in qtypes]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 6))
    fig.suptitle("Per-Query-Type Performance Breakdown  (50 ms network delay)",
                 fontsize=12, fontweight="bold")

    x  = np.arange(len(qtypes))
    bw = 0.35

    bars1 = ax1.bar(x - bw/2, t_avgs, bw, color=TRAD_COLOR,  label="Traditional")
    bars2 = ax1.bar(x + bw/2, p_avgs, bw, color=PROXY_COLOR, label="Proxy")
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels)
    ax1.set_ylabel("Avg Latency (ms)")
    ax1.set_title("Average Query Latency by Type")
    ax1.legend()

    for xi, (tv, pv) in enumerate(zip(t_avgs, p_avgs)):
        if pv > 0.01:
            spd = tv / pv
            ax1.annotate(f"{spd:.1f}x",
                         xy=(xi + bw/2, pv + max(t_avgs) * 0.02),
                         ha="center", fontsize=8.5, color=PROXY_COLOR,
                         fontweight="bold")

    # Speedup ratio chart (log scale)
    spds = [tv / pv if pv else 1 for tv, pv in zip(t_avgs, p_avgs)]
    colors = [PROXY_COLOR if s > 1.5 else MISS_COLOR for s in spds]
    bars = ax2.bar(labels, spds, color=colors, edgecolor="white", lw=0.5)
    ax2.axhline(1.0, ls="--", color="grey", lw=1.2)
    ax2.set_ylabel("Speedup  (Traditional latency / Proxy latency)")
    ax2.set_title("Speedup Ratio by Query Type  (higher = better)")
    ax2.set_yscale("log")
    for bar, spd in zip(bars, spds):
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() * 1.05,
                 f"{spd:.1f}x", ha="center", fontsize=10, fontweight="bold")

    plt.tight_layout()
    plt.savefig(RESULTS_DIR / "plot4_type_breakdown.png", bbox_inches="tight")
    plt.close()
    print("  → plot4_type_breakdown.png")


# ── Plot 5: Read/Write Sweep ──────────────────────────────────────────────────

def plot_rw_sweep(data: dict):
    rws   = data["rw_sweep"]
    fracs = [30, 50, 70, 80, 90, 95]
    t_qps = [rws[str(f)]["traditional"]["throughput_qps"] for f in fracs]
    p_qps = [rws[str(f)]["proxy"]["throughput_qps"]       for f in fracs]
    hits  = [rws[str(f)]["proxy"]["cache_hit_rate"] * 100 for f in fracs]
    spds  = [p/t if t else 0 for p, t in zip(p_qps, t_qps)]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    fig.suptitle("Effect of Read Fraction on Proxy Performance  (50 ms network delay)",
                 fontsize=12, fontweight="bold")

    ax1.plot(fracs, t_qps, "o-", color=TRAD_COLOR,  lw=2, label="Traditional")
    ax1.plot(fracs, p_qps, "s-", color=PROXY_COLOR, lw=2, label="Proxy")
    ax1.set_xlabel("Read Fraction (%)")
    ax1.set_ylabel("Throughput (QPS)")
    ax1.set_title("Throughput vs Read Fraction")
    ax1.legend()
    ax1.fill_between(fracs, t_qps, p_qps, alpha=0.12, color=PROXY_COLOR)

    ax2r = ax2.twinx()
    ax2.plot(fracs, spds, "o-", color=PROXY_COLOR, lw=2.5, label="Speedup")
    ax2r.plot(fracs, hits, "D--", color=HIT_COLOR, lw=2, label="Cache Hit %")
    ax2.set_xlabel("Read Fraction (%)")
    ax2.set_ylabel("Throughput Speedup  (proxy / traditional)")
    ax2r.set_ylabel("Cache Hit Rate (%)")
    ax2.set_title("Speedup & Cache Hit Rate vs Read Fraction")
    ax2.set_ylim(bottom=0)
    ax2r.set_ylim(0, 100)
    lines1, labs1 = ax2.get_legend_handles_labels()
    lines2, labs2 = ax2r.get_legend_handles_labels()
    ax2.legend(lines1 + lines2, labs1 + labs2, loc="upper left")

    plt.tight_layout()
    plt.savefig(RESULTS_DIR / "plot5_rw_sweep.png", bbox_inches="tight")
    plt.close()
    print("  → plot5_rw_sweep.png")


# ── Plot 6: Latency Percentile Comparison ─────────────────────────────────────

def plot_p_comparison(data: dict):
    bd     = data["type_breakdown"]
    qtypes = ["A", "B", "INSERT", "C"]
    labels = ["Type A", "Type B", "INSERT", "Type C"]

    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    fig.suptitle("Latency Percentile Comparison  (50 ms network delay)",
                 fontsize=12, fontweight="bold")

    for ax, mode, color, title in [
            (axes[0], "traditional", TRAD_COLOR, "Traditional"),
            (axes[1], "proxy",       PROXY_COLOR, "Proxy"),
    ]:
        ts = bd[mode]
        p50 = [ts["type_stats"][qt]["p50_ms"] for qt in qtypes]
        p95 = [ts["type_stats"][qt]["p95_ms"] for qt in qtypes]
        avg = [ts["type_stats"][qt]["avg_ms"] for qt in qtypes]
        x = np.arange(len(qtypes))
        ax.bar(x, p95, color=color, alpha=0.3, label="P95")
        ax.bar(x, p50, color=color, alpha=0.6, label="P50 (Median)")
        ax.plot(x, avg, "D-", color=color, lw=1.5, label="Avg", ms=6)
        ax.set_xticks(x)
        ax.set_xticklabels(labels)
        ax.set_ylabel("Latency (ms)")
        ax.set_title(f"{title} — Latency Distribution")
        ax.legend()

    plt.tight_layout()
    plt.savefig(RESULTS_DIR / "plot6_p_comparison.png", bbox_inches="tight")
    plt.close()
    print("  → plot6_p_comparison.png")


# ── Plot 7: Speedup Heatmap ───────────────────────────────────────────────────

def plot_speedup_heatmap(data: dict):
    ls     = data["latency_sensitivity"]
    delays = [0, 10, 25, 50, 100, 200]
    qtypes = ["A", "B", "INSERT", "C"]
    qlabels = ["Type A\n(Aggregate)", "Type B\n(Point Lookup)", "INSERT", "Type C\n(UPDATE)"]

    grid = np.zeros((len(delays), len(qtypes)))
    for di, d in enumerate(delays):
        T = ls[str(d)]["traditional"]["type_stats"]
        P = ls[str(d)]["proxy"]["type_stats"]
        for qi, qt in enumerate(qtypes):
            tv = T[qt]["avg_ms"]
            pv = P[qt]["avg_ms"]
            grid[di, qi] = tv / pv if pv else 1.0

    fig, ax = plt.subplots(figsize=(9, 6))
    im = ax.imshow(grid, cmap="YlGn", aspect="auto")
    plt.colorbar(im, ax=ax, label="Speedup (Traditional / Proxy)")

    ax.set_xticks(range(len(qtypes)))
    ax.set_xticklabels(qlabels)
    ax.set_yticks(range(len(delays)))
    ax.set_yticklabels([f"{d} ms" for d in delays])
    ax.set_xlabel("Query Type")
    ax.set_ylabel("Network Delay")
    ax.set_title("Proxy Speedup Heatmap by Query Type × Network Delay\n"
                 "(greener = greater speedup)")

    for di in range(len(delays)):
        for qi in range(len(qtypes)):
            v = grid[di, qi]
            txt = f"{v:.1f}x" if v < 100 else f"{v:.0f}x"
            ax.text(qi, di, txt, ha="center", va="center",
                    fontsize=9, fontweight="bold",
                    color="black" if v < 5 else "white")

    plt.tight_layout()
    plt.savefig(RESULTS_DIR / "plot7_speedup_heatmap.png", bbox_inches="tight")
    plt.close()
    print("  → plot7_speedup_heatmap.png")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    data = load()
    print("Generating plots …")
    plot_cache_warmup(data)
    plot_insert_throughput(data)
    plot_latency_sensitivity(data)
    plot_type_breakdown(data)
    plot_rw_sweep(data)
    plot_p_comparison(data)
    plot_speedup_heatmap(data)
    print(f"\nAll plots saved in {RESULTS_DIR}/")


if __name__ == "__main__":
    main()
