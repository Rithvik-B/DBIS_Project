#!/usr/bin/env python3
"""
Benchmark simulation for the DBIS proxy caching system.

Compares two execution models:
  - Traditional: every query goes directly to the remote PostgreSQL instance
  - Proxy:       queries are routed through the local caching proxy
                   * Type B (point-lookups)  → served from cache on repeat access
                   * INSERT                  → applied locally, flushed async (~lazy)
                   * Type A (aggregates)     → always forwarded to remote
                   * Type C (UPDATE/DELETE)  → lock acquired remotely, write applied locally

All timing is simulated.  Jitter follows a log-normal distribution so that
outliers look realistic.  Seeds are fixed for reproducibility.
"""

import json
import math
import random
import statistics
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Set, Tuple

import numpy as np

# ── Timing constants (milliseconds) ─────────────────────────────────────────
LOCAL_PG_EXEC_MS       = 0.50   # local SELECT (point-lookup)
LOCAL_PG_INSERT_MS     = 0.80   # local INSERT
REMOTE_EXEC_SIMPLE_MS  = 2.00   # remote point-lookup SELECT
REMOTE_EXEC_COMPLEX_MS = 25.00  # remote aggregate / join
REMOTE_EXEC_INSERT_MS  = 1.50   # remote INSERT execution
REMOTE_EXEC_UPDATE_MS  = 2.50   # remote UPDATE execution
CACHE_LOOKUP_MS        = 0.15   # in-memory cache dict lookup
CACHE_STORE_MS         = 0.30   # write row to local-DB cache table
LOCK_ACQUIRE_MS        = 0.50   # one-way overhead after remote grants lock
JITTER_CV              = 0.25   # coefficient of variation for log-normal jitter

SEED = 42


# ── Helpers ──────────────────────────────────────────────────────────────────

def _jitter(base_ms: float, cv: float = JITTER_CV) -> float:
    """Draw from a log-normal distribution centred on *base_ms*."""
    if base_ms <= 0:
        return 0.01
    sigma = math.sqrt(math.log(1 + cv ** 2))
    mu    = math.log(base_ms) - sigma ** 2 / 2
    return float(np.random.lognormal(mu, sigma))


def _rtt(delay_ms: float) -> float:
    """Simulate a round-trip (send + recv) with light jitter."""
    if delay_ms == 0:
        return _jitter(0.10, cv=0.3)   # tiny LAN overhead
    return _jitter(delay_ms * 2, cv=0.15)


def pct(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    idx = min(int(p / 100 * len(s)), len(s) - 1)
    return s[idx]


# ── Workload generators ───────────────────────────────────────────────────────

def _zipf_keys(n_unique: int, n_samples: int, alpha: float = 1.8) -> List[int]:
    """Zipf-distributed key sample (simulates hot-row / temporal locality)."""
    ranks = np.arange(1, n_unique + 1, dtype=float)
    probs = 1.0 / (ranks ** alpha)
    probs /= probs.sum()
    return list(np.random.choice(np.arange(1, n_unique + 1), size=n_samples, p=probs))


def _q_b(key: int) -> dict:
    return {"type": "B",      "table": "customers", "key": key,
            "sql": f"SELECT * FROM customers WHERE customer_id = {key}"}

def _q_a(i: int) -> dict:
    sqls = [
        "SELECT COUNT(*), AVG(total_amount) FROM orders GROUP BY status",
        "SELECT customer_id, SUM(total_amount) FROM orders GROUP BY customer_id "
        "ORDER BY 2 DESC LIMIT 10",
        "SELECT o.order_id, c.name, o.total_amount FROM orders o "
        "JOIN customers c ON o.customer_id = c.customer_id WHERE o.total_amount > 500",
    ]
    return {"type": "A", "table": "orders", "key": None, "sql": sqls[i % len(sqls)]}

def _q_insert(cid: int) -> dict:
    amt = round(random.uniform(10, 1000), 2)
    return {"type": "INSERT", "table": "orders", "key": None,
            "sql": f"INSERT INTO orders (customer_id, total_amount, status) "
                   f"VALUES ({cid}, {amt}, 'pending')"}

def _q_c(key: int) -> dict:
    return {"type": "C", "table": "customers", "key": key,
            "sql": f"UPDATE customers SET updated_at = NOW() "
                   f"WHERE customer_id = {key}"}


def workload_cache_warmup(n: int = 1000, n_unique: int = 200) -> List[dict]:
    """Read-heavy workload: Zipf access to 200 customer IDs.
    Cache warms up progressively → latency drops over time."""
    keys = _zipf_keys(n_unique, n, alpha=1.8)
    return [_q_b(k) for k in keys]


def workload_insert_heavy(n: int = 500) -> List[dict]:
    """Pure INSERT workload — showcases lazy-write throughput gain."""
    return [_q_insert(random.randint(1, 1000)) for _ in range(n)]


def workload_mixed(n: int = 1000, n_unique: int = 300) -> List[dict]:
    """Realistic TPC-H-inspired mix: 45% B, 25% A, 20% INSERT, 10% C."""
    keys  = _zipf_keys(n_unique, n, alpha=1.5)
    out   = []
    for i, k in enumerate(keys):
        r = random.random()
        if   r < 0.45: out.append(_q_b(k))
        elif r < 0.70: out.append(_q_a(i))
        elif r < 0.90: out.append(_q_insert(k))
        else:          out.append(_q_c(k))
    return out


def workload_read_write_ratio(n: int = 1000, read_frac: float = 0.8,
                               n_unique: int = 200) -> List[dict]:
    """Vary read/write ratio — used to sweep cache effectiveness."""
    keys = _zipf_keys(n_unique, n, alpha=1.5)
    out  = []
    for i, k in enumerate(keys):
        if random.random() < read_frac:
            out.append(_q_b(k))
        else:
            out.append(_q_insert(random.randint(1, 1000)) if random.random() < 0.5
                       else _q_c(k))
    return out


# ── Simulation engine ─────────────────────────────────────────────────────────

@dataclass
class SimResult:
    label:    str
    mode:     str         # "traditional" | "proxy"
    delay_ms: float
    latencies:       List[float] = field(default_factory=list)
    type_latencies:  Dict[str, List[float]] = field(default_factory=dict)
    cache_hits:      int = 0
    cache_misses:    int = 0
    # (seq_idx, latency_ms, query_type) — used for warmup curve
    seq: List[Tuple[int, float, str]] = field(default_factory=list)

    # ── derived statistics ──────────────────────────────────────────────────
    @property
    def n(self):         return len(self.latencies)
    @property
    def avg_ms(self):    return statistics.mean(self.latencies) if self.latencies else 0
    @property
    def p50_ms(self):    return statistics.median(self.latencies) if self.latencies else 0
    @property
    def p95_ms(self):    return pct(self.latencies, 95)
    @property
    def p99_ms(self):    return pct(self.latencies, 99)
    @property
    def throughput_qps(self):
        t = sum(self.latencies) / 1000.0
        return self.n / t if t > 0 else 0
    @property
    def cache_hit_rate(self):
        tot = self.cache_hits + self.cache_misses
        return self.cache_hits / tot if tot > 0 else 0.0

    def type_stat(self, qtype: str) -> dict:
        lats = self.type_latencies.get(qtype, [])
        if not lats:
            return {"count": 0, "avg_ms": 0, "p50_ms": 0, "p95_ms": 0}
        return {"count": len(lats), "avg_ms": statistics.mean(lats),
                "p50_ms": statistics.median(lats), "p95_ms": pct(lats, 95)}

    def to_dict(self) -> dict:
        return {
            "label": self.label, "mode": self.mode, "delay_ms": self.delay_ms,
            "n": self.n, "avg_ms": self.avg_ms, "p50_ms": self.p50_ms,
            "p95_ms": self.p95_ms, "p99_ms": self.p99_ms,
            "throughput_qps": self.throughput_qps,
            "cache_hit_rate": self.cache_hit_rate,
            "cache_hits": self.cache_hits, "cache_misses": self.cache_misses,
            "type_stats": {t: self.type_stat(t) for t in ["A", "B", "INSERT", "C"]},
            "seq": [[i, lat, t] for i, lat, t in self.seq],
        }


def sim_traditional(queries: List[dict], delay_ms: float, label: str) -> SimResult:
    """Direct-to-remote: every query pays the full network round-trip."""
    r = SimResult(label, "traditional", delay_ms,
                  type_latencies={t: [] for t in ["A","B","INSERT","C"]})
    for i, q in enumerate(queries):
        qt  = q["type"]
        rt  = _rtt(delay_ms)
        if   qt == "A":      lat = rt + _jitter(REMOTE_EXEC_COMPLEX_MS)
        elif qt == "B":      lat = rt + _jitter(REMOTE_EXEC_SIMPLE_MS)
        elif qt == "INSERT": lat = rt + _jitter(REMOTE_EXEC_INSERT_MS)
        else:                lat = rt + _jitter(REMOTE_EXEC_UPDATE_MS)
        r.latencies.append(lat)
        r.type_latencies[qt].append(lat)
        r.seq.append((i, lat, qt))
    return r


def sim_proxy(queries: List[dict], delay_ms: float, label: str) -> SimResult:
    """Proxy model: cache hits, lazy INSERTs, row-level locking."""
    r = SimResult(label, "proxy", delay_ms,
                  type_latencies={t: [] for t in ["A","B","INSERT","C"]})
    cache:         Dict[int, bool] = {}
    pending_count: int             = 0
    dirty_tables:  Set[str]        = set()
    FLUSH_AT = 50   # approximate background-flush interval in queries

    for i, q in enumerate(queries):
        qt    = q["type"]
        key   = q.get("key")
        table = q.get("table", "")

        if qt == "B":
            if key is not None and key in cache:
                lat = _jitter(CACHE_LOOKUP_MS) + _jitter(LOCAL_PG_EXEC_MS)
                r.cache_hits += 1
            else:
                lat = _rtt(delay_ms) + _jitter(REMOTE_EXEC_SIMPLE_MS) + _jitter(CACHE_STORE_MS)
                r.cache_misses += 1
                if key is not None:
                    cache[key] = True

        elif qt == "A":
            # Pre-flush: if dirty inserts exist on this table, they must be
            # sent to remote before the aggregate runs (consistency guarantee)
            flush_cost = 0.0
            if table in dirty_tables:
                flush_cost = _rtt(delay_ms) * 0.4 + _jitter(LOCAL_PG_EXEC_MS)
                dirty_tables.discard(table)
            lat = flush_cost + _rtt(delay_ms) + _jitter(REMOTE_EXEC_COMPLEX_MS)

        elif qt == "INSERT":
            # Lazy write: insert locally, queue for background flush
            lat = _jitter(LOCAL_PG_INSERT_MS)
            pending_count += 1
            dirty_tables.add(table)
            if pending_count >= FLUSH_AT:
                pending_count = 0
                dirty_tables.clear()

        else:  # C — UPDATE / DELETE
            # Must acquire row-level write lock from remote (1 RTT),
            # then apply the change locally while holding the lock
            lat = _rtt(delay_ms) + _jitter(LOCK_ACQUIRE_MS) + _jitter(LOCAL_PG_EXEC_MS)
            if key is not None:
                cache.pop(key, None)   # invalidate cached row

        r.latencies.append(lat)
        r.type_latencies[qt].append(lat)
        r.seq.append((i, lat, qt))

    return r


# ── Benchmark scenarios ───────────────────────────────────────────────────────

def scenario_cache_warmup(delay_ms: float = 50.0) -> dict:
    """1000 Type-B reads with Zipf access to 200 keys.
    Shows latency dropping as the cache fills with hot rows."""
    wl   = workload_cache_warmup(1000, 200)
    trad = sim_traditional(wl, delay_ms, "Cache Warmup")
    prox = sim_proxy      (wl, delay_ms, "Cache Warmup")
    return {"traditional": trad.to_dict(), "proxy": prox.to_dict(),
            "delay_ms": delay_ms}


def scenario_insert_throughput() -> dict:
    """500 INSERTs at 4 network-delay levels.
    Lazy writes mean the proxy never blocks on remote ACK."""
    out = {}
    for d in [0, 25, 50, 100]:
        wl   = workload_insert_heavy(500)
        out[str(d)] = {
            "traditional": sim_traditional(wl, d, f"INSERT {d}ms").to_dict(),
            "proxy":       sim_proxy      (wl, d, f"INSERT {d}ms").to_dict(),
        }
    return out


def scenario_latency_sensitivity() -> dict:
    """Mixed workload across 6 network-delay values.
    Demonstrates how the proxy advantage grows with latency."""
    out = {}
    for d in [0, 10, 25, 50, 100, 200]:
        wl   = workload_mixed(1000)
        out[str(d)] = {
            "traditional": sim_traditional(wl, d, f"Mixed {d}ms").to_dict(),
            "proxy":       sim_proxy      (wl, d, f"Mixed {d}ms").to_dict(),
        }
    return out


def scenario_type_breakdown(delay_ms: float = 50.0) -> dict:
    """2000-query mixed workload — per-query-type stats at 50 ms latency."""
    wl   = workload_mixed(2000)
    trad = sim_traditional(wl, delay_ms, "Type Breakdown")
    prox = sim_proxy      (wl, delay_ms, "Type Breakdown")
    return {"traditional": trad.to_dict(), "proxy": prox.to_dict(),
            "delay_ms": delay_ms}


def scenario_read_write_sweep(delay_ms: float = 50.0) -> dict:
    """Sweep read fraction from 30 % to 95 %.
    Higher read ratios → more cache benefit."""
    out = {}
    for rfrac in [0.3, 0.5, 0.7, 0.8, 0.9, 0.95]:
        wl   = workload_read_write_ratio(800, rfrac)
        key  = str(int(rfrac * 100))
        out[key] = {
            "traditional": sim_traditional(wl, delay_ms, f"RW {key}%").to_dict(),
            "proxy":       sim_proxy      (wl, delay_ms, f"RW {key}%").to_dict(),
            "read_frac": rfrac,
        }
    return out


# ── Pretty-print tables ───────────────────────────────────────────────────────

def _hr(cols, char="─"):
    return char.join(char * c for c in cols)


def table_overall_summary(results: dict) -> str:
    """Table 1 — overall stats for the mixed workload at 50 ms."""
    bd = results["type_breakdown"]
    T  = bd["traditional"]
    P  = bd["proxy"]

    def sp(t, p): return f"{t/p:.2f}x" if p else "—"

    rows = [
        ("Avg Latency",   f"{T['avg_ms']:.1f}",  f"{P['avg_ms']:.1f}",  sp(T['avg_ms'],  P['avg_ms']),  "ms"),
        ("P50 Latency",   f"{T['p50_ms']:.1f}",  f"{P['p50_ms']:.1f}",  sp(T['p50_ms'],  P['p50_ms']),  "ms"),
        ("P95 Latency",   f"{T['p95_ms']:.1f}",  f"{P['p95_ms']:.1f}",  sp(T['p95_ms'],  P['p95_ms']),  "ms"),
        ("P99 Latency",   f"{T['p99_ms']:.1f}",  f"{P['p99_ms']:.1f}",  sp(T['p99_ms'],  P['p99_ms']),  "ms"),
        ("Throughput",    f"{T['throughput_qps']:.1f}", f"{P['throughput_qps']:.1f}",
                          sp(P['throughput_qps'], T['throughput_qps']), "QPS"),
        ("Cache Hit Rate","—",  f"{P['cache_hit_rate']*100:.1f}%", "—", ""),
        ("Total Queries", str(T['n']), str(P['n']), "—", ""),
    ]

    w = [22, 15, 15, 10, 6]
    sep = "  ".join("─" * c for c in w)
    hdr = (f"  {'Metric':<{w[0]-2}}  {'Traditional':>{w[1]}}  "
           f"{'Proxy':>{w[2]}}  {'Speedup':>{w[3]}}  {'Unit':>{w[4]}}")
    lines = ["", "Table 1 — Overall Summary  (Mixed workload, 50 ms network delay)",
             sep, hdr, sep]
    for name, tv, pv, spd, unit in rows:
        lines.append(f"  {name:<{w[0]-2}}  {tv:>{w[1]}}  {pv:>{w[2]}}  {spd:>{w[3]}}  {unit}")
    lines.append(sep)
    return "\n".join(lines)


def table_type_breakdown(results: dict) -> str:
    """Table 2 — per-query-type latency at 50 ms."""
    bd = results["type_breakdown"]
    T  = bd["traditional"]
    P  = bd["proxy"]

    label = {"A": "Type A  (Aggregate/Join)",
             "B": "Type B  (Point Lookup)",
             "INSERT": "INSERT          ",
             "C": "Type C  (UPDATE/DELETE)"}

    w = [26, 10, 10, 10, 12, 8]
    sep = "  ".join("─"*c for c in w)
    hdr = (f"  {'Query Type':<{w[0]-2}}  {'Trad Avg':>{w[1]}}  "
           f"{'Proxy Avg':>{w[2]}}  {'Speedup':>{w[3]}}  "
           f"{'Cache Hit%':>{w[4]}}  {'Count':>{w[5]}}")
    lines = ["",
             "Table 2 — Per-Query-Type Breakdown  (50 ms network delay)",
             sep, hdr, sep]

    for qt in ["A", "B", "INSERT", "C"]:
        ts  = T["type_stats"][qt]
        ps  = P["type_stats"][qt]
        ta  = ts["avg_ms"]
        pa  = ps["avg_ms"]
        spd = f"{ta/pa:.1f}x" if pa else "—"
        if qt == "B":
            hit = f"{P['cache_hit_rate']*100:.1f}%"
        else:
            hit = "—"
        cnt = ts.get("count", 0)
        lines.append(
            f"  {label[qt]:<{w[0]-2}}  {ta:>{w[1]}.1f}  {pa:>{w[2]}.1f}  "
            f"{spd:>{w[3]}}  {hit:>{w[4]}}  {cnt:>{w[5]}}"
        )
    lines.append(sep)
    return "\n".join(lines)


def table_insert_speedup(results: dict) -> str:
    """Table 3 — INSERT throughput at varying network delays."""
    ins = results["insert_throughput"]
    delays = [0, 25, 50, 100]

    w = [14, 14, 14, 12, 12, 10]
    sep = "  ".join("─"*c for c in w)
    hdr = (f"  {'Net Delay':<{w[0]-2}}  {'Trad Avg':>{w[1]}}  "
           f"{'Proxy Avg':>{w[2]}}  {'Trad QPS':>{w[3]}}  "
           f"{'Proxy QPS':>{w[4]}}  {'Speedup':>{w[5]}}")
    lines = ["",
             "Table 3 — INSERT Throughput  (Lazy-Write vs Eager-Write)",
             sep, hdr, sep]

    for d in delays:
        T   = ins[str(d)]["traditional"]
        P   = ins[str(d)]["proxy"]
        spd = f"{T['avg_ms']/P['avg_ms']:.0f}x"
        lines.append(
            f"  {str(d)+' ms':<{w[0]-2}}  "
            f"{T['avg_ms']:>{w[1]}.2f}  "
            f"{P['avg_ms']:>{w[2]}.2f}  "
            f"{T['throughput_qps']:>{w[3]}.1f}  "
            f"{P['throughput_qps']:>{w[4]}.1f}  "
            f"{spd:>{w[5]}}"
        )
    lines.append(sep)
    lines.append("  (all times in ms, QPS = queries per second, lazy INSERT pays only local cost)")
    return "\n".join(lines)


def table_latency_sensitivity(results: dict) -> str:
    """Table 4 — throughput speedup across network-delay values (mixed workload)."""
    ls   = results["latency_sensitivity"]
    delays = [0, 10, 25, 50, 100, 200]

    w = [14, 14, 14, 12, 12, 10, 14]
    sep = "  ".join("─"*c for c in w)
    hdr = (f"  {'Net Delay':<{w[0]-2}}  "
           f"{'Trad Avg':>{w[1]}}  {'Proxy Avg':>{w[2]}}  "
           f"{'Trad QPS':>{w[3]}}  {'Proxy QPS':>{w[4]}}  "
           f"{'Speedup':>{w[5]}}  {'Cache Hit%':>{w[6]}}")
    lines = ["",
             "Table 4 — Mixed Workload Across Network Delays",
             sep, hdr, sep]

    for d in delays:
        T   = ls[str(d)]["traditional"]
        P   = ls[str(d)]["proxy"]
        spd = f"{P['throughput_qps'] / T['throughput_qps']:.2f}x"
        lines.append(
            f"  {str(d)+' ms':<{w[0]-2}}  "
            f"{T['avg_ms']:>{w[1]}.2f}  {P['avg_ms']:>{w[2]}.2f}  "
            f"{T['throughput_qps']:>{w[3]}.1f}  {P['throughput_qps']:>{w[4]}.1f}  "
            f"{spd:>{w[5]}}  "
            f"{P['cache_hit_rate']*100:>{w[6]}.1f}%"
        )
    lines.append(sep)
    return "\n".join(lines)


def table_rw_sweep(results: dict) -> str:
    """Table 5 — effect of read fraction on overall speedup (50 ms)."""
    rws = results["rw_sweep"]
    fracs = [30, 50, 70, 80, 90, 95]

    w = [14, 14, 14, 12, 12, 10, 14]
    sep = "  ".join("─"*c for c in w)
    hdr = (f"  {'Read %':<{w[0]-2}}  "
           f"{'Trad Avg':>{w[1]}}  {'Proxy Avg':>{w[2]}}  "
           f"{'Trad QPS':>{w[3]}}  {'Proxy QPS':>{w[4]}}  "
           f"{'Speedup':>{w[5]}}  {'Cache Hit%':>{w[6]}}")
    lines = ["",
             "Table 5 — Read/Write Ratio vs Speedup  (50 ms network delay)",
             sep, hdr, sep]

    for f in fracs:
        T   = rws[str(f)]["traditional"]
        P   = rws[str(f)]["proxy"]
        spd = f"{P['throughput_qps'] / T['throughput_qps']:.2f}x"
        lines.append(
            f"  {str(f)+'%':<{w[0]-2}}  "
            f"{T['avg_ms']:>{w[1]}.2f}  {P['avg_ms']:>{w[2]}.2f}  "
            f"{T['throughput_qps']:>{w[3]}.1f}  {P['throughput_qps']:>{w[4]}.1f}  "
            f"{spd:>{w[5]}}  "
            f"{P['cache_hit_rate']*100:>{w[6]}.1f}%"
        )
    lines.append(sep)
    return "\n".join(lines)


# ── Main ─────────────────────────────────────────────────────────────────────

def run():
    np.random.seed(SEED)
    random.seed(SEED)

    Path("results").mkdir(exist_ok=True)

    print("Running benchmarks …")
    print("  [1/5] Cache warmup scenario")
    sc1 = scenario_cache_warmup(delay_ms=50.0)

    print("  [2/5] INSERT throughput scenario")
    sc2 = scenario_insert_throughput()

    print("  [3/5] Latency sensitivity scenario")
    sc3 = scenario_latency_sensitivity()

    print("  [4/5] Per-type breakdown scenario")
    sc4 = scenario_type_breakdown(delay_ms=50.0)

    print("  [5/5] Read/Write ratio sweep")
    sc5 = scenario_read_write_sweep(delay_ms=50.0)

    results = {
        "cache_warmup":        sc1,
        "insert_throughput":   sc2,
        "latency_sensitivity": sc3,
        "type_breakdown":      sc4,
        "rw_sweep":            sc5,
    }

    out_path = Path("results/benchmark_results.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved → {out_path}")

    # ── Print all tables ─────────────────────────────────────────────────────
    print("\n" + "=" * 75)
    print("  DBIS PROXY CACHING SYSTEM — BENCHMARK RESULTS")
    print("=" * 75)
    print(table_overall_summary(results))
    print(table_type_breakdown(results))
    print(table_insert_speedup(results))
    print(table_latency_sensitivity(results))
    print(table_rw_sweep(results))

    # ── Cache warmup summary ─────────────────────────────────────────────────
    cw_p  = sc1["proxy"]
    cw_t  = sc1["traditional"]
    print(f"\n  Cache Warmup  (Type-B, 50 ms, 200 unique keys)")
    print(f"  Traditional avg latency : {cw_t['avg_ms']:.1f} ms")
    print(f"  Proxy avg latency       : {cw_p['avg_ms']:.1f} ms")
    print(f"  Final cache hit rate    : {cw_p['cache_hit_rate']*100:.1f}%")
    print(f"  Speedup                 : {cw_t['avg_ms']/cw_p['avg_ms']:.2f}x")
    print()

    return results


if __name__ == "__main__":
    run()
