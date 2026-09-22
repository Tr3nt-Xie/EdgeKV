#!/usr/bin/env python3
"""Render the charts in docs/charts/ from bench/results/local.jsonl.

    python3 bench/plot.py [bench/results/local.jsonl]
"""
import json
import sys
from collections import OrderedDict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

src = Path(sys.argv[1] if len(sys.argv) > 1 else "bench/results/local.jsonl")
out = Path("docs/charts")
out.mkdir(parents=True, exist_ok=True)

runs = OrderedDict()
for line in src.read_text().splitlines():
    if line.strip():
        r = json.loads(line)
        runs[r["name"]] = r  # last run with a name wins

def lat(r, op="all", p="p99"):
    return r["latency_ms"].get(op, {}).get(p, 0)

def bar(names, values, title, ylabel, fname, labels=None, color="#2b5797"):
    labels = labels or names
    fig, ax = plt.subplots(figsize=(7, 4))
    bars = ax.bar(labels, values, color=color)
    for b, v in zip(bars, values):
        ax.text(b.get_x() + b.get_width() / 2, b.get_height(), f"{v:,.0f}" if v >= 10 else f"{v:.2f}",
                ha="center", va="bottom", fontsize=9)
    ax.set_title(title); ax.set_ylabel(ylabel); ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout(); fig.savefig(out / fname, dpi=150); plt.close(fig)
    print("wrote", out / fname)

# 1. topology
names = [n for n in ["topo-1node-1shard", "topo-3node-1shard", "topo-3node-3shard", "topo-3node-6shard"] if n in runs]
if names:
    bar(names, [runs[n]["ops_per_s"] for n in names], "Throughput by topology (80/15/5, 50 clients, fsync on)",
        "ops/s", "topology-throughput.png", labels=["1 node\n1 shard", "3 nodes\n1 shard", "3 nodes\n3 shards", "3 nodes\n6 shards"][:len(names)])
    fig, ax = plt.subplots(figsize=(7, 4))
    for op, c in [("put", "#c0392b"), ("get", "#2b5797")]:
        ax.plot(range(len(names)), [lat(runs[n], op, "p99") for n in names], "o-", label=f"{op} p99", color=c)
        ax.plot(range(len(names)), [lat(runs[n], op, "p50") for n in names], "s--", label=f"{op} p50", color=c, alpha=.5)
    ax.set_xticks(range(len(names))); ax.set_xticklabels(["1n/1s", "3n/1s", "3n/3s", "3n/6s"][:len(names)])
    ax.set_ylabel("latency (ms)"); ax.set_title("Latency by topology"); ax.legend(); ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout(); fig.savefig(out / "topology-latency.png", dpi=150); plt.close(fig); print("wrote topology-latency.png")

# 2. fsync
if "fsync-on" in runs and "fsync-off" in runs:
    fig, axes = plt.subplots(1, 2, figsize=(9, 4))
    for ax, (metric, title) in zip(axes, [("ops_per_s", "Write throughput"), ("p99", "Write p99 latency (ms)")]):
        vals = [runs[n][metric] if metric == "ops_per_s" else lat(runs[n], "put", "p99") for n in ["fsync-on", "fsync-off"]]
        bs = ax.bar(["fsync on\n(durable)", "fsync off\n(unsafe)"], vals, color=["#2b5797", "#999"])
        for b, v in zip(bs, vals):
            ax.text(b.get_x() + b.get_width() / 2, b.get_height(), f"{v:,.1f}", ha="center", va="bottom", fontsize=9)
        ax.set_title(title); ax.spines[["top", "right"]].set_visible(False)
    fig.suptitle("The price of durability (3 nodes, 3 shards, 100% PUT, 50 clients)")
    fig.tight_layout(); fig.savefig(out / "fsync-cost.png", dpi=150); plt.close(fig); print("wrote fsync-cost.png")

# 3. concurrency
cl = sorted([(r["clients"], r) for n, r in runs.items() if n.startswith("clients-")])
if cl:
    fig, ax1 = plt.subplots(figsize=(7, 4))
    xs = [c for c, _ in cl]
    ax1.plot(xs, [r["ops_per_s"] for _, r in cl], "o-", color="#2b5797", label="throughput")
    ax1.set_xscale("log"); ax1.set_xlabel("concurrent clients"); ax1.set_ylabel("ops/s", color="#2b5797")
    ax2 = ax1.twinx()
    ax2.plot(xs, [lat(r, "all", "p50") for _, r in cl], "s--", color="#c0392b", label="p50")
    ax2.plot(xs, [lat(r, "all", "p95") for _, r in cl], "^--", color="#e67e22", label="p95")
    ax2.plot(xs, [lat(r, "all", "p99") for _, r in cl], "d--", color="#8e44ad", label="p99")
    ax2.set_ylabel("latency (ms)")
    h1, l1 = ax1.get_legend_handles_labels(); h2, l2 = ax2.get_legend_handles_labels()
    ax1.legend(h1 + h2, l1 + l2, loc="upper left"); ax1.set_title("Throughput and tail latency vs concurrency")
    fig.tight_layout(); fig.savefig(out / "concurrency.png", dpi=150); plt.close(fig); print("wrote concurrency.png")

# 4. distribution
if "dist-uniform" in runs and "dist-zipf" in runs:
    fig, ax = plt.subplots(figsize=(6, 4))
    w = 0.35
    for i, p in enumerate(["p50", "p99"]):
        ax.bar([i - w / 2, i + w / 2], [lat(runs["dist-uniform"], "all", p), lat(runs["dist-zipf"], "all", p)],
               w, label=["uniform", "zipf"] if i == 0 else None, color=["#2b5797", "#c0392b"])
    ax.set_xticks([0, 1]); ax.set_xticklabels(["p50", "p99"]); ax.set_ylabel("latency (ms)")
    ax.set_title(f"Uniform vs Zipf keys  ({runs['dist-uniform']['ops_per_s']:,.0f} vs {runs['dist-zipf']['ops_per_s']:,.0f} ops/s)")
    from matplotlib.patches import Patch
    ax.legend(handles=[Patch(color="#2b5797", label="uniform"), Patch(color="#c0392b", label="zipf")])
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout(); fig.savefig(out / "distribution.png", dpi=150); plt.close(fig); print("wrote distribution.png")

# 5. cache
cache = [n for n in ["cache-origin", "cache-edge-ttl5", "cache-edge-ttl30", "cache-edge-ttl60", "cache-invalidation-failed-ttl5"] if n in runs]
if len(cache) > 1:
    fig, axes = plt.subplots(1, 3, figsize=(13, 4))
    labels = {"cache-origin": "origin\n(no cache)", "cache-edge-ttl5": "edge\nTTL 5s", "cache-edge-ttl30": "edge\nTTL 30s",
              "cache-edge-ttl60": "edge\nTTL 60s", "cache-invalidation-failed-ttl5": "edge TTL 5s\ninvalidation\nFAILING"}
    lab = [labels[n] for n in cache]
    hit = [100 * runs[n].get("cache_hits", 0) / max(1, runs[n].get("cache_reads", 1)) for n in cache]
    # Every cached read that hit the edge did not reach the origin.
    axes[0].bar(lab, hit, color="#27ae60"); axes[0].set_title("Origin requests avoided (%)"); axes[0].set_ylim(0, 100)
    axes[1].bar(lab, [lat(runs[n], "cget", "p50") for n in cache], color="#2b5797"); axes[1].set_title("Cached-read p50 latency (ms)")
    stale = [100 * runs[n].get("stale_reads", 0) / max(1, runs[n].get("cache_reads", 1)) for n in cache]
    mx = [runs[n].get("max_staleness_ms", 0) for n in cache]
    ax = axes[2]; ax.bar(lab, stale, color="#c0392b"); ax.set_title("Stale reads (%)  — label: max observed staleness")
    for i, (sv, m) in enumerate(zip(stale, mx)):
        ax.text(i, sv, f"{sv:.2f}%\nmax {m:.0f} ms", ha="center", va="bottom", fontsize=8)
    ax.set_ylim(0, max(stale) * 1.4 + 0.1)
    for a in axes: a.spines[["top", "right"]].set_visible(False); a.tick_params(axis="x", labelsize=8)
    fig.suptitle("Edge caching: 90% cached GET / 10% PUT on cacheable keys, 50 clients")
    fig.tight_layout(); fig.savefig(out / "cache.png", dpi=150); plt.close(fig); print("wrote cache.png")

# summary table
rows = ["| run | clients | ops/s | p50 ms | p95 ms | p99 ms | errors | cache hit % | stale % | max staleness ms |", "|---|---|---|---|---|---|---|---|---|---|"]
for n, r in runs.items():
    cr = r.get("cache_reads", 0)
    rows.append(f"| {n} | {r['clients']} | {r['ops_per_s']:,.0f} | {lat(r,'all','p50'):.2f} | {lat(r,'all','p95'):.2f} | {lat(r,'all','p99'):.2f} | {r['errors']} | "
                f"{100*r.get('cache_hits',0)/cr:.1f} | {100*r.get('stale_reads',0)/cr:.2f} | {r.get('max_staleness_ms',0):.0f} |" if cr else
                f"| {n} | {r['clients']} | {r['ops_per_s']:,.0f} | {lat(r,'all','p50'):.2f} | {lat(r,'all','p95'):.2f} | {lat(r,'all','p99'):.2f} | {r['errors']} | – | – | – |")
(out / "summary.md").write_text("\n".join(rows) + "\n")
print("wrote", out / "summary.md")
