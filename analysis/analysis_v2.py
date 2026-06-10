#!/usr/bin/env python3
"""
══════════════════════════════════════════════════════════════
Advanced Visualization & Experiment Analysis v2.0
Self-Optimizing Distributed Transaction Scheduling System
══════════════════════════════════════════════════════════════

Generates research-grade graphs:
  1. Latency over Time (all experiments)
  2. Algorithm Switching Timeline
  3. Scoring Heatmap (all 6 algorithms over time)
  4. P50/P95/P99 Comparison (static vs adaptive vs adaptive+LB)
  5. Queue Depth + Load Variance
  6. Worker Load Distribution (heatmap)
  7. Throughput Comparison
  8. CDF Comparison
  9. Full Dashboard

Usage:
  python3 analysis_v2.py [--mode single|experiment|all]
"""

import os, sys, warnings, argparse
warnings.filterwarnings('ignore')

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.ticker import MaxNLocator
from scipy.ndimage import gaussian_filter1d

os.makedirs("metrics", exist_ok=True)
os.makedirs("analysis/graphs_v2", exist_ok=True)

# ── Palette (trading terminal aesthetic) ─────────────────────
PAL = {
    'bg':       '#0D1117',
    'surface':  '#161B22',
    'border':   '#30363D',
    'text':     '#E6EDF3',
    'muted':    '#8B949E',
    'green':    '#3FB950',
    'red':      '#F85149',
    'yellow':   '#D29922',
    'blue':     '#58A6FF',
    'purple':   '#BC8CFF',
    'teal':     '#39D353',
    'orange':   '#FFA657',
}

ALGO_COLORS = {
    'FCFS':        PAL['blue'],
    'PRIORITY':    PAL['yellow'],
    'ROUND_ROBIN': PAL['teal'],
    'SJF':         PAL['purple'],
    'WRR':         PAL['orange'],
    'MLFQ':        PAL['red'],
}

ALGO_ORDER = ['FCFS', 'PRIORITY', 'ROUND_ROBIN', 'SJF', 'WRR', 'MLFQ']

def dark_style():
    plt.rcParams.update({
        'figure.facecolor':  PAL['bg'],
        'axes.facecolor':    PAL['surface'],
        'axes.edgecolor':    PAL['border'],
        'axes.labelcolor':   PAL['text'],
        'axes.titlecolor':   PAL['text'],
        'xtick.color':       PAL['muted'],
        'ytick.color':       PAL['muted'],
        'text.color':        PAL['text'],
        'grid.color':        PAL['border'],
        'grid.linewidth':    0.6,
        'axes.grid':         True,
        'axes.titlesize':    12,
        'axes.titleweight':  'bold',
        'axes.labelsize':    10,
        'font.size':         10,
        'axes.spines.top':   False,
        'axes.spines.right': False,
        'axes.spines.left':  True,
        'axes.spines.bottom':True,
        'legend.facecolor':  PAL['surface'],
        'legend.edgecolor':  PAL['border'],
        'legend.labelcolor': PAL['text'],
    })

# ── Data loaders ───────────────────────────────────────────────
def load_worker_metrics(version='v2', modes=None):
    """Load all worker CSVs. Returns dict {mode: DataFrame}"""
    results = {}
    if modes is None:
        modes = ['static', 'adaptive', 'adaptive_lb']

    for mode in modes:
        dfs = []
        for w in range(1, 9):
            p = f"metrics/{mode}/worker_{w}_{version}_metrics.csv"
            if not os.path.exists(p):
                p = f"metrics/{mode}/worker_{w}_metrics.csv"
            if os.path.exists(p):
                try:
                    df = pd.read_csv(p)
                    df['mode'] = mode
                    dfs.append(df)
                except: pass
        if dfs:
            df = pd.concat(dfs).sort_values('processed_at_ms').reset_index(drop=True)
            df['time_s'] = (df['processed_at_ms'] - df['processed_at_ms'].min()) / 1000.0
            results[mode] = df
    return results

def load_scheduler_metrics(version='v2', modes=None):
    """Load scheduler CSVs. Returns dict {mode: DataFrame}"""
    results = {}
    if modes is None:
        modes = ['static', 'adaptive', 'adaptive_lb']

    for mode in modes:
        p = f"metrics/{mode}/scheduler_{version}_metrics.csv"
        if not os.path.exists(p):
            p = f"metrics/{mode}/scheduler_metrics.csv"
        if os.path.exists(p):
            try:
                df = pd.read_csv(p)
                df['time_s'] = (df['timestamp_ms'] - df['timestamp_ms'].min()) / 1000.0
                results[mode] = df
            except: pass
    return results

# ── Graph 1: Latency Over Time (all modes) ─────────────────────
def plot_latency_over_time(worker_data, out_dir):
    dark_style()
    fig, axes = plt.subplots(len(worker_data), 1,
                             figsize=(15, 4*len(worker_data)),
                             facecolor=PAL['bg'])
    fig.suptitle("Queue-Wait Latency Over Time", fontsize=14, fontweight='bold',
                 color=PAL['text'], y=1.01)

    if len(worker_data) == 1:
        axes = [axes]

    mode_colors = {
        'static':      PAL['red'],
        'adaptive':    PAL['blue'],
        'adaptive_lb': PAL['green'],
    }
    mode_labels = {
        'static':      'Static FCFS',
        'adaptive':    'Adaptive (6 algos)',
        'adaptive_lb': 'Adaptive + Load Balancing',
    }

    for ax, (mode, df) in zip(axes, worker_data.items()):
        col  = mode_colors.get(mode, PAL['blue'])
        ds   = df.sort_values('time_s')
        win  = max(1, len(ds)//60)
        roll = ds['latency_ms'].rolling(win, min_periods=1).mean()
        smooth = gaussian_filter1d(roll.values, sigma=3)
        mt   = ds['time_s'].max()

        # Phase bands
        ax.axvspan(0,  20,    alpha=0.08, color=PAL['green'],  zorder=0)
        ax.axvspan(20, 40,    alpha=0.12, color=PAL['red'],    zorder=0)
        ax.axvspan(40, mt,    alpha=0.08, color=PAL['yellow'], zorder=0)

        ax.scatter(ds['time_s'], ds['latency_ms'],
                   alpha=0.08, s=4, color=col, zorder=1)
        ax.fill_between(ds['time_s'], smooth, alpha=0.15, color=col, zorder=2)
        ax.plot(ds['time_s'], smooth, color=col, linewidth=2.2, zorder=3,
                label=mode_labels.get(mode, mode))

        # P95 line
        p95 = df['latency_ms'].quantile(0.95)
        ax.axhline(p95, color=PAL['yellow'], linestyle='--',
                   linewidth=1, alpha=0.7, label=f'P95={p95:.0f}ms')

        mean_v = df['latency_ms'].mean()
        ax.text(0.99, 0.93,
                f"mean={mean_v:.0f}ms  P95={p95:.0f}ms  P99={df['latency_ms'].quantile(.99):.0f}ms",
                transform=ax.transAxes, ha='right', va='top',
                fontsize=9, color=col,
                bbox=dict(boxstyle='round,pad=0.3', facecolor=PAL['bg'], alpha=0.8))

        ax.set_ylabel("Latency (ms)", color=PAL['text'])
        ax.set_title(mode_labels.get(mode, mode), color=col)
        ax.set_xlim(0, mt); ax.set_ylim(bottom=0)
        ax.legend(loc='upper left', fontsize=8, framealpha=0.5)

        # Phase labels
        for x, lbl in [(10,'LOW'),(30,'SPIKE'),(50,'HIGH')]:
            if x < mt:
                ax.text(x, ax.get_ylim()[1]*0.85, lbl, ha='center',
                        fontsize=8, color=PAL['muted'], style='italic')

        if ax == axes[-1]:
            ax.set_xlabel("Time (seconds)", color=PAL['text'])

    plt.tight_layout()
    out = f"{out_dir}/1_latency_over_time.png"
    plt.savefig(out, dpi=150, bbox_inches='tight', facecolor=PAL['bg'])
    plt.close()
    print(f"✔  {out}")

# ── Graph 2: Algorithm Switching Timeline ─────────────────────
def plot_algo_timeline(sched_data, out_dir):
    if not sched_data:
        print("  Skipping algo timeline (no scheduler data)")
        return

    dark_style()
    modes = [m for m in ['adaptive', 'adaptive_lb'] if m in sched_data]
    if not modes:
        modes = list(sched_data.keys())[:1]

    fig, axes = plt.subplots(len(modes), 1, figsize=(15, 3.5*len(modes)),
                             facecolor=PAL['bg'])
    if len(modes) == 1: axes = [axes]
    fig.suptitle("Algorithm Switching Timeline", fontsize=14, fontweight='bold',
                 color=PAL['text'])

    for ax, mode in zip(axes, modes):
        df = sched_data[mode]
        mt = df['time_s'].max()

        # Draw coloured bands for each algorithm region
        prev_t = df['time_s'].iloc[0]
        prev_s = df['algorithm'].iloc[0] if 'algorithm' in df.columns else \
                 df['strategy'].iloc[0] if 'strategy' in df.columns else 'FCFS'

        algo_col = 'algorithm' if 'algorithm' in df.columns else 'strategy'

        for _, row in df.iterrows():
            s = row[algo_col]
            if s != prev_s:
                ax.axvspan(prev_t, row['time_s'],
                           alpha=0.35,
                           color=ALGO_COLORS.get(prev_s, PAL['muted']))
                ax.axvline(row['time_s'], color=PAL['border'],
                           linewidth=0.8, alpha=0.5)
                prev_t = row['time_s']
                prev_s = s
        ax.axvspan(prev_t, mt,
                   alpha=0.35,
                   color=ALGO_COLORS.get(prev_s, PAL['muted']))

        # Overlay queue length
        ax2 = ax.twinx()
        ax2.plot(df['time_s'], df['queue_len'],
                 color=PAL['text'], linewidth=1.5, alpha=0.6,
                 label='Queue Length')
        ax2.set_ylabel("Queue Length", color=PAL['muted'])
        ax2.tick_params(colors=PAL['muted'])
        ax2.spines['right'].set_color(PAL['border'])
        ax2.spines['top'].set_visible(False)

        # Legend for algorithms
        patches = [mpatches.Patch(color=ALGO_COLORS.get(a, PAL['muted']),
                                  alpha=0.7, label=a)
                   for a in ALGO_ORDER
                   if a in df[algo_col].values]
        ax.legend(handles=patches, loc='upper left', fontsize=8,
                  framealpha=0.7, ncol=3)

        ax.set_xlim(0, mt); ax.set_ylim(-0.1, 1.1)
        ax.set_yticks([])
        ax.set_ylabel("Algorithm Active", color=PAL['text'])
        ax.set_title(f"Strategy Timeline — {mode}", color=PAL['text'])
        if ax == axes[-1]:
            ax.set_xlabel("Time (seconds)", color=PAL['text'])

    plt.tight_layout()
    out = f"{out_dir}/2_algo_timeline.png"
    plt.savefig(out, dpi=150, bbox_inches='tight', facecolor=PAL['bg'])
    plt.close()
    print(f"✔  {out}")

# ── Graph 3: Algorithm Scoring Heatmap ────────────────────────
def plot_scoring_heatmap(sched_data, out_dir):
    score_cols = ['score_fcfs','score_priority','score_rr',
                  'score_sjf','score_wrr','score_mlfq']
    mode_key = next((m for m in ['adaptive','adaptive_lb'] if m in sched_data), None)
    if mode_key is None or not all(c in sched_data[mode_key].columns for c in score_cols):
        print("  Skipping scoring heatmap (need v2 scheduler CSV with score columns)")
        return

    dark_style()
    df = sched_data[mode_key]
    fig, ax = plt.subplots(figsize=(14, 5), facecolor=PAL['bg'])
    fig.suptitle("Algorithm Scoring Heatmap Over Time\n(darker = lower score = better fit)",
                 fontsize=13, fontweight='bold', color=PAL['text'])

    # Build matrix: rows=algorithms, cols=time ticks
    algo_names = ['FCFS','PRIORITY','RR','SJF','WRR','MLFQ']
    matrix = np.array([df[c].values for c in score_cols])

    cmap = LinearSegmentedColormap.from_list(
        'score', [PAL['green'], PAL['yellow'], PAL['red']])

    im = ax.imshow(matrix, aspect='auto', cmap=cmap,
                   extent=[0, df['time_s'].max(), -0.5, len(algo_names)-0.5])

    ax.set_yticks(range(len(algo_names)))
    ax.set_yticklabels(algo_names, color=PAL['text'])
    ax.set_xlabel("Time (seconds)", color=PAL['text'])
    ax.set_title("Lower score = better algorithm for current conditions",
                 color=PAL['muted'], fontsize=9)

    # Mark algorithm in use
    algo_col = 'algorithm' if 'algorithm' in df.columns else 'strategy'
    algo_idx_map = {a: i for i, a in enumerate(
        ['FCFS','PRIORITY','ROUND_ROBIN','SJF','WRR','MLFQ'])}
    for _, row in df.iterrows():
        a = row.get(algo_col, 'FCFS')
        idx = algo_idx_map.get(a, 0)
        ax.plot(row['time_s'], idx, 'w.', markersize=4, alpha=0.8)

    plt.colorbar(im, ax=ax, label='Score (lower = better)',
                 fraction=0.02, pad=0.02)

    plt.tight_layout()
    out = f"{out_dir}/3_scoring_heatmap.png"
    plt.savefig(out, dpi=150, bbox_inches='tight', facecolor=PAL['bg'])
    plt.close()
    print(f"✔  {out}")

# ── Graph 4: Percentile Comparison Bar Chart ───────────────────
def plot_percentile_comparison(worker_data, out_dir):
    dark_style()
    pcts   = [50, 75, 90, 95, 99]
    modes  = list(worker_data.keys())
    mode_colors = {
        'static':      PAL['red'],
        'adaptive':    PAL['blue'],
        'adaptive_lb': PAL['green'],
    }
    mode_labels = {
        'static':      'Static\nFCFS',
        'adaptive':    'Adaptive\n(6 algos)',
        'adaptive_lb': 'Adaptive\n+Load Bal',
    }

    fig, ax = plt.subplots(figsize=(12, 6), facecolor=PAL['bg'])
    fig.suptitle("Latency Percentiles: Static vs Adaptive vs Adaptive+LB",
                 fontsize=13, fontweight='bold', color=PAL['text'])

    x   = np.arange(len(pcts))
    w   = 0.8 / max(len(modes), 1)

    for i, mode in enumerate(modes):
        df   = worker_data[mode]
        vals = [df['latency_ms'].quantile(p/100) for p in pcts]
        col  = mode_colors.get(mode, PAL['blue'])
        bars = ax.bar(x + i*w - (len(modes)-1)*w/2, vals, w,
                      label=mode_labels.get(mode, mode),
                      color=col, alpha=0.85, edgecolor=PAL['bg'], linewidth=1)
        for bar, val in zip(bars, vals):
            ax.text(bar.get_x()+bar.get_width()/2, bar.get_height()+5,
                    f"{val:.0f}", ha='center', va='bottom',
                    fontsize=8, color=col, fontweight='bold')

    # Improvement annotations (adaptive vs static)
    if 'static' in worker_data and 'adaptive' in worker_data:
        for j, p in enumerate(pcts):
            sv = worker_data['static']['latency_ms'].quantile(p/100)
            av = worker_data['adaptive']['latency_ms'].quantile(p/100)
            imp = (sv - av) / sv * 100
            if imp > 0:
                ax.text(x[j] + 0.3, max(sv, av) * 1.05,
                        f"−{imp:.0f}%", ha='center', fontsize=8,
                        color=PAL['green'], fontweight='bold')

    ax.set_xticks(x)
    ax.set_xticklabels([f"P{p}" for p in pcts])
    ax.set_ylabel("Latency (ms)")
    ax.set_title("Lower is better — green % = Adaptive improvement vs Static",
                 color=PAL['muted'], fontsize=9)
    ax.legend(fontsize=9)

    plt.tight_layout()
    out = f"{out_dir}/4_percentile_comparison.png"
    plt.savefig(out, dpi=150, bbox_inches='tight', facecolor=PAL['bg'])
    plt.close()
    print(f"✔  {out}")

# ── Graph 5: Queue Depth + Load Variance ─────────────────────
def plot_queue_and_load(sched_data, out_dir):
    dark_style()
    n = len(sched_data)
    if n == 0:
        print("  Skipping queue graph (no scheduler data)"); return

    fig, axes = plt.subplots(2, 1, figsize=(14, 8), facecolor=PAL['bg'])
    fig.suptitle("Queue Depth & Load Variance Over Time",
                 fontsize=13, fontweight='bold', color=PAL['text'])

    mode_colors = {
        'static':      PAL['red'],
        'adaptive':    PAL['blue'],
        'adaptive_lb': PAL['green'],
    }

    # Top: queue depth
    ax1 = axes[0]
    for mode, df in sched_data.items():
        col = mode_colors.get(mode, PAL['blue'])
        ax1.plot(df['time_s'], df['queue_len'], color=col,
                 linewidth=2, label=mode, alpha=0.85)
        ax1.fill_between(df['time_s'], df['queue_len'],
                         alpha=0.1, color=col)

    ax1.axhline(50,  color=PAL['yellow'], linestyle='--',
                linewidth=1, alpha=0.7, label='Threshold(50)')
    ax1.axhline(150, color=PAL['red'],    linestyle='--',
                linewidth=1, alpha=0.7, label='Threshold(150)')
    ax1.set_ylabel("Queue Length"); ax1.set_title("Queue Depth")
    ax1.legend(fontsize=9); ax1.set_ylim(bottom=0)

    # Bottom: load variance (only if column exists)
    ax2 = axes[1]
    for mode, df in sched_data.items():
        col = mode_colors.get(mode, PAL['blue'])
        if 'load_var' in df.columns:
            ax2.plot(df['time_s'], df['load_var'], color=col,
                     linewidth=2, label=f'{mode} load_var', alpha=0.85)
        elif 'avg_latency_ms' in df.columns:
            ax2.plot(df['time_s'], df['avg_latency_ms'], color=col,
                     linewidth=2, label=f'{mode} avg_lat', alpha=0.85)
        elif 'avg_latency' in df.columns:
            ax2.plot(df['time_s'], df['avg_latency'], color=col,
                     linewidth=2, label=f'{mode} avg_lat', alpha=0.85)

    ax2.set_xlabel("Time (seconds)")
    ax2.set_ylabel("Load Variance / Avg Latency")
    ax2.set_title("Worker Load Variance (lower = better balanced)")
    ax2.legend(fontsize=9); ax2.set_ylim(bottom=0)

    plt.tight_layout()
    out = f"{out_dir}/5_queue_and_load.png"
    plt.savefig(out, dpi=150, bbox_inches='tight', facecolor=PAL['bg'])
    plt.close()
    print(f"✔  {out}")

# ── Graph 6: Throughput Comparison ────────────────────────────
def plot_throughput(worker_data, out_dir):
    dark_style()
    fig, ax = plt.subplots(figsize=(14, 5), facecolor=PAL['bg'])
    fig.suptitle("Throughput Over Time (transactions/second)",
                 fontsize=13, fontweight='bold', color=PAL['text'])

    mode_colors = {
        'static':      PAL['red'],
        'adaptive':    PAL['blue'],
        'adaptive_lb': PAL['green'],
    }

    for mode, df in worker_data.items():
        col = mode_colors.get(mode, PAL['blue'])
        df2 = df.copy()
        df2['bin'] = (df2['time_s'] // 2 * 2).astype(int)
        tp = df2.groupby('bin').size().reset_index(name='count')
        tp['tps'] = tp['count'] / 2.0
        ax.fill_between(tp['bin'], tp['tps'], alpha=0.12, color=col)
        ax.plot(tp['bin'], tp['tps'], color=col, linewidth=2.2,
                label=mode, marker='o', markersize=3)

    mt = max(df['time_s'].max() for df in worker_data.values())
    ax.axvspan(0,  20,    alpha=0.07, color=PAL['green'], zorder=0)
    ax.axvspan(20, 40,    alpha=0.10, color=PAL['red'],   zorder=0)
    ax.axvspan(40, mt,    alpha=0.07, color=PAL['yellow'],zorder=0)
    for x, lbl in [(10,'LOW'),(30,'SPIKE'),(50,'HIGH')]:
        if x < mt:
            ax.text(x, ax.get_ylim()[1]*0.92 if ax.get_ylim()[1]>0 else 5,
                    lbl, ha='center', fontsize=8, color=PAL['muted'], style='italic')

    ax.set_xlabel("Time (seconds)"); ax.set_ylabel("txn/sec")
    ax.set_ylim(bottom=0); ax.set_xlim(0)
    ax.legend(fontsize=9)

    plt.tight_layout()
    out = f"{out_dir}/6_throughput.png"
    plt.savefig(out, dpi=150, bbox_inches='tight', facecolor=PAL['bg'])
    plt.close()
    print(f"✔  {out}")

# ── Graph 7: CDF Comparison ───────────────────────────────────
def plot_cdf(worker_data, out_dir):
    dark_style()
    fig, axes = plt.subplots(1, 2, figsize=(13, 5), facecolor=PAL['bg'])
    fig.suptitle("Latency Cumulative Distribution (CDF)",
                 fontsize=13, fontweight='bold', color=PAL['text'])

    mode_colors = {
        'static':      PAL['red'],
        'adaptive':    PAL['blue'],
        'adaptive_lb': PAL['green'],
    }
    mode_labels = {
        'static':      'Static FCFS',
        'adaptive':    'Adaptive',
        'adaptive_lb': 'Adaptive + LB',
    }

    # Left: CDF all modes
    ax1 = axes[0]
    for mode, df in worker_data.items():
        col  = mode_colors.get(mode, PAL['blue'])
        sl   = np.sort(df['latency_ms'].values)
        cdf  = np.arange(1, len(sl)+1) / len(sl)
        ax1.plot(sl, cdf*100, color=col, linewidth=2.5,
                 label=mode_labels.get(mode, mode))

    for p, ls in [(50,'--'),(90,':'),(95,'-.')]:
        ax1.axhline(p, color=PAL['border'], linestyle=ls, linewidth=0.8)
        ax1.text(ax1.get_xlim()[1]*0.99 if ax1.get_xlim()[1]>0 else 3000,
                 p+0.5, f'P{p}', ha='right', fontsize=8, color=PAL['muted'])

    ax1.set_xlabel("Latency (ms)"); ax1.set_ylabel("Cumulative %")
    ax1.set_title("Full CDF Comparison"); ax1.legend(); ax1.set_ylim(0, 104)

    # Right: histogram overlay
    ax2 = axes[1]
    for mode, df in worker_data.items():
        col = mode_colors.get(mode, PAL['blue'])
        ax2.hist(df['latency_ms'], bins=50, alpha=0.45, color=col,
                 label=mode_labels.get(mode, mode), edgecolor='none',
                 density=True)

    ax2.set_xlabel("Latency (ms)"); ax2.set_ylabel("Density")
    ax2.set_title("Latency Density Histogram"); ax2.legend()

    plt.tight_layout()
    out = f"{out_dir}/7_cdf_comparison.png"
    plt.savefig(out, dpi=150, bbox_inches='tight', facecolor=PAL['bg'])
    plt.close()
    print(f"✔  {out}")

# ── Graph 8: Summary Statistics Table ─────────────────────────
def print_summary_table(worker_data):
    print("\n" + "═"*72)
    print("  EXPERIMENT RESULTS SUMMARY")
    print("═"*72)
    hdr = f"{'Metric':<25}  " + "  ".join(f"{'['+m+']':>16}" for m in worker_data)
    print(hdr)
    print("─"*72)

    metrics = [
        ("Total txns",      lambda d: len(d)),
        ("Avg latency (ms)",lambda d: f"{d['latency_ms'].mean():.1f}"),
        ("P50 (ms)",        lambda d: f"{d['latency_ms'].quantile(.50):.1f}"),
        ("P90 (ms)",        lambda d: f"{d['latency_ms'].quantile(.90):.1f}"),
        ("P95 (ms)",        lambda d: f"{d['latency_ms'].quantile(.95):.1f}"),
        ("P99 (ms)",        lambda d: f"{d['latency_ms'].quantile(.99):.1f}"),
        ("Min (ms)",        lambda d: f"{d['latency_ms'].min():.1f}"),
        ("Max (ms)",        lambda d: f"{d['latency_ms'].max():.1f}"),
        ("HIGH prio avg",   lambda d: f"{d[d['priority']==1]['latency_ms'].mean():.1f}" if 'priority' in d.columns else "N/A"),
    ]

    for name, fn in metrics:
        row = f"{name:<25}  "
        row += "  ".join(f"{str(fn(df)):>16}" for df in worker_data.values())
        print(row)

    print("═"*72)

    # Improvement vs static
    if 'static' in worker_data:
        print("\n  Improvement vs Static FCFS:")
        print("─"*72)
        for mode in [m for m in worker_data if m != 'static']:
            sv = worker_data['static']['latency_ms'].quantile(.95)
            av = worker_data[mode]['latency_ms'].quantile(.95)
            imp = (sv - av) / sv * 100
            print(f"  [{mode}] P95: {sv:.0f}ms → {av:.0f}ms  ({imp:+.1f}%)")
        print("═"*72)

# ── Master Dashboard ──────────────────────────────────────────
def plot_dashboard(worker_data, sched_data, out_dir):
    dark_style()
    fig = plt.figure(figsize=(22, 16), facecolor=PAL['bg'])
    fig.suptitle(
        "Self-Optimizing Distributed Transaction Scheduling System\n"
        "Full Experiment Dashboard — Adaptive vs Static vs Adaptive+LB",
        fontsize=15, fontweight='bold', color=PAL['text'], y=0.99
    )

    gs = gridspec.GridSpec(3, 4, figure=fig, hspace=0.52, wspace=0.40)

    mode_colors = {
        'static':      PAL['red'],
        'adaptive':    PAL['blue'],
        'adaptive_lb': PAL['green'],
    }
    mode_labels = {
        'static':      'Static FCFS',
        'adaptive':    'Adaptive',
        'adaptive_lb': 'Adaptive+LB',
    }

    # ── Row 0: Latency time series per mode ───────────────────
    for ci, (mode, df) in enumerate(worker_data.items()):
        if ci > 2: break
        ax = fig.add_subplot(gs[0, ci])
        col = mode_colors.get(mode, PAL['blue'])
        ds  = df.sort_values('time_s')
        win = max(1, len(ds)//50)
        roll = ds['latency_ms'].rolling(win, min_periods=1).mean()
        smooth = gaussian_filter1d(roll.values, sigma=2)
        mt = ds['time_s'].max()
        ax.axvspan(0,20,alpha=0.08,color=PAL['green'],zorder=0)
        ax.axvspan(20,40,alpha=0.12,color=PAL['red'],zorder=0)
        ax.axvspan(40,mt,alpha=0.08,color=PAL['yellow'],zorder=0)
        ax.fill_between(ds['time_s'], smooth, alpha=0.2, color=col)
        ax.plot(ds['time_s'], smooth, color=col, linewidth=2)
        p95 = df['latency_ms'].quantile(.95)
        ax.axhline(p95, color=PAL['yellow'], linestyle='--',
                   linewidth=1, alpha=0.6)
        ax.set_title(mode_labels.get(mode, mode), color=col)
        ax.set_ylabel("Latency (ms)")
        ax.text(0.97,0.9,f"P95={p95:.0f}ms",transform=ax.transAxes,
                ha='right',fontsize=8,color=col,
                bbox=dict(boxstyle='round',facecolor=PAL['bg'],alpha=0.7))
        ax.set_xlim(0,mt); ax.set_ylim(bottom=0)

    # Percentile bars (top-right)
    ax_p = fig.add_subplot(gs[0, 3])
    pcts = [50, 90, 95, 99]
    x    = np.arange(len(pcts))
    w    = 0.8 / max(len(worker_data), 1)
    for i, (mode, df) in enumerate(worker_data.items()):
        vals = [df['latency_ms'].quantile(p/100) for p in pcts]
        col  = mode_colors.get(mode, PAL['blue'])
        ax_p.bar(x + i*w - (len(worker_data)-1)*w/2, vals, w,
                 label=mode_labels.get(mode,mode), color=col,
                 alpha=0.85, edgecolor=PAL['bg'])
    ax_p.set_xticks(x); ax_p.set_xticklabels([f"P{p}" for p in pcts], fontsize=8)
    ax_p.set_title("Percentiles"); ax_p.set_ylabel("ms"); ax_p.legend(fontsize=7)

    # ── Row 1: Queue depth + algo timeline + throughput ───────
    # Queue depth
    ax_q = fig.add_subplot(gs[1, :2])
    for mode, sdf in sched_data.items():
        col = mode_colors.get(mode, PAL['blue'])
        ax_q.plot(sdf['time_s'], sdf['queue_len'], color=col,
                  linewidth=2, label=mode_labels.get(mode,mode), alpha=0.85)
    ax_q.axhline(50,  color=PAL['yellow'], linestyle='--', linewidth=1, alpha=0.6)
    ax_q.axhline(150, color=PAL['red'],    linestyle='--', linewidth=1, alpha=0.6)
    ax_q.set_title("Queue Depth Over Time"); ax_q.set_ylabel("Length")
    ax_q.legend(fontsize=8); ax_q.set_ylim(bottom=0)

    # Algo timeline (adaptive only)
    ax_t = fig.add_subplot(gs[1, 2:])
    mode_key = next((m for m in ['adaptive','adaptive_lb'] if m in sched_data), None)
    if mode_key:
        df = sched_data[mode_key]
        algo_col = 'algorithm' if 'algorithm' in df.columns else 'strategy'
        prev_t = df['time_s'].iloc[0]; prev_s = df[algo_col].iloc[0]
        for _, row in df.iterrows():
            s = row[algo_col]
            if s != prev_s:
                ax_t.axvspan(prev_t, row['time_s'], alpha=0.5,
                             color=ALGO_COLORS.get(prev_s, PAL['muted']))
                prev_t = row['time_s']; prev_s = s
        ax_t.axvspan(prev_t, df['time_s'].max(), alpha=0.5,
                     color=ALGO_COLORS.get(prev_s, PAL['muted']))
        ax_t.set_yticks([])
        patches = [mpatches.Patch(color=ALGO_COLORS.get(a,PAL['muted']),
                                  alpha=0.7, label=a)
                   for a in ALGO_ORDER if a in df[algo_col].values]
        ax_t.legend(handles=patches, loc='upper left', fontsize=7,
                    framealpha=0.7, ncol=2)
        ax_t.set_title("Algorithm Switching Timeline (Adaptive)")
        ax_t.set_xlim(0, df['time_s'].max())

    # ── Row 2: CDF + priority benefit + throughput ────────────
    ax_cdf = fig.add_subplot(gs[2, :2])
    for mode, df in worker_data.items():
        col = mode_colors.get(mode, PAL['blue'])
        sl  = np.sort(df['latency_ms'].values)
        cdf = np.arange(1, len(sl)+1) / len(sl)
        ax_cdf.plot(sl, cdf*100, color=col, linewidth=2.2,
                    label=mode_labels.get(mode,mode))
    for p, ls in [(50,'--'),(95,':')]:
        ax_cdf.axhline(p, color=PAL['border'], linestyle=ls, linewidth=0.7)
    ax_cdf.set_xlabel("Latency (ms)"); ax_cdf.set_ylabel("Cumulative %")
    ax_cdf.set_title("CDF Comparison"); ax_cdf.legend(fontsize=8)
    ax_cdf.set_ylim(0, 104)

    # Priority benefit
    ax_pr = fig.add_subplot(gs[2, 2])
    if all('priority' in df.columns for df in worker_data.values()):
        modes_list = list(worker_data.keys())
        hi = [worker_data[m][worker_data[m]['priority']==1]['latency_ms'].mean()
              for m in modes_list]
        no = [worker_data[m][worker_data[m]['priority']==0]['latency_ms'].mean()
              for m in modes_list]
        xm = np.arange(len(modes_list))
        ax_pr.bar(xm-0.2, no, 0.4, color=PAL['blue'], alpha=0.8, label='NORMAL')
        ax_pr.bar(xm+0.2, hi, 0.4, color=PAL['red'],  alpha=0.8, label='HIGH')
        ax_pr.set_xticks(xm)
        ax_pr.set_xticklabels([mode_labels.get(m,m) for m in modes_list],fontsize=7)
        ax_pr.set_ylabel("Avg Latency (ms)")
        ax_pr.set_title("Priority Benefit"); ax_pr.legend(fontsize=8)

    # Throughput
    ax_tp = fig.add_subplot(gs[2, 3])
    for mode, df in worker_data.items():
        col = mode_colors.get(mode, PAL['blue'])
        df2 = df.copy()
        df2['bin'] = (df2['time_s'] // 2 * 2).astype(int)
        tp = df2.groupby('bin').size().reset_index(name='tps')
        tp['tps'] /= 2.0
        ax_tp.plot(tp['bin'], tp['tps'], color=col, linewidth=2,
                   label=mode_labels.get(mode,mode))
    ax_tp.set_xlabel("Time (s)"); ax_tp.set_ylabel("txn/sec")
    ax_tp.set_title("Throughput"); ax_tp.legend(fontsize=7)
    ax_tp.set_ylim(bottom=0)

    out = f"{out_dir}/../dashboard_v2.png"
    plt.savefig(out, dpi=150, bbox_inches='tight', facecolor=PAL['bg'])
    plt.close()
    print(f"✔  {out}  ← MASTER DASHBOARD")

# ── Main ──────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--modes', nargs='+',
                    default=['static','adaptive','adaptive_lb'],
                    help='Which experiment modes to load')
    ap.add_argument('--version', default='v2',
                    help='CSV version suffix (v2 or empty for legacy)')
    args = ap.parse_args()

    dark_style()
    out_dir = "analysis/graphs_v2"
    os.makedirs(out_dir, exist_ok=True)

    print("╔══════════════════════════════════════════════════╗")
    print("║  Advanced Visualization & Analysis v2.0          ║")
    print("╚══════════════════════════════════════════════════╝\n")

    worker_data = load_worker_metrics(args.version, args.modes)
    sched_data  = load_scheduler_metrics(args.version, args.modes)

    if not worker_data:
        print("No data found. Run the simulation first.")
        print(f"Expected: metrics/<mode>/worker_N_{args.version}_metrics.csv")
        print("Trying legacy paths...")
        worker_data = load_worker_metrics('', args.modes)
        if not worker_data:
            print("No data found at any path. Exiting.")
            return

    print(f"Loaded modes: {list(worker_data.keys())}")
    for m, df in worker_data.items():
        print(f"  {m}: {len(df)} transactions")
    print()

    print("Generating graphs...")
    plot_latency_over_time(worker_data, out_dir)
    plot_algo_timeline(sched_data, out_dir)
    plot_scoring_heatmap(sched_data, out_dir)
    plot_percentile_comparison(worker_data, out_dir)
    plot_queue_and_load(sched_data, out_dir)
    plot_throughput(worker_data, out_dir)
    plot_cdf(worker_data, out_dir)
    plot_dashboard(worker_data, sched_data, out_dir)

    print_summary_table(worker_data)
    print(f"\n✔ All graphs → {out_dir}/")

if __name__ == '__main__':
    main()
