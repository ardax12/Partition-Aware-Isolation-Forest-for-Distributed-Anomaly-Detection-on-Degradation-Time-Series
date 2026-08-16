"""Figures from the verified result files (results/results.json, scalability.csv)."""
import csv, json, numpy as np
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from .config import RESULTS, FIGURES

_CFG = [("C2_naive", "Naive distributed", "#9DB2CE"),
        ("C1_ref", "Single-node ref", "#6E8BB8"),
        ("C3_win_unaware", "Windowed (unaware)", "#E0A458"),
        ("C4_proposed", "Proposed (aware)", "#1F3864")]
_SUBS = ["FD001", "FD002", "FD003", "FD004"]


def _grouped(results, metric, ylabel, fname, ylo):
    x = np.arange(len(_SUBS)); w = 0.2
    fig, ax = plt.subplots(figsize=(7, 3.6))
    for i, (k, lab, col) in enumerate(_CFG):
        ax.bar(x + (i - 1.5) * w, [results[s][k][metric] for s in _SUBS], w,
               label=lab, color=col, edgecolor="white", linewidth=0.4)
    ax.set_xticks(x); ax.set_xticklabels(_SUBS); ax.set_ylabel(ylabel)
    ax.set_ylim(ylo, 1.0 if metric == "auc" else 0.8); ax.grid(axis="y", alpha=0.25)
    ax.legend(fontsize=8, ncol=2, loc="upper center", bbox_to_anchor=(0.5, 1.18), frameon=False)
    fig.tight_layout(); fig.savefig(FIGURES / fname, dpi=200, bbox_inches="tight"); plt.close()


def make_detection_figures():
    results = json.load(open(RESULTS / "results.json"))
    _grouped(results, "auc", "ROC-AUC", "fig_auc.png", 0.6)
    _grouped(results, "f1", "F1 score", "fig_f1.png", 0.2)


def make_scalability_figure():
    rows = list(csv.DictReader(open(RESULTS / "scalability.csv")))
    rows = sorted(rows, key=lambda d: float(d["workers"]) if d["workers"].replace('.', '', 1).isdigit() else 0)
    x = [d["workers"] for d in rows]; y = [int(d["throughput_rows_s"]) for d in rows]
    fig, ax = plt.subplots(figsize=(6, 3.8))
    ax.plot(x, y, marker="o", color="#1F3864", lw=2)
    ax.set_xlabel("Number of workers"); ax.set_ylabel("Throughput (rows/s)")
    ax.grid(alpha=0.3); fig.tight_layout()
    fig.savefig(FIGURES / "fig_scalability.png", dpi=200); plt.close()
