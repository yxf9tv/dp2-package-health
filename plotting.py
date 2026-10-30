"""Task 6 plot functions: DataFrames in, PNG files out (no database access here).

Each repo has one fixed color and marker in every plot (by repo, never by rank).
The palette passed the dataviz validator (CVD worst pair dE 9.2; normal-vision 24.0).
Aqua is below 3:1 contrast on white, so every plot also has a legend and direct labels.
"""

from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # no display needed; the flow only writes files
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

REPO_STYLE = {  # repo -> (short name, color, marker)
    "scikit-learn/scikit-learn": ("scikit-learn", "#2a78d6", "o"),
    "dask/dask": ("dask", "#eb6834", "s"),
    "plotly/plotly.py": ("plotly", "#1baf7a", "^"),
}
TEXT, MUTED, GRID = "#2b2b2a", "#6b6a64", "#e4e3df"
DIMENSION_LABELS = {
    "responsiveness": "Responsiveness",
    "contributor_base": "Contributor base",
    "activity": "Activity",
    "throughput": "Throughput",
    "release_cadence": "Release cadence",
    "backlog": "Backlog",
}

plt.rcParams.update({
    "font.size": 10, "axes.titlesize": 11, "axes.titleweight": "bold",
    "axes.edgecolor": MUTED, "axes.labelcolor": TEXT, "text.color": TEXT,
    "xtick.color": MUTED, "ytick.color": MUTED, "axes.grid": True,
    "grid.color": GRID, "grid.linewidth": 0.8, "axes.axisbelow": True, "axes.spines.top": False,
    "axes.spines.right": False, "figure.facecolor": "#fcfcfb", "axes.facecolor": "#fcfcfb",
})


def _style(repo: str) -> tuple[str, str, str]:
    """Short name, color and marker of a repo (gray for an unknown repo)."""
    return REPO_STYLE.get(repo, (repo, MUTED, "D"))


def _save(fig: plt.Figure, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


# ---------------------------------------------------------------------------
# 1. Dimension profile: one panel per repo in rank order, one bar per dimension
# ---------------------------------------------------------------------------
def plot_dimension_profile(ranking: pd.DataFrame, path: Path) -> Path:
    """Anchored subscores (0-1) per dimension; panel titles carry rank and score."""
    dims = [d for d in DIMENSION_LABELS if d in ranking.columns]
    ranking = ranking.sort_values("rank")
    fig, axes = plt.subplots(1, len(ranking), figsize=(4.2 * len(ranking), 4.2), sharey=True)
    for ax, (_, row) in zip(axes, ranking.iterrows()):
        name, color, _ = _style(row["repo"])
        values = [row[d] for d in dims]
        ax.barh(range(len(dims)), values, color=color, height=0.6, edgecolor="#fcfcfb",
                linewidth=2)
        ax.axvline(0.5, color=MUTED, linewidth=1, linestyle=(0, (3, 3)))  # midpoint
        for y, v in enumerate(values):  # direct value labels in text ink
            ax.text(v + 0.02, y, f"{v:.2f}", va="center", ha="left", fontsize=9, color=TEXT)
        ax.set_title(f"#{int(row['rank'])} {name}  (score {row['score']:.2f})", color=TEXT)
        ax.set_xlim(0, 1.15)  # room for value labels
        ax.set_xlabel("subscore (0 = bad anchor, 1 = good anchor)")
        ax.grid(axis="y", visible=False)
        ax.tick_params(axis="y", length=0)
    axes[0].set_yticks(range(len(dims)), [DIMENSION_LABELS[d] for d in dims])
    axes[0].invert_yaxis()
    fig.suptitle("Health profile by dimension, ranked healthiest to least healthy",
                 fontsize=13, fontweight="bold", y=1.02)
    return _save(fig, path)


# ---------------------------------------------------------------------------
# 2. Trends: monthly small multiples, one panel per metric, one line per repo
# ---------------------------------------------------------------------------
TREND_PANELS = [  # column, title, y label, log scale
    ("commits", "Commits", "commits per month", False),
    ("prs_opened", "PRs opened", "PRs per month", False),
    ("active_contributors", "Active PR authors", "authors per month", False),
    ("merge_ratio", "Merge ratio", "merged / (merged + closed unmerged)", False),
    ("median_first_review_hours", "Median time to first review", "hours (log scale)", True),
    ("median_merge_hours", "Median time to merge", "hours (log scale)", True),
]


def plot_monthly_trends(monthly: pd.DataFrame, path: Path) -> Path:
    """Six panels over the full window; the partial run month is shaded."""
    monthly = monthly.sort_values("month")
    fig, axes = plt.subplots(2, 3, figsize=(15, 8), sharex=True)
    partial = monthly.loc[monthly["is_partial_month"], "month"]
    for ax, (col, title, ylabel, log) in zip(axes.flat, TREND_PANELS):
        for repo, g in monthly.groupby("repo"):
            name, color, marker = _style(repo)
            ax.plot(g["month"], g[col], color=color, linewidth=2, marker=marker,
                    markersize=4, label=name)
            last = g.dropna(subset=[col]).tail(1)
            if not last.empty:  # direct label at the line end
                ax.annotate(name, (last["month"].iloc[0], last[col].iloc[0]),
                            xytext=(4, 0), textcoords="offset points", fontsize=8,
                            color=TEXT, va="center")
        if not partial.empty:
            start = partial.iloc[0]
            ax.axvspan(start, start + pd.offsets.MonthEnd(1), color=GRID, alpha=0.7, lw=0)
        ax.set_title(title)
        ax.set_ylabel(ylabel)
        if log:
            ax.set_yscale("log")
        if col == "merge_ratio":
            ax.set_ylim(0, 1.05)
    for ax in axes[-1]:
        ax.set_xlabel("month (UTC)")
        ax.tick_params(axis="x", rotation=45)
    handles, labels = axes.flat[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=len(labels), frameon=False,
               bbox_to_anchor=(0.5, 1.0))
    fig.suptitle("Monthly activity, human PRs and commits (shaded = partial current month)",
                 fontsize=13, fontweight="bold", y=1.04)
    fig.tight_layout()
    return _save(fig, path)


# ---------------------------------------------------------------------------
# 3. Relationship: review speed vs merge ratio, bubble size = active authors
# ---------------------------------------------------------------------------
def plot_review_vs_merge(monthly: pd.DataFrame, path: Path) -> Path:
    """One bubble per complete repo-month: x, y, size and color = 4 variables."""
    data = monthly[~monthly["is_partial_month"]].dropna(
        subset=["median_first_review_hours", "merge_ratio"])
    fig, ax = plt.subplots(figsize=(9, 6))
    scale = 400 / max(1, data["active_contributors"].max())
    for repo, g in data.groupby("repo"):
        name, color, marker = _style(repo)
        ax.scatter(g["median_first_review_hours"], g["merge_ratio"],
                   s=g["active_contributors"] * scale + 10, color=color, marker=marker,
                   alpha=0.55, edgecolor="#fcfcfb", linewidth=1.5, label=name)
        # repo median as a labeled cross, so each cloud has a readable center
        cx, cy = g["median_first_review_hours"].median(), g["merge_ratio"].median()
        ax.scatter([cx], [cy], marker="X", s=140, color=color, edgecolor=TEXT, linewidth=1)
        ax.annotate(f"{name} median", (cx, cy), xytext=(8, 6), textcoords="offset points",
                    fontsize=9, color=TEXT)
    ax.set_xscale("log")
    ax.set_ylim(0, 1.05)
    ax.set_xlabel("median time to first review in the month (hours, log scale)")
    ax.set_ylabel("merge ratio in the month (merged / (merged + closed unmerged))")
    ax.set_title("Faster review vs. merge ratio by month (bubble size = active PR authors)")
    legend = ax.legend(title="repo (one bubble per month)", frameon=False, loc="lower left")
    for handle in legend.legend_handles:
        handle.set_sizes([60])
    ax.add_artist(legend)
    # Size key: gray proxy bubbles for three author counts
    top = int(data["active_contributors"].max())
    keys = sorted({max(1, round(top * f)) for f in (0.1, 0.5, 1.0)})
    proxies = [ax.scatter([], [], s=k * scale + 10, color=MUTED, alpha=0.4) for k in keys]
    ax.legend(proxies, [str(k) for k in keys], title="active PR authors", frameon=False,
              loc="lower right", labelspacing=1.4, borderpad=1)
    return _save(fig, path)
