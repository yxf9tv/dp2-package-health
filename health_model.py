"""Task 5 scoring math: metrics -> signed z-scores -> dimension subscores -> weighted score.

Pure functions (no database), so every step is easy to test and explain. The metrics
are read from mart_health_inputs; nothing here recomputes a duration.

Size normalization: every scored metric is a share, a median, a ratio or a percent
trend. Volume measures (commits per week, contributors per month, releases per year)
are left out of the score, so a large project cannot win by size alone.
"""

from typing import NamedTuple

import pandas as pd


class Metric(NamedTuple):
    column: str  # column in mart_health_inputs
    dimension: str
    higher_is_better: bool
    definition: str


METRICS = [
    Metric("pct_reviewed_within_7d", "responsiveness", True,
           "Share of human PRs (at least 7 days old) with a first non-author, non-bot "
           "review within 7 days."),
    Metric("median_hours_to_first_review", "responsiveness", False,
           "Median hours from PR creation to the first non-author, non-bot review."),
    Metric("approving_review_share", "responsiveness", True,
           "Share of merged human PRs with a non-author, non-bot APPROVED review "
           "(review quality, not rubber-stamp merges)."),
    Metric("merge_ratio", "throughput", True,
           "Merged / (merged + closed unmerged) over human PRs in the window."),
    Metric("median_hours_to_merge", "throughput", False,
           "Median hours from PR creation to merge, over merged human PRs."),
    Metric("stale_open_share", "backlog", False,
           "Share of open human PRs with no update in 180 days."),
    Metric("commit_trend_pct", "activity", True,
           "Slope of monthly human commits over complete months / mean monthly commits "
           "(percent change per month; the trend component)."),
    Metric("commit_cv", "activity", False,
           "Coefficient of variation of monthly human commits over complete months "
           "(lower = steadier activity)."),
    Metric("release_gap_cv", "release_cadence", False,
           "Coefficient of variation of days between releases (lower = more regular)."),
    Metric("top_author_commit_share", "contributor_base", False,
           "Share of linked human commits by the single most active author (bus-factor risk)."),
]

# Weights follow the order of ASSESSING-HEALTH.md: responsiveness first, then the bus
# factor and recent, steady activity (trend + consistency), then release practice and
# backlog; throughput sits in the middle because merge speed also depends on review
# policy. They sum to 1.
WEIGHTS = {
    "responsiveness": 0.25,
    "contributor_base": 0.20,
    "activity": 0.20,
    "throughput": 0.15,
    "release_cadence": 0.10,
    "backlog": 0.10,
}


def signed_z_scores(inputs: pd.DataFrame, metrics: list[Metric] = METRICS) -> pd.DataFrame:
    """Long table: repo, metric, dimension, value, z, signed_z (positive = healthier).

    z uses the population std (ddof=0) across the repos. A metric with no spread, or a
    missing value, scores 0 (neutral) so it cannot decide the ranking.
    """
    rows = []
    for m in metrics:
        values = inputs.set_index("repo")[m.column].astype(float)
        std = values.std(ddof=0)
        z = (values - values.mean()) / std if std and std > 0 else values * 0.0
        z = z.fillna(0.0)
        sign = 1.0 if m.higher_is_better else -1.0
        for repo in values.index:
            rows.append({"repo": repo, "metric": m.column, "dimension": m.dimension,
                         "higher_is_better": m.higher_is_better, "value": values[repo],
                         "z": z[repo], "signed_z": sign * z[repo]})
    return pd.DataFrame(rows)


def subscores(scores: pd.DataFrame) -> pd.DataFrame:
    """Wide table: one row per repo, one column per dimension (mean signed z).

    The mean, not the sum, so a dimension with two metrics does not count double.
    """
    return scores.pivot_table(index="repo", columns="dimension", values="signed_z",
                              aggfunc="mean")


def rank_repos(subs: pd.DataFrame, weights: dict[str, float] = WEIGHTS) -> pd.DataFrame:
    """Weighted sum of the subscores -> score; rank 1 = healthiest."""
    if abs(sum(weights.values()) - 1.0) > 1e-9:
        raise ValueError(f"weights must sum to 1, got {sum(weights.values())}")
    missing = set(weights) - set(subs.columns)
    if missing:
        raise ValueError(f"no metrics for weighted dimensions: {sorted(missing)}")
    ranked = subs[list(weights)].copy()
    ranked["score"] = sum(ranked[d] * w for d, w in weights.items())
    ranked = ranked.sort_values("score", ascending=False)
    ranked.insert(0, "rank", range(1, len(ranked) + 1))
    return ranked.reset_index()
