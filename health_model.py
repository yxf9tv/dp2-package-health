"""Task 5 scoring math: metrics -> anchored 0-1 scores -> dimension subscores -> weighted score.

Pure functions (no database), so every step is easy to test and explain. The metrics
are read from mart_health_inputs; nothing here recomputes a duration.

Size normalization: every scored metric is a share, a median, a ratio or a percent
trend. Volume measures (commits per week, contributors per month, releases per year)
are left out of the score, so a large project cannot win by size alone.

Main score: each metric is placed between a fixed "bad" anchor (0) and "good" anchor (1),
as OpenSSF Criticality Score (Pike), OSS Compass and Scorecard do with fixed thresholds.
A score is absolute ("how healthy?"), not relative to the other repos, and a small gap
stays small. Hours use a log scale between the anchors. Signed z-scores across the repos
are kept as a sensitivity check.
"""

import math
from typing import NamedTuple

import pandas as pd


class Metric(NamedTuple):
    column: str  # column in mart_health_inputs
    dimension: str
    definition: str
    bad: float  # value that scores 0
    good: float  # value that scores 1
    log_scale: bool = False  # interpolate on log(value): for skewed durations
    reason: str = ""  # why these anchors

    @property
    def higher_is_better(self) -> bool:
        return self.good > self.bad


# Anchors were set from the course guide and common practice, then locked before the
# full-window run. They are judgment calls; the README lists them with these reasons.
METRICS = [
    Metric("pct_reviewed_within_7d", "responsiveness",
           "Share of human PRs (at least 7 days old) with a first non-author, non-bot "
           "review within 7 days.", 0.2, 0.8,
           reason="80% = most contributors hear back within a week; 20% = most wait longer."),
    Metric("median_hours_to_first_review", "responsiveness",
           "Median hours from PR creation to the first non-author, non-bot review.",
           168, 24, log_scale=True,
           reason="Guide: healthy projects acknowledge 'within days'. 1 day good, 1 week bad."),
    Metric("approving_review_share", "responsiveness",
           "Share of merged human PRs with a non-author, non-bot APPROVED review "
           "(review quality, not rubber-stamp merges).", 0.5, 1.0,
           reason="Ideal: every merge approved by someone else; half unapproved = self-merging."),
    Metric("merge_ratio", "throughput",
           "Merged / (merged + closed unmerged) over human PRs in the window.", 0.3, 0.8,
           reason="Most work accepted at 80% (some closing is healthy curation); 30% = most rejected."),
    Metric("median_hours_to_merge", "throughput",
           "Median hours from PR creation to merge, over merged human PRs.",
           720, 24, log_scale=True,
           reason="Merged within a day is fast; a typical 30-day wait is slow."),
    Metric("stale_open_share", "backlog",
           "Share of open human PRs old enough to go stale (created 180+ days ago) "
           "with no update in 180 days.", 0.5, 0.0,
           reason="No abandoned PRs is ideal; half of the old open PRs abandoned = overwhelmed."),
    Metric("commit_trend_pct", "activity",
           "Slope of monthly human commits over complete months / mean monthly commits "
           "(percent change per month; the trend component).", -0.05, 0.05,
           reason="+/-5% a month is about +/-60% a year; a flat trend scores 0.5."),
    Metric("commit_cv", "activity",
           "Coefficient of variation of monthly human commits over complete months "
           "(lower = steadier activity).", 1.0, 0.2,
           reason="Guide: steady beats bursty. CV 0.2 is steady; CV 1.0 means std = mean."),
    Metric("release_gap_cv", "release_cadence",
           "Coefficient of variation of days between releases (lower = more regular).",
           1.5, 0.3, reason="CV 0.3 is a regular cadence; 1.5 is irregular bursts and gaps."),
    Metric("top_author_commit_share", "contributor_base",
           "Share of linked human commits by the single most active author (bus-factor risk).",
           0.6, 0.2, reason="Guide: one person doing the work is fragile. 60% bad, 20% spread out."),
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


def anchor_score(value: float, m: Metric) -> float:
    """0 at m.bad, 1 at m.good, linear (or log-linear) between, clipped to [0, 1].

    A missing value scores 0.5 (neutral), so it cannot decide the ranking.
    """
    if value is None or pd.isna(value):
        return 0.5
    if m.log_scale:
        value, bad, good = (math.log(max(x, 1e-9)) for x in (value, m.bad, m.good))
    else:
        bad, good = m.bad, m.good
    return min(1.0, max(0.0, (value - bad) / (good - bad)))


def anchored_scores(inputs: pd.DataFrame, metrics: list[Metric] = METRICS) -> pd.DataFrame:
    """Long table: repo, metric, dimension, value, bad, good, score (0-1, 1 = healthy)."""
    rows = [{"repo": r["repo"], "metric": m.column, "dimension": m.dimension,
             "higher_is_better": m.higher_is_better, "value": r[m.column],
             "bad": m.bad, "good": m.good, "score": anchor_score(r[m.column], m)}
            for m in metrics for _, r in inputs.iterrows()]
    return pd.DataFrame(rows)


def signed_z_scores(inputs: pd.DataFrame, metrics: list[Metric] = METRICS) -> pd.DataFrame:
    """Sensitivity check. Long table: repo, metric, z, signed_z (positive = healthier).

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
                         "z": z[repo], "signed_z": sign * z[repo]})
    return pd.DataFrame(rows)


def subscores(scores: pd.DataFrame, value: str = "score") -> pd.DataFrame:
    """Wide table: one row per repo, one column per dimension (mean of `value`).

    The mean, not the sum, so a dimension with two metrics does not count double.
    """
    return scores.pivot_table(index="repo", columns="dimension", values=value,
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
