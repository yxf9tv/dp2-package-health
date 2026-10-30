"""Smoke tests for plotting.py: each plot writes a non-empty PNG from small inputs."""

import pandas as pd

import plotting as pl

REPOS = list(pl.REPO_STYLE)


def _monthly():
    months = pd.date_range("2026-01-01", periods=4, freq="MS")
    rows = [{"repo": r, "month": m, "prs_opened": 5 + i, "prs_merged": 3, "commits": 10 + i,
             "active_contributors": 2 + i, "merge_ratio": 0.6, "is_partial_month": i == 3,
             "median_first_review_hours": 12.0 + i, "median_merge_hours": 30.0}
            for r in REPOS for i, m in enumerate(months)]
    return pd.DataFrame(rows)


def test_dimension_profile_png(tmp_path):
    ranking = pd.DataFrame({"repo": REPOS, "rank": [2, 1, 3], "score": [0.6, 0.8, 0.3],
                            **{d: [0.5, 1.0, 0.0] for d in pl.DIMENSION_LABELS}})
    path = pl.plot_dimension_profile(ranking, tmp_path / "a.png")
    assert path.stat().st_size > 10_000


def test_monthly_trends_and_bubble_png(tmp_path):
    assert pl.plot_monthly_trends(_monthly(), tmp_path / "b.png").stat().st_size > 10_000
    assert pl.plot_review_vs_merge(_monthly(), tmp_path / "c.png").stat().st_size > 10_000
