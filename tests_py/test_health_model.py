"""Tests for health_model.py: directions, z-score math, subscores and ranking."""

import math

import pandas as pd
import pytest

import health_model as hm

METRICS = [
    hm.Metric("fast", "speed", False, "lower is better"),
    hm.Metric("share", "speed", True, "higher is better"),
    hm.Metric("trend", "activity", True, "higher is better"),
]
INPUTS = pd.DataFrame({"repo": ["a", "b", "c"],
                       "fast": [10.0, 20.0, 30.0],
                       "share": [0.9, 0.5, 0.1],
                       "trend": [0.0, 0.0, 0.0]})


def test_lower_is_better_metric_is_sign_flipped():
    s = hm.signed_z_scores(INPUTS, METRICS).set_index(["metric", "repo"])
    assert s.loc[("fast", "a"), "signed_z"] > 0  # fastest repo is healthiest
    assert s.loc[("fast", "a"), "z"] < 0


def test_z_scores_sum_to_zero_and_use_population_std():
    s = hm.signed_z_scores(INPUTS, METRICS)
    for _, g in s.groupby("metric"):
        assert abs(g["z"].sum()) < 1e-9
    # 3 evenly spaced values with ddof=0: z = -sqrt(1.5), 0, +sqrt(1.5)
    assert s[s.metric == "fast"]["z"].max() == pytest.approx(math.sqrt(1.5))


def test_metric_with_no_spread_or_missing_value_is_neutral():
    inputs = INPUTS.assign(share=[0.9, None, 0.1])
    s = hm.signed_z_scores(inputs, METRICS).set_index(["metric", "repo"])
    assert (s.xs("trend")["signed_z"] == 0).all()
    assert s.loc[("share", "b"), "signed_z"] == 0


def test_subscore_is_the_mean_of_its_metrics_and_rank_follows_weights():
    subs = hm.subscores(hm.signed_z_scores(INPUTS, METRICS))
    assert subs.loc["a", "speed"] == pytest.approx(math.sqrt(1.5))  # mean of two equal z
    ranked = hm.rank_repos(subs, {"speed": 0.5, "activity": 0.5})
    assert ranked["repo"].tolist() == ["a", "b", "c"]
    assert ranked["rank"].tolist() == [1, 2, 3]


def test_bad_weights_raise():
    subs = hm.subscores(hm.signed_z_scores(INPUTS, METRICS))
    with pytest.raises(ValueError):
        hm.rank_repos(subs, {"speed": 0.7, "activity": 0.7})
    with pytest.raises(ValueError):
        hm.rank_repos(subs, {"speed": 0.5, "backlog": 0.5})


def test_default_weights_cover_every_metric_dimension():
    assert set(hm.WEIGHTS) == {m.dimension for m in hm.METRICS}
    assert sum(hm.WEIGHTS.values()) == pytest.approx(1.0)
