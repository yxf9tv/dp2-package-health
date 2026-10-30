"""Tests for health_model.py: anchored scores, z-score check, subscores and ranking."""

import math

import pandas as pd
import pytest

import health_model as hm

METRICS = [
    hm.Metric("fast", "speed", "lower is better", bad=30.0, good=10.0),
    hm.Metric("share", "speed", "higher is better", bad=0.0, good=1.0),
    hm.Metric("trend", "activity", "higher is better", bad=-1.0, good=1.0),
]
INPUTS = pd.DataFrame({"repo": ["a", "b", "c"],
                       "fast": [10.0, 20.0, 30.0],
                       "share": [0.9, 0.5, 0.1],
                       "trend": [0.0, 0.0, 0.0]})


def test_anchor_score_is_linear_clipped_and_follows_direction():
    m = METRICS[0]  # lower is better: 30 -> 0, 10 -> 1
    assert not m.higher_is_better
    assert hm.anchor_score(20.0, m) == pytest.approx(0.5)
    assert hm.anchor_score(5.0, m) == 1.0  # better than the good anchor: clipped
    assert hm.anchor_score(99.0, m) == 0.0


def test_log_scale_anchor_uses_the_geometric_midpoint():
    m = hm.Metric("hours", "speed", "", bad=168, good=24, log_scale=True)
    assert hm.anchor_score(math.sqrt(168 * 24), m) == pytest.approx(0.5)


def test_missing_value_scores_neutral():
    assert hm.anchor_score(None, METRICS[1]) == 0.5
    assert hm.anchor_score(float("nan"), METRICS[1]) == 0.5


def test_anchored_subscore_is_mean_and_rank_follows_weights():
    subs = hm.subscores(hm.anchored_scores(INPUTS, METRICS))
    assert subs.loc["a", "speed"] == pytest.approx((1.0 + 0.9) / 2)
    assert subs.loc["b", "activity"] == pytest.approx(0.5)
    ranked = hm.rank_repos(subs, {"speed": 0.5, "activity": 0.5})
    assert ranked["repo"].tolist() == ["a", "b", "c"]
    assert ranked["rank"].tolist() == [1, 2, 3]


def test_z_check_flips_sign_and_uses_population_std():
    s = hm.signed_z_scores(INPUTS, METRICS).set_index(["metric", "repo"])
    assert s.loc[("fast", "a"), "signed_z"] > 0 > s.loc[("fast", "a"), "z"]
    assert s.xs("fast")["z"].max() == pytest.approx(math.sqrt(1.5))
    assert (s.xs("trend")["signed_z"] == 0).all()  # no spread -> neutral


def test_bad_weights_raise():
    subs = hm.subscores(hm.anchored_scores(INPUTS, METRICS))
    with pytest.raises(ValueError):
        hm.rank_repos(subs, {"speed": 0.7, "activity": 0.7})
    with pytest.raises(ValueError):
        hm.rank_repos(subs, {"speed": 0.5, "backlog": 0.5})


def test_default_metrics_have_weights_anchors_and_reasons():
    assert set(hm.WEIGHTS) == {m.dimension for m in hm.METRICS}
    assert sum(hm.WEIGHTS.values()) == pytest.approx(1.0)
    for m in hm.METRICS:
        assert m.bad != m.good and m.reason, m.column
