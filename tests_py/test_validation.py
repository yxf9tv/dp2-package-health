"""Tests for validation.py: the checks fail on bad data and the IQR rule flags the tail."""

import logging

import duckdb
import pytest

import validation as va

LOG = logging.getLogger("test")


@pytest.fixture
def con():
    """In-memory stand-ins for dim_run, a raw table, its staging model, and stg_commits."""
    c = duckdb.connect()
    c.execute("SET TimeZone = 'UTC'")
    c.execute("""CREATE TABLE dim_run AS SELECT TIMESTAMPTZ '2026-04-15 00:00:00+00' AS run_time,
                 TIMESTAMPTZ '2026-01-01 00:00:00+00' AS window_start""")
    c.execute("CREATE TABLE raw_releases AS SELECT 'o/r' repo, 1 id, '{\"draft\": false}'::JSON payload")
    c.execute("INSERT INTO raw_releases VALUES ('o/r', 2, '{\"draft\": true}')")
    c.execute("CREATE TABLE stg_releases AS SELECT 'o/r' repo, 1 release_id")
    # commits in Jan and Mar only: February (complete) is missing; April is the run month
    c.execute("""CREATE TABLE stg_commits AS SELECT * FROM (VALUES
                 ('o/r', TIMESTAMPTZ '2026-01-10 00:00:00+00'),
                 ('o/r', TIMESTAMPTZ '2026-03-10 00:00:00+00')) t(repo, committed_at)""")
    yield c
    c.close()


def test_raw_vs_staging_applies_the_staging_filter(con, monkeypatch):
    monkeypatch.setattr(va, "STAGING", {"stg_releases": ("raw_releases",
                                                         "not (payload ->> '$.draft')::boolean")})
    assert va.check_raw_vs_staging(con, LOG) == []  # the draft is filtered on both sides
    con.execute("DELETE FROM stg_releases")
    assert len(va.check_raw_vs_staging(con, LOG)) == 1


def test_commit_months_flags_a_gap_but_not_the_run_month(con):
    failures = va.check_commit_months(con, LOG)
    assert failures == ["no commits: o/r 2026-02-01"]


def test_iqr_flags_only_values_above_q3_plus_3_iqr(con, tmp_path):
    # 10 values: Q1 = 3.25, Q3 = 7.75, IQR = 4.5, cutoff = 21.25; 100 is flagged, 16 is not
    con.execute("""CREATE TABLE v AS SELECT 'o/r' repo, '#' || x item, x::DOUBLE value
                   FROM (SELECT unnest([1, 2, 3, 4, 5, 6, 7, 8, 16, 100]) x)""")
    flagged = va.iqr_outliers(con, "SELECT * FROM v", "test", LOG, tmp_path / "out.csv")
    assert flagged["item"].tolist() == ["#100"]
    assert (tmp_path / "out.csv").exists()
