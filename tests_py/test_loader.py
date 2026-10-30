"""Tests for loader.py: row splitting, idempotent upserts, _load_state."""

import json
import logging

import duckdb
import pytest

import loader as ld
from gh_client import write_landing

LOG = logging.getLogger("test")


def _review(rid, number, updated="2026-02-01T00:00:00Z"):
    return {"databaseId": rid, "state": "APPROVED", "submittedAt": updated,
            "updatedAt": updated, "author": {"login": "a", "__typename": "User"},
            "pullRequest": {"number": number}}


def _pr(number, updated, reviews=()):
    return {"number": number, "databaseId": number * 10, "state": "MERGED",
            "updatedAt": updated, "createdAt": "2026-01-02T00:00:00Z",
            "author": {"login": "a", "__typename": "User"},
            "reviews": {"totalCount": len(reviews),
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                        "nodes": list(reviews)}}


def _page(endpoint, data, fetched="2026-03-01T00:00:00Z", repo="o/r"):
    return {"repo": repo, "endpoint": endpoint, "page": 1, "fetched_at": fetched, "data": data}


@pytest.fixture
def project(tmp_path):
    """A project root with data/raw/ holding one page of each endpoint."""
    raw = tmp_path / "data" / "raw"
    base = raw / "o__r"
    write_landing(base / "commits" / "page_0001.json", _page("commits", [
        {"sha": "abc", "commit": {"committer": {"date": "2026-01-05T10:00:00Z"}}},
        {"sha": "def", "commit": {"committer": {"date": "2026-01-06T10:00:00+02:00"}}}]))
    write_landing(base / "releases" / "page_0001.json", _page("releases", [
        {"id": 7, "updated_at": "2026-01-07T00:00:00Z"}]))
    write_landing(base / "pulls" / "page_0001.json", _page("pulls", [
        _pr(1, "2026-02-10T00:00:00Z", [_review(100, 1), _review(101, 1)]),
        _pr(2, "2026-02-09T00:00:00Z")]))
    write_landing(base / "reviews" / "pr1_page_0001.json", _page("reviews", [_review(102, 1)]))
    return tmp_path, raw


def test_pulls_page_splits_reviews_and_strips_them_from_pr():
    out = ld.rows_from_record(_page("pulls", [_pr(1, "2026-02-10T00:00:00Z",
                                                  [_review(100, 1)])]), "f.json")
    assert [r[1] for r in out["raw_pull_requests"]] == ["1"]
    assert [r[1] for r in out["raw_pr_reviews"]] == ["100"]
    payload = json.loads(out["raw_pull_requests"][0][5])
    assert payload["reviews"] == {"totalCount": 1}


def test_commit_key_is_sha_and_updated_at_is_committer_date():
    out = ld.rows_from_record(_page("commits", [
        {"sha": "abc", "commit": {"committer": {"date": "2026-01-05T10:00:00Z"}}}]), "f")
    assert out["raw_commits"][0][:3] == ("o/r", "abc", "2026-01-05T10:00:00Z")


def test_unknown_endpoint_raises():
    with pytest.raises(ld.LandingFileError):
        ld.rows_from_record(_page("issues", [{"id": 1}]), "f")


def test_double_load_gives_identical_counts(project):
    root, raw = project
    db = root / "packages.duckdb"
    first = ld.load_raw(raw, db, LOG)
    second = ld.load_raw(raw, db, LOG)
    expected = {"raw_commits": 2, "raw_releases": 1, "raw_pull_requests": 2, "raw_pr_reviews": 3}
    for table, n in expected.items():
        assert first[table]["after"] == second[table]["after"] == n
        assert second[table]["new"] == 0 and second[table]["replaced"] == n


def test_timestamps_are_utc_and_load_state_has_watermarks(project):
    root, raw = project
    db = root / "packages.duckdb"
    ld.load_raw(raw, db, LOG)
    with duckdb.connect(str(db)) as con:
        # +02:00 offset is converted to UTC, not dropped
        assert str(con.execute("SELECT updated_at FROM raw_commits WHERE id='def'")
                   .fetchone()[0]) == "2026-01-06 08:00:00"
        state = dict(((e), str(t)) for _, e, t in
                     con.execute("SELECT repo, endpoint, max_updated_at FROM _load_state").fetchall())
        assert con.execute("SELECT source_file FROM raw_releases").fetchone()[0] == \
            "data/raw/o__r/releases/page_0001.json"
    assert state == {"commits": "2026-01-06 08:00:00", "pulls": "2026-02-10 00:00:00",
                     "reviews": "2026-02-01 00:00:00", "releases": "2026-01-07 00:00:00"}


def test_same_key_on_two_pages_keeps_newest_fetch(project):
    root, raw = project
    write_landing(raw / "o__r" / "pulls" / "page_0002.json",
                  _page("pulls", [_pr(1, "2026-02-11T00:00:00Z")], fetched="2026-03-02T00:00:00Z"))
    db = root / "packages.duckdb"
    ld.load_raw(raw, db, LOG)
    with duckdb.connect(str(db)) as con:
        rows = con.execute("SELECT updated_at, source_file FROM raw_pull_requests "
                           "WHERE id='1'").fetchall()
    assert len(rows) == 1 and rows[0][1].endswith("page_0002.json")


def test_damaged_file_stops_load_and_writes_nothing(project):
    root, raw = project
    (raw / "o__r" / "commits" / "page_0002.json").write_text('{"repo": "o/r", "da')
    db = root / "packages.duckdb"
    with pytest.raises(ld.LandingFileError):
        ld.load_raw(raw, db, LOG)
    assert not db.exists()
