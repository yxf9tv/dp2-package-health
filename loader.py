"""Task 2 helpers: data/raw/ JSON pages -> raw_* tables in packages.duckdb.

One row per record (not per page). Each raw table has typed key columns and the
full record as JSON. Loads upsert on (repo, id), so loading the same files twice
gives the same row counts.

Landing folder -> table:
- commits/   -> raw_commits        id = sha,        updated_at = commit.committer.date
- releases/  -> raw_releases       id = id,         updated_at = updated_at
- pulls/     -> raw_pull_requests  id = number,     updated_at = updatedAt
             -> raw_pr_reviews     (the reviews nested in each PR are split out)
- reviews/   -> raw_pr_reviews     id = databaseId, updated_at = updatedAt (overflow pages)
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

from gh_client import read_landing

TABLES = ["raw_commits", "raw_pull_requests", "raw_pr_reviews", "raw_releases"]
COLUMNS = ["repo", "id", "updated_at", "fetched_at", "source_file", "payload"]
# _load_state endpoint name -> raw table (Task 1 reads the commits and pulls rows)
STATE_SOURCES = {"commits": "raw_commits", "pulls": "raw_pull_requests",
                 "reviews": "raw_pr_reviews", "releases": "raw_releases"}

RAW_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    repo        VARCHAR   NOT NULL,
    id          VARCHAR   NOT NULL,
    updated_at  TIMESTAMP,            -- UTC
    fetched_at  TIMESTAMP NOT NULL,   -- UTC, from the landing file
    source_file VARCHAR   NOT NULL,
    payload     JSON      NOT NULL,
    PRIMARY KEY (repo, id)
)"""

STATE_DDL = """
CREATE TABLE IF NOT EXISTS _load_state (
    repo           VARCHAR   NOT NULL,
    endpoint       VARCHAR   NOT NULL,
    max_updated_at TIMESTAMP,          -- newest updated_at loaded (UTC); Task 1 watermark
    loaded_at      TIMESTAMP NOT NULL, -- when the load finished (UTC)
    PRIMARY KEY (repo, endpoint)
)"""


class LandingFileError(RuntimeError):
    """A landing file is missing its data or is not valid JSON."""


# ---------------------------------------------------------------------------
# Files -> rows (pure functions, no database)
# ---------------------------------------------------------------------------
def _row(record: dict, source: str, key, updated_at, payload: dict) -> tuple:
    return (record["repo"], str(key), updated_at, record["fetched_at"], source,
            json.dumps(payload))


def rows_from_record(record: dict, source: str) -> dict[str, list[tuple]]:
    """Split one landing page into rows per raw table."""
    out: dict[str, list[tuple]] = {t: [] for t in TABLES}
    endpoint = record["endpoint"]
    for item in record["data"]:
        if endpoint == "commits":
            date = item["commit"]["committer"]["date"]  # the date that `since` filters on
            out["raw_commits"].append(_row(record, source, item["sha"], date, item))
        elif endpoint == "releases":
            out["raw_releases"].append(
                _row(record, source, item["id"], item.get("updated_at"), item))
        elif endpoint == "reviews":  # overflow pages: reviews after the first 100
            out["raw_pr_reviews"].append(
                _row(record, source, item["databaseId"], item["updatedAt"], item))
        elif endpoint == "pulls":
            # Nested reviews become their own rows; the PR payload keeps only the count
            reviews = item["reviews"]
            for review in reviews["nodes"]:
                out["raw_pr_reviews"].append(
                    _row(record, source, review["databaseId"], review["updatedAt"], review))
            pr = {**item, "reviews": {"totalCount": reviews["totalCount"]}}
            out["raw_pull_requests"].append(
                _row(record, source, item["number"], item["updatedAt"], pr))
        else:
            raise LandingFileError(f"unknown endpoint {endpoint!r} in {source}")
    return out


def collect_rows(raw_dir: Path, root: Path) -> tuple[dict[str, list[tuple]], int]:
    """Read every page under raw_dir/<repo>/<endpoint>/. Returns rows per table and file count.

    A damaged file stops the load: Task 1 re-fetches it on the next run (no _LOADED yet).
    """
    rows: dict[str, list[tuple]] = {t: [] for t in TABLES}
    files = sorted(raw_dir.glob("*/*/*.json"))
    for path in files:
        record = read_landing(path)
        if record is None:
            raise LandingFileError(f"unreadable landing file: {path}")
        source = str(path.relative_to(root))
        for table, table_rows in rows_from_record(record, source).items():
            rows[table] += table_rows
    return rows, len(files)


# ---------------------------------------------------------------------------
# Rows -> DuckDB (upserts in one transaction)
# ---------------------------------------------------------------------------
def count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    return con.execute(f"SELECT count(*) FROM {table}").fetchall()[0][0]


def upsert(con: duckdb.DuckDBPyConnection, table: str, rows: list[tuple]) -> int:
    """INSERT OR REPLACE rows on (repo, id). Returns the distinct keys in the batch.

    One record can sit on two pages (a PR updated mid-pagination moves pages), and
    DuckDB cannot upsert the same key twice in one statement, so keep the newest copy.
    Timestamps go through TIMESTAMPTZ so that any UTC offset is applied, not dropped.
    """
    if not rows:
        return 0
    con.register("batch", pd.DataFrame(rows, columns=COLUMNS))
    try:
        con.execute(f"""
            INSERT OR REPLACE INTO {table}
            SELECT repo, id,
                   updated_at::TIMESTAMPTZ AT TIME ZONE 'UTC',
                   fetched_at::TIMESTAMPTZ AT TIME ZONE 'UTC',
                   source_file, payload::JSON
            FROM batch
            QUALIFY row_number() OVER (
                PARTITION BY repo, id ORDER BY fetched_at DESC, source_file DESC) = 1
        """).fetchall()  # DuckDB 2.0: drain each result before the next statement
        return con.execute("SELECT count(DISTINCT (repo, id)) FROM batch").fetchall()[0][0]
    finally:
        con.unregister("batch")


def update_load_state(con: duckdb.DuckDBPyConnection, loaded_at: str) -> None:
    """Upsert one row per (repo, endpoint): the newest updated_at now in the raw table."""
    selects = " UNION ALL ".join(
        f"SELECT repo, '{endpoint}', max(updated_at), ?::TIMESTAMP FROM {table} GROUP BY repo"
        for endpoint, table in STATE_SOURCES.items())
    con.execute(f"INSERT OR REPLACE INTO _load_state {selects}",
                [loaded_at] * len(STATE_SOURCES)).fetchall()


def load_raw(raw_dir: Path, db_path: Path, log: logging.Logger) -> dict[str, dict]:
    """Load every landing file into the raw tables, then update _load_state.

    All upserts and the _load_state update run in one transaction: a failure
    rolls everything back, so _load_state never claims data that is not loaded.
    """
    root = raw_dir.parent.parent  # project root, so source_file reads data/raw/...
    rows, n_files = collect_rows(raw_dir, root)
    log.info("read %d landing files from %s", n_files, raw_dir.relative_to(root))

    report: dict[str, dict] = {}
    with duckdb.connect(str(db_path)) as con:
        for table in TABLES:
            con.execute(RAW_DDL.format(table=table))
        con.execute(STATE_DDL)
        con.begin()
        try:
            for table in TABLES:
                before = count(con, table)
                distinct = upsert(con, table, rows[table])
                after = count(con, table)
                report[table] = {"file_rows": len(rows[table]), "distinct": distinct,
                                 "before": before, "after": after, "new": after - before,
                                 "replaced": distinct - (after - before)}
                log.info("%s: %d rows in files (%d distinct keys); table %d -> %d "
                         "(+%d new, %d replaced)", table, len(rows[table]), distinct,
                         before, after, after - before, distinct - (after - before))
            loaded_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            update_load_state(con, loaded_at)
            con.commit()
        except Exception:
            con.rollback()
            raise
        for repo, endpoint, watermark in con.execute(
                "SELECT repo, endpoint, max_updated_at FROM _load_state ORDER BY 1, 2").fetchall():
            log.info("_load_state %s %s: max_updated_at %s", repo, endpoint, watermark)
    return report
