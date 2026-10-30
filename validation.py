"""Task 4 helpers: end-to-end checks, outliers and descriptive statistics (no dbt).

Completeness (any failure raises ValidationError):
1. files -> raw:     every distinct (repo, id) in data/raw/ is in its raw table
2. raw -> staging:   raw row count = staging row count per repo (same filters as staging)
3. reviews:          sum of GitHub's reviews.totalCount per repo = review rows loaded
4. commit months:    every repo has commits in every complete month of the window
Outliers: IQR rule (Q3 + 3*IQR) per repo on hours to merge and on weekly commits.
Statistics: per repo median and p90 hours to first review / merge, commits per week,
releases per year, read from the marts.
"""

import logging
from pathlib import Path

import duckdb
import pandas as pd

import loader as ld

IQR_K = 3.0  # "far out" fence (Tukey); 1.5 would flag ordinary slow PRs
TOP_N = 10  # outliers listed in the log per repo; the full list goes to a CSV

# staging model -> (raw table, filter that staging applies to the raw rows)
STAGING = {
    "stg_commits": ("raw_commits", "true"),
    "stg_pull_requests": ("raw_pull_requests", "true"),
    "stg_pr_reviews": ("raw_pr_reviews", "payload ->> '$.state' <> 'PENDING'"),
    "stg_releases": ("raw_releases", "not (payload ->> '$.draft')::boolean"),
}


class ValidationError(RuntimeError):
    """One or more completeness checks failed."""


def log_frame(log: logging.Logger, title: str, df: pd.DataFrame) -> None:
    """Print a table to the screen and the log, one line per row."""
    log.info("%s\n%s", title, df.to_string(index=False))


# ---------------------------------------------------------------------------
# Completeness
# ---------------------------------------------------------------------------
def check_files_in_raw(con: duckdb.DuckDBPyConnection, raw_dir: Path, root: Path,
                       log: logging.Logger) -> list[str]:
    """Check 1: every distinct (repo, id) in the landing files is in its raw table."""
    rows, n_files = ld.collect_rows(raw_dir, root)
    failures = []
    for table, table_rows in rows.items():
        keys = pd.DataFrame({"repo": [r[0] for r in table_rows],
                             "id": [r[1] for r in table_rows]}).drop_duplicates()
        con.register("file_keys", keys)
        result = con.execute(f"""
            SELECT k.repo, count(*) AS in_files, count(t.id) AS in_raw
            FROM file_keys k LEFT JOIN {table} t USING (repo, id)
            GROUP BY k.repo ORDER BY k.repo""").fetchall()
        con.unregister("file_keys")
        for repo, in_files, in_raw in result:
            ok = in_files == in_raw
            log.info("%s files->raw %s %s: %d distinct in files, %d found in raw",
                     "PASS" if ok else "FAIL", table, repo, in_files, in_raw)
            if not ok:
                failures.append(f"{table} {repo}: {in_files - in_raw} file records missing in raw")
    log.info("checked %d landing files", n_files)
    return failures


def check_raw_vs_staging(con: duckdb.DuckDBPyConnection, log: logging.Logger) -> list[str]:
    """Check 2: raw count (after staging's filter) = staging count, per repo."""
    failures = []
    for model, (table, raw_filter) in STAGING.items():
        result = con.execute(f"""
            WITH r AS (SELECT repo, count(*) n FROM {table} WHERE {raw_filter} GROUP BY repo),
                 s AS (SELECT repo, count(*) n FROM {model} GROUP BY repo)
            SELECT repo, coalesce(r.n, 0), coalesce(s.n, 0)
            FROM r FULL JOIN s USING (repo) ORDER BY repo""").fetchall()
        for repo, n_raw, n_stg in result:
            ok = n_raw == n_stg
            log.info("%s raw->staging %s %s: raw %d, staging %d (filter: %s)",
                     "PASS" if ok else "FAIL", model, repo, n_raw, n_stg, raw_filter)
            if not ok:
                failures.append(f"{model} {repo}: raw {n_raw} != staging {n_stg}")
    return failures


def check_review_totals(con: duckdb.DuckDBPyConnection, log: logging.Logger) -> list[str]:
    """Check 3: GitHub's reviews.totalCount per PR, summed per repo = review rows loaded.

    Proves the nested reviews plus the overflow pages captured every review.
    """
    failures = []
    result = con.execute("""
        WITH reported AS (SELECT repo, sum(review_count_reported) n FROM stg_pull_requests
                          GROUP BY repo),
             loaded AS (SELECT repo, count(*) n FROM raw_pr_reviews GROUP BY repo)
        SELECT repo, coalesce(reported.n, 0), coalesce(loaded.n, 0)
        FROM reported FULL JOIN loaded USING (repo) ORDER BY repo""").fetchall()
    for repo, reported, loaded in result:
        ok = reported == loaded
        log.info("%s reviews %s: GitHub reports %d, loaded %d",
                 "PASS" if ok else "FAIL", repo, reported, loaded)
        if not ok:
            failures.append(f"reviews {repo}: reported {reported} != loaded {loaded}")
    return failures


def check_commit_months(con: duckdb.DuckDBPyConnection, log: logging.Logger) -> list[str]:
    """Check 4: every repo has commits in every complete month (the run month is exempt)."""
    missing = con.execute("""
        WITH months AS (
            SELECT unnest(generate_series(date_trunc('month', window_start),
                                          date_trunc('month', run_time) - INTERVAL 1 MONTH,
                                          INTERVAL 1 MONTH))::DATE AS month
            FROM dim_run),
        repos AS (SELECT DISTINCT repo FROM stg_commits),
        have AS (SELECT DISTINCT repo, date_trunc('month', committed_at)::DATE AS month
                 FROM stg_commits)
        SELECT r.repo, m.month
        FROM repos r CROSS JOIN months m
        LEFT JOIN have h ON h.repo = r.repo AND h.month = m.month
        WHERE h.month IS NULL ORDER BY 1, 2""").fetchall()
    n_months = con.execute("""SELECT date_diff('month', date_trunc('month', window_start),
                                                 date_trunc('month', run_time)) FROM dim_run"""
                           ).fetchall()[0][0]
    if not missing:
        log.info("PASS commit months: every repo has commits in all %d complete months", n_months)
        return []
    for repo, month in missing:
        log.info("FAIL commit months %s: no commits in %s", repo, month)
    return [f"no commits: {repo} {month}" for repo, month in missing]


# ---------------------------------------------------------------------------
# Outliers (IQR rule, per repo)
# ---------------------------------------------------------------------------
def iqr_outliers(con: duckdb.DuckDBPyConnection, values_sql: str, label: str,
                 log: logging.Logger, csv_path: Path) -> pd.DataFrame:
    """Flag values above Q3 + IQR_K * IQR within each repo. values_sql gives repo, item, value."""
    flagged = con.execute(f"""
        WITH v AS ({values_sql}),
        fences AS (
            SELECT repo,
                   quantile_cont(value, 0.25) AS q1,
                   quantile_cont(value, 0.75) AS q3,
                   quantile_cont(value, 0.75)
                       + {IQR_K} * (quantile_cont(value, 0.75) - quantile_cont(value, 0.25))
                       AS cutoff
            FROM v GROUP BY repo)
        SELECT v.repo, v.item, round(v.value, 1) AS value, round(f.cutoff, 1) AS cutoff
        FROM v JOIN fences f USING (repo)
        WHERE v.value > f.cutoff
        ORDER BY v.repo, v.value DESC""").df()
    summary = con.execute(f"""
        WITH v AS ({values_sql})
        SELECT repo, count(*) AS n,
               round(quantile_cont(value, 0.75)
                     + {IQR_K} * (quantile_cont(value, 0.75) - quantile_cont(value, 0.25)), 1)
                   AS cutoff
        FROM v GROUP BY repo ORDER BY repo""").df()
    summary["flagged"] = summary["repo"].map(flagged.groupby("repo").size()).fillna(0).astype(int)
    log_frame(log, f"OUTLIERS {label}: cutoff = Q3 + {IQR_K:g} x IQR per repo", summary)
    if not flagged.empty:
        log_frame(log, f"OUTLIERS {label}: top {TOP_N} per repo "
                       f"(full list: {csv_path.name})",
                  flagged.groupby("repo").head(TOP_N))
        flagged.to_csv(csv_path, index=False)
    return flagged


MERGE_HOURS_SQL = """
    SELECT repo, '#' || number AS item, hours_to_merge AS value
    FROM fct_pull_requests WHERE is_merged AND NOT is_bot"""

# All commits (bots included: an automated burst is what this scan looks for), one row
# per complete week in the window, zero-commit weeks included
WEEKLY_COMMITS_SQL = """
    WITH weeks AS (
        SELECT unnest(generate_series(date_trunc('week', window_start) + INTERVAL 7 DAY,
                                      date_trunc('week', run_time) - INTERVAL 7 DAY,
                                      INTERVAL 7 DAY))::DATE AS week
        FROM dim_run),
    repos AS (SELECT DISTINCT repo FROM stg_commits),
    counts AS (SELECT repo, date_trunc('week', committed_at)::DATE AS week, count(*) AS n
               FROM stg_commits GROUP BY ALL)
    SELECT r.repo, 'week of ' || w.week AS item, coalesce(c.n, 0)::DOUBLE AS value
    FROM repos r CROSS JOIN weeks w
    LEFT JOIN counts c ON c.repo = r.repo AND c.week = w.week"""


# ---------------------------------------------------------------------------
# Descriptive statistics (from the marts; no durations recomputed here)
# ---------------------------------------------------------------------------
def describe(con: duckdb.DuckDBPyConnection, log: logging.Logger) -> pd.DataFrame:
    """Per repo: median and p90 hours to first review and to merge, commits/week, releases/year."""
    stats = con.execute("""
        WITH d AS (
            SELECT repo,
                   count(*) AS human_prs,
                   median(hours_to_first_review)              AS first_review_h_median,
                   quantile_cont(hours_to_first_review, 0.9)  AS first_review_h_p90,
                   median(hours_to_merge)                     AS merge_h_median,
                   quantile_cont(hours_to_merge, 0.9)         AS merge_h_p90
            FROM fct_pull_requests WHERE NOT is_bot GROUP BY repo)
        SELECT d.*, h.commits_per_week, h.releases_per_year
        FROM d JOIN mart_health_inputs h USING (repo) ORDER BY repo""").df()
    log_frame(log, "DESCRIPTIVE STATISTICS (human PRs; hours; medians and 90th percentiles)",
              stats.round(1))
    return stats


# ---------------------------------------------------------------------------
# Entry point for the Task 4 Prefect task
# ---------------------------------------------------------------------------
def run_validation(db_path: Path, raw_dir: Path, log_dir: Path,
                   log: logging.Logger) -> dict:
    """Run every check; raise ValidationError after logging all failures."""
    root = raw_dir.parent.parent
    # Not read_only: dbt-duckdb keeps its connection open in this process, and DuckDB
    # refuses a second connection to the same file with a different configuration.
    # This task only reads.
    with duckdb.connect(str(db_path)) as con:
        con.execute("SET TimeZone = 'UTC'")  # month and week buckets in UTC, as in dbt
        failures = (check_files_in_raw(con, raw_dir, root, log)
                    + check_raw_vs_staging(con, log)
                    + check_review_totals(con, log)
                    + check_commit_months(con, log))
        merge_out = iqr_outliers(con, MERGE_HOURS_SQL, "hours to merge (merged human PRs)",
                                 log, log_dir / "outliers_hours_to_merge.csv")
        week_out = iqr_outliers(con, WEEKLY_COMMITS_SQL, "weekly commits (all commits)",
                                log, log_dir / "outliers_weekly_commits.csv")
        log.info("outlier decision: KEEP all flagged records. The marts and Task 5 use "
                 "medians and shares, which these extremes barely move, and they are real "
                 "events (long-lived PRs, release-week bursts), not data errors.")
        stats = describe(con, log)
    if failures:
        raise ValidationError(f"{len(failures)} completeness check(s) failed: "
                              + "; ".join(failures))
    log.info("validation passed: all completeness checks OK")
    return {"merge_outliers": len(merge_out), "weekly_commit_outliers": len(week_out),
            "stats": stats.round(2).to_dict("records")}
