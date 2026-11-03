"""Open Source Package Health pipeline (DS3022 DP2, package set B).

One Prefect flow runs six tasks in order:
extract (GitHub -> data/raw JSON) -> load (JSON -> DuckDB raw_*) -> dbt build
-> validate (Python) -> model (health score + rank) -> plot (PNGs).
"""

import json
import logging
import os
import shutil
import sys
import time
from pathlib import Path

import duckdb
from prefect import flow, get_run_logger, task, unmapped
from prefect.blocks.system import Secret
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

import extract as ex
import health_model as hm
import loader as ld
import plotting as pl
import validation as va
from gh_client import ODOMETER, make_client

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ROOT = Path(__file__).parent
DATA_RAW = ROOT / "data" / "raw"
LOG_DIR = ROOT / "logs"
PLOTS_DIR = ROOT / "plots"
DB_PATH = ROOT / "packages.duckdb"

REPOS = ["scikit-learn/scikit-learn", "dask/dask", "plotly/plotly.py"]
# REPOS_OVERRIDE (comma-separated) lets us develop against one repo
if os.getenv("REPOS_OVERRIDE"):
    REPOS = [r.strip() for r in os.environ["REPOS_OVERRIDE"].split(",")]

# Fixed analysis window start; DEV_START_DATE shrinks the window during development
WINDOW_START = os.getenv("DEV_START_DATE", "2024-01-01")
WINDOW_START_ISO = f"{WINDOW_START}T00:00:00Z"
LOADED_MARKER = DATA_RAW / "_LOADED"  # created by Task 2 once data/raw is safe in DuckDB

SECRET_BLOCK = "yxf9tv-pat"

# Deployment on the SDS Prefect server (see DEPLOYMENTS.md)
GIT_URL = "https://github.com/yxf9tv/dp2-package-health.git"
WORK_POOL = "uvasds-k8s-pool"
DEPLOYMENT_NAME = "yxf9tv-deployment"


# ---------------------------------------------------------------------------
# Logging: every task logs to the Prefect run logger AND to logs/<name>.log
# ---------------------------------------------------------------------------
class _PrefectForwardHandler(logging.Handler):
    """Forward a stdlib log record to the Prefect logger of the current run."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            get_run_logger().log(record.levelno, record.getMessage())
        except Exception:
            # Outside a flow/task run there is no Prefect logger; the file still has the line
            pass


def get_logger(name: str) -> logging.Logger:
    """Return a logger that writes to logs/<name>.log and to the Prefect UI/console."""
    logger = logging.getLogger(f"dp2.{name}")
    if not logger.handlers:  # add handlers only once per process
        LOG_DIR.mkdir(exist_ok=True)
        file_handler = logging.FileHandler(LOG_DIR / f"{name}.log")
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        )
        logger.addHandler(file_handler)
        logger.addHandler(_PrefectForwardHandler())
        logger.setLevel(logging.INFO)
        logger.propagate = False  # keep task lines out of other loggers
    return logger


# ---------------------------------------------------------------------------
# Secrets: GH_PAT env var first, Prefect Secret block second
# ---------------------------------------------------------------------------
class MissingTokenError(RuntimeError):
    """Raised when no GitHub token is available."""


def get_github_token() -> str:
    """Read the GitHub PAT from $GH_PAT, else from the Prefect Secret block."""
    token = os.getenv("GH_PAT")
    if token:
        return token
    try:
        return Secret.load(SECRET_BLOCK).get()
    except Exception as exc:
        raise MissingTokenError(
            f"No GitHub token: set the GH_PAT environment variable "
            f"or create the Prefect Secret block '{SECRET_BLOCK}'."
        ) from exc


# ---------------------------------------------------------------------------
# The six core tasks (stubs; each section of the plan fills one in)
# ---------------------------------------------------------------------------
def read_watermarks() -> dict[tuple[str, str], str]:
    """Newest updated_at per (repo, endpoint) from _load_state; {} before the first load."""
    if not DB_PATH.exists():
        return {}
    try:
        with duckdb.connect(str(DB_PATH), read_only=True) as con:
            rows = con.execute(
                "SELECT repo, endpoint, strftime(max_updated_at, '%Y-%m-%dT%H:%M:%SZ') "
                "FROM _load_state"
            ).fetchall()
    except duckdb.CatalogException:  # database exists but no load has finished yet
        return {}
    return {(repo, endpoint): iso for repo, endpoint, iso in rows if iso}


@task(name="1-extract")
def extract(token: str) -> dict:
    """Task 1: GitHub API -> data/raw/<owner>__<repo>/<endpoint>/page_NNNN.json.

    _LOADED present -> last run fully loaded: delete data/raw and fetch only new records.
    _LOADED absent  -> last run stopped early: keep data/raw and skip pages on disk.
    """
    log = get_logger("extract")
    watermarks = read_watermarks()
    if LOADED_MARKER.exists():
        shutil.rmtree(DATA_RAW)
        log.info("start fresh: _LOADED found, deleted data/raw/ (incremental from watermarks)")
    elif DATA_RAW.exists():
        log.info("resume: no _LOADED marker, keeping pages already in data/raw/")
    else:
        log.info("first run: no data/raw/, fetching the full window from %s", WINDOW_START_ISO)
    log.info("watermarks: %s", watermarks or "none (full window)")

    started, before = time.monotonic(), ODOMETER.total()
    try:
        with make_client(token) as client:
            # Concurrent fetching: one mapped task run per repo; all share gh_client.LIMITER
            results = ex.extract_repo.map(
                repo=REPOS, client=unmapped(client), base=unmapped(DATA_RAW),
                window_start_iso=unmapped(WINDOW_START_ISO),
                watermarks=unmapped(watermarks), log=unmapped(log),
            ).result()
    except Exception:
        # Keep the reason in logs/extract.log too; pages already on disk stay for a resume
        log.exception("extract failed after %d requests; next run resumes",
                      ODOMETER.total() - before)
        raise
    log.info("extract done in %.0fs: %d requests this task; odometer %s",
             time.monotonic() - started, ODOMETER.total() - before, ODOMETER.summary())
    return dict(zip(REPOS, results))


@task(name="2-load")
def load() -> dict:
    """Task 2: data/raw JSON -> raw_* tables in packages.duckdb (upsert on repo, id).

    _LOADED is touched only after every file is loaded and _load_state is committed.
    """
    log = get_logger("load")
    try:
        report = ld.load_raw(DATA_RAW, DB_PATH, log)
    except Exception:
        # Keep the reason in logs/load.log too; no _LOADED, so the next run resumes
        log.exception("load failed, rolled back, _LOADED not written")
        raise
    LOADED_MARKER.touch()
    log.info("touched %s: next run starts fresh from the watermarks",
             LOADED_MARKER.relative_to(ROOT))
    return report


@task(name="3-dbt-build")
def dbt_build() -> dict:
    """Task 3: dbt build (staging, facts, marts, tests). Only task that runs dbt.

    raise_on_failure=True: any failed model or test raises, so the flow fails here.
    """
    log = get_logger("dbt_task")  # dbt itself writes logs/dbt.log
    runner = PrefectDbtRunner(
        settings=PrefectDbtSettings(project_dir=str(ROOT), profiles_dir=str(ROOT)),
        raise_on_failure=True,
    )
    args = ["build", "--vars", json.dumps({"window_start": WINDOW_START})]
    try:
        res = runner.invoke(args)
    except Exception:
        log.exception("dbt build failed; downstream tasks will not run")
        raise
    # One line per model and test, so the singular test results are in logs/dbt_task.log
    statuses: dict[str, int] = {}
    for r in res.result.results:
        status = str(r.status)
        statuses[status] = statuses.get(status, 0) + 1
        log.info("%s %s %s", status.upper(), r.node.resource_type, r.node.name)
    log.info("dbt build ok (window_start=%s): %s", WINDOW_START, statuses)
    return statuses


@task(name="4-validate")
def validate() -> dict:
    """Task 4: Python checks across files, raw tables and models; outliers; stats.

    Any completeness failure raises, so the flow fails before modeling.
    """
    log = get_logger("validate")
    try:
        return va.run_validation(DB_PATH, DATA_RAW, LOG_DIR, log)
    except Exception:
        log.exception("validation failed; tasks 5-6 will not run")
        raise


@task(name="5-model")
def model() -> list[dict]:
    """Task 5: health score from mart_health_inputs; ranking written to DuckDB.

    Writes health_ranking (one row per repo) and health_metric_scores (repo x metric).
    The main score is anchored 0-1; a signed z-score ranking is logged as a check.
    """
    log = get_logger("model")
    try:
        # Not read_only: dbt-duckdb holds a connection in this process (see validation.py)
        with duckdb.connect(str(DB_PATH)) as con:
            inputs = con.execute("SELECT * FROM mart_health_inputs").df()
            run_time = con.execute("SELECT run_time FROM dim_run").fetchall()[0][0]
            z = hm.signed_z_scores(inputs)
            scores = hm.anchored_scores(inputs).merge(
                z[["repo", "metric", "z", "signed_z"]], on=["repo", "metric"])
            ranked = hm.rank_repos(hm.subscores(scores, "score"))
            z_ranked = hm.rank_repos(hm.subscores(z, "signed_z"))
            ranked["run_time"] = run_time
            con.register("ranked_df", ranked)
            con.register("scores_df", scores)
            con.execute("CREATE OR REPLACE TABLE health_ranking AS SELECT * FROM ranked_df")
            con.execute("CREATE OR REPLACE TABLE health_metric_scores AS SELECT * FROM scores_df")
    except Exception:
        log.exception("health model failed")
        raise
    log.info("METRICS (anchors bad -> good, weight of the dimension)\n%s", "\n".join(
        f"  {m.column:<30} {m.dimension:<17} {m.bad:>6g} -> {m.good:<6g}"
        f"{' log' if m.log_scale else '    '}  w={hm.WEIGHTS[m.dimension]:.2f}  {m.definition}"
        for m in hm.METRICS))
    log.info("ANCHORED METRIC SCORES (0 = bad anchor, 1 = good anchor)\n%s", scores.pivot(
        index="metric", columns="repo", values="score").round(2).to_string())
    log.info("HEALTH RANKING (score = weighted sum of dimension subscores, 0-1)\n%s",
             ranked.drop(columns="run_time").round(2).to_string(index=False))
    same = ranked["repo"].tolist() == z_ranked["repo"].tolist()
    log.info("SENSITIVITY: signed z-score ranking (%s order)\n%s",
             "same" if same else "DIFFERENT", z_ranked[["rank", "repo", "score"]].round(2)
             .to_string(index=False))
    log.info("wrote health_ranking (%d rows) and health_metric_scores (%d rows) to %s",
             len(ranked), len(scores), DB_PATH.name)
    return ranked.drop(columns="run_time").round(3).to_dict("records")


@task(name="6-plot")
def plot() -> list[str]:
    """Task 6: three multi-dimensional PNGs in plots/ (committed to the repo)."""
    log = get_logger("plot")
    try:
        # Not read_only: dbt-duckdb holds a connection in this process (see validation.py)
        with duckdb.connect(str(DB_PATH)) as con:
            con.execute("SET TimeZone = 'UTC'")
            ranking = con.execute("SELECT * FROM health_ranking").df()
            monthly = con.execute("SELECT * FROM mart_repo_monthly").df()
        paths = [
            pl.plot_dimension_profile(ranking, PLOTS_DIR / "1_dimension_profile.png"),
            pl.plot_monthly_trends(monthly, PLOTS_DIR / "2_monthly_trends.png"),
            pl.plot_review_vs_merge(monthly, PLOTS_DIR / "3_review_vs_merge.png"),
        ]
    except Exception:
        log.exception("plotting failed")
        raise
    for path in paths:
        log.info("wrote %s", path.relative_to(ROOT))
    return [str(path) for path in paths]


# ---------------------------------------------------------------------------
# The flow: runs the six tasks in order; any task failure fails the flow
# ---------------------------------------------------------------------------
@flow(name="yxf9tv-package-health-flow")
def package_health_flow() -> None:
    log = get_logger("flow")
    token = get_github_token()
    log.info("flow start: %d repos, window %s -> now", len(REPOS), WINDOW_START)
    extract(token)
    load()
    dbt_build()
    validate()
    model()
    plot()
    # Extended item: API odometer, total requests in this flow run
    log.info("flow complete: API odometer = %d requests %s", ODOMETER.total(), ODOMETER.summary())


@flow(name="yxf9tv-load-only")
def load_only_flow() -> dict:
    """Run Task 2 alone on the files in data/raw/ (proves idempotent loads, no API calls)."""
    return load()


# ---------------------------------------------------------------------------
# Extended item: REST vs. GraphQL benchmark for PR + review ingestion
# ---------------------------------------------------------------------------
@flow(name="yxf9tv-rest-vs-graphql-benchmark")
def rest_benchmark_flow(repo: str = "dask/dask", since: str = "2026-07-01") -> dict:
    """Fetch the same PRs + reviews both ways into data/bench/ and log requests and runtime."""
    log = get_logger("benchmark")
    token, since_iso = get_github_token(), f"{since}T00:00:00Z"
    bench = ROOT / "data" / "bench"
    shutil.rmtree(bench, ignore_errors=True)  # always measure a cold fetch
    report = {}
    with make_client(token) as client:
        t0 = time.monotonic()
        counts, overflow = ex.land_pulls(client, repo, since_iso, bench / "graphql" / "pulls", log)
        for number, cursor in overflow:
            ex.land_review_overflow(client, repo, number, cursor, bench / "graphql" / "reviews", log)
        report["graphql"] = {"requests": ex.requests_for("graphql", repo),
                             "seconds": round(time.monotonic() - t0, 1),
                             "prs": counts["records"], "reviews": counts["reviews"]}
        t0 = time.monotonic()
        rest = ex.land_pulls_rest(client, repo, since_iso, bench / "rest", log)
        report["rest"] = {"requests": ex.requests_for("rest", repo),
                          "seconds": round(time.monotonic() - t0, 1), **rest}
    for api, r in report.items():
        log.info("BENCHMARK %s %s since %s: %s", api.upper(), repo, since, r)
    return report


# ---------------------------------------------------------------------------
# Scheduled deployment: monthly on the 28th, run by the uvasds-k8s-pool worker
# ---------------------------------------------------------------------------
def deploy() -> None:
    """Register (or update) the monthly deployment of package_health_flow."""
    from prefect.runner.storage import GitRepository
    from prefect.schedules import Cron

    # The job image (read-only filesystem) already has our dependencies installed
    package_health_flow.from_source(
        source=GitRepository(
            url=GIT_URL,
            branch="main",
            credentials={"access_token": Secret.load(SECRET_BLOCK)},  # block reference, not the value
        ),
        entrypoint="main.py:package_health_flow",
    ).deploy(
        name=DEPLOYMENT_NAME,
        work_pool_name=WORK_POOL,
        schedule=Cron("0 0 28 * *", timezone="America/New_York"),
        job_variables={
            "cpu_request": "2",
            "cpu_limit": "2",
            "memory_request": "2Gi",
            "memory_limit": "4Gi",
        },
    )


if __name__ == "__main__":
    try:
        if sys.argv[1:2] == ["deploy"]:  # uv run python main.py deploy
            deploy()
        elif sys.argv[1:2] == ["benchmark"]:  # uv run python main.py benchmark
            rest_benchmark_flow()
        elif sys.argv[1:2] == ["load"]:  # uv run python main.py load
            load_only_flow()
        else:
            package_health_flow()
    except MissingTokenError as exc:
        sys.exit(f"ERROR: {exc}")
