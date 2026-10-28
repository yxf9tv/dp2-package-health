"""Open Source Package Health pipeline (DS3022 DP2, package set B).

One Prefect flow runs six tasks in order:
extract (GitHub -> data/raw JSON) -> load (JSON -> DuckDB raw_*) -> dbt build
-> validate (Python) -> model (health score + rank) -> plot (PNGs).
"""

import logging
import os
import sys
from pathlib import Path

from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret

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

SECRET_BLOCK = "yxf9tv-pat"


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
@task(name="1-extract")
def extract(token: str) -> None:
    """Task 1: GitHub API -> data/raw/<owner>__<repo>/<endpoint>/page_NNNN.json."""
    log = get_logger("extract")
    log.info("extract stub: repos=%s window_start=%s", REPOS, WINDOW_START)


@task(name="2-load")
def load() -> None:
    """Task 2: data/raw JSON -> raw_* tables in packages.duckdb."""
    log = get_logger("load")
    log.info("load stub")


@task(name="3-dbt-build")
def dbt_build() -> None:
    """Task 3: dbt build (staging, facts, marts, tests). Only task that runs dbt."""
    log = get_logger("dbt_task")  # dbt itself writes logs/dbt.log
    log.info("dbt_build stub")


@task(name="4-validate")
def validate() -> None:
    """Task 4: Python checks across files, raw tables and models; outliers; stats."""
    log = get_logger("validate")
    log.info("validate stub")


@task(name="5-model")
def model() -> None:
    """Task 5: health score from mart_health_inputs; ranking written to DuckDB."""
    log = get_logger("model")
    log.info("model stub")


@task(name="6-plot")
def plot() -> None:
    """Task 6: three multi-dimensional PNGs in plots/."""
    log = get_logger("plot")
    log.info("plot stub")


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
    log.info("flow complete")


if __name__ == "__main__":
    try:
        package_health_flow()
    except MissingTokenError as exc:
        sys.exit(f"ERROR: {exc}")
