"""GitHub API client helpers for the package-health flow.

- RateLimiter: one shared, thread-safe budget per API ("core" REST, "graphql")
- Odometer: counts every request in the run, per API and per repo
- rest_get / graphql: one request each; classify errors as transient or permanent
- fetch_rest / fetch_graphql: Prefect tasks that retry only transient errors
- write_landing / read_landing: atomic page files under data/raw/
"""

import json
import os
import threading
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import httpx
from prefect import task
from prefect.cache_policies import NO_CACHE
from prefect.tasks import exponential_backoff

API_URL = "https://api.github.com"
GRAPHQL_URL = f"{API_URL}/graphql"
API_VERSION = "2026-03-10"
SAFETY_MARGIN = 50  # stop spending at this many requests/points left
RESET_PADDING_S = 5  # extra seconds after the reset time, for clock drift
SECONDARY_DEFAULT_WAIT_S = 60  # GitHub's advice when Retry-After is missing


class TransientError(Exception):
    """A failure that can succeed on retry (rate limit, 5xx, network)."""


class PermanentError(Exception):
    """A failure that a retry cannot fix (bad token, 404, bad query)."""


# ---------------------------------------------------------------------------
# Rate limiter and odometer (module-level singletons shared by all threads)
# ---------------------------------------------------------------------------
def seconds_until(reset_epoch: float, now: float) -> float:
    """Seconds to sleep until reset_epoch (+ padding); never negative."""
    return max(0.0, reset_epoch - now) + RESET_PADDING_S


class RateLimiter:
    """Tracks remaining budget per API and sleeps before the budget runs out."""

    def __init__(self, margin: int = SAFETY_MARGIN, sleep=time.sleep, clock=time.time):
        self._lock = threading.Lock()
        self._state: dict[str, tuple[int, float]] = {}  # api -> (remaining, reset_epoch)
        self._margin = margin
        self._sleep = sleep
        self._clock = clock

    def update(self, api: str, remaining: int, reset_epoch: float) -> None:
        """Record the budget that GitHub reported on the latest response."""
        with self._lock:
            self._state[api] = (remaining, reset_epoch)

    def wait(self, api: str) -> float:
        """Before a request: sleep until reset if the budget is below the margin.

        The lock is held while sleeping, so every thread on this API waits too.
        Returns the seconds slept (0 if no wait).
        """
        with self._lock:
            remaining, reset_epoch = self._state.get(api, (None, 0.0))
            if remaining is None or remaining >= self._margin:
                return 0.0
            pause = seconds_until(reset_epoch, self._clock())
            self._sleep(pause)
            del self._state[api]  # unknown again until the next response
            return pause

    def pause(self, seconds: float) -> None:
        """Sleep for a secondary rate limit (Retry-After)."""
        self._sleep(seconds)

    def snapshot(self) -> dict[str, tuple[int, float]]:
        with self._lock:
            return dict(self._state)


class Odometer:
    """Counts API requests for the whole flow run (extended item: API odometer)."""

    def __init__(self):
        self._lock = threading.Lock()
        self._counts: Counter = Counter()  # (api, repo) -> requests

    def tick(self, api: str, repo: str) -> None:
        with self._lock:
            self._counts[(api, repo)] += 1

    def total(self) -> int:
        with self._lock:
            return sum(self._counts.values())

    def summary(self) -> dict[str, int]:
        """Requests per 'api repo' key, e.g. {'graphql dask/dask': 12}."""
        with self._lock:
            return {f"{api} {repo}": n for (api, repo), n in sorted(self._counts.items())}


LIMITER = RateLimiter()
ODOMETER = Odometer()


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
def make_client(token: str) -> httpx.Client:
    """One shared HTTP client (thread-safe) with auth and API version headers."""
    return httpx.Client(
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": API_VERSION,
        },
        timeout=httpx.Timeout(60.0),
    )


def parse_next_link(link_header: str | None) -> str | None:
    """Return the rel="next" URL from a GitHub Link header, or None on the last page."""
    if not link_header:
        return None
    for part in link_header.split(","):
        url_part, _, rel_part = part.partition(";")
        if 'rel="next"' in rel_part:
            return url_part.strip().strip("<>")
    return None


def check_response(resp: httpx.Response, limiter: RateLimiter = LIMITER) -> None:
    """Raise TransientError or PermanentError for a non-2xx response."""
    status, url = resp.status_code, str(resp.request.url)
    if status < 400:
        return
    if status in (403, 429):
        if resp.headers.get("x-ratelimit-remaining") == "0":
            # Primary limit: the limiter now holds remaining=0, so the retry sleeps to reset
            raise TransientError(f"{status} primary rate limit exhausted: {url}")
        if "retry-after" in resp.headers or "rate limit" in resp.text.lower():
            wait_s = int(resp.headers.get("retry-after", SECONDARY_DEFAULT_WAIT_S))
            limiter.pause(wait_s)
            raise TransientError(f"{status} secondary rate limit, waited {wait_s}s: {url}")
    if status >= 500:
        raise TransientError(f"{status} server error: {url}")
    raise PermanentError(f"{status} {url}: {resp.text[:300]}")


def _record_rest_budget(resp: httpx.Response, limiter: RateLimiter) -> None:
    """Read X-RateLimit-Remaining / X-RateLimit-Reset from every REST response."""
    remaining = resp.headers.get("x-ratelimit-remaining")
    reset = resp.headers.get("x-ratelimit-reset")
    if remaining is not None and reset is not None:
        limiter.update("core", int(remaining), float(reset))


def rest_get(client: httpx.Client, url: str, repo: str, params: dict | None = None):
    """One REST GET. Returns (json_body, next_url)."""
    LIMITER.wait("core")
    try:
        resp = client.get(url, params=params)
    except httpx.TransportError as exc:  # timeouts, dropped connections
        raise TransientError(f"network error on {url}: {exc!r}") from exc
    ODOMETER.tick("rest", repo)
    _record_rest_budget(resp, LIMITER)
    check_response(resp)
    try:
        body = resp.json()
    except json.JSONDecodeError as exc:
        raise TransientError(f"unparseable JSON from {url}") from exc
    return body, parse_next_link(resp.headers.get("link"))


def graphql(client: httpx.Client, query: str, variables: dict, repo: str) -> dict:
    """One GraphQL POST. The query must select rateLimit { remaining resetAt }."""
    LIMITER.wait("graphql")
    try:
        resp = client.post(GRAPHQL_URL, json={"query": query, "variables": variables})
    except httpx.TransportError as exc:
        raise TransientError(f"network error on GraphQL ({repo}): {exc!r}") from exc
    ODOMETER.tick("graphql", repo)
    check_response(resp)
    try:
        body = resp.json()
    except json.JSONDecodeError as exc:
        raise TransientError(f"unparseable GraphQL JSON ({repo})") from exc
    if body.get("errors"):
        if any(e.get("type") == "RATE_LIMITED" for e in body["errors"]):
            raise TransientError(f"GraphQL rate limited ({repo})")
        raise PermanentError(f"GraphQL errors ({repo}): {body['errors']}")
    data = body["data"]
    # Track the 5,000-point hourly budget from the rateLimit field
    rate = data.get("rateLimit")
    if rate:
        reset = datetime.fromisoformat(rate["resetAt"].replace("Z", "+00:00")).timestamp()
        LIMITER.update("graphql", rate["remaining"], reset)
    return data


def check_rate_limit(client: httpx.Client) -> dict:
    """GET /rate_limit (free: does not count against the budget)."""
    body, _ = rest_get(client, f"{API_URL}/rate_limit", repo="-")
    return {k: body["resources"][k] for k in ("core", "graphql")}


# ---------------------------------------------------------------------------
# Prefect retry wrappers: retry only TransientError, with exponential backoff
# ---------------------------------------------------------------------------
def _is_transient(task, task_run, state) -> bool:
    """Prefect retry_condition_fn: retry only when the failure was transient."""
    try:
        state.result()
    except TransientError:
        return True
    except Exception:
        return False
    return False


_RETRY = dict(
    retries=4,
    retry_delay_seconds=exponential_backoff(backoff_factor=10),  # 10, 20, 40, 80 s
    retry_jitter_factor=0.5,
    retry_condition_fn=_is_transient,
    cache_policy=NO_CACHE,  # the client argument cannot be hashed; never cache API calls
)


@task(name="fetch-rest", **_RETRY)
def fetch_rest(client: httpx.Client, url: str, repo: str, params: dict | None = None):
    return rest_get(client, url, repo, params)


@task(name="fetch-graphql", **_RETRY)
def fetch_graphql(client: httpx.Client, query: str, variables: dict, repo: str) -> dict:
    return graphql(client, query, variables, repo)


# ---------------------------------------------------------------------------
# Landing files: write atomically, read defensively
# ---------------------------------------------------------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def write_landing(path: Path, record: dict) -> None:
    """Write a page file via temp file + rename, so a crash never leaves half a page."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(record))
    os.replace(tmp, path)  # atomic on the same filesystem


def read_landing(path: Path) -> dict | None:
    """Return the page record, or None if the file is missing or not valid JSON."""
    try:
        record = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    return record if isinstance(record, dict) and "data" in record else None
