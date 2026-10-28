"""Task 1 helpers: land every page of every endpoint under data/raw/.

Endpoints per repo (folders under data/raw/<owner>__<repo>/):
- commits/   REST, since=<window start or watermark>, Link pagination
- releases/  REST, fetched in full (small)
- pulls/     GraphQL, PRs sorted by UPDATED_AT desc, each with its first 100 reviews nested
- reviews/   GraphQL, overflow pages only, for PRs with more than 100 reviews

Each page file is written once, atomically. A valid page already on disk is skipped,
so an interrupted run resumes where it stopped without spending quota.
"""

import logging
import time
from pathlib import Path

import httpx
from prefect import task
from prefect.cache_policies import NO_CACHE

from gh_client import (
    API_URL,
    GRAPHQL_URL,
    ODOMETER,
    fetch_graphql,
    fetch_rest,
    read_landing,
    utc_now_iso,
    write_landing,
)

PER_PAGE = 100
PRS_PER_QUERY = 50  # 50 PRs x up to 100 nested reviews per GraphQL query

_REVIEW_FIELDS = """
  databaseId state submittedAt updatedAt
  author { login __typename }
  pullRequest { number }
"""

PULLS_QUERY = f"""
query($owner: String!, $name: String!, $cursor: String) {{
  rateLimit {{ cost remaining resetAt }}
  repository(owner: $owner, name: $name) {{
    pullRequests(first: {PRS_PER_QUERY}, after: $cursor,
                 orderBy: {{field: UPDATED_AT, direction: DESC}}) {{
      pageInfo {{ hasNextPage endCursor }}
      nodes {{
        number databaseId state isDraft createdAt updatedAt closedAt mergedAt
        author {{ login __typename }}
        reviews(first: 100) {{
          totalCount
          pageInfo {{ hasNextPage endCursor }}
          nodes {{ {_REVIEW_FIELDS} }}
        }}
      }}
    }}
  }}
}}"""

REVIEWS_QUERY = f"""
query($owner: String!, $name: String!, $number: Int!, $cursor: String) {{
  rateLimit {{ cost remaining resetAt }}
  repository(owner: $owner, name: $name) {{
    pullRequest(number: $number) {{
      reviews(first: 100, after: $cursor) {{
        pageInfo {{ hasNextPage endCursor }}
        nodes {{ {_REVIEW_FIELDS} }}
      }}
    }}
  }}
}}"""


def repo_dir(base: Path, repo: str) -> Path:
    """data/raw/numpy__numpy for 'numpy/numpy'."""
    return base / repo.replace("/", "__")


def page_file(folder: Path, page: int, prefix: str = "page") -> Path:
    return folder / f"{prefix}_{page:04d}.json"


# ---------------------------------------------------------------------------
# REST: follow the Link header; skip pages already on disk
# ---------------------------------------------------------------------------
def land_rest(client: httpx.Client, repo: str, endpoint: str, first_url: str,
              folder: Path, log: logging.Logger) -> dict:
    """Land every page of a REST endpoint. Returns counts for the log."""
    url, page = first_url, 1
    counts = {"records": 0, "fetched": 0, "skipped": 0}
    while url:
        path = page_file(folder, page)
        record = read_landing(path)
        if record is not None:
            counts["skipped"] += 1  # resume: valid page on disk, reuse its next link
            log.info("%s %s: skipping page %d (on disk)", repo, endpoint, page)
        else:
            body, next_url = fetch_rest(client, url, repo)
            record = {"repo": repo, "endpoint": endpoint, "url": url, "page": page,
                      "fetched_at": utc_now_iso(), "next_url": next_url, "data": body}
            write_landing(path, record)
            counts["fetched"] += 1
        counts["records"] += len(record["data"])
        url, page = record["next_url"], page + 1
    return counts


# ---------------------------------------------------------------------------
# GraphQL: PRs (with nested reviews) by cursor; stop once past the stop date
# ---------------------------------------------------------------------------
def land_pulls(client: httpx.Client, repo: str, stop_iso: str, folder: Path,
               log: logging.Logger) -> tuple[dict, list[tuple[int, str]]]:
    """Land PR pages until a page reaches PRs updated before stop_iso.

    Returns counts and the (pr_number, reviews_end_cursor) of PRs with >100 reviews.
    """
    owner, name = repo.split("/")
    cursor, page = None, 1
    counts = {"records": 0, "reviews": 0, "fetched": 0, "skipped": 0}
    overflow: list[tuple[int, str]] = []
    while True:
        path = page_file(folder, page)
        record = read_landing(path)
        if record is not None:
            counts["skipped"] += 1
            log.info("%s pulls: skipping page %d (on disk)", repo, page)
        else:
            variables = {"owner": owner, "name": name, "cursor": cursor}
            data = fetch_graphql(client, PULLS_QUERY, variables, repo)
            conn = data["repository"]["pullRequests"]
            record = {"repo": repo, "endpoint": "pulls", "url": GRAPHQL_URL, "page": page,
                      "fetched_at": utc_now_iso(), "query_variables": variables,
                      "end_cursor": conn["pageInfo"]["endCursor"],
                      "has_next": conn["pageInfo"]["hasNextPage"],
                      "rate_limit": data["rateLimit"], "data": conn["nodes"]}
            write_landing(path, record)
            counts["fetched"] += 1
        prs = record["data"]
        counts["records"] += len(prs)
        counts["reviews"] += sum(len(pr["reviews"]["nodes"]) for pr in prs)
        overflow += [(pr["number"], pr["reviews"]["pageInfo"]["endCursor"])
                     for pr in prs if pr["reviews"]["pageInfo"]["hasNextPage"]]
        # Sorted by updatedAt desc: once the oldest PR on a page is before the stop
        # date, every later page is older too. ISO-8601 UTC strings compare correctly.
        oldest = min((pr["updatedAt"] for pr in prs), default=None)
        if not record["has_next"] or oldest is None or oldest < stop_iso:
            return counts, overflow
        cursor, page = record["end_cursor"], page + 1


def land_review_overflow(client: httpx.Client, repo: str, number: int, cursor: str,
                         folder: Path, log: logging.Logger) -> dict:
    """Land the reviews after the first 100 for one PR (rare: very long reviews)."""
    owner, name = repo.split("/")
    page = 1
    counts = {"records": 0, "fetched": 0, "skipped": 0}
    while True:
        path = page_file(folder, page, prefix=f"pr{number}_page")
        record = read_landing(path)
        if record is not None:
            counts["skipped"] += 1
        else:
            variables = {"owner": owner, "name": name, "number": number, "cursor": cursor}
            data = fetch_graphql(client, REVIEWS_QUERY, variables, repo)
            conn = data["repository"]["pullRequest"]["reviews"]
            record = {"repo": repo, "endpoint": "reviews", "url": GRAPHQL_URL, "page": page,
                      "fetched_at": utc_now_iso(), "query_variables": variables,
                      "end_cursor": conn["pageInfo"]["endCursor"],
                      "has_next": conn["pageInfo"]["hasNextPage"],
                      "rate_limit": data["rateLimit"], "data": conn["nodes"]}
            write_landing(path, record)
            counts["fetched"] += 1
        counts["records"] += len(record["data"])
        if not record["has_next"]:
            log.info("%s reviews: PR #%d overflow -> %d more reviews", repo, number,
                     counts["records"])
            return counts
        cursor, page = record["end_cursor"], page + 1


# ---------------------------------------------------------------------------
# One repo, all endpoints (mapped over the repos for concurrency)
# ---------------------------------------------------------------------------
@task(name="extract-repo", cache_policy=NO_CACHE)  # client/logger are not hashable
def extract_repo(client: httpx.Client, repo: str, base: Path, window_start_iso: str,
                 watermarks: dict, log: logging.Logger) -> dict:
    """Land commits, releases, pulls (+ nested reviews) and review overflow for one repo.

    watermarks: {(repo, endpoint): iso} from _load_state; missing -> full window.
    """
    started = time.monotonic()
    root = repo_dir(base, repo)
    for leftover in root.rglob("*.tmp"):  # half-written pages from a crash
        leftover.unlink()

    commits_since = watermarks.get((repo, "commits"), window_start_iso)
    pulls_stop = watermarks.get((repo, "pulls"), window_start_iso)
    log.info("%s: commits since %s, pulls until updatedAt < %s", repo, commits_since, pulls_stop)

    commits_url = str(httpx.URL(f"{API_URL}/repos/{repo}/commits",
                                params={"since": commits_since, "per_page": PER_PAGE}))
    releases_url = f"{API_URL}/repos/{repo}/releases?per_page={PER_PAGE}"
    result = {
        "commits": land_rest(client, repo, "commits", commits_url, root / "commits", log),
        "releases": land_rest(client, repo, "releases", releases_url, root / "releases", log),
    }
    result["pulls"], overflow = land_pulls(client, repo, pulls_stop, root / "pulls", log)

    extra = {"records": 0, "fetched": 0, "skipped": 0}
    for number, cursor in overflow:
        counts = land_review_overflow(client, repo, number, cursor, root / "reviews", log)
        extra = {k: extra[k] + counts[k] for k in extra}
    result["review_overflow"] = extra

    for endpoint, c in result.items():
        log.info("%s %s: %d records (%d pages fetched, %d skipped)", repo, endpoint,
                 c["records"], c["fetched"], c["skipped"])
    log.info("%s: %d nested reviews, %d PRs with >100 reviews, %.0fs", repo,
             result["pulls"]["reviews"], len(overflow), time.monotonic() - started)
    return result


# ---------------------------------------------------------------------------
# REST path for PRs + reviews (benchmark only; never loaded)
# ---------------------------------------------------------------------------
def land_pulls_rest(client: httpx.Client, repo: str, stop_iso: str, folder: Path,
                    log: logging.Logger) -> dict:
    """REST version of land_pulls: PR pages, then one reviews call per PR in the window."""
    url = str(httpx.URL(f"{API_URL}/repos/{repo}/pulls",
                        params={"state": "all", "sort": "updated", "direction": "desc",
                                "per_page": PER_PAGE}))
    numbers, page = [], 1
    while url:
        body, next_url = fetch_rest(client, url, repo)
        write_landing(page_file(folder / "pulls", page),
                      {"repo": repo, "endpoint": "pulls", "url": url, "page": page,
                       "fetched_at": utc_now_iso(), "next_url": next_url, "data": body})
        numbers += [pr["number"] for pr in body if pr["created_at"] >= stop_iso]
        if not body or min(pr["updated_at"] for pr in body) < stop_iso:
            break
        url, page = next_url, page + 1
    reviews = 0
    for number in numbers:  # the per-PR calls that GraphQL nesting replaces
        r_url = f"{API_URL}/repos/{repo}/pulls/{number}/reviews?per_page={PER_PAGE}"
        r_page = 1
        while r_url:
            body, r_url_next = fetch_rest(client, r_url, repo)
            write_landing(page_file(folder / "reviews", r_page, prefix=f"pr{number}_page"),
                          {"repo": repo, "endpoint": "reviews", "url": r_url, "page": r_page,
                           "fetched_at": utc_now_iso(), "next_url": r_url_next, "data": body})
            reviews += len(body)
            r_url, r_page = r_url_next, r_page + 1
    log.info("REST %s: %d PR pages, %d PRs created in window, %d reviews", repo, page,
             len(numbers), reviews)
    return {"prs_in_window": len(numbers), "reviews": reviews}


def requests_for(api: str, repo: str) -> int:
    """Odometer reading for one API and repo (used by the benchmark)."""
    return ODOMETER.summary().get(f"{api} {repo}", 0)
