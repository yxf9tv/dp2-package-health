"""Unit tests for the pure parts of gh_client (no network)."""

import httpx
import pytest

import gh_client as g


def _resp(status: int, headers: dict | None = None, text: str = "") -> httpx.Response:
    """Build a fake GitHub response with a request attached (check_response reads its URL)."""
    req = httpx.Request("GET", "https://api.github.com/repos/x/y/pulls")
    return httpx.Response(status, headers=headers or {}, text=text, request=req)


# --- Link header pagination -------------------------------------------------
def test_parse_next_link_middle_page():
    link = ('<https://api.github.com/x?page=3>; rel="next", '
            '<https://api.github.com/x?page=9>; rel="last"')
    assert g.parse_next_link(link) == "https://api.github.com/x?page=3"


def test_parse_next_link_last_page_and_missing():
    assert g.parse_next_link('<https://api.github.com/x?page=1>; rel="prev"') is None
    assert g.parse_next_link(None) is None


# --- Rate limiter -----------------------------------------------------------
def test_seconds_until_adds_padding_and_never_negative():
    assert g.seconds_until(reset_epoch=100, now=40) == 60 + g.RESET_PADDING_S
    assert g.seconds_until(reset_epoch=100, now=500) == g.RESET_PADDING_S


def test_limiter_sleeps_below_margin_only():
    slept = []
    lim = g.RateLimiter(margin=50, sleep=slept.append, clock=lambda: 1000.0)
    assert lim.wait("core") == 0  # unknown budget: no sleep
    lim.update("core", remaining=51, reset_epoch=1600)
    assert lim.wait("core") == 0  # above margin: no sleep
    lim.update("core", remaining=49, reset_epoch=1600)
    assert lim.wait("core") == 600 + g.RESET_PADDING_S
    assert slept == [600 + g.RESET_PADDING_S]
    assert lim.wait("core") == 0  # state cleared after the reset sleep


def test_limiter_budgets_are_separate_per_api():
    slept = []
    lim = g.RateLimiter(margin=50, sleep=slept.append, clock=lambda: 0.0)
    lim.update("graphql", remaining=10, reset_epoch=100)
    assert lim.wait("core") == 0
    assert lim.wait("graphql") > 0


def test_odometer_counts_per_api_and_repo():
    odo = g.Odometer()
    for _ in range(3):
        odo.tick("rest", "dask/dask")
    odo.tick("graphql", "dask/dask")
    assert odo.total() == 4
    assert odo.summary() == {"graphql dask/dask": 1, "rest dask/dask": 3}


# --- Error classification ---------------------------------------------------
def test_ok_response_passes():
    g.check_response(_resp(200))


def test_primary_limit_is_transient():
    with pytest.raises(g.TransientError, match="primary"):
        g.check_response(_resp(403, {"x-ratelimit-remaining": "0"}))


def test_secondary_limit_honors_retry_after():
    slept = []
    lim = g.RateLimiter(sleep=slept.append)
    with pytest.raises(g.TransientError, match="secondary"):
        g.check_response(_resp(429, {"retry-after": "7"}), limiter=lim)
    assert slept == [7]


def test_server_error_is_transient():
    with pytest.raises(g.TransientError):
        g.check_response(_resp(502))


@pytest.mark.parametrize("status", [401, 404, 422])
def test_client_errors_are_permanent(status):
    with pytest.raises(g.PermanentError, match=str(status)):
        g.check_response(_resp(status, text="nope"))


# --- Landing files ----------------------------------------------------------
def test_write_then_read_landing(tmp_path):
    path = tmp_path / "dask__dask" / "commits" / "page_0001.json"
    g.write_landing(path, {"page": 1, "data": [{"sha": "abc"}]})
    assert g.read_landing(path) == {"page": 1, "data": [{"sha": "abc"}]}
    assert not list(path.parent.glob("*.tmp"))  # temp file was renamed away


def test_read_landing_rejects_truncated_or_missing(tmp_path):
    path = tmp_path / "page_0002.json"
    path.write_text('{"page": 2, "data": [{"sha": "ab')  # half-written page
    assert g.read_landing(path) is None
    assert g.read_landing(tmp_path / "nope.json") is None
