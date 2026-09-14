"""Offline tests for the agent HTTP transport's retry wiring (US-002).

These tests sit BETWEEN the two layers: ``agent._http_get_text`` / ``agent._http_get_bytes``
remain the documented monkeypatch seam (so ``tests/test_agent_fetch.py`` and
``tests/test_agent_branches.py`` keep passing unmodified), while the REAL production path now
retries through :mod:`tablassert.net`. Here the transport is patched at
``tablassert.net.urlopen`` -- one level BELOW the seam -- so the retry is exercised for real.
Nothing sleeps in real time: ``net.retry_transient`` is wrapped with an injected sleeper, which
is exactly the injection contract US-001 froze.

WHY this file exists: the fleet lost 2,194 articles (87.5% of queue failures) to transient
DNS/socket blips because each HTTP wrapper made a single ``urlopen`` call, and those 6,008
failures produced ZERO worker-log lines because the error only ever landed in ``state.json``.
"""

from __future__ import annotations

from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest

from tablassert import net
from tablassert.agent import fetch_pmc_article
from tablassert.errors import NetworkTransientError

# Mirror of test_agent_fetch.py's fixtures (kept independent on purpose: that file is frozen
# as a regression contract and must never gain these imports).
LISTING_XML: str = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
    "<Name>pmc-oa-opendata</Name>"
    "<Prefix>PMC11708054.</Prefix>"
    "<CommonPrefixes><Prefix>PMC11708054.1/</Prefix></CommonPrefixes>"
    "</ListBucketResult>"
)
METADATA_OA: str = '{"is_pmc_openaccess": true, "license_code": "CC-BY"}'
OBJECT_LISTING_XML: str = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
    "<Name>pmc-oa-opendata</Name>"
    "<Contents><Key>PMC11708054.1/PMC11708054.1.json</Key><Size>1</Size></Contents>"
    "<Contents><Key>PMC11708054.1/PMC11708054.1.xml</Key><Size>1</Size></Contents>"
    "<Contents><Key>PMC11708054.1/table1.xlsx</Key><Size>1</Size></Contents>"
    "</ListBucketResult>"
)


def _dns_error() -> Exception:
    """Build the fleet's dominant failure: a DNS blip as ``urlopen`` raises it (URLError-wrapped gaierror)."""
    import socket
    from urllib.error import URLError

    return URLError(socket.gaierror(-2, "Name or service not known"))


class _FakeResponse:
    """Minimal ``urlopen`` result: a context manager whose ``read()`` returns one payload."""

    def __init__(self, body: bytes) -> None:
        self._body = body

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self) -> bytes:
        return self._body


def _payload_for(url: str) -> bytes:
    """Route a urlopen URL to the body the fetch ladder expects (listing / metadata / objects / file)."""
    if "list-type=2" in url and "delimiter=" in url:
        return LISTING_XML.encode("utf-8")
    if "list-type=2" in url:
        return OBJECT_LISTING_XML.encode("utf-8")
    if url.endswith(".json"):
        return METADATA_OA.encode("utf-8")
    return b"FILEBYTES"


def _no_sleep_retry(monkeypatch: pytest.MonkeyPatch, sleeps: list[float]) -> None:
    """Wrap the REAL driver with an injected sleeper so tests exercise retry logic without waiting."""

    real = net.retry_transient

    def wrapped(operation: Callable[[], Any], **kwargs: Any) -> Any:
        kwargs["sleep"] = sleeps.append
        kwargs["rng"] = lambda: 1.0
        return real(operation, **kwargs)

    monkeypatch.setattr(net, "retry_transient", wrapped)


def _patch_urlopen(monkeypatch: pytest.MonkeyPatch, failures: dict[str, int], raised: Exception | None = None) -> list[str]:
    """Monkeypatch ``tablassert.net.urlopen`` to fail each matching URL prefix N times, then succeed.

    Args:
        failures: Mapping of substring -> number of leading failures for any URL containing it.
        raised: The exception to raise on a failure; a fresh DNS blip when omitted.

    Returns:
        The recorded request URLs, in call order.
    """
    requested: list[str] = []
    counts: dict[str, int] = {}

    def fake_urlopen(request: Any, timeout: int = 120) -> _FakeResponse:
        url: str = str(request.full_url)
        requested.append(url)
        for marker, wanted in failures.items():
            if marker in url:
                used = counts.get(marker, 0)
                counts[marker] = used + 1
                if used < wanted:
                    raise raised if raised is not None else _dns_error()
        return _FakeResponse(_payload_for(url))

    monkeypatch.setattr(net, "urlopen", fake_urlopen)
    return requested


def test_http_get_text_delegates_to_the_retrying_net_seam(monkeypatch: pytest.MonkeyPatch) -> None:
    """The agent seam is a thin delegate: patching ``net.urlopen`` reaches it, and retry works through it.

    WHY: ``tests/test_agent_fetch.py`` replaces ``agent._http_get_text`` wholesale, so it can never see
    the retry -- by design. This is the only place the delegate is proven end-to-end: two DNS blips
    against the REAL ``net.urlopen`` are retried by ``net.http_get_text`` and the call still returns
    the decoded text.
    """
    from tablassert import agent

    sleeps: list[float] = []
    _no_sleep_retry(monkeypatch, sleeps)
    requested = _patch_urlopen(monkeypatch, {"metadata": 2})

    text: str = agent._http_get_text("https://pmc.example/PMC1.1/metadata.json")

    assert text == METADATA_OA
    assert len(requested) == 3, "two failures plus the successful third call"
    assert sleeps == [1.0, 2.0], "the equal-jitter schedule with rng() == 1.0, zero cost had it succeeded first"


def test_fetch_pmc_article_retries_transient_dns_blip_on_listing(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """A one-shot DNS blip on ANY of the fetch ladder's calls no longer loses the article.

    WHY: the fleet's 2,194 DNS failures came from exactly this: one of the ~13-18 sequential calls
    blipping once and the article being recorded as a terminal skip. Each stage (version listing,
    metadata, object listing, one file download) blips once here and the article still completes.
    """
    sleeps: list[float] = []
    _no_sleep_retry(monkeypatch, sleeps)
    requested = _patch_urlopen(monkeypatch, {"delimiter=": 1, ".json": 1, "table1.xlsx": 1, "PMC11708054.1/PMC11708054.1.xml": 1})

    downloaded = fetch_pmc_article("PMC11708054", tmp_path)

    assert {path.name for path in downloaded} == {"PMC11708054.1.json", "PMC11708054.1.xml", "table1.xlsx"}
    assert sleeps == [1.0, 1.0, 1.0, 1.0], "each blipped stage (listing, metadata, xml, xlsx) paid exactly one retry sleep"
    listing_calls: int = sum(1 for url in requested if "delimiter=" in url)
    assert listing_calls == 2, "the version listing was retried exactly once"


def test_fetch_pmc_article_raises_network_transient_after_exhausted_retries(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """A sustained outage exhausts the budget and raises ``NetworkTransientError``, never the raw blip.

    WHY: US-004 records ``error_code`` from the exception's stable ``code``. A raw ``URLError`` has no
    ``code``, so without this wrap the supervisor could not mark the article ``network-transient`` and
    the fleet consumer would keep keyword-matching notes strings.
    """
    sleeps: list[float] = []
    _no_sleep_retry(monkeypatch, sleeps)
    _patch_urlopen(monkeypatch, {"delimiter=": 10})

    with pytest.raises(NetworkTransientError) as exc_info:
        fetch_pmc_article("PMC11708054", tmp_path)

    assert exc_info.value.code == "network-transient"
    assert exc_info.value.attempts == net.DEFAULT_ATTEMPTS
    assert sleeps == [1.0, 2.0, 4.0], "four attempts, three sleeps, none after the final failure"


def test_transient_retry_is_logged_at_warning_with_url_and_delay(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """Every retry and the final give-up are VISIBLE: warning per sleep, error on exhaustion.

    WHY: the 6,008 fleet DNS failures produced zero log lines -- the failure only ever reached
    ``state.json``. A worker log must now name the URL, the attempt index, and the delay for every
    retry, and emit one error line when the budget is gone, so an outage is diagnosable from logs
    alone.
    """
    records: list[tuple[str, str, dict[str, Any]]] = []

    def record(level: str) -> Any:
        def _inner(message: str, **kwargs: Any) -> None:
            records.append((level, message, kwargs))

        return _inner

    monkeypatch.setattr(net, "logger", SimpleNamespace(warning=record("warning"), error=record("error")))
    sleeps: list[float] = []
    _no_sleep_retry(monkeypatch, sleeps)

    # Case 1: a single blip -> exactly one warning, no error, success.
    _patch_urlopen(monkeypatch, {"delimiter=": 1})
    downloaded = fetch_pmc_article("PMC11708054", tmp_path / "a")
    assert downloaded, "the retried fetch must still succeed"
    warnings = [entry for entry in records if entry[0] == "warning"]
    assert len(warnings) == 1, "one blip logs exactly one retry warning"
    _level, template, fields = warnings[0]
    for placeholder in ("{target}", "{attempt}", "{delay:.2f}", "{error}"):
        assert placeholder in template, f"retry warning template must carry {placeholder}"
    assert fields["target"].startswith("https://pmc-oa-opendata.s3"), f"the warning must name the failing URL: {fields['target']}"
    assert fields["attempt"] == 1
    assert fields["delay"] == 1.0
    assert records[-1][0] == "warning", "no error is logged for a recovered fetch"

    # Case 2: sustained outage -> warnings per retry plus exactly one terminal error.
    records.clear()
    _patch_urlopen(monkeypatch, {"delimiter=": 10})
    with pytest.raises(NetworkTransientError):
        fetch_pmc_article("PMC11708054", tmp_path / "b")
    warnings = [entry for entry in records if entry[0] == "warning"]
    errors = [entry for entry in records if entry[0] == "error"]
    assert len(warnings) == 3, "one warning per retry sleep"
    assert len(errors) == 1, "exhaustion logs exactly one error"
    assert errors[0][2]["target"].startswith("https://pmc-oa-opendata.s3")


def test_permanent_fetch_errors_still_propagate_unwrapped(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """A permanent 404 is never retried and never wrapped: the raw ``HTTPError`` escapes on attempt one.

    WHY: ``fetch_pmc_article``'s documented contract is that permanent failures propagate unwrapped so
    existing ``except FileNotFoundError`` / ``except PermissionError`` callers keep working. A 404 is a
    mistyped bucket key or an S3 API change -- retrying it four times would only delay the real bug.
    """
    from email.message import Message
    from urllib.error import HTTPError

    sleeps: list[float] = []
    _no_sleep_retry(monkeypatch, sleeps)
    requested = _patch_urlopen(monkeypatch, {"delimiter=": 10}, raised=HTTPError("https://pmc.example/PMC1.", 404, "Not Found", Message(), None))

    with pytest.raises(HTTPError) as exc_info:
        fetch_pmc_article("PMC11708054", tmp_path)

    assert exc_info.value.code == 404
    assert len(requested) == 1, "a permanent error must not be retried"
    assert sleeps == [], "a permanent error must not pay a backoff sleep"
