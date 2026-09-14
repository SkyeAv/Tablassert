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
from pathlib import Path
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


# --------------------------------------------------------------------------- #
# US-003: idempotent, atomic, bounded-parallel downloads (R4, R6)
#
# The byte-level tests below patch the AGENT seam (``agent._http_get_bytes``) the way
# ``tests/test_agent_fetch.py`` does, because they must control WHICH file fails/completes and
# observe the exact in-flight order -- behavior the generic ``net.urlopen`` patch cannot express.
# Everything is offline and nothing sleeps: coordination uses ``threading.Event``/counters, never
# wall-clock.
# --------------------------------------------------------------------------- #


def _object_listing_xml(keys: list[str]) -> str:
    """Build a list-objects-v2 XML body whose ``<Contents><Key>`` entries are ``keys``."""
    contents: str = "".join(f"<Contents><Key>{key}</Key><Size>1</Size></Contents>" for key in keys)
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        "<Name>pmc-oa-opendata</Name>"
        f"{contents}"
        "</ListBucketResult>"
    )


TWO_FILE_LISTING_XML: str = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
    "<Name>pmc-oa-opendata</Name>"
    "<Contents><Key>PMC11708054.1/a.xml</Key><Size>1</Size></Contents>"
    "<Contents><Key>PMC11708054.1/t.xlsx</Key><Size>1</Size></Contents>"
    "<Contents><Key>PMC11708054.1/pic.jpg</Key><Size>1</Size></Contents>"
    "</ListBucketResult>"
)


def _patch_agent_http(monkeypatch: pytest.MonkeyPatch, get_bytes: Callable[[str], bytes], *, object_listing: str = OBJECT_LISTING_XML) -> list[str]:
    """Patch the agent text seam with fixed payloads and the byte seam with ``get_bytes``.

    Returns:
        The recorded byte-request URLs, in call order.
    """
    from tablassert import agent

    def get_text(url: str, *, timeout: int = 120) -> str:
        if "list-type=2" in url and "delimiter=" in url:
            return LISTING_XML
        if "list-type=2" in url:
            return object_listing
        if url.endswith(".json"):
            return METADATA_OA
        raise AssertionError(f"unexpected text url: {url}")

    downloaded: list[str] = []

    def bytes_stub(url: str, *, timeout: int = 120) -> bytes:
        downloaded.append(url)
        return get_bytes(url)

    monkeypatch.setattr(agent, "_http_get_text", get_text)
    monkeypatch.setattr(agent, "_http_get_bytes", bytes_stub)
    return downloaded


def test_fetch_pmc_article_skips_files_already_downloaded(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """A rerun after success costs ZERO HTTP requests: non-empty destinations are skipped outright.

    WHY: the fleet retried whole articles during the outage, and each retry re-downloaded 10-15 files --
    multiplying DNS-lookup volume by N per article and re-exposing it to N more transient blips. The
    skip is also why REQ-FETCH-4 names a non-empty regular file as the completion marker: it is exactly
    what the atomic ``os.replace`` leaves behind.
    """
    for name in ("PMC11708054.1.json", "PMC11708054.1.xml", "table1.xlsx"):
        target: Path = tmp_path / "PMC11708054.1" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"COMPLETE")
    downloaded_bytes = _patch_agent_http(monkeypatch, lambda url: (_ for _ in ()).throw(AssertionError("no HTTP request may be made")))

    downloaded = fetch_pmc_article("PMC11708054", tmp_path)

    assert {path.name for path in downloaded} == {"PMC11708054.1.json", "PMC11708054.1.xml", "table1.xlsx"}
    assert downloaded_bytes == [], "every file was already complete -- no byte request at all"
    assert all((tmp_path / "PMC11708054.1" / name).read_bytes() == b"COMPLETE" for name in ("PMC11708054.1.json", "PMC11708054.1.xml", "table1.xlsx"))


def test_fetch_pmc_article_redownloads_zero_length_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """A zero-length destination is a torn write, not a valid empty file: it is re-downloaded.

    WHY: a killed worker can leave a zero-byte ``.xlsx`` behind (created by ``write_bytes`` mid-crash
    before US-003 made writes atomic). Trusting it would feed a corrupt empty table to
    ``candidate_tables`` and skip the article with a misleading "no qualifying table" reason.
    """
    for name in ("PMC11708054.1.json", "PMC11708054.1.xml"):
        target: Path = tmp_path / "PMC11708054.1" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"COMPLETE")
    torn: Path = tmp_path / "PMC11708054.1" / "table1.xlsx"
    torn.parent.mkdir(parents=True, exist_ok=True)
    torn.write_bytes(b"")  # zero-length: the torn write
    downloaded_bytes = _patch_agent_http(monkeypatch, lambda url: b"REPAIRED")

    downloaded = fetch_pmc_article("PMC11708054", tmp_path)

    assert [url.rsplit("/", 1)[-1] for url in downloaded_bytes] == ["table1.xlsx"], "only the torn file is re-fetched"
    assert torn.read_bytes() == b"REPAIRED"
    assert len(downloaded) == 3


def test_fetch_pmc_article_writes_atomically_and_cleans_up_part(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """Success leaves the final file and no ``.part``; failure leaves NEITHER file NOR ``.part``.

    WHY: before this, a crash mid-``write_bytes`` left a truncated ``.xlsx`` that the next run read as
    a corrupt table. The ``.part`` + ``os.replace`` contract makes a torn write invisible to readers,
    and the ``finally``-style cleanup makes it invisible to the NEXT run too.
    """
    from email.message import Message
    from urllib.error import HTTPError

    def bytes_router(url: str) -> bytes:
        if url.endswith("table1.xlsx"):
            raise HTTPError(url, 404, "Not Found", Message(), None)
        return b"BODY"

    _patch_agent_http(monkeypatch, bytes_router)

    with pytest.raises(HTTPError):
        fetch_pmc_article("PMC11708054", tmp_path)

    final: Path = tmp_path / "PMC11708054.1" / "table1.xlsx"
    part: Path = tmp_path / "PMC11708054.1" / "table1.xlsx.part"
    assert not part.exists(), "a failed download must remove its .part before propagating"
    assert not final.exists(), "no torn/partial file may be observable"
    assert (tmp_path / "PMC11708054.1" / "PMC11708054.1.xml").read_bytes() == b"BODY"

    # Success path: final file present, no .part sibling remains.
    _patch_agent_http(monkeypatch, lambda url: b"BODY")
    ok_dir: Path = tmp_path / "ok"
    fetch_pmc_article("PMC11708054", ok_dir)
    leftovers: list[Path] = list(ok_dir.rglob("*.part"))
    assert leftovers == [], "no .part file may survive a successful download"


def test_fetch_pmc_article_preserves_listing_order_under_parallelism(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """The returned list is S3 listing order even when the FIRST file finishes LAST.

    WHY: ``candidate_tables`` feeds ``tables[0]`` into the agent's table binding, so ordering is a
    semantic contract, not cosmetics. The stub blocks the first file until the last has completed
    (events, no sleeps), forcing a completion order that is the reverse of submission order.
    """
    import threading

    names: list[str] = ["PMC11708054.1.json", "PMC11708054.1.xml", "table1.xlsx"]
    first_done: threading.Event = threading.Event()
    others_done: threading.Event = threading.Event()
    completed: list[str] = []
    lock: threading.Lock = threading.Lock()

    def bytes_router(url: str) -> bytes:
        name: str = url.rsplit("/", 1)[-1]
        if name == names[0]:
            others_done.wait(timeout=30)  # finish LAST, on purpose
            first_done.set()
        else:
            with lock:
                completed.append(name)
                if len(completed) == len(names) - 1:
                    others_done.set()
        return b"BODY"

    _patch_agent_http(monkeypatch, bytes_router)

    downloaded = fetch_pmc_article("PMC11708054", tmp_path, concurrency=3)

    assert first_done.is_set(), "the blocked first file must complete"
    assert [path.name for path in downloaded] == names, "submission order, not completion order"


def test_fetch_pmc_article_bounds_in_flight_downloads(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """In-flight downloads reach ``concurrency`` and NEVER exceed it -- asserted with counters, not clocks.

    WHY: a timing assertion is flaky under ``pytest -n auto``; a counter cannot be. The stub parks each
    worker until ``concurrency`` downloads are simultaneously in flight, which is only possible if the
    pool truly reaches the bound, and records the maximum simultaneously-in-flight count.
    """
    import threading

    keys: list[str] = [f"PMC11708054.1/f{i:02d}.xml" for i in range(12)] + ["PMC11708054.1/t.xlsx"]
    listing: str = _object_listing_xml(keys)
    in_flight: int = 0
    max_in_flight: list[int] = [0]
    all_parked: threading.Event = threading.Event()
    lock: threading.Lock = threading.Lock()
    concurrency: int = 8

    def bytes_router(url: str) -> bytes:
        nonlocal in_flight
        with lock:
            in_flight += 1
            max_in_flight[0] = max(max_in_flight[0], in_flight)
            if in_flight == concurrency:
                all_parked.set()
        all_parked.wait(timeout=30)  # park until the pool is FULL: any leak past the bound jams here
        with lock:
            in_flight -= 1
        return b"BODY"

    _patch_agent_http(monkeypatch, bytes_router, object_listing=listing)

    downloaded = fetch_pmc_article("PMC11708054", tmp_path, concurrency=concurrency)

    assert len(downloaded) == len(keys)
    assert max_in_flight[0] == concurrency, "the pool reaches exactly the configured bound ..."
    assert max_in_flight[0] <= concurrency, "... and never exceeds it"


def test_fetch_pmc_article_rejects_non_positive_concurrency(tmp_path: Any) -> None:
    """``concurrency < 1`` is a loud ``ValueError``, never a silent degrade to serial.

    WHY: the user's bar is fail-loudly. Silently running serial for ``concurrency=0`` would hide a
    caller bug and make the throughput story untestable.
    """
    with pytest.raises(ValueError, match="concurrency"):
        fetch_pmc_article("PMC11708054", tmp_path, concurrency=0)
    with pytest.raises(ValueError, match="concurrency"):
        fetch_pmc_article("PMC11708054", tmp_path, concurrency=-3)


def test_fetch_pmc_article_first_failure_propagates_and_keeps_completed_files(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """The FIRST failing file's error wins (submission order), and earlier files stay on disk.

    WHY: this mirrors the old serial loop's observable behavior -- the article fails on the first
    broken file -- while completed files survive so the NEXT attempt's idempotent skip makes it cheap.
    """
    import threading
    from email.message import Message
    from urllib.error import HTTPError

    first_completed: threading.Event = threading.Event()

    def bytes_router(url: str) -> bytes:
        if url.endswith("a.xml"):
            body: bytes = b"KEPT"
            first_completed.set()
            return body
        first_completed.wait(timeout=30)  # make the failure order deterministic
        raise HTTPError(url, 404, "Not Found", Message(), None)

    _patch_agent_http(monkeypatch, bytes_router, object_listing=TWO_FILE_LISTING_XML)

    with pytest.raises(HTTPError) as exc_info:
        fetch_pmc_article("PMC11708054", tmp_path)

    assert exc_info.value.code == 404, "the first (and only) failing file's error propagates verbatim"
    assert (tmp_path / "PMC11708054.1" / "a.xml").read_bytes() == b"KEPT", "the completed file stays on disk"
    assert not (tmp_path / "PMC11708054.1" / "t.xlsx").exists()
    assert not list(tmp_path.rglob("*.part")), "no .part survives the failure"

    # When BOTH fail, the first in SUBMISSION order wins even though the later one fails instantly.
    # This scenario drives the REAL net.urlopen (not the agent seam) so the 500 is genuinely retried
    # and wrapped by the seam -- proving the submission-order rule holds across DIFFERENT error shapes.
    # ``monkeypatch.undo()`` restores scenario 1's agent-seam stubs first, or they would shadow net.
    monkeypatch.undo()

    def both_fail_urlopen(request: Any, timeout: int = 120) -> _FakeResponse:
        url: str = str(request.full_url)
        if url.endswith("a.xml"):
            raise HTTPError(url, 500, "Internal Server Error", Message(), None)
        if url.endswith("t.xlsx"):
            raise HTTPError(url, 404, "Not Found", Message(), None)
        if "list-type=2" in url and "delimiter=" in url:
            return _FakeResponse(LISTING_XML.encode("utf-8"))
        if "list-type=2" in url:
            return _FakeResponse(TWO_FILE_LISTING_XML.encode("utf-8"))
        return _FakeResponse(METADATA_OA.encode("utf-8"))

    sleeps: list[float] = []
    _no_sleep_retry(monkeypatch, sleeps)
    monkeypatch.setattr(net, "urlopen", both_fail_urlopen)

    with pytest.raises(NetworkTransientError) as transient:
        fetch_pmc_article("PMC11708054", tmp_path / "both")

    assert "500" in str(transient.value.last_error), "the 500 from the first file (retried then wrapped) beats the instant 404"
