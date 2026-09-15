"""Offline unit tests for :mod:`tablassert.net`, the single network-resilience seam.

Nothing here touches a socket and nothing here waits: the driver's ``sleep`` / ``rng`` are recording
stubs, and the HTTP seam is driven through ``tablassert.net.urlopen`` monkeypatched at the module. The
module is pure stdlib, so there is deliberately NO ``importorskip`` -- these tests must always run.

WHY this file exists: a 16-worker fleet run over 42,981 PMC articles produced 193 successes and 2,507
failures, and 2,194 of those failures (87.5%) were DNS-shaped -- 2,031 x ``[Errno -2] Name or service not
known``, 154 x ``[Errno -3] Temporary failure in name resolution``, 9 x ``No address associated with
hostname``. ``/etc/nsswitch.conf`` reads ``hosts: mymachines resolve [!UNAVAIL=return] files myhostname
dns``, so a NOTFOUND from a saturated ``systemd-resolved`` returns IMMEDIATELY as ``EAI_NONAME``: a
transient failure that looks permanent. Every test below names the fleet failure class it protects.
"""

from __future__ import annotations

import ast
import http.client
import json
import pickle
import socket
import ssl
import sys
from datetime import UTC, datetime, timedelta
from email.message import Message
from email.utils import format_datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request

import pytest

from tablassert import net
from tablassert.errors import DOCS_URL, NetworkTransientError


def _http_error(code: int, message: str = "status", headers: Message | None = None) -> HTTPError:
    """Build an offline ``HTTPError``; ``fp`` is unused by the classifier.

    Args:
        code: HTTP status to carry.
        message: The reason phrase.
        headers: Response headers; an empty ``Message`` when omitted.

    Returns:
        A constructed ``HTTPError`` that never touched a socket.
    """
    return HTTPError(f"https://pmc-oa-opendata.s3.amazonaws.com/{code}", code, message, Message() if headers is None else headers, None)


def _header(name: str, value: str) -> Message:
    """Build a one-header ``email.message.Message`` shaped like ``HTTPError.headers``."""
    message: Message = Message()
    message[name] = value
    return message


def _dns_error(errno: int, text: str) -> URLError:
    """Build the exact ``URLError``-wrapped ``gaierror`` shape ``urlopen`` raises on a DNS failure."""
    return URLError(socket.gaierror(errno, text))


class _Broken(Exception):
    """An exception whose ``__str__`` raises, standing in for a badly-written third-party error type."""

    def __str__(self) -> str:
        raise RuntimeError("cannot format myself")


class _RecordingLogger:
    """Offline stand-in for the logger bound to ``net.logger`` (mirrors ``tests/test_cover_cli.py``)."""

    def __init__(self) -> None:
        self.warnings: list[tuple[str, dict[str, Any]]] = []
        self.errors: list[tuple[str, dict[str, Any]]] = []

    def warning(self, message: str, /, **fields: Any) -> None:
        self.warnings.append((message, fields))

    def error(self, message: str, /, **fields: Any) -> None:
        self.errors.append((message, fields))


class _FakeResponse:
    """Offline stand-in for ``urllib``'s context-managed response."""

    def __init__(self, data: bytes) -> None:
        self._data: bytes = data

    def read(self) -> bytes:
        return self._data

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, *exc_info: object) -> bool:
        return False


class _RecordingUrlopen:
    """A fake ``urlopen`` replaying a scripted outcome list and recording every request it saw.

    The FINAL outcome repeats forever, so a driver that keeps retrying never runs out of script.
    """

    def __init__(self, outcomes: list[bytes | BaseException]) -> None:
        self.outcomes: list[bytes | BaseException] = list(outcomes)
        self.requests: list[Request] = []
        self.timeouts: list[int] = []

    def __call__(self, request: Request, timeout: int) -> _FakeResponse:
        self.requests.append(request)
        self.timeouts.append(timeout)
        outcome: bytes | BaseException = self.outcomes.pop(0)
        if not self.outcomes:
            self.outcomes.append(outcome)
        if isinstance(outcome, BaseException):
            raise outcome
        return _FakeResponse(outcome)


def _stub_retry_clock(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Replace ``retry_transient``'s DEFAULT sleep/rng with recording stubs; return the sleep log.

    WHY ``__kwdefaults__`` rather than ``monkeypatch.setattr(net.time, "sleep", ...)``: ``sleep=time.sleep``
    and ``rng=random.random`` are bound as default arguments at function-DEFINITION time, so patching the
    ``time`` module never reaches them. Overriding ``__kwdefaults__`` lets ``http_get_text`` call the REAL
    driver through its REAL default resolution while staying instant. Pinning ``rng`` to 1.0 makes every
    delay equal its un-jittered schedule, so the arithmetic is assertable exactly.

    Args:
        monkeypatch: The pytest fixture that restores the original defaults after the test.

    Returns:
        The list that every driver sleep is appended to.
    """
    sleeps: list[float] = []
    patched: dict[str, Any] = dict(net.retry_transient.__kwdefaults__ or {})
    patched["sleep"] = sleeps.append
    patched["rng"] = lambda: 1.0
    monkeypatch.setattr(net.retry_transient, "__kwdefaults__", patched)
    return sleeps


def test_net_imports_only_stdlib_and_errors_and_log() -> None:
    """Guard REQ-NET-1: ``net.py`` pulls in nothing but stdlib plus ``errors`` and ``log``.

    WHY: this is the shared seam BOTH ``agent.py`` (behind the ``[agent]`` extra) and ``cli.py`` (base
    install) must be able to import. A single ``import httpx`` / ``import smolagents`` here would make
    ``tablassert build`` -- which downloads BABEL through ``cli.download_babel_file`` -- fail on any
    install without the agent extra, and would add a dependency the work is forbidden from adding.
    """
    source: str = Path(net.__file__).read_text(encoding="utf-8")
    modules: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None and node.level == 0:
            modules.add(node.module)
    assert modules, "net.py imports nothing; this guard went vacuous"
    first_party: set[str] = {name for name in modules if name.split(".")[0] not in sys.stdlib_module_names}
    assert first_party == {"tablassert.errors", "tablassert.log"}


def test_is_transient_dns_eai_noname_is_transient() -> None:
    """Every failure class observed in the fleet run must classify TRANSIENT (REQ-NET-3).

    WHY: these are the literal 2,507 failure notes from the 16-worker run. 2,031 articles died on
    ``[Errno -2] Name or service not known`` and 154 on ``[Errno -3] Temporary failure in name
    resolution``; a live burst test resolved the same S3 host 300/300 at 60-way concurrency, so the host
    was never actually down -- ``resolve [!UNAVAIL=return]`` in ``/etc/nsswitch.conf`` turned a saturated
    resolver's NOTFOUND into an immediate ``EAI_NONAME``. If any row here classified permanent, that whole
    failure class would still be lost on the first attempt.
    """
    transient: list[BaseException] = [
        _dns_error(-2, "Name or service not known"),  # 2,031 fleet failures
        _dns_error(-3, "Temporary failure in name resolution"),  # 154 fleet failures
        socket.gaierror(-2, "No address associated with hostname"),  # 9 fleet failures, bare (no URLError)
        socket.gaierror(-3, "Temporary failure in name resolution"),
        ConnectionResetError(104, "Connection reset by peer"),
        ConnectionAbortedError(103, "Software caused connection abort"),
        TimeoutError("The handshake operation timed out"),
        TimeoutError("The read operation timed out"),  # IS TimeoutError on >= 3.10
        ssl.SSLEOFError(8, "TLS/SSL connection has been closed (EOF)"),
        ssl.SSLError(1, "[SSL: WRONG_VERSION_NUMBER] wrong version number"),
        http.client.RemoteDisconnected("Remote end closed connection without response"),
        http.client.IncompleteRead(b"partial"),
        http.client.BadStatusLine("''"),
        _http_error(408, "Request Timeout"),
        _http_error(425, "Too Early"),
        _http_error(429, "Too Many Requests"),
        _http_error(500, "Internal Server Error"),
        _http_error(502, "Bad Gateway"),
        _http_error(503, "Service Unavailable"),
        _http_error(504, "Gateway Timeout"),
        URLError("timed out"),
        # 331 worker notes and 547 log hits carried this exact gateway shape: a 503 from the local
        # OpenAI-compatible gateway wrapping an upstream 429 with a reset hint in the message body.
        RuntimeError(
            "Error in generating model output: Error code: 503 - {'error': {'message': "
            "'Rate limit reached for requests (reset after 4m 16s)', 'type': 'requests', 'code': '429'}}"
        ),
    ]
    permanent_leaks: list[str] = [repr(exc) for exc in transient if not net.is_transient(exc)]
    assert not permanent_leaks, f"transient fleet failures misclassified as permanent: {permanent_leaks}"


def test_is_transient_permanent_4xx_and_value_errors_are_not_retried() -> None:
    """Permanent failures must NOT be retried, and the unknown must default to permanent (REQ-NET-4/7).

    WHY: ``fetch_pmc_article`` raises ``ValueError`` for a bad PMC id, ``FileNotFoundError`` when an
    article has no OA version or no table files, and ``PermissionError`` when the metadata is readable
    but not CC-licensed. ``run_supervisor`` branches on exactly those types to record a legitimate
    ``SKIPPED``. Retrying them four times would burn ~7 s per skip across 42,981 queued articles and --
    worse -- a wrapper would break the ``except PermissionError`` ladder at ``agent.py:459-465``. The
    unknown-exception default is the fail-loudly half: a novel bug must surface on attempt 1, not hide
    behind four retries.
    """
    permanent: list[BaseException] = [
        _http_error(400, "Bad Request"),
        _http_error(401, "Unauthorized"),
        _http_error(403, "Forbidden"),  # the not-open-access case the fleet hits constantly
        _http_error(404, "Not Found"),
        _http_error(405, "Method Not Allowed"),
        _http_error(410, "Gone"),
        _http_error(422, "Unprocessable Entity"),
        ValueError("PMC12345 is not a valid PMC id"),
        TypeError("expected str, got None"),
        KeyError("is_pmc_openaccess"),
        PermissionError("article is not CC-licensed"),
        FileNotFoundError("no open-access versions"),
        IsADirectoryError("expected a file"),
        NotADirectoryError("expected a directory"),
        json.JSONDecodeError("Expecting value", "<s3 listing>", 0),  # a ValueError subclass
        RuntimeError("a novel bug nobody has seen before"),  # the table's conservative default
        OSError(28, "No space left on device"),  # a genuine local fault, not a network blip
        KeyboardInterrupt(),
        SystemExit(1),
    ]
    retried: list[str] = [repr(exc) for exc in permanent if net.is_transient(exc)]
    assert not retried, f"permanent failures would be retried: {retried}"


def test_is_transient_ordinary_messages_containing_numbers_or_paths_are_permanent() -> None:
    """Message patterns are word-bounded: digits/paths/identifiers that merely CONTAIN a token are permanent.

    WHY: the table is a fleet's last-resort heuristic applied to ~42,981 articles. A bare-substring
    ``"429"" would also match ``row count 4290``; a bare ``"ssl"" any ``/etc/ssl/...`` path; a bare
    ``"timeout"`` an identifier like ``timeout_budget``. Those are deterministic bugs -- classifying them
    transient wastes four attempts EACH and stamps them ``network-transient``, the exact mislabel that
    made 2,194 recoverable DNS failures indistinguishable from 7,002 real skips.
    """
    ordinary: tuple[str, ...] = (
        "row count 4290 does not match header",  # '429' inside a digit RUN
        "failed to parse table for PMC11429345",  # '429' inside a PMC id
        "config /etc/ssl/openssl.cnf is malformed",  # 'ssl' inside a path
        "the assertion timeout_budget <= 0 held",  # 'timeout' inside an identifier
        "invalid rate_limit_per_hour setting",  # 'rate_limit' followed by '_' is not a boundary
        "error code: 404 not found",
    )
    for message in ordinary:
        assert not net.is_transient(RuntimeError(message)), f"an ordinary message must stay permanent: {message!r}"


def test_is_transient_bare_network_errno_oserrors_are_transient() -> None:
    """A bare ``OSError`` with a NETWORK errno is transient; a local one is permanent.

    WHY: ``stream_copy``'s mid-stream ``response.read()`` raises the raw socket ``OSError`` unwrapped by
    any library exception, so only its errno distinguishes ``ENETUNREACH`` from ``ENOSPC``. Before the
    errno table existed, ``download_babel_file`` silently narrowed its 5-attempt guarantee for these to
    a single attempt -- a regression the Tier-2 review demonstrated live (HEAD: 5 attempts; broken: 1).
    """
    transient: tuple[OSError, ...] = (
        OSError(101, "Network is unreachable"),
        OSError(113, "No route to host"),
        OSError(100, "Network is down"),
        OSError(102, "Network dropped connection on reset"),
        OSError(24, "Too many open files"),  # EMFILE: plausible under 16 concurrent workers
        OSError(32, "Broken pipe"),
    )
    for exc in transient:
        assert net.is_transient(exc), f"a network errno must be transient: {exc!r}"
    assert not net.is_transient(OSError(28, "No space left on device")), "ENOSPC is a local fault"
    assert not net.is_transient(OSError(13, "Permission denied")), "EACCES is a local fault"
    assert not net.is_transient(OSError()), "no errno at all falls through to the conservative default"


def test_is_transient_non_openai_5xx_renderings_are_transient() -> None:
    """A 5xx rendered WITHOUT openai's ``Error code: NNN`` prefix still classifies transient.

    WHY: the LLM path talks to a LOCAL gateway whose raw error bodies are httpx-shaped
    (``HTTPStatusError.response.status_code``) or plain reason phrases (``503 Service Unavailable``).
    Matching only openai's rendering would miss exactly the errors a gateway outage produces.
    """

    class _HTTPStatusError(Exception):
        """Duck-typed ``httpx.HTTPStatusError``: status on the response object, not the error."""

    err: _HTTPStatusError = _HTTPStatusError("Server error '503 Service Unavailable' for url 'http://localhost:20128/v1/chat/completions'")
    err.response = SimpleNamespace(status_code=503)  # pyright: ignore[reportAttributeAccessIssue]
    assert net.is_transient(err), "httpx-shape status on .response.status_code"
    assert net.is_transient(RuntimeError("503 Service Unavailable")), "bare reason phrase"
    assert net.is_transient(RuntimeError("502 Bad Gateway")), "bare reason phrase"
    assert net.is_transient(RuntimeError("upstream returned Gateway Timeout after 2s")), "lowercase phrase"
    assert net.is_transient(RuntimeError("500 Internal Server Error")), "bare reason phrase"
    assert not net.is_transient(RuntimeError("404 Not Found")), "a 4xx reason phrase stays permanent"


def test_is_transient_survives_truthiness_hostile_chain_links() -> None:
    """The cause/context walk never lets a hostile attribute override make it raise.

    WHY: ``is_transient`` is contracted never to raise, and dependent stories call it INSIDE their own
    ``except`` blocks, where an escaping error would replace the original. Third-party exceptions can
    override ``__bool__`` to raise on truthiness testing and can shadow ``__cause__``/``errno`` with
    raising properties -- the walk and the errno read must survive all of them.
    """

    class Hostile(Exception):
        def __bool__(self) -> bool:
            raise RuntimeError("__bool__ must never be consulted")

    head: RuntimeError = RuntimeError("wrapper")
    hostile: Hostile = Hostile()
    head.__cause__ = hostile
    hostile.__context__ = _dns_error(-2, "Name or service not known")
    assert net.is_transient(head) is True, "the transient context under a truthiness-hostile cause"

    class RaisingCause(Exception):
        @property
        def __cause__(self) -> BaseException:  # pyright: ignore[reportIncompatibleVariableOverride, reportIncompatibleMethodOverride]
            raise RuntimeError("__cause__ must not be trusted")

    class RaisingErrno(OSError):
        @property
        def errno(self) -> list[int]:  # pyright: ignore[reportIncompatibleVariableOverride, reportIncompatibleMethodOverride]
            """Unhashable on purpose: ``in frozenset`` would TypeError without the suppress."""
            raise RuntimeError("errno must not be trusted")

    for hostile_exc in (RaisingCause("boom"), RaisingErrno("boom")):
        verdict: bool = net.is_transient(hostile_exc)  # must not raise
        assert verdict is False, "an unreadable chain/errno falls through to the conservative default"


def test_is_transient_matches_optional_dependency_class_names_without_importing_them() -> None:
    """``openai`` / ``litellm`` / ``httpx`` error types are recognized by CLASS NAME (REQ-NET-1 + step 4).

    WHY: the fleet's 331 ``Error in generating model output`` notes and its 337 log hits on ``429`` came
    through ``openai``'s error hierarchy, but ``net.py`` may not import ``openai`` -- it lives in the base
    install path that ``cli.download_babel_file`` uses, where the ``[agent]`` extra is absent. Matching on
    ``type(exc).__name__`` keeps the seam stdlib-only while still classifying those errors. Every message
    here is deliberately token-free so the assertion proves the NAME table fired, not the message
    fallback. A status-bearing 4xx must still lose to the status rule, or an ``APIStatusError`` for a bad
    request would be retried four times.
    """

    class APIConnectionError(Exception):
        pass

    class APITimeoutError(Exception):
        pass

    class RateLimitError(Exception):
        pass

    class InternalServerError(Exception):
        pass

    class ConnectTimeout(Exception):
        pass

    class RemoteProtocolError(Exception):
        pass

    class BadRequestError(Exception):
        """Duck-typed ``openai.BadRequestError``: a status-bearing 4xx from an optional dependency."""

        status_code: int = 400

    named: list[BaseException] = [
        APIConnectionError(""),
        APITimeoutError(""),
        RateLimitError(""),
        InternalServerError(""),
        ConnectTimeout(""),
        RemoteProtocolError(""),
    ]
    missed: list[str] = [type(exc).__name__ for exc in named if not net.is_transient(exc)]
    assert not missed, f"optional-dependency error names not recognized: {missed}"
    assert net.is_transient(BadRequestError("malformed body")) is False, "a status-bearing 4xx must outrank the name table"


def test_is_transient_unwraps_cause_chain() -> None:
    """An unrecognized wrapper is classified by its ``__cause__`` / ``__context__``, up to depth 3.

    WHY: ``urlopen`` wraps ``socket.gaierror`` in ``URLError`` and ``smolagents`` re-raises transport
    failures as its own ``RuntimeError``-shaped wrapper -- the fleet's 331 ``Error in generating model
    output: ...`` notes are exactly that shape. Classifying only the outermost object would call those
    permanent. The bound matters just as much: an unbounded walk over a self-referential ``__context__``
    would hang a worker, so both the depth limit and the cycle case are pinned here.
    """
    wrapper: RuntimeError = RuntimeError("Error in generating model output")
    wrapper.__cause__ = _dns_error(-2, "Name or service not known")
    assert net.is_transient(wrapper) is True, "an explicit `raise ... from dns_error` must classify transient"

    implicit: RuntimeError
    try:
        try:
            raise socket.gaierror(-3, "Temporary failure in name resolution")
        except socket.gaierror:
            raise RuntimeError("supervisor wrapper") from None
    except RuntimeError as caught:
        implicit = caught
    implicit.__cause__ = None  # `from None` suppresses the cause; only __context__ links the DNS error
    assert net.is_transient(implicit) is True, "an implicit __context__ link must classify transient"

    shallow_tail: RuntimeError = RuntimeError("hop 2")
    shallow_tail.__cause__ = _dns_error(-2, "Name or service not known")
    shallow_head: RuntimeError = RuntimeError("hop 1")
    shallow_head.__cause__ = shallow_tail
    assert net.is_transient(shallow_head) is True, "two links is inside the depth-3 bound"

    deep_tail: RuntimeError = RuntimeError("hop 3")
    deep_tail.__cause__ = _dns_error(-2, "Name or service not known")
    deep_mid: RuntimeError = RuntimeError("hop 2")
    deep_mid.__cause__ = deep_tail
    deep_head: RuntimeError = RuntimeError("hop 1")
    deep_head.__cause__ = deep_mid
    assert net.is_transient(deep_head) is False, "the walk must stop at MAX_CHAIN_DEPTH, not recurse forever"

    cycle: RuntimeError = RuntimeError("self-referential")
    cycle.__cause__ = cycle
    assert net.is_transient(cycle) is False, "a __cause__ cycle must terminate, not hang"

    permanent_link: RuntimeError = RuntimeError("hop 1")
    permanent_link.__cause__ = PermissionError("not CC-licensed")
    assert net.is_transient(permanent_link) is False, "a permanent link must win over further unwrapping"


def test_is_transient_never_raises_on_a_broken_str() -> None:
    """``is_transient`` is total: an exception whose ``__str__`` blows up still classifies (REQ-NET-2).

    WHY: the classifier runs inside an ``except`` block on the failure path of a 42,981-article fleet
    run. If classifying an error could itself raise, one malformed third-party exception type would
    replace a recoverable network blip with an unhandled crash in the supervisor -- and the fleet's logs
    already proved how invisible that is (6,008 ``[Errno -2]`` failures produced zero log output).
    """
    assert net.is_transient(_Broken()) is False


def test_is_permanent_http_status_matches_the_transient_status_table() -> None:
    """The named status predicate agrees with ``TRANSIENT_HTTP_STATUS`` and the 5xx range.

    WHY: this is the exact rule ``cli.download_babel_file`` used to hand-roll inline as
    ``e.code not in (408, 429) and 400 <= e.code < 500`` -- which silently omitted 425 Too Early. Naming
    it once is what makes the single-sourced table auditable, so the predicate itself is pinned here.
    """
    permanent: list[int] = [400, 401, 403, 404, 405, 410, 418, 422, 451, 499]
    transient: list[int] = [408, 425, 429, 500, 501, 502, 503, 504, 599, 200, 301, 304]
    assert [status for status in permanent if not net.is_permanent_http_status(status)] == []
    assert [status for status in transient if net.is_permanent_http_status(status)] == []
    assert net.is_permanent_http_status(425) is False, "425 Too Early is transient; the old inline rule missed it"


def test_retry_after_parses_header_and_gateway_reset_hint() -> None:
    """A server-supplied wait is honored from both header shapes AND from a gateway reset hint (REQ-NET-5).

    WHY: the fleet's 337 ``429`` log hits came through a local gateway that wrapped an upstream rate limit
    in a ``503`` whose BODY carried ``(reset after 4m 16s)``. Retrying before that instant just re-earns
    the 429; ignoring the header entirely throws away the only signal the server gives. Both shapes matter
    because ``urllib.error.HTTPError`` puts headers on the exception while ``openai.APIStatusError`` /
    ``httpx`` put them on ``exc.response`` -- and disabling ``openai``'s own transport retry (a later
    story) means this function is the only ``Retry-After`` reader left.
    """
    delta: HTTPError = _http_error(429, "Too Many Requests", _header("Retry-After", "30"))
    assert net.retry_after_seconds(delta) == 30.0

    when: datetime = datetime.now(UTC) + timedelta(seconds=45)
    dated: HTTPError = _http_error(503, "Service Unavailable", _header("Retry-After", format_datetime(when, usegmt=True)))
    parsed: float | None = net.retry_after_seconds(dated)
    assert parsed is not None, "an HTTP-date Retry-After must parse"
    assert 43.0 <= parsed <= 45.0, f"HTTP-date Retry-After mis-parsed: {parsed}"

    class _Response:
        """Duck-typed ``httpx.Response``: carries only the headers the classifier reads."""

        def __init__(self, headers: dict[str, str]) -> None:
            self.headers: dict[str, str] = headers

    class APIStatusError(Exception):
        """Duck-typed ``openai.APIStatusError``: the header lives on ``.response``, not on the exception."""

        def __init__(self, headers: dict[str, str]) -> None:
            super().__init__("Error code: 503")
            self.response: _Response = _Response(headers)

    assert net.retry_after_seconds(APIStatusError({"Retry-After": "12"})) == 12.0

    gateway: str = "Error code: 503 - {'error': {'message': 'Rate limit reached for requests (reset after 4m 16s)'}}"
    assert net.retry_after_seconds(RuntimeError(gateway)) == 60.0, "the 256 s hint must clamp to the 60 s cap"
    assert net.retry_after_seconds(RuntimeError(gateway), cap=600.0) == 256.0
    assert net.retry_after_seconds(RuntimeError("rate limited (reset after 30s)")) == 30.0
    assert net.retry_after_seconds(RuntimeError("rate limited (reset after 5m)"), cap=600.0) == 300.0
    assert net.retry_after_seconds(RuntimeError("rate limited (reset after 5m)")) == 60.0, "the default cap clamps a 300 s hint"
    assert net.retry_after_seconds(RuntimeError("rate limited (reset after 5m)"), cap=120.0) == 120.0


def test_retry_after_returns_none_for_absent_zero_or_unparseable_hints() -> None:
    """No hint, a zero hint, or an unparseable one all yield ``None`` -- never ``0`` (REQ-NET-5).

    WHY: ``None`` is what lets the driver distinguish "the server told us how long to wait" from "use our
    own schedule". Returning ``0.0`` for an absent header would be indistinguishable from a real hint and
    would silently disable backoff -- the exact silent-default failure mode this work exists to remove.
    """
    assert net.retry_after_seconds(ValueError("no headers here")) is None
    assert net.retry_after_seconds(_http_error(429, "Too Many Requests")) is None
    assert net.retry_after_seconds(_http_error(429, "Too Many Requests", _header("Retry-After", "0"))) is None
    assert net.retry_after_seconds(_http_error(503, "Service Unavailable", _header("Retry-After", "not-a-date"))) is None
    assert net.retry_after_seconds(RuntimeError("reset after")) is None
    past: datetime = datetime.now(UTC) - timedelta(seconds=300)
    stale: HTTPError = _http_error(503, "Service Unavailable", _header("Retry-After", format_datetime(past, usegmt=True)))
    assert net.retry_after_seconds(stale) is None, "an already-past HTTP-date must not produce a negative wait"
    assert net.retry_after_seconds(_http_error(503, "x", _header("Retry-After", "3600")), cap=60.0) == 60.0


def test_backoff_delay_is_bounded_and_equal_jittered() -> None:
    """Backoff is exponential, capped, 1-based, and EQUAL-jittered into ``[scheduled/2, scheduled]``.

    WHY equal jitter and not full jitter: with 16 workers sharing one gateway, full jitter
    (``rng() * scheduled``) hands several of them a near-zero delay and re-stampedes the same saturated
    resolver in the same millisecond -- the exact failure being fixed. The floor at ``scheduled / 2`` is
    the whole point, so both endpoints are pinned. The ``max_delay`` cap is what keeps a long retry
    sequence from producing a 64 s sleep that a 90-minute per-article budget cannot absorb.
    """
    assert net.backoff_delay(1, rng=lambda: 0.0) == 0.5
    assert net.backoff_delay(1, rng=lambda: 1.0) == 1.0
    assert net.backoff_delay(2, rng=lambda: 0.0) == 1.0
    assert net.backoff_delay(3, rng=lambda: 0.0) == 2.0
    assert net.backoff_delay(4, rng=lambda: 0.0) == 4.0
    assert net.backoff_delay(6, rng=lambda: 1.0) == 20.0, "the schedule must clamp at max_delay, not reach 32 s"
    assert net.backoff_delay(9, rng=lambda: 1.0) == 20.0

    for attempt in range(1, 8):
        scheduled: float = min(net.DEFAULT_MAX_DELAY, net.DEFAULT_BASE_DELAY * 2 ** (attempt - 1))
        for draw in (0.0, 0.25, 0.5, 0.75, 1.0):
            delay: float = net.backoff_delay(attempt, rng=lambda draw=draw: draw)
            assert scheduled / 2 <= delay <= scheduled, f"attempt {attempt} with rng={draw} left the equal-jitter band"

    assert net.backoff_delay(3, base_delay=2.0, max_delay=5.0, rng=lambda: 1.0) == 5.0
    assert net.backoff_delay(2) != 0.0, "the DEFAULT rng must be wired (random.random), not a stub"

    with pytest.raises(ValueError, match="attempt must be >= 1"):
        net.backoff_delay(0)


def test_retry_transient_never_sleeps_on_first_success() -> None:
    """A first-attempt success costs zero sleeps, zero backoff, and one single call (REQ-NET-7/OVER-4).

    WHY: 193 articles DID succeed in the fleet run and thousands more would have on a healthy resolver.
    If the happy path paid for the resilience, this seam would slow down every successful PMC listing and
    every successful LLM call -- a regression measured in hours across 42,981 queued articles. This is the
    assertion that keeps the fix from becoming a tax.
    """
    sleeps: list[float] = []
    calls: list[int] = []
    rng_draws: list[int] = []

    def draw() -> float:
        rng_draws.append(1)
        return 0.5

    def operation() -> str:
        calls.append(1)
        return "body"

    result: str = net.retry_transient(
        operation, target="https://pmc-oa-opendata.s3.amazonaws.com/pmc-oa-files/11/PMC11708054.xml", sleep=sleeps.append, rng=draw
    )
    assert result == "body"
    assert len(calls) == 1, "the operation must be called exactly once on success"
    assert sleeps == [], "a first-attempt success must not sleep at all"
    assert rng_draws == [], "a first-attempt success must not even compute a jitter draw"


def test_retry_transient_uses_injected_sleep_and_stops_at_total_budget() -> None:
    """Sleeps are injected, truncated to ``max_total_backoff``, and the budget ends the retry (REQ-NET-8/9).

    WHY the total budget: ``fetch_pmc_article`` performs ~13-18 sequential calls, so the only way to make
    the worst-case added wall clock provable arithmetic against the harness's 90-minute per-article
    timeout is to bound the SUM of sleeps per call, not just each one. Without it, ``attempts=10`` with a
    20 s cap could sleep for minutes on one listing request -- and the fleet run showed a fully saturated
    resolver produces exactly that kind of every-call-fails condition. The truncation to what remains is
    pinned too: a partial sleep is better than blowing the budget.
    """
    sleeps: list[float] = []
    calls: list[int] = []

    def operation() -> str:
        calls.append(1)
        raise _dns_error(-2, "Name or service not known")

    with pytest.raises(NetworkTransientError):
        net.retry_transient(
            operation,
            target="https://pmc.example/listing",
            attempts=10,
            base_delay=1.0,
            max_delay=20.0,
            max_total_backoff=5.0,
            sleep=sleeps.append,
            rng=lambda: 1.0,
        )
    assert len(calls) == 4, "the 5 s budget, not the 10 configured attempts, must end the retry"
    assert sleeps == [1.0, 2.0, 2.0], "the third sleep is truncated to the 2 s that remained of the budget"
    assert sum(sleeps) == 5.0, "the driver must never sleep past max_total_backoff"

    zero_budget: list[float] = []
    zero_calls: list[int] = []

    def operation_zero() -> str:
        zero_calls.append(1)
        raise ConnectionResetError(104, "Connection reset by peer")

    with pytest.raises(NetworkTransientError):
        net.retry_transient(
            operation_zero, target="https://pmc.example/reset", attempts=4, max_total_backoff=0.0, sleep=zero_budget.append, rng=lambda: 1.0
        )
    assert zero_budget == [], "an exhausted budget must raise immediately rather than sleep"
    assert len(zero_calls) == 1, "with no budget left, only the initial attempt runs"


def test_retry_transient_sleeps_the_larger_of_retry_after_and_backoff() -> None:
    """A server hint raises the wait, and our own schedule wins when it is already longer (REQ-NET-5/7).

    WHY ``max`` and not "hint replaces schedule": the fleet's gateway advertised ``(reset after 4m 16s)``
    inside a wrapped 429. Honoring only our 1 s/2 s/4 s schedule would have re-hit the rate limit three
    times in seven seconds and then failed the article anyway -- four wasted transport calls that made the
    gateway MORE saturated. Taking the maximum keeps the exponential floor while never retrying before
    the server said it was allowed.
    """
    sleeps: list[float] = []
    calls: list[int] = []
    outcomes: list[BaseException | None] = [
        _http_error(429, "Too Many Requests", _header("Retry-After", "5")),
        _http_error(429, "Too Many Requests", _header("Retry-After", "5")),
        _http_error(429, "Too Many Requests", _header("Retry-After", "1")),
        None,
    ]

    def operation() -> str:
        index: int = len(calls)
        calls.append(1)
        pending: BaseException | None = outcomes[index]
        if pending is not None:
            raise pending
        return "ok"

    assert net.retry_transient(operation, target="https://gateway/v1/chat/completions", sleep=sleeps.append, rng=lambda: 1.0) == "ok"
    assert len(calls) == 4
    assert sleeps == [5.0, 5.0, 4.0], "the 5 s hint outruns the 1 s/2 s schedule; the 4 s schedule outruns a 1 s hint"


def test_retry_transient_reports_attempts_actually_made_on_budget_exhaustion() -> None:
    """The exhaustion error's ``attempts`` is the count ACTUALLY made, not the configured maximum.

    WHY: a truncated ``max_total_backoff`` legitimately ends the loop before ``attempts`` is reached, and
    an operator reading ``after N attempts`` in a fleet log must be able to trust N. The configured
    maximum is already visible in the driver call itself; the error message is the only place the real
    count appears.
    """
    sleeps: list[float] = []
    calls: list[int] = []

    def operation() -> str:
        calls.append(1)
        raise _dns_error(-2, "Name or service not known")

    with pytest.raises(NetworkTransientError) as exc_info:
        net.retry_transient(
            operation, target="https://pmc.example/listing", attempts=10, base_delay=1.0, max_total_backoff=3.0, sleep=sleeps.append, rng=lambda: 1.0
        )
    assert exc_info.value.attempts == len(calls) == 3, "3 calls made: the budget, not the 10 configured attempts, ended it"


def test_retry_after_parses_a_naive_http_date_as_utc() -> None:
    """An RFC-violating ``Retry-After`` date with no timezone is read as UTC, not local time (REQ-NET-5).

    WHY: RFC 9110 requires a GMT-suffixed date, but a real server can omit it; assuming the worker's
    local zone would silently shift the wait by hours for a non-UTC fleet and retry far too early (or
    far too late). The UTC reading is also what the round-trip test above relies on implicitly.
    """
    when: datetime = datetime.now(UTC) + timedelta(seconds=45)
    naive: str = when.strftime("%a, %d %b %Y %H:%M:%S")  # deliberately no GMT suffix
    err: HTTPError = _http_error(503, "Service Unavailable", _header("Retry-After", naive))
    parsed: float | None = net.retry_after_seconds(err)
    assert parsed is not None
    assert 43.0 <= parsed <= 45.0, f"a naive HTTP-date must be read as UTC, got {parsed}"


def test_retry_transient_raises_coded_network_transient_after_exhaustion() -> None:
    """Exhaustion raises the coded ``NetworkTransientError``, chained ``from`` the last error (REQ-NET-10).

    WHY a distinct code and a real chain: the fleet recorded a DNS blip as ``status=SKIPPED`` with
    ``notes="SKIPPED: <urlopen error [Errno -2] ...>"`` -- byte-identical to a legitimate
    not-open-access skip, with no machine-readable signal, which is how 2,194 recoverable articles became
    indistinguishable from 7,002 real skips. A stable ``network-transient`` code is what lets a fleet
    consumer requeue without keyword-matching notes. Chaining ``from last_error`` is what keeps the
    original ``gaierror`` traceback instead of replacing it with a bare "we gave up".
    """
    sleeps: list[float] = []
    calls: list[int] = []
    last: URLError = _dns_error(-3, "Temporary failure in name resolution")

    def operation() -> str:
        calls.append(1)
        if len(calls) == 4:
            raise last
        raise _dns_error(-2, "Name or service not known")

    with pytest.raises(NetworkTransientError) as exc_info:
        net.retry_transient(operation, target="https://pmc.example/metadata.json", sleep=sleeps.append, rng=lambda: 1.0)

    assert len(calls) == net.DEFAULT_ATTEMPTS == 4
    assert sleeps == [1.0, 2.0, 4.0], "three sleeps between four attempts, and none after the final failure"
    error: NetworkTransientError = exc_info.value
    assert error.code == "network-transient"
    assert error.target == "https://pmc.example/metadata.json"
    assert error.attempts == 4
    assert error.last_error is last, "the error must carry the LAST observed failure, not the first"
    assert error.__cause__ is last, "the original traceback must survive via explicit chaining"
    assert str(error).endswith(DOCS_URL + "network-transient")


def test_retry_logs_bind_the_error_as_text_so_the_enqueued_sink_can_pickle_it(monkeypatch: pytest.MonkeyPatch) -> None:
    """Both retry log lines bind ``error`` as ``str``, never the exception OBJECT.

    WHY: ``log.py:122`` configures loguru with ``enqueue=True``, which pickles every bound kwarg and ships
    it to a writer process. ``HTTPError.__init__`` requires five positional arguments, so an ``HTTPError``
    bound as an object fails to unpickle there with ``TypeError: HTTPError.__init__() missing 5 required
    positional arguments``; loguru swallows that as an internal '--- End of logging error ---' and DROPS
    the record. That would silently disable the very log lines added here to make transient failures
    visible -- the fleet's 6,008 DNS failures produced ZERO worker-log lines, which is why they went
    unnoticed for a whole run. Observed live before the fix, so this pins it at the seam.
    """
    bound: list[dict[str, object]] = []
    monkeypatch.setattr(
        net, "logger", SimpleNamespace(warning=lambda _message, **kwargs: bound.append(kwargs), error=lambda _message, **kwargs: bound.append(kwargs))
    )

    def operation() -> str:
        raise _http_error(503, "Service Unavailable")

    with pytest.raises(NetworkTransientError):
        net.retry_transient(operation, target="https://pmc.example/x.json", attempts=2, sleep=lambda _s: None, rng=lambda: 1.0)

    assert len(bound) == 2, "one retry line plus one exhaustion line"
    for kwargs in bound:
        assert isinstance(kwargs["error"], str), f"error must be pre-stringified, got {type(kwargs['error'])}"
        pickle.loads(pickle.dumps(kwargs))


def test_retry_transient_honours_a_custom_error_factory() -> None:
    """``error_factory`` lets a caller raise its OWN coded error on exhaustion (REQ-NET-10).

    WHY: the LLM path (a later story) must distinguish a saturated gateway from a saturated resolver --
    an ``llm-transient`` failure means "requeue this article's model call", a ``network-transient`` one
    means "requeue this article's fetch". One driver with an injectable factory is what keeps them from
    growing two competing retry loops, which is the whole reason ``net.py`` exists.
    """
    sleeps: list[float] = []
    seen: list[tuple[str, int, BaseException]] = []

    class LlmTransientError(RuntimeError):
        pass

    def factory(target: str, attempts: int, last_error: BaseException) -> BaseException:
        seen.append((target, attempts, last_error))
        return LlmTransientError(f"{target} gave up after {attempts}: {last_error}")

    def operation() -> str:
        raise _http_error(503, "Service Unavailable")

    with pytest.raises(LlmTransientError) as exc_info:
        net.retry_transient(operation, target="9router/gpt-4o", attempts=2, sleep=sleeps.append, rng=lambda: 1.0, error_factory=factory)
    assert len(seen) == 1
    assert seen[0][0] == "9router/gpt-4o"
    assert seen[0][1] == 2
    assert isinstance(seen[0][2], HTTPError)
    assert exc_info.value.__cause__ is not None, "a custom factory's error must still chain from last_error"


def test_retry_transient_reraises_permanent_error_immediately() -> None:
    """A permanent failure propagates on the FIRST occurrence, unwrapped and unchained (REQ-NET-7).

    WHY: ``fetch_pmc_article`` is a fail-fast ladder whose ``ValueError`` (bad id), ``FileNotFoundError``
    (no OA version / no tables) and ``PermissionError`` (not CC-licensed) branches are matched BY TYPE in
    ``run_supervisor`` and around the metadata read at ``agent.py:459-465``. A retry layer that wrapped
    them -- or even attached a ``__cause__`` -- would silently change the ladder and turn 7,002 legitimate
    skips into something the supervisor no longer recognizes. One call, zero sleeps, same object identity.
    """
    for permanent in (
        PermissionError("article is not CC-licensed"),
        FileNotFoundError("no open-access versions"),
        ValueError("PMC99999999 is not a valid PMC id"),
        _http_error(404, "Not Found"),
        _http_error(403, "Forbidden"),
    ):
        sleeps: list[float] = []
        calls: list[int] = []

        def operation(exc: BaseException = permanent, seen: list[int] = calls) -> str:
            seen.append(1)
            raise exc

        with pytest.raises(type(permanent)) as exc_info:
            net.retry_transient(operation, target="https://pmc.example/metadata.json", sleep=sleeps.append, rng=lambda: 1.0)
        assert exc_info.value is permanent, f"{type(permanent).__name__} must propagate as the identical object"
        assert len(calls) == 1, f"{type(permanent).__name__} must not be retried"
        assert sleeps == [], f"{type(permanent).__name__} must not trigger a backoff sleep"


def test_retry_transient_lets_base_exceptions_through() -> None:
    """The driver catches ``Exception``, never ``BaseException``, so Ctrl-C still kills a worker.

    WHY: 16 fleet workers ran unattended for hours. If ``retry_transient`` caught ``BaseException``, an
    operator's Ctrl-C -- or the harness's 90-minute timeout signalling through ``SystemExit`` -- would be
    swallowed up to four times with sleeps in between, and a runaway fleet could not be stopped promptly.
    """
    sleeps: list[float] = []
    calls: list[int] = []

    def operation() -> str:
        calls.append(1)
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        net.retry_transient(operation, target="https://pmc.example/interrupted", sleep=sleeps.append, rng=lambda: 1.0)
    assert len(calls) == 1
    assert sleeps == []


def test_retry_transient_rejects_attempts_below_one() -> None:
    """``attempts < 1`` is a misconfiguration and must fail loudly before any work happens.

    WHY: ``attempts=0`` silently becoming "no attempts" would make ``operation()`` never run and the
    driver raise an exhaustion error for work that was never tried -- an invisible no-op in a fleet where
    the whole point is that failures must be loud. Raising ``ValueError`` up front, with the operation
    untouched, is the only version a caller can debug from the message alone.
    """
    calls: list[int] = []

    def operation() -> str:
        calls.append(1)
        return "body"

    for bad in (0, -1):
        with pytest.raises(ValueError, match="attempts must be >= 1"):
            net.retry_transient(operation, target="https://pmc.example/misconfigured", attempts=bad)
    assert calls == [], "the operation must never run when the configuration is invalid"


def test_retry_transient_logs_every_retry_and_the_exhaustion(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every retry sleeps logs at ``warning`` and exhaustion logs at ``error`` (REQ-NET-11 / the R7 fix).

    WHY: ``grep -c "Name or service not known" .tablassert/log/worker-*.out`` returned **0 in every file**
    of the fleet run -- 6,008 DNS failures produced ZERO log output, because the supervisor's catch-all
    only wrote to ``state.json``. Diagnosing that run required reconstructing the failure distribution
    from a queue database by hand. A transient failure can no longer be silent: each backoff names the
    target, the attempt index, the delay, and the error, and giving up is an ``error`` line.
    """
    recorder: _RecordingLogger = _RecordingLogger()
    monkeypatch.setattr(net, "logger", recorder)
    sleeps: list[float] = []
    calls: list[int] = []

    def operation() -> str:
        calls.append(1)
        raise _dns_error(-2, "Name or service not known")

    with pytest.raises(NetworkTransientError):
        net.retry_transient(operation, target="https://pmc.example/oadata.json", attempts=3, sleep=sleeps.append, rng=lambda: 1.0)

    assert len(calls) == 3
    assert len(recorder.warnings) == 2, "one warning per RETRY, and none after the final failure"
    for index, (template, fields) in enumerate(recorder.warnings, start=1):
        # A brace-style template with kwargs, never a pre-formatted f-string.
        for placeholder in ("{target}", "{attempt}", "{delay:.2f}", "{error}"):
            assert placeholder in template, f"the retry log template must carry {placeholder}"
        assert fields["target"] == "https://pmc.example/oadata.json"
        assert fields["attempt"] == index
        assert fields["attempts"] == 3
        assert fields["delay"] == sleeps[index - 1]
        # Pre-stringified, never the exception object: the enqueue=True sink pickles kwargs.
        assert fields["error"] == str(_dns_error(-2, "Name or service not known"))
    assert len(recorder.errors) == 1, "exhaustion must be logged at error level exactly once"
    template, fields = recorder.errors[0]
    for placeholder in ("{target}", "{error}"):
        assert placeholder in template, f"the exhaustion log template must carry {placeholder}"
    assert fields["target"] == "https://pmc.example/oadata.json"
    assert fields["attempt"] == 3
    assert fields["slept"] == sum(sleeps)
    assert fields["error"] == str(_dns_error(-2, "Name or service not known"))


def test_http_get_text_and_bytes_retry_with_monkeypatched_urlopen(monkeypatch: pytest.MonkeyPatch) -> None:
    """The real HTTP seam retries a transient failure and preserves the request exactly (REQ-NET-13).

    WHY: ``tests/test_agent_fetch.py`` monkeypatches ``agent._http_get_text`` wholesale, so the retry
    INSIDE the seam is unreachable from the existing 62 fetch tests by design. This is the only place the
    real ``urlopen`` call is exercised. The ``User-Agent: tablassert`` header and the ``timeout`` argument
    are asserted verbatim because PMC's S3 endpoint and the harness both depend on them, and the UTF-8
    decode is asserted because ``agent._http_get_text`` has always decoded that way.
    """
    sleeps: list[float] = _stub_retry_clock(monkeypatch)

    text_opener: _RecordingUrlopen = _RecordingUrlopen([_dns_error(-2, "Name or service not known"), b'{"is_pmc_openaccess": true}'])
    monkeypatch.setattr(net, "urlopen", text_opener)
    body: str = net.http_get_text("https://pmc.example/PMC11708054/metadata.json", timeout=7, attempts=3)
    assert body == '{"is_pmc_openaccess": true}'
    assert len(text_opener.requests) == 2, "one DNS failure then one success must be exactly two urlopen calls"
    assert text_opener.timeouts == [7, 7], "the caller's timeout must reach every attempt, not just the first"
    for request in text_opener.requests:
        assert request.full_url == "https://pmc.example/PMC11708054/metadata.json"
        assert request.get_header("User-agent") == "tablassert", "the User-Agent must be preserved verbatim"
    assert sleeps == [1.0], "one backoff between the two attempts, and none after the success"

    bytes_opener: _RecordingUrlopen = _RecordingUrlopen(
        [ConnectionResetError(104, "Connection reset by peer"), http.client.RemoteDisconnected("Remote end closed connection"), b"\x1f\x8bpayload"]
    )
    monkeypatch.setattr(net, "urlopen", bytes_opener)
    assert net.http_get_bytes("https://pmc.example/PMC11708054/supp.tar.gz") == b"\x1f\x8bpayload"
    assert len(bytes_opener.requests) == 3
    assert bytes_opener.timeouts == [net.DEFAULT_TIMEOUT] * 3, "the 120 s default must apply to every attempt"
    assert sleeps == [1.0, 1.0, 2.0], "the bytes call added its own two backoffs to the shared log"


def test_http_get_text_raises_the_coded_error_when_every_attempt_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """An exhausted fetch surfaces the coded error naming the URL, not a bare ``URLError``.

    WHY: this is the shape that reaches ``run_supervisor``'s catch-all for the 2,031 articles that died on
    ``[Errno -2]``. Without the code, the record's notes string is the only signal and the fleet cannot
    tell a requeueable DNS blip from a permanent not-open-access skip.
    """
    sleeps: list[float] = _stub_retry_clock(monkeypatch)
    opener: _RecordingUrlopen = _RecordingUrlopen([_dns_error(-2, "Name or service not known")])
    monkeypatch.setattr(net, "urlopen", opener)

    with pytest.raises(NetworkTransientError) as exc_info:
        net.http_get_text("https://pmc.example/PMC11708054/metadata.json", attempts=2)

    assert len(opener.requests) == 2
    assert sleeps == [1.0], "no sleep after the final failed attempt"
    error: NetworkTransientError = exc_info.value
    assert error.code == "network-transient"
    assert error.target == "https://pmc.example/PMC11708054/metadata.json"
    assert error.attempts == 2
    assert isinstance(error.last_error, URLError)
    assert str(error).endswith(DOCS_URL + "network-transient")
