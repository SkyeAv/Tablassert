"""Network resilience: transient-failure classification, bounded jittered backoff, and a retrying HTTP seam.

One stdlib-only seam that answers two questions about ANY exception -- *is this transient?* and *how long
should we wait?* -- plus the single driver that applies the answer. Both the PMC HTTP path
(:mod:`tablassert.agent`) and the BABEL downloader (:func:`tablassert.cli.download_babel_file`) consult the
same table, so a gateway ``503``-wrapping-``429`` and a ``systemd-resolved`` ``EAI_NONAME`` are handled
identically instead of by two competing heuristics.

Notes:
    WHY this module exists: a 16-worker fleet run over 42,981 PMC articles produced 193 successes and
    2,507 failures, and 2,194 of those failures (87.5%) were DNS-shaped -- 2,031 x ``[Errno -2] Name or
    service not known``, 154 x ``[Errno -3] Temporary failure in name resolution``, 9 x ``No address
    associated with hostname``. ``/etc/nsswitch.conf`` reads
    ``hosts: mymachines resolve [!UNAVAIL=return] files myhostname dns``, so a NOTFOUND from a saturated
    or roaming-broken ``systemd-resolved`` returns IMMEDIATELY as ``EAI_NONAME``: a transient failure that
    looks permanent. The HTTP seam that hit it was a single-attempt ``urlopen`` with zero retry, and
    ``fetch_pmc_article`` makes ~13-18 such calls per article, so any one blip lost the article.

    WHY stdlib-only: ``tenacity`` / ``urllib3.util.retry`` / ``requests`` would land in the base install
    for a problem solvable in ~200 lines, and ``smolagents``' own retryer is not importable without the
    ``[agent]`` extra -- so it cannot be the shared seam for :mod:`tablassert.cli`.
"""

from __future__ import annotations

import errno
import http.client
import random
import re
import socket
import ssl
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import TypeVar
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from tablassert.errors import NetworkTransientError
from tablassert.log import cat

logger = cat("NET")

T = TypeVar("T")

DEFAULT_ATTEMPTS: int = 4
"""Attempts per retried operation (1 initial try + 3 retries)."""

DEFAULT_BASE_DELAY: float = 1.0
"""Seconds slept before the second attempt; doubles per attempt up to ``DEFAULT_MAX_DELAY``."""

DEFAULT_MAX_DELAY: float = 20.0
"""Upper bound on ONE scheduled backoff step, before jitter and before any ``Retry-After`` hint."""

DEFAULT_MAX_TOTAL_BACKOFF: float = 60.0
"""Upper bound on the SUM of sleeps inside one :func:`retry_transient` call.

WHY a total budget and not just per-step caps: the worst-case wall clock of a fleet article must be
provable arithmetic. ``fetch_pmc_article`` performs ~13-18 calls, so a per-call bound of 60 s is what
makes "3 metadata calls + 2 download waves <= ~5 min added" a stated fact rather than a hope.
"""

DEFAULT_RETRY_AFTER_CAP: float = 60.0
"""Upper bound on an honored server ``Retry-After`` / ``(reset after ...)`` hint."""

DEFAULT_TIMEOUT: int = 120
"""Per-request socket timeout in seconds for :func:`http_get_text` / :func:`http_get_bytes`."""

USER_AGENT: str = "tablassert"
"""``User-Agent`` sent by every request this module makes (unchanged from the pre-seam HTTP calls)."""

MAX_CHAIN_DEPTH: int = 3
"""How many ``__cause__`` / ``__context__`` links :func:`is_transient` unwraps.

WHY bounded: ``urlopen`` wraps ``socket.gaierror`` in ``URLError`` and ``openai`` wraps
``httpx.ConnectError`` in ``APIConnectionError`` -- one hop is what the real stacks show. A hard depth
also makes a self-referential ``__context__`` cycle terminate instead of recursing forever.
"""

#: HTTP statuses that are transient by definition (RFC 9110 408 Request Timeout, 425 Too Early,
#: 429 Too Many Requests). Every 5xx is transient too and is matched by range, not enumeration.
TRANSIENT_HTTP_STATUS: frozenset[int] = frozenset({408, 425, 429})

#: Exception types that are ALWAYS permanent, checked BEFORE the transient table because
#: ``PermissionError`` / ``FileNotFoundError`` subclass ``OSError``, which ``URLError`` also subclasses.
PERMANENT_EXCEPTION_TYPES: tuple[type[BaseException], ...] = (
    ValueError,  # bad PMC id, json.JSONDecodeError, malformed listing XML
    TypeError,
    KeyError,
    PermissionError,  # article metadata readable but not CC-licensed => terminal skip
    FileNotFoundError,  # no OA versions / no table files => terminal skip
    IsADirectoryError,
    NotADirectoryError,
)

#: Exception types that are ALWAYS transient (stdlib only; no optional dependency is imported).
TRANSIENT_EXCEPTION_TYPES: tuple[type[BaseException], ...] = (
    URLError,  # wraps socket.gaierror from urlopen (DNS EAI_NONAME / EAI_AGAIN)
    socket.gaierror,  # bare DNS failures raised outside urlopen
    ConnectionError,  # ConnectionResetError / ConnectionAbortedError / BrokenPipeError
    TimeoutError,  # socket.timeout IS TimeoutError on >= 3.10
    http.client.HTTPException,  # RemoteDisconnected, IncompleteRead, BadStatusLine, LineTooLong
    ssl.SSLError,  # "TLS/SSL connection has been closed (EOF)", handshake timeouts
)

#: ``type(exc).__name__`` values from OPTIONAL dependencies (``openai`` / ``litellm`` / ``httpx``) that
#: must be recognized WITHOUT importing them, so this module stays stdlib-only.
TRANSIENT_EXCEPTION_NAMES: frozenset[str] = frozenset(
    {
        "APIConnectionError",
        "APIError",
        "APIStatusError",
        "APITimeoutError",
        "APITokenExpiredError",
        "ConnectError",
        "ConnectTimeout",
        "HTTPStatusError",
        "InternalServerError",
        "NetworkError",
        "PoolTimeout",
        "RateLimitError",
        "RateLimitException",
        "ReadError",
        "ReadTimeout",
        "RemoteProtocolError",
        "ServiceUnavailableError",
        "Timeout",
        "WriteError",
        "WriteTimeout",
    }
)

#: ``OSError.errno`` values that make a BARE ``OSError`` (wrapped in no library exception) transient.
#: ``stream_copy``'s mid-stream ``response.read()`` raises the raw socket ``OSError`` unwrapped, so the
#: type tables above never see it -- only its errno does. Local-resource errnos (``ENOSPC``, ``EACCES``,
#: ``ENOENT``) deliberately stay ABSENT: retrying a full disk or a missing file cannot succeed.
TRANSIENT_ERRNOS: frozenset[int] = frozenset(
    {
        errno.EINTR,
        errno.EAGAIN,
        errno.ENOBUFS,
        errno.ENETDOWN,
        errno.ENETUNREACH,
        errno.ENETRESET,
        errno.ECONNABORTED,
        errno.ECONNRESET,
        errno.ETIMEDOUT,
        errno.EHOSTUNREACH,
        errno.EPIPE,
        errno.ESHUTDOWN,
        errno.EMFILE,
        errno.ENFILE,
    }
)

#: Last-resort message patterns, applied ONLY when the type, status, and errno tables say nothing.
#: Mirrors (and widens) ``smolagents.models.is_rate_limit_error`` so a gateway ``503``-wrapping-``429``
#: is retried even when the raising class is a plain ``RuntimeError``.
#:
#: WHY compiled, word-bounded regexes and not bare substrings: every numeric-looking token in a fleet
#: message is a candidate false positive. A bare ``"429"`` also matches ``row count 4290``; a bare
#: ``"timeout"" matches ``timeout_budget``; a bare ``"ssl"" matches any ``/etc/ssl/...`` path. Those
#: are deterministic bugs, and classifying them transient both wastes four attempts on them AND stamps
#: them ``network-transient`` -- which is precisely the mislabel this module exists to prevent. Word
#: boundaries (``\\b``) keep the fleet's real renderings (``'code': '429'``, ``_ssl.c:1015 ... timed
#: out``, ``TLS/SSL connection has been closed``) while rejecting digit runs and identifiers that merely
#: CONTAIN the token.
TRANSIENT_MESSAGE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\b429\b"),
    re.compile(r"\brate limit\b"),
    re.compile(r"\brate_limit\b"),
    re.compile(r"\btoo many requests\b"),
    re.compile(r"\berror code: 5\d\d\b"),
    re.compile(r"\bconnection reset\b"),
    re.compile(r"\bconnection aborted\b"),
    re.compile(r"\bremote end closed connection\b"),
    re.compile(r"\btimed out\b"),
    re.compile(r"\btimeout\b"),
    re.compile(r"\bname or service not known\b"),
    re.compile(r"\btemporary failure in name resolution\b"),
    re.compile(r"\bno address associated with hostname\b"),
    re.compile(r"\bssl connection\b"),
    re.compile(r"\bssl error\b"),
    re.compile(r"\bssl:\b"),
    re.compile(r"\beof occurred in violation of protocol\b"),
    re.compile(r"\bservice unavailable\b"),
    re.compile(r"\bbad gateway\b"),
    re.compile(r"\bgateway timeout\b"),
    re.compile(r"\binternal server error\b"),
)

_DELTA_SECONDS: re.Pattern[str] = re.compile(r"\d+")
"""A ``Retry-After`` value in delta-seconds form (RFC 9110 10.2.3)."""

_RESET_HINT: re.Pattern[str] = re.compile(r"reset after\s*(?:(\d+)\s*m)?\s*(?:(\d+)\s*s)?")
"""A gateway reset hint such as ``(reset after 4m 16s)`` / ``(reset after 30s)`` / ``(reset after 5m)``.

WHY lowercase-only and not ``re.IGNORECASE``: the only caller passes :func:`_message_of` output, which is
already lowercased.
"""


@dataclass(frozen=True, slots=True)
class RetryAttempt:
    """One observed transient failure: what was attempted, which try, and what it raised.

    Attributes:
        target: Human-readable name of the operation (a URL, a model id).
        attempt: 1-based index of the FAILED attempt.
        attempts: Configured maximum number of attempts.
        error: The exception that attempt raised.
    """

    target: str
    attempt: int
    attempts: int
    error: BaseException


def _message_of(exc: BaseException) -> str:
    """Return the lowercased ``str()`` of ``exc``, or ``""`` when formatting it raises.

    Args:
        exc: Any exception.

    Returns:
        The lowercased message text; empty when ``str(exc)`` itself failed.

    Notes:
        WHY the suppression: :func:`is_transient` is contracted never to raise, and a third-party
        exception can override ``__str__`` badly -- a gateway error object whose ``__str__`` re-reads an
        already-closed response body does exactly that. An unformattable message simply contributes no
        token evidence, which falls through to the conservative permanent default.
    """
    message: str = ""
    with suppress(Exception):
        message = str(exc).lower()
    return message


def _status_of(exc: BaseException) -> int | None:
    """Return an integer HTTP status carried by ``exc`` (``status_code`` then ``status``), else ``None``.

    Args:
        exc: Any exception.

    Returns:
        The integer status, or ``None`` when the exception carries no usable one.
    """
    status: object = None
    with suppress(Exception):
        status = getattr(exc, "status_code", None)
        if status is None:
            # The httpx shape: the status lives on the response OBJECT the error carries
            # (``HTTPStatusError.response.status_code``), not on the error itself.
            response: object = getattr(exc, "response", None)
            status = getattr(response, "status_code", None) if response is not None else None
        if status is None:
            status = getattr(exc, "status", None)
    if isinstance(status, bool) or not isinstance(status, int):
        return None
    return status


def _header_value(holder: object, name: str) -> str | None:
    """Read ``name`` off ``holder.headers`` when that attribute behaves like a mapping.

    Args:
        holder: An exception, or an exception's ``.response``; ``None`` is tolerated.
        name: Header name to look up.

    Returns:
        The non-empty header value, or ``None`` when absent, unreadable, or not a string.
    """
    if holder is None:
        return None
    headers: object = getattr(holder, "headers", None)
    if headers is None:
        return None
    get: object = getattr(headers, "get", None)
    if not callable(get):
        return None
    value: object = None
    with suppress(Exception):
        value = get(name)
    if isinstance(value, str) and value.strip():
        return value
    return None


def _http_date_seconds(text: str) -> float | None:
    """Convert an HTTP-date ``Retry-After`` value into seconds from now.

    Args:
        text: The raw header value, e.g. ``Wed, 21 Oct 2026 07:28:00 GMT``.

    Returns:
        Seconds until that instant (negative when it is already past), or ``None`` when unparseable.
    """
    when: datetime | None = None
    with suppress(ValueError, TypeError):
        when = parsedate_to_datetime(text)
    if when is None:
        return None
    if when.tzinfo is None:
        # RFC 9110 requires a zone, but a server that omits one means UTC, not the worker's local zone.
        when = when.replace(tzinfo=UTC)
    return when.timestamp() - time.time()


def _header_seconds(exc: BaseException) -> float | None:
    """Parse a ``Retry-After`` header from either supported exception shape.

    Args:
        exc: Any exception.

    Returns:
        The requested wait in seconds, or ``None`` when the header is absent or unparseable.

    Notes:
        WHY two shapes: ``urllib.error.HTTPError`` puts the headers on the exception itself, while
        ``openai.APIStatusError`` / ``httpx`` put them on ``exc.response``. Giving up on ``openai``'s
        header would mean losing the cheap ``Retry-After`` handling its own (now disabled) transport
        retry used to provide.
    """
    raw: str | None = _header_value(exc, "Retry-After")
    if raw is None:
        raw = _header_value(getattr(exc, "response", None), "Retry-After")
    if raw is None:
        return None
    text: str = raw.strip()
    delta: re.Match[str] | None = _DELTA_SECONDS.fullmatch(text)
    if delta is not None:
        return float(delta.group())
    return _http_date_seconds(text)


def _reset_hint_seconds(message: str) -> float | None:
    """Parse a gateway ``(reset after ...)`` hint out of an already-lowercased message.

    Args:
        message: Lowercased ``str(exc)``.

    Returns:
        The hinted wait in seconds, or ``None`` when the message carries no hint.
    """
    match: re.Match[str] | None = _RESET_HINT.search(message)
    if match is None:
        return None
    minutes: str | None = match.group(1)
    seconds: str | None = match.group(2)
    if minutes is None and seconds is None:
        return None
    total: float = 60.0 * int(minutes) if minutes is not None else 0.0
    return total + float(seconds) if seconds is not None else total


def is_permanent_http_status(status: int) -> bool:
    """Return whether an HTTP status is a permanent client-side rejection.

    Args:
        status: An HTTP status code.

    Returns:
        ``True`` for a 4xx that is NOT in :data:`TRANSIENT_HTTP_STATUS`; ``False`` otherwise.

    Notes:
        WHY a named predicate instead of an inline ``400 <= code < 500``: this is the exact rule
        :func:`tablassert.cli.download_babel_file` used to hand-roll, and 425 Too Early was missing from
        it. Naming it makes the single-sourced table auditable in one place.
    """
    return 400 <= status < 500 and status not in TRANSIENT_HTTP_STATUS


def _classify(exc: BaseException) -> bool | None:
    """Apply the ordered classification table to ONE exception, without walking its chain.

    Args:
        exc: The exception to classify.

    Returns:
        ``True`` for transient, ``False`` for permanent, or ``None`` when the table has no opinion --
        which is what tells :func:`is_transient` to look at ``__cause__`` / ``__context__`` next.

    Notes:
        WHY the order is load-bearing: ``HTTPError`` subclasses ``URLError``, so the status rule must run
        first or every 404 would classify transient. ``PermissionError`` / ``FileNotFoundError`` are
        ``OSError``s and ``URLError`` is too, so the permanent table must run before the transient one or
        a legitimate not-open-access skip would be retried four times.
    """
    if isinstance(exc, HTTPError):
        return not is_permanent_http_status(exc.code)
    if isinstance(exc, PERMANENT_EXCEPTION_TYPES):
        return False
    if isinstance(exc, TRANSIENT_EXCEPTION_TYPES):
        return True
    # A bare OSError with a NETWORK errno is transient (mid-stream socket errors escape "stream_copy"
    # unwrapped); one with a local errno (ENOSPC/EACCES/ENOENT) or no errno falls through to permanent.
    # The errno read is suppressed because :func:`is_transient` is contracted never to raise and a
    # third-party OSError subclass can override ``errno`` as a property that raises or returns an
    # unhashable value (``in`` on the frozenset would then TypeError).
    with suppress(Exception):
        if isinstance(exc, OSError) and exc.errno in TRANSIENT_ERRNOS:
            return True
    # An integer status on the exception object (the openai / httpx shape) outranks its CLASS NAME: a
    # status-bearing 4xx must fail loudly rather than be retried because its class happens to be listed.
    status: int | None = _status_of(exc)
    if status is not None and status >= 400:
        return not is_permanent_http_status(status)
    if type(exc).__name__ in TRANSIENT_EXCEPTION_NAMES:
        return True
    message: str = _message_of(exc)
    if any(pattern.search(message) for pattern in TRANSIENT_MESSAGE_PATTERNS):
        return True
    return None


def is_transient(exc: BaseException) -> bool:
    """Return whether ``exc`` is a transient failure worth retrying.

    Pure, total, and contracted never to raise. Evaluation order (each step returns immediately):
    ``HTTPError`` status, then :data:`PERMANENT_EXCEPTION_TYPES`, then
    :data:`TRANSIENT_EXCEPTION_TYPES`, then a bare ``OSError`` whose ``errno`` is in
    :data:`TRANSIENT_ERRNOS`, then an integer ``status_code`` / ``status`` on the object, then
    :data:`TRANSIENT_EXCEPTION_NAMES` (optional dependencies, matched by class NAME), then
    :data:`TRANSIENT_MESSAGE_PATTERNS` on the lowercased message, then the ``__cause__`` /
    ``__context__`` chain up to :data:`MAX_CHAIN_DEPTH` links.

    Args:
        exc: Any exception, including a ``BaseException`` such as ``KeyboardInterrupt``.

    Returns:
        ``True`` when retrying later could plausibly succeed, ``False`` otherwise.

    Notes:
        WHY the default is ``False``: an exception the table does not recognize is treated as PERMANENT so
        a novel bug surfaces on the first attempt instead of being masked behind four tries and a
        seven-second sleep. Retrying the unknown is how a deterministic crash becomes a slow one.
    """
    current: BaseException | None = exc
    for _ in range(MAX_CHAIN_DEPTH):
        if current is None:
            return False
        verdict: bool | None = _classify(current)
        if verdict is not None:
            return verdict
        # ``is not None`` and NOT truthiness: a hostile exception can override ``__bool__`` to raise,
        # and this function is contracted NEVER to raise. Truthiness would also silently SKIP a
        # ``__context__`` sibling whenever ``__cause__`` is set, so prefer the cause only when present.
        # The attribute reads are guarded for the same contract: a subclass shadowing either slot with a
        # raising property must not escape this function; a failed read stops the walk, which is safe
        # because it can only make the verdict MORE conservative (permanent).
        try:
            nxt: BaseException | None = current.__cause__
            current = nxt if nxt is not None else current.__context__
        except Exception:
            break
    return False


def retry_after_seconds(exc: BaseException, *, cap: float = DEFAULT_RETRY_AFTER_CAP) -> float | None:
    """Return a server-supplied wait for ``exc``, or ``None`` when there is none.

    Priority: a ``Retry-After`` header in delta-seconds form, then the same header in HTTP-date form
    (via :func:`email.utils.parsedate_to_datetime`), then a ``(reset after <N>m <N>s)`` /
    ``(reset after <N>s)`` / ``(reset after <N>m)`` hint in the message. Both header shapes are read --
    ``exc.headers`` (``urllib.error.HTTPError``) and ``exc.response.headers`` (``openai`` / ``httpx``).

    Args:
        exc: Any exception.
        cap: Upper bound in seconds for the returned wait.

    Returns:
        Seconds to wait, clamped to ``[0, cap]``; ``None`` when no hint exists, when the hint is zero or
        negative, or when it could not be parsed.

    Notes:
        WHY ``None`` and never ``0``: the driver must be able to tell "the server told us how long to
        wait" from "use our own schedule". A zero would be indistinguishable from the latter and would
        silently discard a real hint.
    """
    seconds: float | None = _header_seconds(exc)
    if seconds is None:
        seconds = _reset_hint_seconds(_message_of(exc))
    if seconds is None or seconds <= 0.0:
        return None
    return min(seconds, cap)


def backoff_delay(
    attempt: int, *, base_delay: float = DEFAULT_BASE_DELAY, max_delay: float = DEFAULT_MAX_DELAY, rng: Callable[[], float] = random.random
) -> float:
    """Return the equal-jittered backoff for a 1-based ``attempt`` number.

    ``scheduled = min(max_delay, base_delay * 2 ** (attempt - 1))``; the result is
    ``scheduled * (0.5 + 0.5 * rng())``, i.e. always within ``[scheduled / 2, scheduled]``.

    Args:
        attempt: 1-based index of the attempt that just failed.
        base_delay: Seconds for the first backoff step.
        max_delay: Upper bound on the scheduled (pre-jitter) step.
        rng: Zero-argument callable returning a float in ``[0, 1)``; injectable so tests are deterministic.

    Returns:
        Seconds to sleep, in ``[scheduled / 2, scheduled]``.

    Raises:
        ValueError: If ``attempt`` is less than 1.

    Notes:
        WHY equal jitter and not full jitter: with 16 workers hitting one gateway, full jitter
        (``rng() * scheduled``) hands several of them a near-zero delay and re-stampedes the same
        saturated resolver in the same millisecond -- the exact failure this seam exists to fix. Keeping
        the floor at ``scheduled / 2`` preserves the decorrelation while forbidding the collapse.
    """
    if attempt < 1:
        raise ValueError("attempt must be >= 1")
    scheduled: float = min(max_delay, base_delay * 2 ** (attempt - 1))
    return scheduled * (0.5 + 0.5 * rng())


def retry_transient(
    operation: Callable[[], T],
    *,
    target: str,
    attempts: int = DEFAULT_ATTEMPTS,
    base_delay: float = DEFAULT_BASE_DELAY,
    max_delay: float = DEFAULT_MAX_DELAY,
    max_total_backoff: float = DEFAULT_MAX_TOTAL_BACKOFF,
    retry_after_cap: float = DEFAULT_RETRY_AFTER_CAP,
    sleep: Callable[[float], None] = time.sleep,
    rng: Callable[[], float] = random.random,
    error_factory: Callable[[str, int, BaseException], BaseException] | None = None,
) -> T:
    """Call ``operation()`` until it succeeds, its failures stop being transient, or its budget runs out.

    The ONLY retry driver in the codebase. Sleeps only BETWEEN attempts (never after the final failure),
    so a first-attempt success costs zero sleeps and zero logging. Each sleep is
    ``max(retry_after, backoff)`` when the failure carried a server hint, else ``backoff``; every sleep is
    truncated to the remaining ``max_total_backoff``, and when nothing remains the driver raises even if
    attempts are left.

    Args:
        operation: Zero-argument callable performing the fallible work.
        target: Human-readable name of the work (a URL, a model id) used in logs and the final error.
        attempts: Maximum number of calls to ``operation``; must be >= 1.
        base_delay: Seconds for the first backoff step.
        max_delay: Upper bound on one scheduled backoff step.
        max_total_backoff: Upper bound on the SUM of all sleeps in this call.
        retry_after_cap: Upper bound on an honored ``Retry-After`` / ``(reset after ...)`` hint.
        sleep: Sleeper; injectable so tests never wait.
        rng: Jitter source; injectable so tests are deterministic.
        error_factory: Builds the exhaustion error from ``(target, attempts_made, last_error)`` -- the
            attempts ACTUALLY made (a truncated budget can end the loop early); defaults to
            :class:`tablassert.errors.NetworkTransientError`. The LLM path passes a factory producing
            ``LlmTransientError`` instead.

    Returns:
        Whatever ``operation()`` returned on its first successful call.

    Raises:
        ValueError: If ``attempts`` is less than 1.
        Exception: The PERMANENT error from the first attempt that raised one, re-raised unwrapped and
            unchained so existing ``except PermissionError`` / ``except FileNotFoundError`` callers keep
            working. ``BaseException`` subclasses (``KeyboardInterrupt``, ``SystemExit``) are never
            caught, so Ctrl-C still aborts a fleet worker immediately.
        BaseException: ``error_factory(target, attempts, last_error)`` once every attempt is exhausted or
            the backoff budget is spent, chained ``from`` the last observed error so the original
            traceback survives.
    """
    if attempts < 1:
        raise ValueError("attempts must be >= 1")
    factory: Callable[[str, int, BaseException], BaseException] = NetworkTransientError if error_factory is None else error_factory
    history: list[RetryAttempt] = []
    slept: float = 0.0
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except Exception as error:  # deliberately NOT BaseException: Ctrl-C must propagate
            if not is_transient(error):
                raise
            history.append(RetryAttempt(target=target, attempt=attempt, attempts=attempts, error=error))
            if attempt == attempts:
                break
            hint: float | None = retry_after_seconds(error, cap=retry_after_cap)
            delay: float = backoff_delay(attempt, base_delay=base_delay, max_delay=max_delay, rng=rng)
            if hint is not None:
                # max(), not the hint alone: a hint can be SHORTER than the equal-jitter backoff already
                # computed, and honoring only the hint would then bypass the jitter that decorrelates
                # 16 fleet workers retrying the same saturated resolver in lockstep.
                delay = max(delay, hint)
            remaining: float = max_total_backoff - slept
            # Truncation (NOT abandonment): when a hint exceeds the budget we sleep what remains and
            # retry before the server permitted it, likely re-earning the 429. That costs one attempt
            # but keeps the TOTAL wall clock provably bounded -- the alternative (sleeping past the
            # budget) would trade a per-call arithmetic guarantee for a maybe-faster retry.
            delay = min(delay, remaining)
            if delay <= 0.0:
                break
            slept += delay
            logger.warning(
                "Retrying {target} after a transient failure (attempt {attempt}/{attempts}, sleeping {delay:.2f}s): {error}",
                target=target,
                attempt=attempt,
                attempts=attempts,
                delay=delay,
                # str(), never the exception OBJECT: log.py configures loguru with enqueue=True, which
                # pickles every kwarg through a queue to the writer process. HTTPError (and other
                # exceptions with required __init__ args) fails to unpickle there, so passing the object
                # would make THIS log line -- the one that exists to stop transient failures from being
                # invisible -- silently die in the handler.
                error=str(error),
            )
            sleep(delay)
    # Invariant: every exit from the loop above appends first, so `history` is never empty here.
    last: RetryAttempt = history[-1]
    logger.error(
        "Giving up on {target} after {attempt} transient failure(s) and {slept:.2f}s of backoff: {error}",
        target=last.target,
        attempt=last.attempt,
        slept=slept,
        error=str(last.error),  # see the enqueue=True pickling note above
    )
    raise factory(target, last.attempt, last.error) from last.error


def http_get_text(url: str, *, timeout: int = DEFAULT_TIMEOUT, attempts: int = DEFAULT_ATTEMPTS) -> str:
    """GET ``url`` with bounded retry and return the body decoded as UTF-8.

    Args:
        url: Absolute URL to fetch.
        timeout: Per-request socket timeout in seconds.
        attempts: Maximum number of ``urlopen`` calls.

    Returns:
        The response body decoded as UTF-8.

    Raises:
        NetworkTransientError: If every attempt failed with a transient error.
        Exception: Any permanent failure, re-raised unwrapped on the attempt that produced it.
    """

    def fetch() -> str:
        with urlopen(Request(url, headers={"User-Agent": USER_AGENT}), timeout=timeout) as response:
            body: bytes = response.read()
        return body.decode("utf-8")

    return retry_transient(fetch, target=url, attempts=attempts)


def http_get_bytes(url: str, *, timeout: int = DEFAULT_TIMEOUT, attempts: int = DEFAULT_ATTEMPTS) -> bytes:
    """GET ``url`` with bounded retry and return the raw body bytes.

    Args:
        url: Absolute URL to fetch.
        timeout: Per-request socket timeout in seconds.
        attempts: Maximum number of ``urlopen`` calls.

    Returns:
        The response body as bytes.

    Raises:
        NetworkTransientError: If every attempt failed with a transient error.
        Exception: Any permanent failure, re-raised unwrapped on the attempt that produced it.
    """

    def fetch() -> bytes:
        with urlopen(Request(url, headers={"User-Agent": USER_AGENT}), timeout=timeout) as response:
            return response.read()

    return retry_transient(fetch, target=url, attempts=attempts)
