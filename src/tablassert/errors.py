from __future__ import annotations

import re
from collections.abc import Sequence
from pathlib import Path
from typing import Literal

DOCS_URL: str = "https://tablassert.readthedocs.io/errors/"

_SECRET_ASSIGNMENT_RE: re.Pattern[str] = re.compile(
    r"""(?ix)
    (?P<prefix>
        ["']?(?:api[_-]?key|x-api-key)["']?\s*[:=]\s*
      | ["']?authorization["']?\s*[:=]\s*(?:bearer\s+)?
      | \bbearer\s+
      | \b(?:token|secret)\b\s*[:=]\s*
    )
    (?P<quote>["']?)(?P<value>[^,\s;}"']+)(?P=quote)
    """
)
_SECRET_TOKEN_RE: re.Pattern[str] = re.compile(r"(?i)\bsk(?:ant)?-[A-Za-z0-9_-]{8,}\b")


def redact_secrets(text: str, secrets: Sequence[str] = ()) -> str:
    """Redact configured credentials and common credential-shaped values from ``text``.

    This is used at both the retry log boundary and coded-error construction boundary. Exact configured
    values are replaced first; the shape rules cover provider messages that expose a credential under
    ``api_key=``, ``Authorization: Bearer``, or a public ``sk-`` token without requiring optional SDKs.
    """
    redacted: str = text
    for secret in secrets:
        if secret:
            redacted = redacted.replace(secret, "[REDACTED]")
    redacted = _SECRET_ASSIGNMENT_RE.sub(lambda match: f"{match.group('prefix')}{match.group('quote')}[REDACTED]{match.group('quote')}", redacted)
    return _SECRET_TOKEN_RE.sub("[REDACTED]", redacted)


TablassertErrorCodes = Literal[
    "qc-runtime-missing",
    "missing-extra",
    "graph-validation-failed",
    "section-validation-failed",
    "babel-download-failed",
    "source-file-unreadable",
    "resolve-bad-specs",
    "config-rows-and-row-slice-conflict",
    "comparison-bad-comparator-type",
    "comparison-nonnumeric-comparator",
    "encoding-bad-excel-column",
    "regex-bad-pattern",
    "regex-bad-replacement",
    "encoding-bad-remove-entry",
    "graph-bad-infores",
    "override-bad-publication",
    "override-bad-sources",
    "override-bad-upstream-infores",
    "override-bad-upstream-urls",
    "provenance-bad-pmc-id",
    "provenance-missing-publication",
    "provenance-publication-and-override",
    "encoding-list-method-removed",
    "annotation-split-by-requires-column",
    "annotation-split-by-empty",
    "field-disabled",
    "qualifier-bad-value",
    "qualifier-unsatisfiable",
    "qualifier-nullable-literal",
    "qualifier-duplicated",
    "rig-bad-infores",
    "rig-bad-artifact-url",
    "rig-bad-access-location",
    "rig-terms-empty",
    "rig-legacy-keys",
    "rig-validation-failed",
    "uuid-bad-fields",
    "uuid-fields-not-a-key",
    "uuid-merge-without-fields",
    "network-transient",
    "llm-transient",
    "reward-config-invalid",
]


class _Coded:
    """Mixin providing a stable kebab-case slug and an auto-appended docs URL on ``str()``."""

    message: str
    code: TablassertErrorCodes

    def __str__(self) -> str:
        return f"{self.message}\n\nFor further information visit {DOCS_URL}{self.code}"


class TablassertError(_Coded, RuntimeError):
    def __init__(self, message: str, *, code: TablassertErrorCodes) -> None:
        super().__init__(message)
        self.message = message
        self.code = code


class TablassertValidationError(_Coded, ValueError):
    """Base class for validation failures raised through Pydantic v2.

    Notes:
        Inherits ``ValueError`` so Pydantic v2 still wraps it inside ``ValidationError``.
    """

    def __init__(self, message: str, *, code: TablassertErrorCodes) -> None:
        super().__init__(message)
        self.message = message
        self.code = code


class BiolinkRelocationWarning(UserWarning):
    """An annotation is valid but will not land on the edge under its own name.

    Distinct from a deprecation: nothing is wrong with the config and nothing is lost. The value is
    relocated -- onto the inlined ``StudyResult`` for a slot Biolink attaches to no class, or into
    ``supporting_text`` for a name that is not an association slot at all. Its own category so
    callers can silence or assert on relocations without touching the deprecation scaffold.
    """


class UnpairedEffectAnnotationWarning(UserWarning):
    """One half of the ``effect_size`` / ``effect_type`` pair was declared without its sibling.

    Neither half carries evidence alone: a bare effect size is uninterpretable (0.85 of *what* --
    an odds ratio or a Spearman rho?), and Biolink PR #1774 only populates ``effect_type``
    alongside a numeric ``effect_size``, so the build nulls an unpaired type outright. Validation
    therefore DROPS the unpaired annotation from the section -- with this warning naming what was
    dropped and from where -- and keeps the section and its edges, instead of failing the whole
    section over a value the build would have discarded anyway. Its own category so callers can
    silence or assert on these drops without touching the relocation or deprecation scaffolds.
    """


def _direct_error_code(exc: BaseException) -> str | None:
    """Return the stable ``code`` attached directly to ``exc``, without inspecting its chain."""
    if not isinstance(exc, _Coded):
        return None
    try:
        code: object = exc.code
    except BaseException:  # a hostile `code` descriptor must not escape the caller's error handler
        return None
    return code if isinstance(code, str) else None


def error_code_of(exc: BaseException) -> str | None:
    """Return the stable kebab-case ``code`` of a coded Tablassert error, else ``None``.

    Args:
        exc: Any caught exception.

    Returns:
        ``exc.code`` when ``exc`` mixes in :class:`_Coded` (``TablassertError``,
        ``TablassertValidationError``, ``MissingExtraError``, ``NetworkTransientError``, and any coded
        error added later); ``None`` for everything else.

    Notes:
        WHY a helper: ``_Coded`` is private by design, but a consumer (the agent supervisor) needs the
        code without an ``isinstance`` ladder over every concrete class -- a ladder that would silently
        stop covering new codes. Reading it through one function means a newly added coded error is
        machine-readable in ``state.json`` with no further wiring.

        This function NEVER raises for a bad ``code``: a non-string degrades to ``None``, and even a
        subclass whose ``code`` descriptor raises a ``BaseException`` cannot take the caller's error
        handler down with it. This deliberately treats hostile ``KeyboardInterrupt``/``SystemExit``
        descriptors as malformed code attributes, because this helper runs inside a catch-all handler.
    """
    # smolagents wraps provider failures in an uncoded ``AgentGenerationError``; a cause/context walk
    # preserves the machine-readable ``llm-transient`` code through that wrapper, matching the bounded
    # classification walk already used by ``net.is_transient``.
    current: BaseException | None = exc
    for _ in range(3):
        if current is None:
            return None
        code: str | None = _direct_error_code(current)
        if code is not None:
            return code
        try:
            nxt: BaseException | None = current.__cause__
            current = nxt if nxt is not None else current.__context__
        except Exception:
            break
    return None


def format_missing_extra(extra: str, problem: str) -> str:
    """Append the install instructions for ``extra`` to a one-sentence ``problem``.

    Every missing-extra failure — whichever module noticed it — ends in the same
    two commands, so a user never has to guess the package name behind an extra or
    which installer their CLI came from.

    Args:
        extra: Extra name as it appears in ``pyproject.toml`` (``qc``, ``agent``, ...).
        problem: One complete sentence stating what is missing and what needed it.

    Returns:
        The full message body (the docs URL is appended separately by :class:`_Coded`).
    """
    return f'{problem} Install the [{extra}] extra: pip install "tablassert[{extra}]" (uv: uv tool install "tablassert[{extra}]")'


def describe_missing(missing: Sequence[str], required_by: str) -> str:
    """Phrase the ``problem`` sentence naming the absent distributions and the feature that wanted them.

    Args:
        missing: DISTRIBUTION names (what a user types into pip: ``scikit-learn``,
            not the ``sklearn`` import name).
        required_by: The feature that needs them, phrased to follow "required by".

    Returns:
        A single sentence, or a generic one when ``missing`` is empty.
    """
    if not missing:
        return f"{required_by} requires optional dependencies that are not installed."
    plural: str = "y" if len(missing) == 1 else "ies"
    names: str = ", ".join(repr(name) for name in missing)
    return f"Missing optional dependenc{plural} {names} — required by {required_by}."


class MissingExtraError(_Coded, ImportError):
    """An optional extra is not installed, reported with the exact install command.

    Notes:
        Inherits ``ImportError`` rather than :class:`TablassertError` (a ``RuntimeError``)
        so the existing ``except ImportError`` guards wrapping lazy optional imports keep
        catching it, and so a missing package still reads as an import failure to callers
        that never heard of Tablassert's error hierarchy.
    """

    def __init__(self, extra: str, problem: str, *, missing: Sequence[str] = ()) -> None:
        message: str = format_missing_extra(extra, problem)
        super().__init__(message)
        self.message = message
        self.code = "missing-extra"
        self.extra = extra
        self.missing: tuple[str, ...] = tuple(missing)


class QcRuntimeMissingError(TablassertError):
    """The ``[qc]`` extra is absent on a QC code path.

    Notes:
        Keeps its own ``qc-runtime-missing`` code and ``TablassertError`` base for
        back-compat (``build_and_audit`` catches it by name, and the code is documented),
        but shares :class:`MissingExtraError`'s message so both read identically.
    """

    def __init__(self, missing: Sequence[str] = ()) -> None:
        super().__init__(format_missing_extra("qc", describe_missing(missing, "the QC audit")), code="qc-runtime-missing")
        self.missing: tuple[str, ...] = tuple(missing)


class GraphValidationError(TablassertError):
    def __init__(self, config: Path, detail: str) -> None:
        super().__init__(f"Graph config validation failed: {config}\n{detail}", code="graph-validation-failed")


class SectionValidationError(TablassertError):
    def __init__(self, config: Path, section_hash: str, detail: str) -> None:
        super().__init__(f"Section validation failed: {config} (hash {section_hash[:8]})\n{detail}", code="section-validation-failed")


class BabelDownloadError(TablassertError):
    def __init__(self, url: str, retries: int, last_error: BaseException) -> None:
        super().__init__(
            f"BABEL download failed after {retries} attempts: {url} (last error: {last_error}). Check network connectivity or pin a different BABEL version.",
            code="babel-download-failed",
        )


class NetworkTransientError(TablassertError):
    """A network operation exhausted its retries on a failure classified as TRANSIENT.

    Notes:
        WHY its own code: the agent supervisor records ``error_code`` on the article record, and a fleet
        consumer must be able to requeue these without keyword-matching the notes string. The underlying
        condition -- DNS ``EAI_NONAME`` behind ``resolve [!UNAVAIL=return]``, a gateway 503 wrapping an
        upstream 429, a reset socket -- is recoverable by simply trying again later, which is the opposite
        of the terminal ``PermissionError`` / ``FileNotFoundError`` skips it was previously
        indistinguishable from.
    """

    def __init__(self, target: str, attempts: int, last_error: BaseException, *, secrets: Sequence[str] = ()) -> None:
        safe_error: str = redact_secrets(str(last_error), secrets)
        super().__init__(
            f"Transient network failure for {target} after {attempts} attempts (last error: {safe_error}). "
            "This is retryable later — it is not a permanent rejection of the request.",
            code="network-transient",
        )
        self.target: str = target
        self.attempts: int = attempts
        self.last_error: BaseException = last_error


class LlmTransientError(TablassertError):
    """An LLM call exhausted its bounded retries on a transient failure."""

    def __init__(self, target: str, attempts: int, last_error: BaseException, *, secrets: Sequence[str] = ()) -> None:
        # Provider exceptions may include request URLs, headers, or provider messages. The retry
        # wrapper sanitizes its model target, but this final error must also avoid echoing credentials
        # into state.json and worker logs.
        safe_error: str = redact_secrets(str(last_error), secrets)
        super().__init__(
            f"Transient LLM failure for {target} after {attempts} attempts (last error: {safe_error}). "
            "This is retryable later — it is not a permanent rejection of the request.",
            code="llm-transient",
        )
        self.target: str = target
        self.attempts: int = attempts
        self.last_error: BaseException = last_error


class RewardConfigError(TablassertError):
    """A reward-config file failed to load or validate.

    Notes:
        The caller builds the full message (naming the offending file and key and
        stating the valid set); this class only pins the stable
        ``reward-config-invalid`` slug so a bad weighting policy is greppable and
        docs-linkable like every other coded failure.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message, code="reward-config-invalid")


class SourceFileError(TablassertError):
    """A table section's ``source.local`` file could not be read for content-hashing.

    Notes:
        ``config`` and ``section_label`` are optional keyword context: the utils layer
        that first notices the failure often lacks build context, but callers that have
        it (the CLI's section loop) should pass them so the message names the exact
        table config and section label (config stem + 8-char section digest).
    """

    def __init__(self, path: Path, detail: str, *, config: Path | None = None, section_label: str | None = None) -> None:
        context: str = ""
        if config is not None and section_label is not None:
            context = f" (config {config}, section {section_label})"
        elif config is not None:
            context = f" (config {config})"
        elif section_label is not None:
            context = f" (section {section_label})"
        super().__init__(f"Source file unreadable{context}: {path} — {detail}", code="source-file-unreadable")
