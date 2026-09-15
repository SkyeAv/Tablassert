from __future__ import annotations

import socket
from pathlib import Path
from urllib.error import URLError

from tablassert.errors import (
    DOCS_URL,
    BabelDownloadError,
    GraphValidationError,
    LlmTransientError,
    NetworkTransientError,
    QcRuntimeMissingError,
    RewardConfigError,
    SectionValidationError,
    SourceFileError,
    TablassertValidationError,
    error_code_of,
    redact_secrets,
)


def test_qc_runtime_missing_error_code_and_docs_url() -> None:
    """Guard: the QC-runtime failure carries a stable slug and a docs link.

    A user who hits this mid-build needs the `qc-runtime-missing` code and a docs
    URL pointing straight at the fix (install tablassert[qc]), not a bare traceback.
    """
    err: QcRuntimeMissingError = QcRuntimeMissingError()
    assert err.code == "qc-runtime-missing"
    assert str(err).endswith(DOCS_URL + "qc-runtime-missing")


def test_graph_validation_error_code_and_docs_url() -> None:
    """Guard: a rejected graph config carries a stable slug and a docs link.

    The `graph-validation-failed` code plus docs URL is what tells a user their graph
    YAML is invalid before a multi-hour build ever starts.
    """
    err: GraphValidationError = GraphValidationError(Path("graph.yaml"), "tables field is required")
    assert err.code == "graph-validation-failed"
    assert str(err).endswith(DOCS_URL + "graph-validation-failed")


def test_section_validation_error_code_and_docs_url() -> None:
    """Guard: a rejected table section carries a stable slug and a docs link.

    The `section-validation-failed` code plus docs URL is what tells a user which table
    section is invalid before a multi-hour build ever starts.
    """
    err: SectionValidationError = SectionValidationError(Path("table.yaml"), "0123456789abcdef", "source field is required")
    assert err.code == "section-validation-failed"
    assert str(err).endswith(DOCS_URL + "section-validation-failed")


def test_babel_download_error_code_and_docs_url() -> None:
    """Guard: an exhausted BABEL download carries a stable slug and a docs link.

    The `babel-download-failed` code plus docs URL is what tells a user a network fetch
    gave up after retries, and where to read about pinning a different BABEL version.
    """
    err: BabelDownloadError = BabelDownloadError("https://stars.renci.org/var/babel_outputs/x.gz", 5, RuntimeError("network down"))
    assert err.code == "babel-download-failed"
    assert str(err).endswith(DOCS_URL + "babel-download-failed")


def test_network_transient_error_code_and_docs_url() -> None:
    """Guard: an exhausted retry budget on a TRANSIENT failure carries a stable slug and a docs link.

    The `network-transient` code is what lets a fleet consumer requeue an article WITHOUT keyword-matching
    `notes`. A 16-worker run over 42,981 PMC articles recorded 2,194 DNS-shaped failures (2,031 x
    `[Errno -2] Name or service not known`) as `status=SKIPPED` with notes byte-identical in shape to a
    legitimate not-open-access skip, so 87.5% of the failures were indistinguishable from the 7,002 real
    skips. The message must also name the target, the attempt count, and the last error, and state plainly
    that the failure is retryable later.
    """
    last: URLError = URLError(socket.gaierror(-2, "Name or service not known"))
    err: NetworkTransientError = NetworkTransientError("https://pmc-oa-opendata.s3.amazonaws.com/PMC11708054.xml", 4, last)
    assert err.code == "network-transient"
    assert str(err).endswith(DOCS_URL + "network-transient")
    assert err.target == "https://pmc-oa-opendata.s3.amazonaws.com/PMC11708054.xml"
    assert err.attempts == 4
    assert err.last_error is last
    assert "4 attempts" in str(err)
    assert "Name or service not known" in str(err)
    assert "retryable later" in str(err)


def test_redact_secrets_preserves_context_and_avoids_false_positive_text() -> None:
    """Credential redaction covers gateway shapes without corrupting ordinary provider diagnostics."""
    cases: dict[str, str] = {
        "api_key=VALUE123": "api_key=[REDACTED]",
        "api_key: VALUE123": "api_key: [REDACTED]",
        'x-api-key: "VALUE123"': 'x-api-key: "[REDACTED]"',
        "Authorization: Bearer JWT.VALUE": "Authorization: Bearer [REDACTED]",
        "Bearer JWT.VALUE": "Bearer [REDACTED]",
        '{"api_key": "VALUE123"}': '{"api_key": "[REDACTED]"}',
        "sk-proj-TOKEN123456": "[REDACTED]",
        "max_tokens=4096": "max_tokens=4096",
        "8192 tokens. tokenizer failed; secret sauce": "8192 tokens. tokenizer failed; secret sauce",
        "qwen-token-plan": "qwen-token-plan",
    }
    for raw, expected in cases.items():
        assert redact_secrets(raw) == expected, raw
    assert redact_secrets("prefix-SECRET-suffix", ("SECRET",)) == "prefix-[REDACTED]-suffix"
    assert redact_secrets("a", ("a",)) == "[REDACTED]"


def test_error_code_of_walks_wrapped_coded_errors() -> None:
    """smolagents-style wrapper exceptions still expose the inner LLM code to the supervisor."""
    inner: LlmTransientError = LlmTransientError("llm", 4, RuntimeError("503"))
    outer: RuntimeError = RuntimeError("AgentGenerationError")
    outer.__cause__ = inner
    assert error_code_of(outer) == "llm-transient"


def test_llm_transient_error_code_and_docs_url() -> None:
    """Guard: an exhausted LLM retry budget carries the stable slug and docs URL."""
    last: RuntimeError = RuntimeError("gateway 503")
    err: LlmTransientError = LlmTransientError("gateway/model", 4, last)
    assert err.code == "llm-transient"
    assert str(err).endswith(DOCS_URL + "llm-transient")
    assert err.target == "gateway/model"
    assert err.attempts == 4
    assert err.last_error is last
    assert "retryable later" in str(err)


def test_source_file_error_carries_config_section_and_path() -> None:
    """Guard: an unreadable source file names the config, the section, and the path.

    The `source-file-unreadable` code plus docs URL tells a user which section's
    ``source.local`` could not be hashed, and the message must carry the table config
    path, the section label, and the offending path so they can find it without a
    debugger.
    """
    err: SourceFileError = SourceFileError(Path("/data/missing.csv"), "no such file", config=Path("table.yaml"), section_label="table · 0123abcd")
    assert err.code == "source-file-unreadable"
    assert str(err).endswith(DOCS_URL + "source-file-unreadable")
    assert "table.yaml" in str(err)
    assert "table · 0123abcd" in str(err)
    assert "/data/missing.csv" in str(err)


def test_reward_config_error_code_and_docs_url() -> None:
    """Guard: a rejected reward config carries a stable slug and a docs link.

    Why: the reward config decides which examples train the LoRA, so a typo'd knob must surface as
    the greppable, docs-linked `reward-config-invalid` failure — never as a silently-defaulted
    weighting policy. The message is built by the loader (it names the file, the key, and the
    valid set); this class only pins the slug and the URL.
    """
    err: RewardConfigError = RewardConfigError("Reward config invalid: reward.yaml — unknown key 'w_covrage' (valid: w_coverage, ...)")
    assert err.code == "reward-config-invalid"
    assert str(err).endswith(DOCS_URL + "reward-config-invalid")
    assert "w_covrage" in str(err)


def test_source_file_error_without_build_context() -> None:
    """Guard: the error is still informative when raised without build context.

    The utils layer that first notices the failure often lacks the config path and
    section label; the message must still name the offending path and the OS detail.
    """
    err: SourceFileError = SourceFileError(Path("orphan.csv"), "permission denied")
    assert err.code == "source-file-unreadable"
    assert "orphan.csv" in str(err)
    assert "permission denied" in str(err)


def test_error_code_of_reads_coded_and_plain_exceptions() -> None:
    """``error_code_of`` exposes the private ``_Coded`` slug publicly, and NEVER raises.

    WHY a helper rather than an ``isinstance`` ladder in the supervisor: a ladder over concrete classes
    silently stops covering every code added later, while this reads the mixin once -- so a newly coded
    error becomes machine-readable in ``state.json`` with no further wiring. WHY never-raise: it runs
    INSIDE the supervisor's catch-all, where a raising helper would turn "one bad pmc" into "the whole
    16-worker batch aborts".
    """
    coded: NetworkTransientError = NetworkTransientError("https://pmc/x", 4, URLError(socket.gaierror(-2, "Name or service not known")))
    assert error_code_of(coded) == "network-transient"
    assert error_code_of(BabelDownloadError("https://babel/x.gz", 5, RuntimeError("down"))) == "babel-download-failed"
    assert error_code_of(GraphValidationError(Path("g.yaml"), "bad")) == "graph-validation-failed"
    assert error_code_of(SectionValidationError(Path("g.yaml"), "deadbeefcafe", "bad")) == "section-validation-failed"
    assert error_code_of(QcRuntimeMissingError(["scikit-learn"])) == "qc-runtime-missing"
    # A coded VALIDATION error is machine-readable too -- an intentional side effect that makes
    # non-network terminal skips readable without touching `status` or `notes`.
    assert error_code_of(TablassertValidationError("nope", code="field-disabled")) == "field-disabled"

    # Non-coded exceptions -- including the deterministic gates' own errors -- yield None.
    assert error_code_of(FileNotFoundError("No supplementary tables found for PMC1.")) is None
    assert error_code_of(PermissionError("not open access")) is None
    assert error_code_of(RuntimeError("coverage gate")) is None
    assert error_code_of(ValueError("bad config")) is None
    assert error_code_of(KeyboardInterrupt()) is None  # BaseException, not just Exception
    # A plain object wearing a `code` attribute is NOT coded: the mixin, not duck typing, decides.
    assert error_code_of(type("Fake", (Exception,), {"code": "not-a-real-code"})()) is None

    # A non-string `code` degrades to None instead of poisoning state.json with a non-str value.
    class IntCoded(NetworkTransientError):
        def __init__(self) -> None:
            super().__init__("t", 1, RuntimeError("x"))
            object.__setattr__(self, "code", 42)  # shadow the documented str slug with junk

    assert error_code_of(IntCoded()) is None

    # The hostile descriptor must not escape: the supervisor's handler stays alive. Built via
    # ``__new__`` because the base ``__init__`` assigns ``self.code``, which a read-only property rejects.
    class HostileCoded(NetworkTransientError):
        @property
        def code(self) -> object:  # type: ignore[override]
            raise KeyboardInterrupt("hostile descriptor")

    assert error_code_of(HostileCoded.__new__(HostileCoded)) is None
