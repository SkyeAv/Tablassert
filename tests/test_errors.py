from __future__ import annotations

from pathlib import Path

from tablassert.errors import (
    DOCS_URL,
    BabelDownloadError,
    GraphValidationError,
    QcRuntimeMissingError,
    RewardConfigError,
    SectionValidationError,
    SourceFileError,
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
