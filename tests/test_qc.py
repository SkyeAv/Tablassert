from __future__ import annotations

from typing import Any

import numpy as np
import polars as pl
from rapidfuzz import fuzz
from rapidfuzz import process as rf_process
from sklearn.metrics import pairwise

import tablassert.qc as qc


def test_fullmap_audit_suppresses_logs(monkeypatch: Any) -> None:
    """fullmap_audit suppresses failed QC logs when disabled."""
    messages: list[str] = []

    class DummyLogger:
        def info(self, message: str, *args: Any, **kwargs: Any) -> None:
            messages.append(message.format(*args, **kwargs) if kwargs else message)

    class DummySapBERT:
        def encode(self, values: list[str]) -> object:
            return np.array([[0.0], [1.0]])

    def fake_cpdist(left: list[str], right: list[str], scorer: Any) -> object:
        return np.array([0.0])

    def fake_cosine_similarity(left: object, right: object) -> object:
        return np.array([[0.1]])

    monkeypatch.setattr(qc, "logger", DummyLogger())
    monkeypatch.setattr(qc, "get_sapbert", lambda: DummySapBERT())
    monkeypatch.setattr(rf_process, "cpdist", fake_cpdist)
    monkeypatch.setattr(pairwise, "cosine_similarity", fake_cosine_similarity)

    lf: pl.LazyFrame = pl.DataFrame({"subject": ["FOO:1"], "subject_pre_resolution": ["foo"], "subject_name": ["bar"]}).lazy()
    result: pl.DataFrame = qc.fullmap_audit(lf, "subject", "store123", "config.yaml", log=False).collect()

    assert result.height == 0
    assert messages == []


def test_fullmap_audit_logs_failures(monkeypatch: Any) -> None:
    """fullmap_audit logs failed QC rows when enabled."""
    messages: list[str] = []

    class DummyLogger:
        def info(self, message: str, *args: Any, **kwargs: Any) -> None:
            messages.append(message.format(*args, **kwargs) if kwargs else message)

    class DummySapBERT:
        def encode(self, values: list[str]) -> object:
            return np.array([[0.0], [1.0]])

    def fake_cpdist(left: list[str], right: list[str], scorer: Any) -> object:
        assert scorer in {fuzz.ratio, fuzz.partial_token_sort_ratio, fuzz.token_set_ratio}
        return np.array([0.0])

    def fake_cosine_similarity(left: object, right: object) -> object:
        return np.array([[0.1]])

    monkeypatch.setattr(qc, "logger", DummyLogger())
    monkeypatch.setattr(qc, "get_sapbert", lambda: DummySapBERT())
    monkeypatch.setattr(rf_process, "cpdist", fake_cpdist)
    monkeypatch.setattr(pairwise, "cosine_similarity", fake_cosine_similarity)

    lf: pl.LazyFrame = pl.DataFrame({"subject": ["FOO:1"], "subject_pre_resolution": ["foo"], "subject_name": ["bar"]}).lazy()
    result: pl.DataFrame = qc.fullmap_audit(lf, "subject", "store123", "config.yaml", log=True).collect()

    assert result.height == 0
    assert len(messages) == 1
    assert "QC rejected" in messages[0]
    assert "hash=store123" in messages[0]
    assert "config=config.yaml" in messages[0]
    assert "fuzz=" in messages[0]
    assert "sapbert=" in messages[0]


def test_fullmap_audit_log_score_values(monkeypatch: Any) -> None:
    """fullmap_audit log message contains expected score values."""
    messages: list[str] = []

    class DummyLogger:
        def info(self, message: str, *args: Any, **kwargs: Any) -> None:
            messages.append(message.format(*args, **kwargs) if kwargs else message)

    class DummySapBERT:
        def encode(self, values: list[str]) -> object:
            return np.array([[0.0], [1.0]])

    def fake_cpdist(left: list[str], right: list[str], scorer: Any) -> object:
        return np.array([5.0]) if scorer == fuzz.ratio else np.array([8.0])

    def fake_cosine_similarity(left: object, right: object) -> object:
        return np.array([[0.05]])

    monkeypatch.setattr(qc, "logger", DummyLogger())
    monkeypatch.setattr(qc, "get_sapbert", lambda: DummySapBERT())
    monkeypatch.setattr(rf_process, "cpdist", fake_cpdist)
    monkeypatch.setattr(pairwise, "cosine_similarity", fake_cosine_similarity)

    lf: pl.LazyFrame = pl.DataFrame({"subject": ["X:1"], "subject_pre_resolution": ["foo"], "subject_name": ["bar"]}).lazy()
    result: pl.DataFrame = qc.fullmap_audit(lf, "subject", "store456", "cfg.yaml", log=True).collect()

    assert result.height == 0
    assert len(messages) == 1
    assert "fuzz=8.0" in messages[0]
    assert "sapbert=0.05" in messages[0]


def test_fullmap_audit_on_phase_fires_all_stages_when_sapbert_runs(monkeypatch: Any) -> None:
    """fullmap_audit fires qc:exact, qc:fuzzy, qc:abbrev, qc:sapbert in order when Stage 4 runs; output unchanged."""
    phases: list[str] = []

    class DummySapBERT:
        def encode(self, values: list[str]) -> object:
            return np.array([[0.0], [1.0]])

    def fake_cpdist(left: list[str], right: list[str], scorer: Any) -> object:
        return np.array([0.0])

    def fake_cosine_similarity(left: object, right: object) -> object:
        return np.array([[0.1]])

    monkeypatch.setattr(qc, "get_sapbert", lambda: DummySapBERT())
    monkeypatch.setattr(rf_process, "cpdist", fake_cpdist)
    monkeypatch.setattr(pairwise, "cosine_similarity", fake_cosine_similarity)

    lf: pl.LazyFrame = pl.DataFrame({"subject": ["FOO:1"], "subject_pre_resolution": ["foo"], "subject_name": ["bar"]}).lazy()
    with_cb: pl.DataFrame = qc.fullmap_audit(lf, "subject", "s", "c.yaml", log=False, on_phase=phases.append).collect()
    without_cb: pl.DataFrame = qc.fullmap_audit(lf, "subject", "s", "c.yaml", log=False).collect()

    assert phases == ["qc:exact", "qc:fuzzy", "qc:abbrev", "qc:sapbert"]
    assert with_cb.to_dicts() == without_cb.to_dicts()


def test_fullmap_audit_on_phase_skips_abbrev_and_sapbert_on_fuzzy_quick_exit(monkeypatch: Any) -> None:
    """fullmap_audit fires qc:exact then qc:fuzzy but NOT qc:abbrev/qc:sapbert when fuzzy resolves every row."""
    phases: list[str] = []

    class DummySapBERT:
        def encode(self, values: list[str]) -> object:
            raise AssertionError("Stages 3/4 (abbreviation/SapBERT) must not run on the fuzzy quick-exit path")

    def fake_cpdist(left: list[str], right: list[str], scorer: Any) -> object:
        # High ratio score: fuzzy resolves every pending row, so Stage 3 is skipped.
        return np.array([100.0])

    monkeypatch.setattr(qc, "get_sapbert", lambda: DummySapBERT())
    monkeypatch.setattr(rf_process, "cpdist", fake_cpdist)

    lf: pl.LazyFrame = pl.DataFrame({"subject": ["FOO:1"], "subject_pre_resolution": ["foo"], "subject_name": ["bar"]}).lazy()
    result: pl.DataFrame = qc.fullmap_audit(lf, "subject", "s", "c.yaml", log=False, on_phase=phases.append).collect()

    assert phases == ["qc:exact", "qc:fuzzy"]
    assert result.height == 1  # fuzzy passed the row; nothing reached abbreviation/SapBERT


def test_is_abbrev_schwartz_hearst() -> None:
    """_is_abbrev matches Schwartz-Hearst abbreviation/expansion pairs and rejects near-misses."""
    positives: list[tuple[str, str]] = [
        ("AML", "acute myeloid leukemia"),
        ("EGFR", "epidermal growth factor receptor"),
        ("TP53", "tumor protein p53"),
        ("ER-alpha", "estrogen receptor alpha"),
        ("wt", "Wilms tumor 1"),
        ("  aml  ", "  Acute Myeloid Leukemia  "),
    ]
    negatives: list[tuple[str, str]] = [
        ("AML", "chronic myeloid leukemia"),  # no word-boundary match for the leading 'A'
        ("na", "banana"),  # 'n' never lands on a word boundary
        ("A", "acute myeloid leukemia"),  # single-character short forms rejected
        ("", "acute myeloid leukemia"),
        ("--", "double dash"),  # no alphanumeric characters
        ("acute myeloid leukemia", "AML"),  # direction matters; the stage ORs both
    ]
    for sf, lf in positives:
        assert qc._is_abbrev(sf, lf), (sf, lf)
    for sf, lf in negatives:
        assert not qc._is_abbrev(sf, lf), (sf, lf)


def test_fullmap_audit_passes_abbreviation_pairs_without_sapbert(monkeypatch: Any) -> None:
    """Pairs fuzzy rejects but Schwartz-Hearst accepts pass at Stage 3; SapBERT never loads."""
    phases: list[str] = []

    class DummySapBERT:
        def encode(self, values: list[str]) -> object:
            raise AssertionError("Stage 4 (SapBERT) must not run when the abbreviation stage passes every row")

    def fake_cpdist(left: list[str], right: list[str], scorer: Any) -> object:
        return np.zeros(len(left))

    monkeypatch.setattr(qc, "get_sapbert", lambda: DummySapBERT())
    monkeypatch.setattr(rf_process, "cpdist", fake_cpdist)

    lf: pl.LazyFrame = pl.DataFrame(
        {
            "subject": ["FOO:1", "FOO:2"],
            "subject_pre_resolution": ["aml", "epidermal growth factor receptor"],
            "subject_name": ["acute myeloid leukemia", "EGFR"],
        }
    ).lazy()
    result: pl.DataFrame = qc.fullmap_audit(lf, "subject", "s", "c.yaml", log=False, on_phase=phases.append).collect()

    assert phases == ["qc:exact", "qc:fuzzy", "qc:abbrev"]
    assert result.height == 2


def test_qc_text_restores_faers_separator_noise() -> None:
    """Punctuation-mangled source names normalize to the same lexical form as clean names."""
    assert qc._qc_text("PFIZER?BIONTECH COVID?19 VACCINE") == "pfizer biontech covid 19 vaccine"
    assert qc._qc_text("NATURE?THROID") == "nature throid"


def test_qc_alias_resolves_brand_name_before_scoring() -> None:
    """Configured aliases provide deterministic brand-to-generic control without SapBERT."""
    assert qc._qc_alias("BETOLVEX", {"betolvex": "Cyanocobalamin"}) == "Cyanocobalamin"
    assert qc._qc_alias("XEFO", {}) == "XEFO"
