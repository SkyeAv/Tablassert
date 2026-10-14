"""Coverage tests for the ``get_sapbert`` loader body in ``tablassert.qc``.

``tests/test_qc.py`` monkeypatches ``qc.get_sapbert`` itself, so the real loader
body never runs. These tests exercise that body directly — the cached local-load
branch, the download-and-save branch, and the ``ImportError`` guard — by faking
``sentence_transformers`` and pointing ``qc.MODEL`` at a temp path, so no real
model is ever downloaded.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

import tablassert.qc as qc
from tablassert.errors import QcRuntimeMissingError

HF_REPO: str = "RenaissanceComputing/SapBERT-PubMedBERT-BabelSynonyms-v2025-01-23"


class FakeModel:
    """Stand-in for a ``SentenceTransformer`` recording ``save`` calls."""

    def __init__(self) -> None:
        self.saved_to: list[object] = []

    def save(self, path: object) -> None:
        self.saved_to.append(path)


class FakeSentenceTransformers:
    """Fake ``sentence_transformers`` module recording constructor arguments."""

    def __init__(self, model: FakeModel | None = None, error: Exception | None = None) -> None:
        self.model: FakeModel = model if model is not None else FakeModel()
        self.error: Exception | None = error
        self.calls: list[str] = []

    def SentenceTransformer(self, name: str) -> FakeModel:
        self.calls.append(name)
        if self.error is not None:
            raise self.error
        return self.model


@pytest.fixture(autouse=True)
def _clear_sapbert_cache() -> Any:
    """Clear the ``functools.cache`` around ``get_sapbert`` so each test runs the body."""
    qc.get_sapbert.cache_clear()
    yield
    qc.get_sapbert.cache_clear()


def test_get_sapbert_loads_from_local_cache(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """When ``MODEL`` exists, load it from disk and return (no download/save)."""
    model_dir: Path = tmp_path / "sapbert"
    model_dir.mkdir()
    fake: FakeSentenceTransformers = FakeSentenceTransformers()
    monkeypatch.setattr(qc, "MODEL", model_dir)
    monkeypatch.setattr(qc, "sentence_transformers", fake)

    result: object = qc.get_sapbert()

    assert result is fake.model
    assert fake.calls == [str(model_dir)]  # loaded from the local cache path, not the HF repo
    assert fake.model.saved_to == []  # cache-hit path never saves


def test_get_sapbert_downloads_and_saves_when_cache_missing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """When ``MODEL`` is absent, download from HF, mkdir the cache, and save."""
    model_dir: Path = tmp_path / "sapbert"
    assert not model_dir.exists()
    fake: FakeSentenceTransformers = FakeSentenceTransformers()
    monkeypatch.setattr(qc, "MODEL", model_dir)
    monkeypatch.setattr(qc, "sentence_transformers", fake)

    result: object = qc.get_sapbert()

    assert result is fake.model
    assert fake.calls == [HF_REPO]  # downloaded the canonical model, not a local path
    assert model_dir.is_dir()  # MODEL.mkdir(parents=True, exist_ok=True) ran
    assert fake.model.saved_to == [model_dir]  # model.save(MODEL) ran


def test_get_sapbert_raises_qc_runtime_missing_on_import_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """An ``ImportError`` from the backend is re-raised as ``QcRuntimeMissingError``."""
    model_dir: Path = tmp_path / "sapbert"
    model_dir.mkdir()
    fake: FakeSentenceTransformers = FakeSentenceTransformers(error=ImportError("no sentence_transformers"))
    monkeypatch.setattr(qc, "MODEL", model_dir)
    monkeypatch.setattr(qc, "sentence_transformers", fake)

    with pytest.raises(QcRuntimeMissingError):
        qc.get_sapbert()
