"""Validate real-world production configurations vendored under ``tests/fixtures/realworld``.

The positive corpus mirrors configs from the MultiomicsNext, DAKP, and TableConfigs
repositories (provenance in ``tests/fixtures/realworld/README.md``) with every ``local:``
payload path rewritten to ``payload.placeholder`` — validation never reads payloads, so the
configs validate offline. The negative corpus holds deliberate legacy configs that 12.x must
reject.

An env-gated sweep runs the same validation over the live corpora on this machine: set
``TABLASSERT_CONFIG_CORPUS`` to a JSON array of directory paths, e.g.
``TABLASSERT_CONFIG_CORPUS='["/path/to/mokg-v12", "/path/to/DAKP/tables"]'``.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pydantic
import pytest

from tablassert.errors import UnpairedEffectAnnotationWarning
from tablassert.ingests import from_yaml, to_sections
from tablassert.lib import Tcode
from tablassert.models import Graph

ROOT: Path = Path(__file__).resolve().parent.parent
REALWORLD: Path = ROOT / "tests" / "fixtures" / "realworld"
POSITIVE: Path = REALWORLD / "positive"
NEGATIVE: Path = REALWORLD / "negative"
STORE: Path = ROOT / ".tablassert" / "store" / "realworld.parquet"

ENV_CONFIG_CORPUS: str = "TABLASSERT_CONFIG_CORPUS"

# Known-legacy production configs the sweep EXPECTS to fail, mapped to a substring the
# failure must contain. MIN1.yaml declares `anatomical_context_qualifier` both on the
# template and on individual sections, and `to_sections` list-merges them into duplicates;
# its cleaned 12.x conversion (MultiomicsNext mokg-v12/MIN1.v12.yaml) removed the
# template-level qualifier, so this is the pre-cleanup original, not a validation bug.
# Entries that unexpectedly PASS also fail the sweep, keeping this list honest.
KNOWN_CORPUS_FAILURES: dict[str, str] = {"/home/skyeav/Code/ISB/TableConfigs/TABLE/MOKG/MIN1.yaml": "is declared more than once in `qualifiers`"}

# Legacy keys each negative fixture must be rejected over; the assertion checks the
# ValidationError text names them so a rejection for the WRONG reason still fails.
NEGATIVE_EXPECTED_KEYS: dict[str, list[str]] = {"DRUGIBD.yaml": ["syntax", "status", "contributors"], "MOKG.yaml": ["rig", "description"]}


def _positive_yaml_files() -> list[Path]:
    """Return every vendored positive configuration.

    Returns:
        Sorted list of positive YAML paths.
    """
    return sorted(POSITIVE.rglob("*.yaml"))


def _negative_yaml_files() -> list[Path]:
    """Return every vendored negative (known-legacy) configuration.

    Returns:
        Sorted list of negative YAML paths.
    """
    return sorted(NEGATIVE.glob("*.yaml"))


def _validate_config(path: Path) -> None:
    """Validate one configuration the same way ``tablassert validate`` does.

    Graph configurations (top-level ``tables:`` key) go through ``Graph``; table
    configurations are expanded via ``to_sections`` and each section is validated
    through ``Tcode``.

    Args:
        path: Configuration YAML to validate.
    """
    raw: Any = from_yaml(path)
    assert isinstance(raw, dict), f"{path.name} did not parse to a mapping"

    if "tables" in raw:
        # Graph configuration.
        Graph.model_validate(raw)
        return

    sections: list[dict[str, Any]] = to_sections(raw, path)  # pyright: ignore
    assert sections, f"{path.name} produced no sections"
    for section in sections:
        Tcode.model_validate({**section, "store": STORE})


def _validated_tcode(path: Path) -> Tcode:
    """Expand and validate the (single) section of a vendored table configuration.

    Args:
        path: Table configuration YAML.

    Returns:
        The validated ``Tcode`` for the first expanded section.
    """
    raw: Any = from_yaml(path)
    section: dict[str, Any] = to_sections(raw, path)[0]  # pyright: ignore
    return Tcode.model_validate({**section, "store": STORE})


def test_positive_corpus_nonempty() -> None:
    """The vendored positive corpus is present."""
    assert _positive_yaml_files(), f"no positive YAML files found under {POSITIVE}"


@pytest.mark.parametrize("path", _positive_yaml_files(), ids=lambda p: p.name)
def test_positive_corpus_validates(path: Path) -> None:
    """Each vendored real-world configuration validates against the live schema.

    Args:
        path: Configuration YAML to validate.
    """
    _validate_config(path)


@pytest.mark.parametrize("path", _negative_yaml_files(), ids=lambda p: p.name)
def test_negative_corpus_rejected(path: Path) -> None:
    """Each vendored legacy configuration is rejected, naming its retired keys.

    Args:
        path: Legacy configuration YAML expected to fail validation.
    """
    expected_keys: list[str] = NEGATIVE_EXPECTED_KEYS[path.name]
    with pytest.raises(pydantic.ValidationError) as excinfo:
        _validate_config(path)
    message: str = str(excinfo.value)
    for key in expected_keys:
        assert key in message, f"{path.name} rejection did not mention retired key {key!r}:\n{message}"


def test_dakp_approved_treats_semantics() -> None:
    """DAKP approved-treats keeps its ``split_by`` annotations and provenance override."""
    tcode: Tcode = _validated_tcode(POSITIVE / "dakp" / "approved_treats.yaml")
    split_by: dict[str, Any] = {str(a.annotation): a.split_by for a in tcode.annotations or []}
    assert split_by["has_evidence"] == "|"

    override: Any = tcode.provenance.override
    assert override is not None
    urls: dict[str, Any] = override.upstream_source_record_urls
    assert "infores:dailymed" in urls
    assert "infores:faers" in urls


def test_dakp_contraindications_nullable_qualifier() -> None:
    """DAKP contraindications carries a nullable ``disease_context_qualifier``."""
    tcode: Tcode = _validated_tcode(POSITIVE / "dakp" / "contraindications.yaml")
    qualifiers: dict[str, Any] = {str(q.qualifier): q for q in tcode.statement.qualifiers or []}
    assert "disease_context_qualifier" in qualifiers
    assert qualifiers["disease_context_qualifier"].nullable is True


def test_ayoglu1_v12_drops_unpaired_effect_size() -> None:
    """The unpaired ``effect_size`` annotation is dropped with a warning, section kept."""
    path: Path = POSITIVE / "mokg-v12" / "AYOGLU1.v12.yaml"
    raw: Any = from_yaml(path)
    section: dict[str, Any] = to_sections(raw, path)[0]  # pyright: ignore
    assert any(a["annotation"] == "effect_size" for a in section["annotations"])
    with pytest.warns(UnpairedEffectAnnotationWarning):
        tcode: Tcode = Tcode.model_validate({**section, "store": STORE})
    targets: list[str] = [str(a.annotation) for a in tcode.annotations or []]
    assert "effect_size" not in targets
    assert "p_value" in targets  # the rest of the section survives the drop


def test_refconfigs_anchors_resolve() -> None:
    """YAML anchor/alias provenance expands onto every section of the refconfig."""
    path: Path = POSITIVE / "refconfigs" / "AYOGLU1.yaml"
    raw: Any = from_yaml(path)
    sections: list[dict[str, Any]] = to_sections(raw, path)  # pyright: ignore
    assert sections, f"{path.name} produced no sections"
    for section in sections:
        tcode: Tcode = Tcode.model_validate({**section, "store": STORE})
        assert tcode.provenance.publication == "PMC4119355"


def test_multisection_v12_expands_all_sections() -> None:
    """The multi-section v12 fixture expands to its full section count."""
    path: Path = POSITIVE / "mokg-v12" / "CORREIA3.v12.yaml"
    sections: list[dict[str, Any]] = to_sections(from_yaml(path), path)  # pyright: ignore
    assert len(sections) == 6


def test_dakp_graph_rig_populated() -> None:
    """The DAKP graph configuration validates with a fully populated RIG."""
    graph: Graph = Graph.model_validate(from_yaml(POSITIVE / "dakp" / "graph.yaml"))
    assert graph.rig is not None
    assert graph.rig.source_info.infores_id == "infores:multiomics-drugapprovals"
    assert graph.rig is not None
    assert graph.rig.ingest_info is not None
    assert graph.rig is not None
    assert graph.rig.provenance_info.contributions


def _corpus_dirs(spec: str) -> list[Path]:
    """Parse ``TABLASSERT_CONFIG_CORPUS`` as a JSON array of directory paths.

    Args:
        spec: Raw environment variable value.

    Returns:
        The decoded directory paths.
    """
    try:
        entries: Any = json.loads(spec)
    except json.JSONDecodeError as exc:
        pytest.fail(f"{ENV_CONFIG_CORPUS} must be a JSON array of directory paths; got invalid JSON {spec!r}: {exc}")
    if not isinstance(entries, list):
        pytest.fail(f"{ENV_CONFIG_CORPUS} must be a JSON array of directory paths; got non-array JSON {spec!r}")
    for entry in entries:
        if not isinstance(entry, str) or not entry.strip():
            pytest.fail(f"{ENV_CONFIG_CORPUS} entries must be non-empty path strings; got {entry!r} in {spec!r}")
    return [Path(entry) for entry in entries]


def test_external_corpus_sweep() -> None:
    """REAL corpus sweep, gated on ``TABLASSERT_CONFIG_CORPUS``; skips with a reason when unset.

    ``TABLASSERT_CONFIG_CORPUS`` is a JSON array of directory paths, each recursively
    globbed for ``*.yaml`` and validated like the vendored positive corpus. Every failure
    is collected and reported in one assertion message, except the known-legacy entries in
    ``KNOWN_CORPUS_FAILURES`` (which must fail, and for the recorded reason). Documented
    directories on this machine: MultiomicsNext ``.tablassert/mokg-v12``, DAKP ``tables/``,
    and TableConfigs ``TABLE/{FLAKASSIST,MBKG,MOKG}`` (QI is excluded as known-legacy).
    """
    spec: str | None = os.environ.get(ENV_CONFIG_CORPUS)
    if not spec:
        reason: str = (
            f"set {ENV_CONFIG_CORPUS} to a JSON array of directory paths — "
            f'["<config-dir>", ...] — '
            "to sweep the live corpora (e.g. MultiomicsNext .tablassert/mokg-v12, DAKP tables/, "
            "TableConfigs TABLE/{FLAKASSIST,MBKG,MOKG})"
        )
        print(reason)
        pytest.skip(reason)

    failures: list[str] = []
    for directory in _corpus_dirs(spec):
        assert directory.is_dir(), f"{ENV_CONFIG_CORPUS} directory does not exist: {directory}"
        for path in sorted(directory.rglob("*.yaml")):
            known_reason: str | None = KNOWN_CORPUS_FAILURES.get(str(path))
            try:
                _validate_config(path)
            except Exception as exc:  # collect ALL failures; report them together below
                if known_reason is None or known_reason not in str(exc):
                    failures.append(f"{path}\n{exc}")
            else:
                if known_reason is not None:
                    failures.append(f"{path}\nexpected known-legacy failure ({known_reason!r}) but the config validated")
    assert not failures, f"{len(failures)} external config(s) failed validation:\n\n" + "\n\n".join(failures)
