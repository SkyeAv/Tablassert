"""Outcome assembly for distillation weighting: pure builders over a run's terminal state.

:func:`build_outcome` turns the data the supervisor has ALREADY computed at a run's status-decision
point — the ``build_and_audit`` report, the ``ConfigRecord``, the step-callback tallies, the gate
thresholds and the terminal config — into one schema-uniform outcome dict over EXACTLY
:data:`tablassert.distill.OUTCOME_KEYS`, with every fixed nested struct pinned to its sub-tuple.
Capture performs NO new build, audit, coverage or KGX read: the only environment access is
installed-distribution metadata for the ``versions`` struct (rewards are not comparable across
biolink-model releases, so an append-only corpus pooled over time must stay splittable by version).
:func:`provenance_ok` is the config-derived provenance-completeness signal recorded as metadata on
every outcome line.

This module is the DERIVED layer beside the fail-soft capture layer in ``distill.py`` and plays by
the opposite error contract: it FAILS LOUD on programmer error (a violated call contract raises
``TypeError`` immediately) but NEVER on data — a missing, partial or legacy report defaults each
figure to its declared neutral value instead of raising, because a crashed run's outcome (a
negative example) is exactly the row the corpus cannot afford to lose. It is pure stdlib + ``yaml``
(a core dependency) so it imports in the base environment: no smolagents, no datasets. The reward,
the weight, the selection policies and the records/outcomes join are layered on top of this module
elsewhere; they are deliberately NOT here.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as distribution_version
from typing import Any, Final

import yaml

from tablassert.distill import GATE_KEYS, OUTCOME_KEYS, RECORD_TYPE_OUTCOME, SCHEMA_VERSION, TOOL_CALL_KEYS, VERSION_KEYS

#: The Biolink ``KnowledgeLevelEnum``/``AgentTypeEnum`` sentinel meaning "no assertion made" (both
#: spellings tolerated: the enum VALUE is ``"not provided"``, but a hand-authored config may use the
#: member name). An explicit sentinel is the ONLY KL/AT failure: an absent key falls back to the
#: schema defaults (``statistical_association`` / ``data_analysis_pipeline``), which are usable.
_NOT_PROVIDED: Final[frozenset[str]] = frozenset({"not provided", "not_provided"})


def _require_str_or_none(name: str, value: object) -> str | None:
    """Return ``value`` unchanged; a non-str, non-None value is a call-contract violation (loud)."""
    if value is not None and not isinstance(value, str):
        raise TypeError(f"{name} must be a str or None, got {type(value).__name__}")
    return value


def _require_number(name: str, value: object) -> float:
    """Coerce a required numeric argument to float; anything else is a call-contract violation (loud)."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a number, got {type(value).__name__}")
    return float(value)


def _float_or_none(value: object) -> float | None:
    """Coerce a JSON number to float; anything else (including a bool) is unmeasured -> ``None``."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _int_or_none(value: object) -> int | None:
    """Coerce a JSON int; anything else (including a bool) is unmeasured -> ``None``."""
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _count(value: object) -> int:
    """An int tally defaulting to 0 — the convention ``reliability_metric`` already uses for these tallies."""
    counted: int | None = _int_or_none(value)
    return counted if counted is not None else 0


def _bool_or_none(value: object) -> bool | None:
    """Pass a bool through; anything else is unmeasured -> ``None`` (never guessed)."""
    return value if isinstance(value, bool) else None


def _len_of(value: object) -> int:
    """``len()`` of a list/tuple field; an absent or non-list collection counts as 0 items.

    A run that never produced the collection (a crashed build, a legacy report) produced zero of
    the items it counts; the run's own ``ok``/``run_status`` columns carry the failure, so a count
    column stays an honest, stably-typed int.
    """
    return len(value) if isinstance(value, (list, tuple)) else 0


def _distribution_versions() -> dict[str, object]:
    """The ``versions`` struct: the live ``tablassert`` version + the installed ``biolink-model`` one.

    ``biolink_model`` is ``None`` when the distribution is absent (REQ-OUT-9): rewards are not
    comparable across biolink-model releases (v4.4.4 relocated the ``supporting_study_*`` slots),
    so the corpus must stay splittable by the exact versions that produced each row. ``tablassert``
    falls back to ``None`` only when running from an uninstalled source tree (no metadata to read).
    """
    try:
        tablassert_version: str | None = distribution_version("tablassert")
    except PackageNotFoundError:  # an uninstalled source tree has no dist metadata
        tablassert_version = None
    try:
        biolink_version: str | None = distribution_version("biolink-model")
    except PackageNotFoundError:
        biolink_version = None
    return {"tablassert": tablassert_version, "biolink_model": biolink_version}


def _is_not_provided(value: object) -> bool:
    """True when a KL/AT value is explicitly the Biolink ``not provided`` sentinel."""
    return isinstance(value, str) and value.strip().lower() in _NOT_PROVIDED


def _kl_at_usable(provenance: Mapping[str, Any]) -> bool:
    """True unless knowledge_level/agent_type is explicitly ``not provided``; absent keys are usable.

    The override carries its OWN KL/AT pair which replaces the section-level pair whenever the
    override is present (``lib.py`` reads them exactly this way), so it is the carrier when given.
    An absent KL/AT takes the schema defaults (``statistical_association`` /
    ``data_analysis_pipeline``), which ARE usable — only an explicit sentinel fails.
    """
    override: object = provenance.get("override")
    carrier: Mapping[str, Any] = override if isinstance(override, Mapping) else provenance
    return not _is_not_provided(carrier.get("knowledge_level")) and not _is_not_provided(carrier.get("agent_type"))


def _declares_identity(provenance: Mapping[str, Any]) -> bool:
    """True when the block names a publication or carries a COMPLETE manual override.

    A bare ``override: {}`` satisfies the schema's publication requirement while identifying
    NOTHING, so completeness means at least one identity field (``publications``, ``sources`` or
    ``upstream_resource_ids``) is a non-empty list.
    """
    publication: object = provenance.get("publication")
    if isinstance(publication, str) and publication.strip():
        return True
    override: object = provenance.get("override")
    if not isinstance(override, Mapping):
        return False
    for field_name in ("publications", "sources", "upstream_resource_ids"):
        entries: object = override.get(field_name)
        if isinstance(entries, (list, tuple)) and len(entries) > 0:
            return True
    return False


def _merged_sections(config: Mapping[str, Any]) -> list[Mapping[str, Any]] | None:
    """Expand a parsed config into per-section mappings with the template merged UNDER each section.

    Mirrors ``to_sections``' merge semantics (the template provides defaults, the section wins, and
    the ``provenance`` sub-dicts merge key-wise) closely enough for a metadata signal, WITHOUT
    importing the pydantic-backed ingest layer into this pure module. A bare merged-section dict is
    one section. ``None`` marks a shape that cannot be read as a config at all (a non-list
    ``sections`` key), which the caller reports as unparseable.
    """
    template: object = config.get("template")
    raw_sections: object = config.get("sections")
    if template is None and raw_sections is None:
        return [config]  # a bare merged section dict
    base: Mapping[str, Any] = template if isinstance(template, Mapping) else {}
    entries: list[object]
    if raw_sections is None:
        entries = [{}]  # to_sections' default: one section inheriting the whole template
    elif isinstance(raw_sections, list):
        entries = raw_sections
    else:
        return None
    merged: list[Mapping[str, Any]] = []
    for entry in entries:
        section: Mapping[str, Any] = entry if isinstance(entry, Mapping) else {}
        combined: dict[str, Any] = {**base, **section}
        base_provenance: object = base.get("provenance")
        section_provenance: object = section.get("provenance")
        if isinstance(base_provenance, Mapping) or isinstance(section_provenance, Mapping):
            provenance: dict[str, Any] = {}
            if isinstance(base_provenance, Mapping):
                provenance.update(base_provenance)
            if isinstance(section_provenance, Mapping):
                provenance.update(section_provenance)
            combined["provenance"] = provenance
        merged.append(combined)
    return merged


def provenance_ok(config_yaml: str) -> bool | None:
    """Report whether EVERY section of a config declares usable provenance; ``None`` when unparseable.

    "Usable" is a provenance IDENTITY (a non-empty ``provenance.publication``, or a complete
    ``provenance.override``) AND a ``knowledge_level``/``agent_type`` pair that is not the Biolink
    ``not provided`` sentinel (an absent KL/AT takes the usable schema defaults). ``None`` means the
    string cannot be read as a config at all — unknown, never a guess.

    Recorded as outcome METADATA only: the section schema requires a provenance block, so for a
    schema-valid config this is structurally ``True`` — which is precisely why a reward must never
    read it (the same structural-constant trap as ``qc_pass_rate``).
    """
    if not isinstance(config_yaml, str):
        raise TypeError(f"config_yaml must be a str, got {type(config_yaml).__name__}")
    try:
        data: Any = yaml.safe_load(config_yaml)
    except yaml.YAMLError:
        return None
    if not isinstance(data, Mapping):
        return None  # a scalar/list YAML document cannot be a section config
    sections: list[Mapping[str, Any]] | None = _merged_sections(data)
    if sections is None:
        return None
    if not sections:
        return False  # no section exists to carry an identity
    for section in sections:
        provenance: object = section.get("provenance")
        if not isinstance(provenance, Mapping) or not _declares_identity(provenance) or not _kl_at_usable(provenance):
            return False
    return True


def build_outcome(
    *,
    run_id: str | None,
    pmc_id: str | None,
    model_id: str | None,
    run_status: str,
    report: Mapping[str, Any] | None,
    record: Mapping[str, Any] | None,
    metrics: Mapping[str, Any] | None,
    config_yaml: str | None,
    map_threshold: float,
    biolink_threshold: float,
    judge_threshold: float | None,
    judge_verdict: Mapping[str, Any] | None = None,
    timestamp: str | None = None,
) -> dict[str, Any]:
    """Assemble one run's outcome over EXACTLY :data:`OUTCOME_KEYS` — pure: no globals, no I/O.

    Every figure is sourced from data already computed at the supervisor's status-decision point:
    ``report`` (the ``build_and_audit`` return), ``record`` (the ``ConfigRecord`` as a mapping),
    ``metrics`` (the step-callback tallies), the three gate thresholds and the terminal
    ``config_yaml``. List-valued audit fields land as COUNTS (the lists themselves are too bulky for
    a per-run column); ``error_codes`` is the one list kept verbatim — short, enum-valued, and the
    hard-gate evidence. ``judge_score``/``judge_dimensions`` are LLM-generated METADATA only (null
    when no judge ran) and ``qc_pass_rate`` is convenience-only (``build_and_audit`` hard-wires it
    to ``1.0 if qc else None``); neither may ever enter a reward.

    Data never raises: a missing, partial or legacy ``report``/``record``/``metrics`` defaults each
    figure to its declared neutral value — ``null`` for measurement scalars (floats, bools, strings
    and the declared-nullable ``config_chars``), ``0`` for counts and lengths, ``[]`` for
    ``error_codes``, and a full all-null struct for the fixed ``tool_calls`` members when no tallies
    exist. Programmer error raises ``TypeError`` immediately: a non-string ``run_status``, a
    non-numeric threshold, or a non-str/None identity argument is a caller bug, and silently
    recording it would poison an append-only corpus that can never be rewritten.

    Returns:
        A dict whose key set AND order is exactly :data:`OUTCOME_KEYS`, whose ``tool_calls``/``gate``
        /``versions`` structs carry exactly :data:`TOOL_CALL_KEYS`/:data:`GATE_KEYS`/:data:`VERSION_KEYS`.
    """
    if not isinstance(run_status, str):
        raise TypeError(f"run_status must be a str, got {type(run_status).__name__}")
    report_map: Mapping[str, Any] = report if isinstance(report, Mapping) else {}
    record_map: Mapping[str, Any] = record if isinstance(record, Mapping) else {}
    metrics_map: Mapping[str, Any] = metrics if isinstance(metrics, Mapping) else {}
    verdict_map: Mapping[str, Any] = judge_verdict if isinstance(judge_verdict, Mapping) else {}
    config_text: str | None = _require_str_or_none("config_yaml", config_yaml)

    raw_error_codes: object = report_map.get("error_codes")
    error_codes: list[Any] = list(raw_error_codes) if isinstance(raw_error_codes, (list, tuple)) else []
    raw_dimensions: object = verdict_map.get("scores")
    tool_call_sources: dict[str, object] = {
        "total": metrics_map.get("total_tool_calls"),
        "failed": metrics_map.get("failed_tool_calls"),
        "wrong": metrics_map.get("wrong_tool_calls"),
        "redundant": metrics_map.get("redundant_tool_calls"),
    }
    tool_calls: dict[str, object] = {key: _count(tool_call_sources[key]) for key in TOOL_CALL_KEYS}
    gate_values: dict[str, object] = {
        "map_threshold": _require_number("map_threshold", map_threshold),
        "biolink_threshold": _require_number("biolink_threshold", biolink_threshold),
        "judge_threshold": _require_number("judge_threshold", judge_threshold) if judge_threshold is not None else None,
    }
    gate: dict[str, object] = {key: gate_values[key] for key in GATE_KEYS}
    distribution_versions: dict[str, object] = _distribution_versions()
    versions: dict[str, object] = {key: distribution_versions[key] for key in VERSION_KEYS}

    # Seeded in OUTCOME_KEYS order with explicit null defaults; the update fills every slot IN PLACE
    # (dict update never reorders an existing key). Comprehension, not dict.fromkeys: fromkeys infers
    # a Literal key type that pyright strict rejects as invariant-incompatible with dict[str, Any].
    outcome: dict[str, Any] = {key: None for key in OUTCOME_KEYS}  # noqa: C420
    outcome.update(
        {
            "record_type": RECORD_TYPE_OUTCOME,
            "schema_version": SCHEMA_VERSION,
            "run_id": _require_str_or_none("run_id", run_id),
            "timestamp": _require_str_or_none("timestamp", timestamp) or datetime.now(UTC).isoformat(),
            "pmc_id": _require_str_or_none("pmc_id", pmc_id),
            "model_id": _require_str_or_none("model_id", model_id),
            "run_status": run_status,
            "ok": _bool_or_none(report_map.get("ok")),
            "measured": _bool_or_none(report_map.get("measured")),
            "head": _bool_or_none(report_map.get("head")),
            "coverage_pct": _float_or_none(report_map.get("coverage_pct")),
            "best_coverage": _float_or_none(record_map.get("best_coverage")),
            "coverage_history_len": _len_of(record_map.get("coverage_history")),
            "section_coverages_len": _len_of(record_map.get("section_coverages")),
            "biolink_valid_pct": _float_or_none(report_map.get("biolink_valid_pct")),
            "biolink_valid_pct_strict": _float_or_none(report_map.get("biolink_valid_pct_strict")),
            "demoted_edge_pct": _float_or_none(report_map.get("demoted_edge_pct")),
            "node_count": _count(report_map.get("node_count")),
            "edge_count": _count(report_map.get("edge_count")),
            "unresolved_count": _len_of(report_map.get("unresolved")),
            "predicate_advice_count": _len_of(report_map.get("predicate_advice")),
            "multivalued_suspect_count": _len_of(report_map.get("multivalued_suspects")),
            "error_codes": error_codes,
            "attempts": _count(record_map.get("attempts")),
            # ConfigRecord.config_chars is int | None BY DECLARATION (null = no best config was
            # persisted), so its neutral default is null, not 0.
            "config_chars": _int_or_none(record_map.get("config_chars")),
            "config_yaml_sha256": hashlib.sha256(config_text.encode("utf-8")).hexdigest() if config_text is not None else None,
            "provenance_ok": provenance_ok(config_text) if config_text is not None else None,
            "qc_pass_rate": _float_or_none(report_map.get("qc_pass_rate")),
            "tool_calls": tool_calls,
            "tokens_total": _count(metrics_map.get("total_tokens")),
            "steps": _count(metrics_map.get("steps")),
            "judge_score": _float_or_none(verdict_map.get("normalized")),
            "judge_dimensions": dict(raw_dimensions) if isinstance(raw_dimensions, Mapping) else None,
            "gate": gate,
            "versions": versions,
        }
    )
    return outcome
