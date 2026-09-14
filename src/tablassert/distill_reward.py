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
(a core dependency) so it imports in the base environment: no smolagents, no datasets.

The module also carries the DETERMINISTIC REWARD layer — :class:`RewardConfig`,
:func:`load_reward_config`, :func:`reward` and :func:`median_edge_ref` — the weight that decides
which recorded examples train the LoRA. The reward is pure (no I/O, no randomness, no clock: same
outcome in, same weight out, so two exports of one append-only corpus stay comparable), reads ONLY
deterministic outcome fields, and applies its hard gates MULTIPLICATIVELY, never additively. It
deliberately never reads ``judge_score``/``judge_dimensions`` (a second hackable LLM proxy),
``provenance_ok`` or ``qc_pass_rate`` (both structurally constant for any schema-valid config — the
same degeneracy documented for ``quality_score``), or any F1 term (no gold KGX exists in
production), and it never imports or calls ``quality_score``, the GEPA path's composite.

On top of the reward sits the DERIVED-ROW layer: :func:`read_ndjson`/:func:`write_ndjson`/
:func:`iter_record_files`/:func:`is_outcome_file` (the corpus I/O), :func:`join_records_outcomes`
(the ``run_id`` join), :func:`flatten_outcome` (nested outcome -> flat stable-typed ``outcome_*``
columns), :func:`select` (the three selection policies) and :func:`union_keys`/
:func:`normalize_rows`/:func:`detect_type_conflicts` (the schema normalization that keeps
``datasets`` from inferring a wrong schema off the first block). It is the layer that decides which
recorded examples train the LoRA, so it fails loud where the capture layer stays fail-soft: a
malformed NDJSON line, a wrong-typed policy knob, or a row that reaches :func:`select` unweighed is
an error, never a silent default. It is pure stdlib and ``datasets``-free, so every decision here is
testable in the base environment.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import statistics
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as distribution_version
from pathlib import Path
from typing import Any, Final, NamedTuple

import yaml

from tablassert.distill import GATE_KEYS, OUTCOME_KEYS, RECORD_KEYS, RECORD_TYPE_OUTCOME, SCHEMA_VERSION, TOOL_CALL_KEYS, VERSION_KEYS
from tablassert.errors import RewardConfigError

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


# ───────────────────────────── deterministic reward ─────────────────────────────
# The weight that decides which recorded examples train the LoRA. Everything below is pure: no
# I/O, no environment, no clock, no randomness — determinism is a HARD requirement because the
# corpus is append-only and long-lived, so a weight that drifted between runs would make two
# exports of the same corpus incomparable.

#: Tolerance for the five-coefficient sum check in :func:`load_reward_config`. Coefficients are
#: documented to sum to exactly 1.00; 1e-9 absorbs binary-float representation noise (e.g.
#: 0.40 + 0.28 + 0.17 + 0.07 + 0.08) without ever admitting a policy that is genuinely off —
#: a mis-summed policy silently rescales every weight the corpus produces.
COEFFICIENT_SUM_TOLERANCE: Final[float] = 1e-9

#: The eleven knobs a reward-config file may set — exactly :class:`RewardConfig`'s fields. The
#: tuple (not the dataclass) is the source of truth for the unknown-key check so the error message
#: can name the valid set without introspection, and so an unknown key is rejected rather than
#: silently ignored into the default policy (a typo'd coefficient must never train a LoRA).
REWARD_CONFIG_FIELDS: Final[tuple[str, ...]] = (
    "w_coverage",
    "w_biolink",
    "w_specificity",
    "w_cleanliness",
    "w_breadth",
    "demoted_gate",
    "demoted_penalty",
    "redundant_gate",
    "redundant_penalty",
    "unmeasured_weight",
    "edge_ref",
)

#: The five additive-term coefficients, whose resolved values must sum to 1.0 within
#: :data:`COEFFICIENT_SUM_TOLERANCE` — the raw score is a convex combination, so the weight stays
#: interpretable (a fraction of a perfect build) across retunes.
_COEFFICIENT_FIELDS: Final[tuple[str, ...]] = ("w_coverage", "w_biolink", "w_specificity", "w_cleanliness", "w_breadth")

#: The two multiplicative-penalty knobs, bounded to (0, 1]: 0 would zero the row outright (that is
#: what the hard gates are for) and > 1 would REWARD the failure mode the penalty exists to punish.
_PENALTY_FIELDS: Final[tuple[str, ...]] = ("demoted_penalty", "redundant_penalty")

#: The knobs that are fractions of something, bounded to [0, 1]: the two farming gates (compared
#: against a demoted-edge fraction and a redundant-call ratio) and the unmeasured fallback weight.
_UNIT_INTERVAL_FIELDS: Final[tuple[str, ...]] = ("demoted_gate", "redundant_gate", "unmeasured_weight")


@dataclass(frozen=True, slots=True)
class RewardConfig:
    """The complete, immutable reward policy; ``RewardConfig()`` IS the documented default policy.

    Frozen + slots because a policy that could be mutated between two rows of one export would
    silently break cross-row comparability — the entire point of a deterministic reward. The five
    ``w_*`` coefficients sum to exactly 1.00 so the raw score is a convex combination; the gates
    and penalties are the multiplicative farming guards, and ``edge_ref`` is the optional
    file-level override for the breadth normalizer (``None`` = derive the per-corpus median).

    Why these defaults: they mirror the SHAPE of the existing ``quality_score`` while replacing its
    two degenerate terms — ``qc_pass_rate`` (structurally constant) and ``mean_f1`` (needs a gold
    KGX that does not exist in production) — with ``specificity`` (the anti generic-predicate
    signal), ``breadth`` (non-degeneracy) and ``cleanliness`` (tool-call correctness, the varying
    signal that took over brief §6's structurally-constant ``prov`` slot at the same 0.07).
    """

    w_coverage: float = 0.40  # completeness / entity-resolution success — the primary signal
    w_biolink: float = 0.28  # semantic/ontological validity of the built edges
    w_specificity: float = 0.17  # 1 - demoted_edge_pct: predicate specificity vs generic fallback
    w_cleanliness: float = 0.07  # 1 - (failed + wrong) / total tool calls: correctness, not efficiency
    w_breadth: float = 0.08  # log-scaled edge_count against the per-corpus reference
    demoted_gate: float = 0.50  # demoted_edge_pct above this trips the generic-predicate penalty
    demoted_penalty: float = 0.5  # multiplies the raw score when the demoted gate trips
    redundant_gate: float = 0.30  # redundant/total tool-call ratio above this trips the spam penalty
    redundant_penalty: float = 0.7  # multiplies the raw score when the redundant gate trips
    unmeasured_weight: float = 0.0  # floor for BUILT_UNMEASURED / unmeasured rows — kept, not selectable
    edge_ref: float | None = None  # breadth-reference override; None = the corpus median


def _clamp01(value: float) -> float:
    """Clamp to [0, 1] — the weight's documented range, guaranteed even for a hand-built config."""
    return min(max(value, 0.0), 1.0)


def _coerce_knob(path: Path, key: str, value: object) -> float | None:
    """Validate one config-file value against its knob's declared range; wrong type/shape is LOUD.

    No silent defaulting and no coercion of a wrong type: a typo'd or mis-scaled knob must stop the
    weigh, because the resulting policy decides which examples train the LoRA. ``edge_ref: null``
    is the one accepted non-number — it spells the documented default ("derive the corpus median").
    """
    if key == "edge_ref" and value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RewardConfigError(f"Reward config invalid: {path} — key {key!r} must be a number, got {value!r} ({type(value).__name__})")
    number: float = float(value)
    if not math.isfinite(number):
        raise RewardConfigError(f"Reward config invalid: {path} — key {key!r} must be finite, got {number!r}")
    if key in _PENALTY_FIELDS:
        if number <= 0.0 or number > 1.0:
            raise RewardConfigError(f"Reward config invalid: {path} — penalty {key!r} must be in (0, 1], got {number!r}")
    elif key == "edge_ref":
        if number <= 0.0:
            raise RewardConfigError(f"Reward config invalid: {path} — key 'edge_ref' must be positive (it is the log1p denominator), got {number!r}")
    elif number < 0.0:
        raise RewardConfigError(f"Reward config invalid: {path} — key {key!r} must be non-negative, got {number!r}")
    if key in _UNIT_INTERVAL_FIELDS and number > 1.0:
        raise RewardConfigError(f"Reward config invalid: {path} — key {key!r} is a fraction and must be in [0, 1], got {number!r}")
    return number


def load_reward_config(path: Path) -> RewardConfig:
    """Load a YAML (.yaml/.yml) or JSON (.json) reward policy, failing LOUD on anything invalid.

    A file may set any subset of :data:`REWARD_CONFIG_FIELDS`; omitted knobs take the documented
    :class:`RewardConfig` defaults, and the RESOLVED five coefficients must still sum to 1.0 within
    :data:`COEFFICIENT_SUM_TOLERANCE` (so a file that moves one weight must move another to
    compensate — an accidentally rescaled policy is rejected, not shipped).

    Raises:
        TypeError: ``path`` is not a ``Path`` (a call-contract violation, not bad data).
        RewardConfigError: the file is missing/unreadable, has an unsupported suffix, is not a
            mapping, names an unknown key (the message lists the valid set), carries a non-numeric
            or non-finite value, a negative weight/gate/penalty, a penalty outside (0, 1], a
            coefficient sum off 1.0 (the message reports the actual sum), or a non-positive
            ``edge_ref``.
    """
    if not isinstance(path, Path):
        raise TypeError(f"path must be a Path, got {type(path).__name__}")
    try:
        text: str = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RewardConfigError(f"Reward config unreadable: {path} — {exc}") from exc
    document: Any
    suffix: str = path.suffix.lower()
    if suffix in (".yaml", ".yml"):
        try:
            document = yaml.safe_load(text)
        except yaml.YAMLError as exc:
            raise RewardConfigError(f"Reward config invalid: {path} — not parseable YAML: {exc}") from exc
    elif suffix == ".json":
        try:
            document = json.loads(text)
        except json.JSONDecodeError as exc:
            raise RewardConfigError(f"Reward config invalid: {path} — not parseable JSON: {exc}") from exc
    else:
        raise RewardConfigError(f"Reward config invalid: {path} — unsupported suffix {path.suffix!r}; expected one of .yaml, .yml, .json")
    if not isinstance(document, Mapping):
        raise RewardConfigError(
            f"Reward config invalid: {path} — expected a mapping of knob -> value, got {type(document).__name__}; valid keys: {', '.join(REWARD_CONFIG_FIELDS)}"
        )
    provided: dict[str, float | None] = {}
    for raw_key, raw_value in document.items():
        key: object = raw_key
        if not isinstance(key, str) or key not in REWARD_CONFIG_FIELDS:
            raise RewardConfigError(f"Reward config invalid: {path} — unknown key {key!r}; valid keys: {', '.join(REWARD_CONFIG_FIELDS)}")
        provided[key] = _coerce_knob(path, key, raw_value)

    defaults: RewardConfig = RewardConfig()

    def knob(field: str) -> float:
        """Resolve a non-nullable knob: the file's value when given, else the documented default."""
        value: float | None = provided.get(field)
        if value is not None:
            return value
        default: float = getattr(defaults, field)  # one canonical default policy, not eleven repeated literals
        return default

    coefficient_sum: float = sum(knob(field) for field in _COEFFICIENT_FIELDS)
    if abs(coefficient_sum - 1.0) > COEFFICIENT_SUM_TOLERANCE:
        raise RewardConfigError(
            f"Reward config invalid: {path} — the five coefficients must sum to 1.0 within {COEFFICIENT_SUM_TOLERANCE}, got {coefficient_sum!r}"
        )
    return RewardConfig(
        w_coverage=knob("w_coverage"),
        w_biolink=knob("w_biolink"),
        w_specificity=knob("w_specificity"),
        w_cleanliness=knob("w_cleanliness"),
        w_breadth=knob("w_breadth"),
        demoted_gate=knob("demoted_gate"),
        demoted_penalty=knob("demoted_penalty"),
        redundant_gate=knob("redundant_gate"),
        redundant_penalty=knob("redundant_penalty"),
        unmeasured_weight=knob("unmeasured_weight"),
        edge_ref=provided.get("edge_ref"),
    )


def _required(outcome: Mapping[str, Any], key: str) -> object:
    """Fetch a required outcome key; a MISSING key is a malformed outcome and fails loud.

    Weighting is the fail-loud counterpart of the fail-soft recorder: a row missing a field the
    reward reads is a schema violation (a pre-v2 or hand-built corpus), and silently defaulting it
    would invent a weight for an example that was never measured.
    """
    if key not in outcome:
        raise ValueError(f"malformed outcome: missing required key {key!r}")
    return outcome[key]


def _nullable_float(outcome: Mapping[str, Any], key: str) -> float | None:
    """A measurement scalar: a real number or explicit ``null`` (unmeasured); anything else is loud."""
    value: object = _required(outcome, key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"malformed outcome: {key!r} must be a number or null, got {type(value).__name__}")
    return float(value)


def _nullable_bool(outcome: Mapping[str, Any], key: str) -> bool | None:
    """A flag: a bool or explicit ``null`` (unmeasured); a truthy string is never accepted as one."""
    value: object = _required(outcome, key)
    if value is not None and not isinstance(value, bool):
        raise TypeError(f"malformed outcome: {key!r} must be a bool or null, got {type(value).__name__}")
    return value


def _tool_tally(tool_calls: Mapping[str, Any], key: str) -> float:
    """One tool-call tally: a required, real (non-bool) number — the struct is pinned, so null is loud."""
    if key not in tool_calls:
        raise ValueError(f"malformed outcome: 'tool_calls' is missing required tally {key!r}")
    value: object = tool_calls[key]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"malformed outcome: 'tool_calls' tally {key!r} must be a number, got {type(value).__name__}")
    return float(value)


def _resolve_edge_ref(config: RewardConfig, edge_ref: float | None) -> float | None:
    """Resolve the breadth reference: the explicit argument wins, then the config's file override.

    The CLI derives the per-corpus median and passes it explicitly; ``RewardConfig.edge_ref`` is the
    file-level escape hatch for pinning a fixed reference (e.g. comparing against a frozen corpus).
    A non-positive reference would make the log1p denominator zero or negative, so it fails loud
    rather than silently zeroing the breadth term for every row.
    """
    reference: float | None
    if edge_ref is not None:
        if isinstance(edge_ref, bool) or not isinstance(edge_ref, (int, float)):
            raise TypeError(f"edge_ref must be a number or None, got {type(edge_ref).__name__}")
        reference = float(edge_ref)
    else:
        reference = config.edge_ref
    if reference is not None and reference <= 0.0:
        raise ValueError(f"edge_ref must be positive (it is the log1p denominator), got {reference!r}")
    return reference


def reward(outcome: Mapping[str, Any], config: RewardConfig, *, edge_ref: float | None) -> float:
    """The deterministic weight of one captured outcome, clamped to [0, 1].

    Five additive terms (defaults in parentheses, summing to 1.00) — ``coverage_pct`` (0.40),
    ``biolink_valid_pct`` (0.28), ``specificity = 1 - demoted_edge_pct`` (0.17),
    ``cleanliness = 1 - (failed + wrong) / total_tool_calls`` (0.07), and
    ``breadth = clamp(log1p(edge_count) / log1p(edge_ref), 0, 1)`` (0.08) — then the hard gates,
    MULTIPLICATIVELY, never additively: ``ok is not True``, ``head is True``, or a
    ``SKIPPED``/``FAILED`` status zero the row outright; ``BUILT_UNMEASURED`` or ``measured is not
    True`` floors it at ``config.unmeasured_weight`` (the record is KEPT, just not selectable);
    ``demoted_edge_pct > demoted_gate`` multiplies by ``demoted_penalty`` (generic-predicate
    farming — a forbidden predicate never raises, it silently demotes the edge, so coverage can be
    farmed); ``redundant / total > redundant_gate`` multiplies by ``redundant_penalty``.

    Unmeasurable is 0.0, never a free pass: a null ``biolink_valid_pct`` contributes 0.0 and a null
    ``demoted_edge_pct`` makes specificity 0.0. The reward reads ONLY deterministic outcome fields —
    never ``judge_score``/``judge_dimensions`` (a second hackable LLM proxy), ``provenance_ok`` or
    ``qc_pass_rate`` (structurally constant for schema-valid configs), or any F1 term (no gold KGX
    in production) — and uses no logprobs/perplexity, which the smolagents path never captures.

    Raises:
        ValueError: a required key is missing, or ``edge_ref`` resolves non-positive.
        TypeError: a value is present but of the wrong type (the outcome is malformed; silently
            defaulting it would invent a weight for an example that was never measured).
    """
    if not isinstance(outcome, Mapping):
        raise TypeError(f"outcome must be a Mapping, got {type(outcome).__name__}")
    ok: bool | None = _nullable_bool(outcome, "ok")
    head: bool | None = _nullable_bool(outcome, "head")
    measured: bool | None = _nullable_bool(outcome, "measured")
    run_status: object = _required(outcome, "run_status")
    if not isinstance(run_status, str):
        raise TypeError(f"malformed outcome: 'run_status' must be a str, got {type(run_status).__name__}")
    coverage_pct: float | None = _nullable_float(outcome, "coverage_pct")
    biolink_valid_pct: float | None = _nullable_float(outcome, "biolink_valid_pct")
    demoted_edge_pct: float | None = _nullable_float(outcome, "demoted_edge_pct")
    edge_count: float | None = _nullable_float(outcome, "edge_count")
    tool_calls_value: object = _required(outcome, "tool_calls")
    if not isinstance(tool_calls_value, Mapping):
        raise TypeError(f"malformed outcome: 'tool_calls' must be a mapping, got {type(tool_calls_value).__name__}")
    total: float = _tool_tally(tool_calls_value, "total")
    failed: float = _tool_tally(tool_calls_value, "failed")
    wrong: float = _tool_tally(tool_calls_value, "wrong")
    redundant: float = _tool_tally(tool_calls_value, "redundant")
    reference: float | None = _resolve_edge_ref(config, edge_ref)

    # Hard gates first — multiplicative, so a gated row is 0.0 no matter how good its figures are.
    if ok is not True:
        return 0.0
    if head is True:
        return 0.0
    if run_status in ("SKIPPED", "FAILED"):  # FAILED is defensive: no live status emits it, but a hard gate that silently passed one would be worse
        return 0.0
    if run_status == "BUILT_UNMEASURED" or measured is not True:
        return _clamp01(config.unmeasured_weight)  # coverage was never certified: kept, not selectable
    if edge_count is not None and edge_count < 0.0:
        raise ValueError(f"malformed outcome: 'edge_count' must be non-negative, got {edge_count!r}")

    coverage: float = coverage_pct if coverage_pct is not None else 0.0
    biolink: float = biolink_valid_pct if biolink_valid_pct is not None else 0.0
    specificity: float = 1.0 - demoted_edge_pct if demoted_edge_pct is not None else 0.0
    cleanliness: float = 1.0 - (failed + wrong) / total if total > 0.0 else 0.0
    breadth: float = 0.0
    if edge_count is not None and reference is not None:
        breadth = _clamp01(math.log1p(edge_count) / math.log1p(reference))

    r_raw: float = (
        config.w_coverage * coverage
        + config.w_biolink * biolink
        + config.w_specificity * specificity
        + config.w_cleanliness * cleanliness
        + config.w_breadth * breadth
    )
    r: float = r_raw
    if demoted_edge_pct is not None and demoted_edge_pct > config.demoted_gate:
        r *= config.demoted_penalty
    if total > 0.0 and redundant / total > config.redundant_gate:
        r *= config.redundant_penalty
    return _clamp01(r)


def median_edge_ref(outcomes: Iterable[Mapping[str, Any]]) -> float | None:
    """The per-corpus breadth reference ``E_ref``: the median edge_count over comparable builds.

    Only COMPARABLE builds count: ``head is not True`` (a head build samples ~5 rows/section, so
    its edge_count is structurally smaller), ``ok is True``, and a non-null ``edge_count > 0``.
    ``None`` when no comparable outcome exists — the caller then warns that breadth contributes
    0.0 for every row rather than inventing a reference. Pure filter semantics: rows that fail the
    comparable-build criteria are excluded, never fatal.
    """
    edges: list[float] = []
    for outcome in outcomes:
        if not isinstance(outcome, Mapping):
            continue  # a non-row in the iterable is not a comparable build; exclusion, never a crash
        if outcome.get("head") is True or outcome.get("ok") is not True:
            continue
        edge_count: object = outcome.get("edge_count")
        if isinstance(edge_count, bool) or not isinstance(edge_count, (int, float)):
            continue
        if edge_count > 0:
            edges.append(float(edge_count))
    if not edges:
        return None
    return float(statistics.median(edges))


# ───────────────────── derived rows: corpus I/O, join, flatten, select ─────────────────────
# The layer that turns the two append-only NDJSON corpora (records + outcomes) into ONE
# training-ready row set. Everything below is pure stdlib, order-deterministic, and fails loud:
# this is where the corpus decides which examples train the LoRA, so a malformed line, a
# wrong-typed knob or an unweighed row is an error rather than a default.

#: The three documented selection policies (REQ-RW-15). Only the annotation VALUES differ between
#: them — the emitted key set never does — so a training pipeline written against one policy's
#: output keeps working when the operator retunes to another.
POLICIES: Final[tuple[str, ...]] = ("threshold", "best-of-n", "replication")

#: Policy A's default hard threshold: RAFT/RFT rejection sampling keeps ``weight >= threshold``,
#: the only selection TRL consumes with zero trainer code (no per-example weight column exists).
DEFAULT_THRESHOLD: Final[float] = 0.75

#: Policy B's default per-prompt keep count. Top-2 rather than top-1 because RFT gains scale with
#: the number of DISTINCT reasoning paths, so a naive argmax throws away what it is meant to buy.
DEFAULT_TOP_N: Final[int] = 2

#: Policy C's default replication slope: the best row is emitted with ``1 + k`` replicas.
DEFAULT_REPLICATION_K: Final[int] = 2

#: Policy C's slope ceiling. ``k > 3`` would replicate the top example more than fourfold, and
#: under ``packing=True`` the effective weight becomes token-proportional — an unbounded slope
#: would let one trajectory dominate the LoRA without the manifest's diversity counters showing it.
MAX_REPLICATION_K: Final[int] = 3

#: Prefix of every derived outcome column. The prefix is what keeps an outcome figure
#: (``outcome_edge_count``) distinguishable from the record column beside it (``n_messages``).
OUTCOME_COLUMN_PREFIX: Final[str] = "outcome_"

#: The five annotation keys :func:`select` stamps on every row, in emitted order. ``replicas`` is a
#: COUNT on one row per example — rows are never physically duplicated, because duplication would
#: inflate the file, destroy per-example identity under ``packing=True``, and make the trainer's
#: own replication step double-count.
SELECTION_KEYS: Final[tuple[str, ...]] = ("weight", "selected", "replicas", "policy", "threshold")

#: The keys of the stats dict :func:`join_records_outcomes` returns, in emitted order.
JOIN_STAT_KEYS: Final[tuple[str, ...]] = ("records", "outcomes", "matched", "unmatched", "duplicate_run_ids")

#: The declared column kind of every non-struct, non-JSON :data:`~tablassert.distill.OUTCOME_KEYS`
#: entry. Declaring the kind (rather than passing the captured value's type through) is what makes
#: the derived column's Python type stable for the corpus's lifetime — ``datasets`` would otherwise
#: silently JSON-encode a column whose type varies (``JsonConfig.on_mixed_types = "use_json"``).
_SCALAR_KINDS: Final[dict[str, str]] = {
    "record_type": "str",
    "schema_version": "int",
    "run_id": "str",
    "timestamp": "str",
    "pmc_id": "str",
    "model_id": "str",
    "run_status": "str",
    "ok": "bool",
    "measured": "bool",
    "head": "bool",
    "coverage_pct": "float",
    "best_coverage": "float",
    "coverage_history_len": "int",
    "section_coverages_len": "int",
    "biolink_valid_pct": "float",
    "biolink_valid_pct_strict": "float",
    "demoted_edge_pct": "float",
    "node_count": "int",
    "edge_count": "int",
    "unresolved_count": "int",
    "predicate_advice_count": "int",
    "multivalued_suspect_count": "int",
    "attempts": "int",
    "config_chars": "int",
    "config_yaml_sha256": "str",
    "provenance_ok": "bool",
    "qc_pass_rate": "float",
    "tokens_total": "int",
    "steps": "int",
    "judge_score": "float",
}

#: The outcome entries whose SHAPE is not pinned by this module — ``error_codes`` (the report's own
#: list) and ``judge_dimensions`` (the judge's own dimension names). They are emitted as stable
#: JSON TEXT, not as columns per element: a column set derived from a judge version's dimension
#: names would change the schema mid-corpus, which is exactly the ``CastError`` trap.
_JSON_KINDS: Final[tuple[str, ...]] = ("error_codes", "judge_dimensions")

#: The outcome entries holding a FIXED struct, mapped to the pinned sub-key tuple the capture layer
#: already guarantees (``distill.canonical_struct``) — so each sub-key becomes its own column.
_STRUCT_SUB_KEYS: Final[dict[str, tuple[str, ...]]] = {"tool_calls": TOOL_CALL_KEYS, "gate": GATE_KEYS, "versions": VERSION_KEYS}

#: The declared kind of each fixed struct's sub-columns: the tool-call tallies are counts, the gate
#: thresholds are fractions, and the version strings are text.
_STRUCT_KINDS: Final[dict[str, str]] = {"tool_calls": "int", "gate": "float", "versions": "str"}


class _Column(NamedTuple):
    """One flat derived column: where its value comes from and the single type it must hold."""

    source: str  # the OUTCOME_KEYS entry the value is read from
    sub: str | None  # the sub-key inside a fixed struct, else None for a top-level entry
    name: str  # the emitted ``outcome_``-prefixed column name
    kind: str  # the declared kind: one of "str", "int", "float", "bool", "json"


def _text_or_none(value: object) -> str | None:
    """A text column: a str passes through; anything else is unmeasured -> ``None`` (never coerced).

    The lenient sibling of :func:`_require_str_or_none`: flattening shapes EMITTED METADATA columns
    over a corpus that may span releases, so a legacy value of the wrong type becomes the corpus's
    own "unmeasured" spelling instead of failing one stale line — while :func:`reward`, which reads
    the NESTED outcome and decides the weight, keeps failing loud on exactly that value.
    """
    return value if isinstance(value, str) else None


def _json_text(value: object) -> str | None:
    """A variable-shape value (a list of codes, the judge's dimension dict) as stable JSON text.

    Mapping keys are sorted so the text is byte-stable across runs; list order is preserved because
    it is the report's own (deterministic) occurrence order. Anything neither list nor mapping is
    unmeasured -> ``None``.
    """
    if isinstance(value, (list, tuple)):
        return json.dumps([str(item) for item in value], ensure_ascii=False)
    if isinstance(value, Mapping):
        return json.dumps({str(key): item for key, item in value.items()}, ensure_ascii=False, sort_keys=True, default=str)
    return None


#: The per-kind coercion every flattened value goes through. A dict dispatch (not an if-chain) so an
#: unknown kind in the plan is a loud ``KeyError`` at the first row rather than a silent fallthrough.
_COERCIONS: Final[dict[str, Callable[[object], str | int | float | bool | None]]] = {
    "str": _text_or_none,
    "int": _int_or_none,
    "float": _float_or_none,
    "bool": _bool_or_none,
    "json": _json_text,
}


def _flat_plan() -> tuple[_Column, ...]:
    """Derive the flat-column plan from :data:`~tablassert.distill.OUTCOME_KEYS`, in its order.

    Derived rather than hardcoded so the two can never drift: a new outcome entry with no declared
    kind is a ``KeyError`` at import (a programmer error, loud) instead of a silently missing
    training column. Structs expand in place onto their pinned sub-keys and the variable-shape
    entries become ``*_json`` text columns.
    """
    plan: list[_Column] = []
    for key in OUTCOME_KEYS:
        if key in _STRUCT_SUB_KEYS:
            sub_kind: str = _STRUCT_KINDS[key]
            for sub_key in _STRUCT_SUB_KEYS[key]:
                plan.append(_Column(key, sub_key, f"{OUTCOME_COLUMN_PREFIX}{key}_{sub_key}", sub_kind))
        elif key in _JSON_KINDS:
            plan.append(_Column(key, None, f"{OUTCOME_COLUMN_PREFIX}{key}_json", "json"))
        else:
            plan.append(_Column(key, None, f"{OUTCOME_COLUMN_PREFIX}{key}", _SCALAR_KINDS[key]))
    return tuple(plan)


#: The ordered column plan :func:`flatten_outcome` walks — the single source of truth for both the
#: emitted column names and their declared types.
_FLAT_PLAN: Final[tuple[_Column, ...]] = _flat_plan()

#: Every flattened outcome column name, in :data:`~tablassert.distill.OUTCOME_KEYS` order with the
#: fixed structs expanded in place.
OUTCOME_COLUMNS: Final[tuple[str, ...]] = tuple(column.name for column in _FLAT_PLAN)

#: The canonical emitted training-row key set (REQ-DS-5): the record's own keys, the join flag, the
#: flattened outcome columns, then the selection annotations. EVERY row carries EVERY key with an
#: explicit ``null`` where a figure is unknown — the same rule the capture layer applies, because
#: ``datasets`` infers its features from the first block of the first file and raises ``CastError``
#: on a column that first appears later in an append-only corpus.
TRAIN_ROW_KEYS: Final[tuple[str, ...]] = (*RECORD_KEYS, "outcome_matched", *OUTCOME_COLUMNS, *SELECTION_KEYS)


def flatten_outcome(row: Mapping[str, Any]) -> dict[str, Any]:
    """Project one captured outcome row onto flat, stable-typed ``outcome_``-prefixed columns.

    Every :data:`~tablassert.distill.OUTCOME_KEYS` entry becomes at least one SCALAR column, in
    :data:`OUTCOME_COLUMNS` order: the fixed ``tool_calls``/``gate``/``versions`` structs expand onto
    their pinned sub-keys (``outcome_tool_calls_total``, ``outcome_gate_map_threshold``,
    ``outcome_versions_biolink_model``) and the two variable-shape entries (``error_codes``,
    ``judge_dimensions``) become stable JSON text. No struct and no list is ever passed through.

    Why flat: ``datasets`` infers a STRUCT column from the first block of the first file, so a
    sub-field that first appears in a later line of an append-only, multi-release corpus raises
    ``CastError``, and a key whose type varies is silently JSON-encoded into a string column
    (``JsonConfig.on_mixed_types = "use_json"``) — a silent demotion that corrupts a training corpus
    without an error. Flat scalars with a DECLARED type each are the only shape that survives.

    A value of the wrong type for its column flattens to ``None`` (the corpus's own "unmeasured"
    spelling) rather than raising over one legacy line or inventing a coercion; :func:`reward` still
    reads the NESTED outcome and fails loud there, so a malformed figure can never silently set a
    weight. Missing keys flatten to ``None`` for the same reason.

    Args:
        row: One outcome line as :func:`read_ndjson` returned it (a mapping over ``OUTCOME_KEYS``).

    Returns:
        A dict whose key set AND order is exactly :data:`OUTCOME_COLUMNS`.

    Raises:
        TypeError: ``row`` is not a ``Mapping`` (a call-contract violation, not bad data).
    """
    if not isinstance(row, Mapping):
        raise TypeError(f"row must be a Mapping, got {type(row).__name__}")
    flat: dict[str, Any] = {}
    for column in _FLAT_PLAN:
        source: object = row.get(column.source)
        value: object = source
        if column.sub is not None:  # a fixed struct: read the pinned sub-key, never the struct itself
            struct: Mapping[str, Any] = source if isinstance(source, Mapping) else {}
            value = struct.get(column.sub)
        flat[column.name] = _COERCIONS[column.kind](value)
    return flat


def _train_row(record: Mapping[str, Any], outcome: Mapping[str, Any] | None) -> dict[str, Any]:
    """One canonical :data:`TRAIN_ROW_KEYS` row: the record's columns plus the flattened outcome's.

    Seeded in canonical order with explicit ``null`` and then filled IN PLACE (dict assignment never
    reorders an existing key), so an absent figure stays distinguishable from a measured zero and the
    key order a ``datasets`` first block infers is the canonical one. An unknown record key is KEPT —
    appended after the canonical set — rather than dropped: this is a derived layer over an
    append-only corpus, and silently discarding a column the corpus carries is unrecoverable.

    ``weight`` is ``0.0`` for an unjoinable row (REQ-DS-3: no outcome exists to reward, so it can
    never earn one) and ``None`` for a matched one, because the join has neither a ``RewardConfig``
    nor an ``edge_ref`` in scope — the reward step derives it from the NESTED outcome. ``select``
    refuses a null weight, so a row that was never weighed can never be silently selected.
    """
    row: dict[str, Any] = {key: None for key in TRAIN_ROW_KEYS}  # noqa: C420  # fromkeys infers a Literal key type pyright strict rejects
    for key, value in record.items():
        row[key] = value
    if outcome is None:
        row["outcome_matched"] = False
        row["weight"] = 0.0
    else:
        row["outcome_matched"] = True
        row.update(flatten_outcome(outcome))
    row["selected"] = False  # nothing is selectable until a policy annotates it
    row["replicas"] = 0
    return row


def join_records_outcomes(records: Sequence[Mapping[str, Any]], outcomes: Sequence[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Join the record corpus to the outcome corpus on ``run_id``; returns ``(rows, stats)``.

    The outcome index is built ONCE. A duplicate ``run_id`` keeps the LAST line (append-only
    semantics: a later line supersedes an earlier one for the same run) and increments
    ``stats["duplicate_run_ids"]`` — counted, never a silent overwrite, because a duplicated run id
    means two supervisors wrote one run's outcome and the operator must be able to see it.

    Rows come back in RECORD order (file order, then line order), one per record, always carrying
    the full :data:`TRAIN_ROW_KEYS` set. A record whose ``run_id`` is null or has no outcome is
    KEPT with ``outcome_matched = False``, every ``outcome_*`` column null, ``weight = 0.0``,
    ``selected = False`` and ``replicas = 0`` — a v1 corpus and every judge/reflexion row recorded
    outside a run scope must stay visible as a zero-weight row rather than vanish.

    Args:
        records: The record lines, in the order they should be emitted.
        outcomes: The outcome lines, in append order (the last line for a run id wins).

    Returns:
        ``(rows, stats)`` where ``stats`` carries exactly :data:`JOIN_STAT_KEYS`.

    Raises:
        TypeError: a record or outcome is not a ``Mapping`` (a call-contract violation).
    """
    index: dict[str, Mapping[str, Any]] = {}
    duplicates: int = 0
    for outcome_row in outcomes:
        if not isinstance(outcome_row, Mapping):
            raise TypeError(f"outcome must be a Mapping, got {type(outcome_row).__name__}")
        outcome_run_id: object = outcome_row.get("run_id")
        if not isinstance(outcome_run_id, str):
            continue  # an outcome with no run id can never join; it still counts toward stats["outcomes"]
        if outcome_run_id in index:
            duplicates += 1
        index[outcome_run_id] = outcome_row
    rows: list[dict[str, Any]] = []
    matched: int = 0
    for record in records:
        if not isinstance(record, Mapping):
            raise TypeError(f"record must be a Mapping, got {type(record).__name__}")
        record_run_id: object = record.get("run_id")
        joined: Mapping[str, Any] | None = index.get(record_run_id) if isinstance(record_run_id, str) else None
        if joined is not None:
            matched += 1
        rows.append(_train_row(record, joined))
    stats: dict[str, int] = {
        "records": len(records),
        "outcomes": len(outcomes),
        "matched": matched,
        "unmatched": len(records) - matched,
        "duplicate_run_ids": duplicates,
    }
    return rows, stats


def read_ndjson(path: Path) -> list[dict[str, Any]]:
    """Read one NDJSON file into a list of objects, skipping blank lines; malformed input is LOUD.

    Why loud rather than skip-and-continue: the corpus is append-only and a supervisor may be
    mid-write when a weigh reads it, so a torn last line is a real risk — silently truncating there
    would drop the newest examples and report a smaller corpus as if it were complete. The message
    names the FILE and the 1-based LINE NUMBER so the operator can look at exactly that line.

    A line that parses to something other than a JSON object (a bare array, number or string) is
    malformed too: every corpus line is one record or one outcome object.

    A line is a run of characters ending at ``\\n`` — the file handle is iterated, NOT
    ``str.splitlines()``. ``splitlines()`` also breaks on U+0085/U+2028/U+2029, and the capture layer
    writes ``json.dumps(payload, ensure_ascii=False)``, so those code points land RAW inside the
    string values of any LLM output over PMC full text (U+0085 also arises from latin-1 mojibake):
    slicing on them would split one VALID record into two invalid ones, fail loud on a non-error and
    report a line number no editor shows. Iterating the handle matches the writer's ``\\n`` semantics
    and :func:`is_outcome_file`, gives the true physical 1-based line number, and streams — a
    multi-gigabyte corpus is never held twice in memory (once as text, once as dicts).

    Raises:
        TypeError: ``path`` is not a ``Path`` (a call-contract violation, not bad data).
        ValueError: a line is not parseable JSON, or is not a JSON object.
        OSError: the file is missing or unreadable (propagated, never swallowed).
    """
    if not isinstance(path, Path):
        raise TypeError(f"path must be a Path, got {type(path).__name__}")
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for number, line in enumerate(handle, start=1):
            if not line.strip():
                continue  # a blank line separates nothing; it is not a record
            try:
                parsed: object = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"malformed NDJSON: {path} line {number}: {exc.msg}") from exc
            if not isinstance(parsed, dict):
                raise ValueError(f"malformed NDJSON: {path} line {number}: expected a JSON object, got {type(parsed).__name__}")
            rows.append(parsed)
    return rows


def _canonical_order(row: Mapping[str, Any]) -> dict[str, Any]:
    """Re-order one row: :data:`TRAIN_ROW_KEYS` first in canonical order, then any extra key.

    Why: the on-disk key order is what ``datasets`` infers its schema from and what makes two weighs
    of one corpus byte-identical, so it must come from the canonical tuple rather than from whatever
    order a caller's dict happened to have. Extra keys keep their own relative order after the
    canonical set, so nothing is dropped.
    """
    ordered: dict[str, Any] = {key: row[key] for key in TRAIN_ROW_KEYS if key in row}
    for key, value in row.items():
        if key not in ordered:
            ordered[key] = value
    return ordered


def write_ndjson(path: Path, rows: Iterable[Mapping[str, Any]]) -> int:
    """Write rows as NDJSON (one object per line) in canonical key order; returns the row count.

    Creates the parent directory and OVERWRITES the destination: the derived layer is a reproducible
    build artifact of one reward config plus one policy, not an append-only corpus — appending would
    silently double every example on a re-weigh. ``ensure_ascii=False`` keeps article text readable
    and byte-stable.

    A value JSON cannot serialize raises rather than being stringified: the capture layer's
    ``default=str`` fail-soft belongs to recording, and here it would quietly turn a structured
    column into text (the exact silent demotion :func:`detect_type_conflicts` exists to prevent).

    The write is ATOMIC (sibling temp + ``os.replace``, the repo's idiom): rows serialize into
    ``.{name}.tmp`` and the destination is swapped in only once every row succeeded. Opening the
    destination ``"w"`` first would truncate it before row N>0 raised, leaving a half-written
    dataset on disk — and a consumer reading that path sees a smaller, silently truncated training
    set rather than an error. On failure the destination keeps its previous content (or is never
    created), and the temp is unlinked; ``.{name}.tmp`` does not match :func:`iter_record_files`'
    ``*.ndjson`` glob, so a crashed write is never read back as corpus.

    Raises:
        TypeError: ``path`` is not a ``Path``, a row is not a ``Mapping``, or a value is not
            JSON-serializable.
        OSError: the destination is unwritable.
    """
    if not isinstance(path, Path):
        raise TypeError(f"path must be a Path, got {type(path).__name__}")
    path.parent.mkdir(parents=True, exist_ok=True)  # the destination's parent need not exist yet
    tmp: Path = path.with_name(f".{path.name}.tmp")
    written: int = 0
    try:
        with tmp.open("w", encoding="utf-8") as handle:
            for row in rows:
                if not isinstance(row, Mapping):
                    raise TypeError(f"row must be a Mapping, got {type(row).__name__}")
                handle.write(json.dumps(_canonical_order(row), ensure_ascii=False) + "\n")
                written += 1
        os.replace(tmp, path)  # ATOMIC: overwrite, never append, and never observable half-written
    finally:
        tmp.unlink(missing_ok=True)  # after a successful replace this is a no-op; after a raise it cleans up
    return written


def iter_record_files(distill_dir: Path) -> list[Path]:
    """Every ``*.ndjson`` FILE under ``distill_dir``, sorted by path.

    Sorted so the corpus order — and therefore the emitted row order and every rank-based policy —
    is a function of the directory's contents alone, never of filesystem enumeration order. This
    returns BOTH kinds of NDJSON (records and outcomes); the caller partitions them with
    :func:`is_outcome_file`, which is content-based so a renamed or relocated outcomes file is still
    recognized. A missing directory yields ``[]`` rather than raising: "no corpus here yet" is the
    caller's exit-2 message to write, not an I/O error.
    """
    if not isinstance(distill_dir, Path):
        raise TypeError(f"distill_dir must be a Path, got {type(distill_dir).__name__}")
    return sorted(path for path in distill_dir.glob("*.ndjson") if path.is_file())


def is_outcome_file(path: Path) -> bool:
    """True when ``path``'s FIRST NON-BLANK line is an object whose ``record_type`` is ``"outcome"``.

    Content-based, not name-based (REQ-DS-2): the discriminator is the ``record_type`` column the
    capture layer stamps on every line, so an outcomes file that was renamed, relocated or merged
    into a differently named corpus is still recognized — and a RECORDS file that happens to be
    called ``outcomes.ndjson`` is not mistaken for one. Only the head of the file is read, so this
    stays cheap on a multi-gigabyte corpus.

    An unreadable, empty, binary or malformed file is NOT an outcome file: this predicate only
    partitions the corpus, and the subsequent :func:`read_ndjson` is what fails loud on the
    malformed line (with its file and line number).

    Raises:
        TypeError: ``path`` is not a ``Path`` (a call-contract violation, not bad data).
    """
    if not isinstance(path, Path):
        raise TypeError(f"path must be a Path, got {type(path).__name__}")
    try:
        with path.open("r", encoding="utf-8") as handle:
            first_line: str | None = next((line for line in handle if line.strip()), None)
        first: object = json.loads(first_line) if first_line is not None else None
    except (OSError, ValueError):  # unreadable, undecodable or malformed: discriminates nothing
        return False
    return isinstance(first, dict) and first.get("record_type") == RECORD_TYPE_OUTCOME


def _row_weight(row: Mapping[str, Any], position: int) -> float:
    """The row's derived ``weight`` as a float; a missing, null or mistyped weight fails LOUD.

    Why loud: ``weight`` is the only thing a policy ranks on. A row that reaches :func:`select`
    unweighed means the reward step was skipped, and defaulting it to ``0.0`` would silently empty
    the training set (or, with a negative threshold, select rows that were never measured).
    """
    if not isinstance(row, Mapping):
        raise TypeError(f"row {position} must be a Mapping, got {type(row).__name__}")
    if "weight" not in row:
        raise ValueError(f"row {position} carries no 'weight' key: weigh the corpus before selecting it")
    value: object = row["weight"]
    if value is None:
        raise ValueError(f"row {position} has a null 'weight': run the reward step before selecting")
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise TypeError(f"row {position} 'weight' must be a finite number, got {value!r} ({type(value).__name__})")
    return float(value)


def _group_key(row: Mapping[str, Any]) -> str:
    """Policy B's per-prompt group key: the row's ``pmc_id``, with a null id forming its own group."""
    pmc_id: object = row.get("pmc_id")
    # ``str()`` merges ``5`` with ``"5"`` into one group. That is accepted, not a bug to guard:
    # ``pmc_id`` is a str column in every schema version, and a corpus that really alternated its
    # type is exactly what ``detect_type_conflicts`` reports before the rows are ever selected.
    return "" if pmc_id is None else str(pmc_id)


def _rank_value(row: Mapping[str, Any], *names: str) -> tuple[int, float]:
    """The first numeric value among ``names``, tagged so an unknown one sorts AFTER every known one.

    The bare outcome spellings are accepted beside the canonical ``outcome_``-prefixed columns so a
    row assembled straight from a nested outcome ranks identically to a joined one. The
    ``(present, value)`` tag makes the ordering total without a sentinel number: a row whose
    ``attempts`` were never measured neither wins nor loses a tie by accident.
    """
    for name in names:
        value: object = row.get(name)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return (0, float(value))
    return (1, 0.0)


#: Policy B's total sort key: highest weight, then fewer attempts, then fewer failed tool calls,
#: then the earlier call, then the row's own position (which makes the order total even for two
#: rows identical on all four, independent of the sort implementation's stability).
_RankKey = tuple[float, tuple[int, float], tuple[int, float], tuple[int, float], int]


def _rank_key(rows: Sequence[Mapping[str, Any]], weights: Sequence[float], position: int) -> _RankKey:
    """The Best-of-N ranking key of one row (REQ-RW-17's ``(-weight, attempts, failed, call_index)``)."""
    row: Mapping[str, Any] = rows[position]
    return (
        -weights[position],
        _rank_value(row, "outcome_attempts", "attempts"),
        _rank_value(row, "outcome_tool_calls_failed", "tool_calls_failed"),
        _rank_value(row, "call_index"),
        position,
    )


def _best_of_n(rows: Sequence[Mapping[str, Any]], weights: Sequence[float], top_n: int) -> list[tuple[bool, int]]:
    """Policy B's flags: the top ``top_n`` rows of each ``pmc_id`` group, in the input's own order.

    This is literally RAFT's ``y := argmax_j r(x, y_j)`` generalized to top-N, with the brief's
    tie-breaks. Groups are visited in sorted key order and rows are returned by their input
    position, so neither the group iteration nor the ranking can make the emitted order depend on
    dict or filesystem ordering.
    """
    flags: list[tuple[bool, int]] = [(False, 0) for _ in rows]
    groups: dict[str, list[int]] = {}
    for position, row in enumerate(rows):
        groups.setdefault(_group_key(row), []).append(position)
    for group in sorted(groups):
        ranked: list[int] = sorted(groups[group], key=lambda position: _rank_key(rows, weights, position))
        for position in ranked[:top_n]:
            flags[position] = (True, 1)
    return flags


def _replication(weights: Sequence[float], replication_k: float) -> list[tuple[bool, int]]:
    """Policy C's flags: ``selected = weight > 0`` with a monotone replica count across the spread.

    ``replicas = 1 + round(k * (w - w_min) / (w_max - w_min))`` over the selected rows, so the worst
    selected row is emitted once and the best ``1 + k`` times. When every selected row weighs the
    same there is no gradient to encode, so each gets exactly one replica rather than an invented
    one. ``round`` is Python's banker's rounding — deliberate: it is deterministic and has no
    systematic upward bias, so a corpus re-weighed with the same knobs replicates identically.
    """
    positive: list[float] = [weight for weight in weights if weight > 0.0]
    if not positive:
        return [(False, 0) for _ in weights]
    low: float = min(positive)
    span: float = max(positive) - low
    flags: list[tuple[bool, int]] = []
    for weight in weights:
        if weight <= 0.0:
            flags.append((False, 0))
        elif span > 0.0:
            flags.append((True, 1 + round(replication_k * (weight - low) / span)))
        else:
            flags.append((True, 1))
    return flags


def select(
    rows: Sequence[Mapping[str, Any]],
    *,
    policy: str,
    threshold: float = DEFAULT_THRESHOLD,
    top_n: int = DEFAULT_TOP_N,
    replication_k: int = DEFAULT_REPLICATION_K,
) -> list[dict[str, Any]]:
    """Annotate every row with :data:`SELECTION_KEYS` under one of :data:`POLICIES`.

    Returns NEW dicts — the input rows are never mutated — in the INPUT's order for all three
    policies, so two runs over one corpus emit byte-identical output and a policy change never
    reorders the dataset. The emitted key set is identical for every policy (only the values
    differ): a row that already carries the five keys (a joined row does) is updated in place and
    keeps its canonical :data:`TRAIN_ROW_KEYS` order. ``threshold`` is therefore stamped under
    ``best-of-n`` and ``replication`` too, where NO cutoff is applied — the value is the CONFIGURED
    cutoff the run was invoked with, recorded for reproducibility, never a claim that a filter ran.
    Dropping the key for those two policies would make the emitted schema policy-dependent, which is
    precisely what a pooled corpus cannot tolerate.

    Policies:
        threshold: RAFT/RFT rejection sampling — ``selected = weight >= threshold`` and
            ``replicas = 1 if selected else 0``. The only policy TRL consumes with zero code,
            because ``SFTConfig`` has no per-example sample-weight column.
        best-of-n: rank-based Best-of-N per prompt — group by ``pmc_id`` (a null id forms its own
            group), sort each group by ``(-weight, attempts, tool_calls_failed, call_index)``, keep
            the first ``top_n``. There is NO weight floor: the policy keeps the best AVAILABLE
            sample per prompt, so a prompt whose every trajectory weighs 0.0 still contributes its
            top-ranked row (an operator who wants a floor combines this with ``threshold``).
        replication: soft monotone weighting — ``selected = weight > 0`` and ``replicas`` scaled
            across the selected rows' weight spread by ``replication_k`` (see :func:`_replication`).

    ``replicas`` is a COUNT, not a duplication: physically repeating a row would inflate the file,
    destroy per-example identity under ``packing=True`` and double-count once the trainer applies
    its own replication.

    Args:
        rows: Weighed rows (each must carry a numeric ``weight``).
        policy: One of :data:`POLICIES`.
        threshold: Policy A's cutoff, in ``[0, 1]``; stamped on every row of EVERY policy as the
            configured value (under the other two policies it filters nothing — see above).
        top_n: Policy B's per-group keep count, ``>= 1``.
        replication_k: Policy C's slope, in ``[0, MAX_REPLICATION_K]``.

    Returns:
        One new dict per input row, in input order.

    Raises:
        ValueError: an unknown policy (the message names the valid set), a ``threshold`` outside
            ``[0, 1]``, a ``top_n < 1``, a ``replication_k`` outside ``[0, MAX_REPLICATION_K]``, or
            a row with a missing/null ``weight``.
        TypeError: a knob or a row is of the wrong type.
    """
    if policy not in POLICIES:
        raise ValueError(f"unknown selection policy {policy!r}; valid policies: {', '.join(POLICIES)}")
    if isinstance(threshold, bool) or not isinstance(threshold, (int, float)) or not math.isfinite(threshold):
        raise TypeError(f"threshold must be a finite number, got {threshold!r} ({type(threshold).__name__})")
    cutoff: float = float(threshold)
    if not 0.0 <= cutoff <= 1.0:
        raise ValueError(f"threshold must be in [0, 1], got {threshold!r}")
    if isinstance(top_n, bool) or not isinstance(top_n, int):
        raise TypeError(f"top_n must be an int, got {top_n!r} ({type(top_n).__name__})")
    if top_n < 1:
        raise ValueError(f"top_n must be >= 1 (it is a per-prompt keep count), got {top_n!r}")
    if isinstance(replication_k, bool) or not isinstance(replication_k, (int, float)) or not math.isfinite(replication_k):
        raise TypeError(f"replication_k must be a finite number, got {replication_k!r} ({type(replication_k).__name__})")
    slope: float = float(replication_k)
    if not 0.0 <= slope <= MAX_REPLICATION_K:
        raise ValueError(f"replication_k must be in [0, {MAX_REPLICATION_K}], got {replication_k!r}")

    weights: list[float] = [_row_weight(row, position) for position, row in enumerate(rows)]
    flags: list[tuple[bool, int]]
    if policy == "threshold":
        flags = [(weight >= cutoff, 1 if weight >= cutoff else 0) for weight in weights]
    elif policy == "best-of-n":
        flags = _best_of_n(rows, weights, top_n)
    else:  # "replication" — POLICIES was validated above, so this is the third and last policy
        flags = _replication(weights, slope)

    annotated: list[dict[str, Any]] = []
    for position, row in enumerate(rows):
        selected: bool
        replicas: int
        selected, replicas = flags[position]
        new_row: dict[str, Any] = dict(row)
        new_row["weight"] = weights[position]  # a float, so the column's type is stable even for an int weight
        new_row["selected"] = selected
        new_row["replicas"] = replicas
        new_row["policy"] = policy
        new_row["threshold"] = cutoff
        annotated.append(new_row)
    return annotated


def union_keys(rows: Sequence[Mapping[str, Any]]) -> tuple[str, ...]:
    """The corpus-wide key union, in FIRST-APPEARANCE order; ``()`` for an empty corpus.

    Why union rather than first-row: ``datasets.load_dataset("json")`` infers its features from the
    first block of the first file and raises ``CastError`` when a later block of an append-only
    corpus carries a column the inferred schema lacks. Unioning every row's keys and normalizing to
    that union makes the inferred schema correct by construction.

    First-appearance order (not sorted) preserves the corpus's own canonical order — a uniform v2
    corpus unions to exactly :data:`~tablassert.distill.RECORD_KEYS` rather than an alphabetized
    version of it — and it is deterministic because the row order is (files sorted, lines in order).
    """
    union: dict[str, None] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise TypeError(f"row must be a Mapping, got {type(row).__name__}")
        for key in row:
            union[key] = None  # re-assigning an existing key never reorders it
    return tuple(union)


def normalize_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Every row projected onto :func:`union_keys`, with an explicit ``None`` for an absent key.

    One uniform key set across every row — and therefore across every block of the temporary file
    the exporter writes — is the mitigation for first-block schema inference. Values are shared by
    reference (a row's ``messages`` list is not copied), so callers must treat the result as
    read-only; only the key SET is normalized, never a value's type (that is
    :func:`detect_type_conflicts`'s job to report).
    """
    keys: tuple[str, ...] = union_keys(rows)
    return [{key: row.get(key) for key in keys} for row in rows]


def _type_name(value: object) -> str:
    """The column type name of one value, reporting ``bool`` as ``int``.

    ``bool`` IS an ``int`` subclass in Python and ``datasets`` maps the two predictably
    (``Value("bool")``/``Value("int64")``), so a column alternating ``True`` and ``1`` is not the
    silent-demotion hazard this scan exists to catch. ``int`` and ``float`` are NOT conflated: a
    column alternating them is exactly the case ``on_mixed_types = "use_json"`` would stringify.
    """
    return "int" if isinstance(value, bool) else type(value).__name__


def detect_type_conflicts(rows: Sequence[Mapping[str, Any]]) -> dict[str, set[str]]:
    """Per key, the set of distinct NON-NULL Python type names observed across the whole corpus.

    Returns EVERY key (a key whose values are all null maps to an empty set, meaning ``datasets``
    would infer an untyped/null column); the caller fails loud on any set with more than one member,
    because ``JsonConfig.on_mixed_types = "use_json"`` would otherwise JSON-encode that column into
    a string — a silent type demotion that corrupts a training corpus without raising.

    Scanning the whole corpus (not the first block) is the point: a type that first varies in block
    two is invisible to ``load_dataset``'s inference and fatal to the column's meaning. ``None`` is
    skipped rather than reported as a type — null is the corpus's "unmeasured" spelling, and every
    nullable column carries it.

    Raises:
        TypeError: a row is not a ``Mapping`` (a call-contract violation, not bad data).
    """
    observed: dict[str, set[str]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise TypeError(f"row must be a Mapping, got {type(row).__name__}")
        for key, value in row.items():
            names: set[str] = observed.setdefault(key, set())
            if value is not None:
                names.add(_type_name(value))
    return observed
