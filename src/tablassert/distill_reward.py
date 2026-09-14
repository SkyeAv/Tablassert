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
production), and it never imports or calls ``quality_score``, the GEPA path's composite. The
selection policies and the records/outcomes join are layered on top elsewhere; they are deliberately
NOT here.
"""

from __future__ import annotations

import hashlib
import json
import math
import statistics
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as distribution_version
from pathlib import Path
from typing import Any, Final

import yaml

from tablassert.distill import GATE_KEYS, OUTCOME_KEYS, RECORD_TYPE_OUTCOME, SCHEMA_VERSION, TOOL_CALL_KEYS, VERSION_KEYS
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
