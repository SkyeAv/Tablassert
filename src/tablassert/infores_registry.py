"""Information Resource (infores) registry validation.

Tablassert emits ``infores:`` CURIEs as knowledge-source provenance on nodes, edges, and
the generated RIG. Until now only the ``infores:`` prefix was validated
(``models.validate_infores_curie``); the suffix was freeform, so a typo'd or invented
identifier could ship silently. This module closes that loop against the authoritative
NCATS Translator registry: ``biolink/information-resource-registry``'s
``infores_catalog.yaml``, vendored as a bundled snapshot
(``data/infores_catalog.yaml``) so the default path is offline and deterministic.

Classification is three-way, mirroring the Translator reality that Tablassert legitimately
mints graph-local CURIEs (``rig.infores()``) that will never appear in the registry:

* ``registered``: the CURIE is in the registry.
* ``allowed``: the CURIE is on the caller's allowlist (locally-minted ids).
* ``unregistered``: neither; the only class that ``--strict`` mode fails on.

The default posture is advisory: the report counts every class and exits clean. Nothing
here rewrites emitted artifacts; registration of a new resource remains the upstream
contribution process documented by the registry repo.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import yaml

from tablassert.errors import TablassertError

#: Bundled snapshot of the upstream ``infores_catalog.yaml`` (vendored, see the header
#: comment in that file for the source commit). The default validation path reads only
#: this file; live refresh is an explicit opt-in that lands in the user cache dir.
REGISTRY_SNAPSHOT_PATH: Path = Path(__file__).parent / "data" / "infores_catalog.yaml"

#: Prefix every registry identifier carries. Values without it are malformed output,
#: not merely unregistered.
INFORES_PREFIX: str = "infores:"

#: Provenance fields scanned on edges. ``primary_knowledge_source`` is a list on emitted
#: edges but a scalar survives hand-built fixtures, so both are accepted.
_EDGE_INFORES_FIELDS: tuple[str, ...] = ("primary_knowledge_source", "sources")


def load_registry_snapshot(path: Path = REGISTRY_SNAPSHOT_PATH) -> frozenset[str]:
    """Load the bundled registry snapshot into a set of ``infores:`` CURIEs.

    Args:
        path: Path to an ``infores_catalog.yaml`` document; defaults to the bundled
            snapshot shipped as package data.

    Returns:
        Frozenset of registry identifiers (``infores:`` CURIEs). Stanza entries without
        a CURIE-shaped ``id`` are ignored: the catalog legitimately carries non-resource
        metadata sections alongside the stanzas.

    Raises:
        TablassertError: With code ``infores-registry-unreadable`` when the file is
            missing, unparsable, or carries no ``information_resources`` stanza list --
            a truncated or corrupt snapshot must fail loudly, never read as an empty
            registry.
    """
    try:
        document: Any = yaml.safe_load(path.read_bytes())
    except (OSError, yaml.YAMLError) as error:
        raise TablassertError(f"Cannot read the infores registry snapshot at {path}: {error}", code="infores-registry-unreadable") from error
    stanzas: Any = document.get("information_resources") if isinstance(document, dict) else None
    if not isinstance(stanzas, list):
        raise TablassertError(
            f"The infores registry snapshot at {path} carries no `information_resources` stanza list; it is truncated or corrupt.",
            code="infores-registry-unreadable",
        )
    identifiers: set[str] = {
        str(stanza["id"]) for stanza in stanzas if isinstance(stanza, dict) and str(stanza.get("id") or "").startswith(INFORES_PREFIX)
    }
    if not identifiers:
        raise TablassertError(
            f"The infores registry snapshot at {path} parsed but contains zero infores: identifiers.", code="infores-registry-unreadable"
        )
    return frozenset(identifiers)


def _as_list(value: Any) -> list[Any]:
    """Coerce a KGX field value to a list: emitted fields are lists, fixtures may pass scalars."""
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _iter_file_records(path: Path) -> Iterator[dict[str, Any]]:
    """Yield decoded NDJSON records from ``path``; blank lines are skipped silently."""
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                record: Any = json.loads(line)
                if isinstance(record, dict):
                    yield record


def _edge_observations(record: dict[str, Any]) -> Iterator[tuple[str, str]]:
    """Yield ``(value, field)`` pairs for the provenance fields of one edge record."""
    for value in _as_list(record.get("primary_knowledge_source")):
        yield str(value), "primary_knowledge_source"
    for sources in _as_list(record.get("sources")):
        if not isinstance(sources, dict):
            continue
        resource_id: Any = sources.get("resource_id")
        if resource_id is not None:
            yield str(resource_id), "sources.resource_id"
        for upstream in _as_list(sources.get("upstream_resource_ids")):
            yield str(upstream), "sources.upstream_resource_ids"


def _node_observations(record: dict[str, Any]) -> Iterator[tuple[str, str]]:
    """Yield ``(value, field)`` pairs for the provenance fields of one node record."""
    for value in _as_list(record.get("provided_by")):
        yield str(value), "provided_by"


def _rig_observations(document: Any) -> Iterator[tuple[str, str]]:
    """Yield ``(value, path)`` pairs for every infores-shaped string in a RIG document.

    The walk is generic on purpose: the RIG schema grows (``source_info.infores_id``,
    ``target_info`` edge-type ``primary_knowledge_sources``, ``supporting_data_source_info``),
    and a field-by-field list would silently miss the next section. The typed subtrees
    are the only places Tablassert writes infores values, so a value-shaped match there
    is exact, not heuristic.
    """
    if isinstance(document, dict):
        for key, value in document.items():
            for child_value, child_path in _rig_observations(value):
                yield child_value, f"{key}.{child_path}" if child_path else key
    elif isinstance(document, list):
        for index, item in enumerate(document):
            for child_value, child_path in _rig_observations(item):
                yield child_value, f"[{index}].{child_path}" if child_path else f"[{index}]"
    elif isinstance(document, str) and document.startswith(INFORES_PREFIX):
        yield document, ""


def _observations(nodes_path: Path, edges_path: Path, rig_path: Path | None) -> tuple[list[tuple[str, str, str]], dict[str, bool]]:
    """Collect ``(value, where, field)`` triples plus per-input ``missing`` flags."""
    observations: list[tuple[str, str, str]] = []
    missing: dict[str, bool] = {}
    for label, path, walk in (("nodes", nodes_path, _node_observations), ("edges", edges_path, _edge_observations)):
        missing[label] = not path.is_file()
        if missing[label]:
            continue
        for record in _iter_file_records(path):
            for value, field in walk(record):
                observations.append((value, label, field))
    if rig_path is not None:
        missing["rig"] = not rig_path.is_file()
        if not missing["rig"]:
            loaded: Any = yaml.safe_load(rig_path.read_bytes())
            for value, field in _rig_observations(loaded):
                observations.append((value, "rig", field))
    return observations, missing


def validate_infores(
    nodes_path: Path,
    edges_path: Path,
    *,
    rig_path: Path | None = None,
    allow: tuple[str, ...] | list[str] = (),
    registry: frozenset[str] | set[str] | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    """Classify every emitted infores CURIE against the registry.

    Args:
        nodes_path: Path to ``<name>_<version>.nodes.ndjson``.
        edges_path: Path to ``<name>_<version>.edges.ndjson``.
        rig_path: Optional RIG yaml; when given, its infores values are checked too.
        allow: Locally-minted CURIEs accepted without registry membership (e.g. the
            graph's own ``infores:`` id from ``rig.infores()``).
        registry: Pre-loaded identifier set; defaults to the bundled snapshot. Tests
            inject tiny fixtures here.
        limit: Maximum number of example unregistered/malformed observations retained.

    Returns:
        Mapping with ``registered`` / ``allowed`` / ``unregistered`` / ``malformed``
        counts, ``examples`` (``{"curie", "where", "field", "problem"}``), a ``missing``
        map of absent input files, ``registry`` provenance, and the verdicts ``ok``
        (no missing inputs, no malformed values) and ``ok_strict`` (``ok`` and zero
        unregistered CURIEs). The default CLI posture is advisory; only ``ok_strict``
        may drive a non-zero exit, and only under an explicit flag.
    """
    registry_ids: frozenset[str] = load_registry_snapshot() if registry is None else frozenset(registry)
    observations, missing = _observations(nodes_path, edges_path, rig_path)
    registered: set[str] = set()
    allowed: set[str] = set()
    unregistered: dict[str, tuple[str, str]] = {}
    malformed: list[tuple[str, str, str]] = []
    examples: list[dict[str, str]] = []
    for value, where, field in observations:
        if not value.startswith(INFORES_PREFIX):
            malformed.append((value, where, field))
            continue
        if value in registry_ids:
            registered.add(value)
        elif value in allow:
            allowed.add(value)
        else:
            unregistered.setdefault(value, (where, field))
    for value in sorted(unregistered):
        if len(examples) >= limit:
            break
        where, field = unregistered[value]
        examples.append({"curie": value, "where": where, "field": field, "problem": "unregistered"})
    for value, where, field in malformed:
        if len(examples) >= limit:
            break
        examples.append({"curie": value, "where": where, "field": field, "problem": "malformed"})
    return {
        "registered": len(registered),
        "allowed": len(allowed),
        "unregistered": len(unregistered),
        "malformed": len(malformed),
        "examples": examples,
        "missing": missing,
        "registry": {"source": str(REGISTRY_SNAPSHOT_PATH), "entries": len(registry_ids)},
        "ok": not any(missing.values()) and not malformed,
        "ok_strict": not any(missing.values()) and not malformed and not unregistered,
    }
