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
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import yaml

from tablassert import net
from tablassert.errors import TablassertError

#: Bundled snapshot of the upstream ``infores_catalog.yaml`` (vendored, see the header
#: comment in that file for the source commit). The default validation path reads only
#: this file; live refresh is an explicit opt-in that lands in the user cache dir.
REGISTRY_SNAPSHOT_PATH: Path = Path(__file__).parent / "data" / "infores_catalog.yaml"

#: Upstream document the snapshot vendors and ``--registry refresh`` fetches.
REGISTRY_RAW_URL: str = "https://raw.githubusercontent.com/biolink/information-resource-registry/main/infores_catalog.yaml"


#: Cache location for ``--registry refresh`` (honors ``XDG_CACHE_HOME``). The cache is a
#: convenience artifact for inspection and offline re-reads; every refresh run refetches.
def registry_cache_path() -> Path:
    """Return the user cache path the refreshed registry document is written to."""
    cache_root: str | None = os.environ.get("XDG_CACHE_HOME")
    return (Path(cache_root) if cache_root else Path.home() / ".cache") / "tablassert" / "infores_catalog.yaml"


#: Prefix every registry identifier carries. Values without it are malformed output,
#: not merely unregistered.
INFORES_PREFIX: str = "infores:"

#: Provenance fields scanned on edges. ``primary_knowledge_source`` is a list on emitted
#: edges but a scalar survives hand-built fixtures, so both are accepted.
_EDGE_INFORES_FIELDS: tuple[str, ...] = ("primary_knowledge_source", "sources")


def _extract_identifiers(document: Any, path_label: str) -> frozenset[str]:
    """Pull the ``infores:`` identifier set out of a parsed catalog document.

    Shared by the snapshot loader and the refresh path so both enforce the identical
    bar: a document without a non-empty identifier set is corrupt and must fail loudly
    BEFORE the refresh path caches it.
    """
    stanzas: Any = document.get("information_resources") if isinstance(document, dict) else None
    if not isinstance(stanzas, list):
        raise TablassertError(
            f"The infores registry document at {path_label} carries no `information_resources` stanza list; it is truncated or corrupt.",
            code="infores-registry-unreadable",
        )
    identifiers: set[str] = {
        str(stanza["id"]) for stanza in stanzas if isinstance(stanza, dict) and str(stanza.get("id") or "").startswith(INFORES_PREFIX)
    }
    if not identifiers:
        raise TablassertError(
            f"The infores registry document at {path_label} parsed but contains zero infores: identifiers.", code="infores-registry-unreadable"
        )
    return frozenset(identifiers)


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
    return _extract_identifiers(document, str(path))


def refresh_registry(destination: Path | None = None) -> frozenset[str]:
    """Fetch the live upstream registry and load it.

    The document is parsed BEFORE it is written, so a truncated or hostile body can never
    poison the cache file; on success the cache is a plain copy of the upstream yaml, left
    for inspection and offline re-reads.

    Args:
        destination: Cache path; defaults to :func:`registry_cache_path`.

    Returns:
        The live registry's identifier set, identical in shape to
        :func:`load_registry_snapshot`.

    Raises:
        NetworkTransientError: When every fetch attempt fails with a transient error.
        TablassertError: With code ``infores-registry-unreadable`` when the fetched body
            is not a parseable catalog document.
    """
    cache: Path = destination if destination is not None else registry_cache_path()
    text: str = net.http_get_text(REGISTRY_RAW_URL)
    try:
        document: Any = yaml.safe_load(text)
    except yaml.YAMLError as error:
        raise TablassertError(
            f"Fetched infores registry from {REGISTRY_RAW_URL} is not valid yaml: {error}", code="infores-registry-unreadable"
        ) from error
    # Validate the identifier set BEFORE writing: a body with an empty stanza list
    # would otherwise be cached as a "valid" registry that classifies every real
    # CURIE unregistered on some later hand-inspection.
    identifiers: frozenset[str] = _extract_identifiers(document, REGISTRY_RAW_URL)
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text(text, encoding="utf-8")
    return identifiers


def _as_list(value: Any) -> list[Any]:
    """Coerce a KGX field value to a list: emitted fields are lists, fixtures may pass scalars."""
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _iter_file_records(path: Path) -> Iterator[tuple[dict[str, Any] | None, str | None]]:
    """Yield ``(record, None)`` per decoded NDJSON record, ``(None, line)`` for non-JSON lines.

    Blank lines are skipped. A line that is not JSON at all is surfaced to the caller
    (the same defect class ``validate-kgx`` counts explicitly) instead of crashing the
    walk with a traceback.
    """
    # errors="replace" keeps a bad byte from raising UnicodeDecodeError mid-file;
    # the replacement glyph makes the line non-JSON, which reports as malformed.
    with path.open(encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                record: Any = json.loads(line)
            except json.JSONDecodeError:
                yield None, line
                continue
            if isinstance(record, dict):
                yield record, None


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
        for record, bad_line in _iter_file_records(path):
            if record is None:
                # A line that is not JSON at all is malformed output, matching how
                # validate-kgx counts its own non-JSON lines.
                observations.append((str(bad_line).strip()[:120], label, "<not-json>"))
                continue
            for value, field in walk(record):
                observations.append((value, label, field))
    if rig_path is not None:
        missing["rig"] = not rig_path.is_file()
        if not missing["rig"]:
            try:
                loaded: Any = yaml.safe_load(rig_path.read_bytes())
            except yaml.YAMLError as error:
                raise TablassertError(f"Cannot parse the RIG document at {rig_path}: {error}", code="infores-registry-unreadable") from error
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
    check_registry: bool = True,
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
            inject tiny fixtures here (the report's ``registry.source`` then reads
            ``injected``). Ignored when ``check_registry`` is False.
        check_registry: False runs the structural pass only (the ``--registry off``
            mode): CURIEs are counted as malformed or not, but never registered vs
            unregistered, so membership never influences any verdict.
        limit: Maximum number of example unregistered/malformed observations retained.

    Returns:
        Mapping with ``registered`` / ``allowed`` / ``unregistered`` / ``malformed``
        counts -- all four are DISTINCT-value counts, so a defect repeated on every
        record counts once -- ``examples`` (``{"curie", "where", "field", "problem"}``),
        a ``missing`` map of absent input files, ``registry`` provenance, and the
        verdicts ``ok`` (no missing inputs, no malformed values) and ``ok_strict``
        (``ok`` and zero unregistered CURIEs). The default CLI posture is advisory;
        only ``ok_strict`` may drive a non-zero exit, and only under an explicit flag.
    """
    registry_ids: frozenset[str] = (load_registry_snapshot() if registry is None else frozenset(registry)) if check_registry else frozenset()
    registry_source: str = "injected" if registry is not None and check_registry else ("off" if not check_registry else str(REGISTRY_SNAPSHOT_PATH))
    observations, missing = _observations(nodes_path, edges_path, rig_path)
    registered: set[str] = set()
    allowed: set[str] = set()
    unregistered: dict[str, tuple[str, str]] = {}
    malformed: set[tuple[str, str, str]] = set()
    examples: list[dict[str, str]] = []
    for value, where, field in observations:
        if not value.startswith(INFORES_PREFIX):
            malformed.add((value, where, field))
            continue
        if not check_registry:
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
    for value, where, field in sorted(malformed):
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
        "registry": {"source": registry_source, "entries": len(registry_ids)},
        "ok": not any(missing.values()) and not malformed,
        "ok_strict": not any(missing.values()) and not malformed and not unregistered,
    }
