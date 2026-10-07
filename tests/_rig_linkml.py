"""Minimal LinkML-subset validator for RIG documents (TEST-ONLY helper).

Validates a decoded RIG document against the vendored
``resource_ingest_guide_schema.yaml`` WITHOUT the ``linkml`` package (not installed,
and too heavy to add for a test). The subset enforces exactly the features the schema
uses, verified against the vendored copy:

* class-level ``attributes`` (the schema declares no global ``slots``),
* ``required`` slots,
* closed world: keys outside the class's ``attributes`` are errors,
* ``range`` resolution to scalar types (``string`` / ``uriorcurie`` / ``boolean`` /
  ``integer``), enums (``permissible_values`` membership), and nested classes,
* ``multivalued`` list handling.

HONEST LIMITATIONS (documented so the subset is never mistaken for full LinkML):
``pattern`` regexes are NOT enforced; ``equals_string``/``any_of``/``union_of``/
``is_a`` inheritance and ``designates_type`` are absent from this schema and therefore
unmodeled; ``exact_mappings`` annotations are ignored. If upstream adopts an unmodeled
feature, the staleness test (which refetches the schema) still passes -- feature-growth
drift is caught by review of that diff, not mechanically here.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

#: Scalar ranges the schema actually uses (verified against the vendored copy).
_SCALAR_RANGES: frozenset[str] = frozenset({"string", "uriorcurie", "boolean", "integer"})


@dataclass(frozen=True)
class LinkMLSubsetError(Exception):
    """One schema violation, named by class, slot path, and reason."""

    path: str
    reason: str

    def __str__(self) -> str:
        return f"{self.path}: {self.reason}"


def load_schema(path: Path) -> dict[str, Any]:
    """Load the vendored LinkML schema document."""
    return yaml.safe_load(path.read_bytes())


def validate_document(document: Any, schema: dict[str, Any], class_name: str = "ReferenceIngestGuide") -> list[LinkMLSubsetError]:
    """Validate ``document`` as ``class_name``; return every violation (empty = conformant)."""
    errors: list[LinkMLSubsetError] = []
    _validate_node(document, schema, class_name, class_name, errors)
    return errors


def _validate_node(node: Any, schema: dict[str, Any], class_name: str, path: str, errors: list[LinkMLSubsetError]) -> None:
    """Recursive half of :func:`validate_document`; appends violations to ``errors``."""
    attributes: dict[str, Any] = schema["classes"][class_name].get("attributes", {}) or {}
    if not isinstance(node, dict):
        errors.append(LinkMLSubsetError(path, f"expected a mapping for class {class_name}, got {type(node).__name__}"))
        return
    for key in node:
        if key not in attributes:
            errors.append(LinkMLSubsetError(f"{path}.{key}", "unknown slot for class " + class_name))
    for key, attribute in attributes.items():
        if attribute.get("required") and key not in node:
            errors.append(LinkMLSubsetError(f"{path}.{key}", "required slot missing"))
    for key, value in node.items():
        if key in attributes:
            _check_range(value, attributes[key], schema, f"{path}.{key}", errors)


def _check_range(value: Any, attribute: dict[str, Any], schema: dict[str, Any], path: str, errors: list[LinkMLSubsetError]) -> None:
    """Check one slot's value against its ``range`` (recursing into classes)."""
    rng: str = attribute.get("range", "string")
    if attribute.get("multivalued"):
        if not isinstance(value, list):
            errors.append(LinkMLSubsetError(path, f"multivalued slot must be a list, got {type(value).__name__}"))
            return
        for index, item in enumerate(value):
            _check_scalar_or_class(item, rng, schema, f"{path}[{index}]", errors)
        return
    _check_scalar_or_class(value, rng, schema, path, errors)


def _check_scalar_or_class(value: Any, rng: str, schema: dict[str, Any], path: str, errors: list[LinkMLSubsetError]) -> None:
    """Dispatch one value against a scalar type, an enum, or a nested class range."""
    if rng in _SCALAR_RANGES:
        if rng == "boolean" and not isinstance(value, bool):
            errors.append(LinkMLSubsetError(path, f"expected boolean, got {type(value).__name__}"))
        elif rng == "integer" and (not isinstance(value, int) or isinstance(value, bool)):
            errors.append(LinkMLSubsetError(path, f"expected integer, got {type(value).__name__}"))
        elif rng in ("string", "uriorcurie") and not isinstance(value, str):
            # Type-only: the schema declares no patterns or min_length, so empty
            # strings are legal (the generator emits '' for field lists on empty
            # artifacts). Tightening here would misreport conformance.
            errors.append(LinkMLSubsetError(path, f"expected {rng}, got {type(value).__name__}"))
        return
    if rng in schema.get("enums", {}):
        permissible: dict[str, Any] = schema["enums"][rng].get("permissible_values", {}) or {}
        if value not in permissible:
            errors.append(LinkMLSubsetError(path, f"{value!r} is not a {rng} permissible value"))
        return
    if rng in schema.get("classes", {}):
        _validate_node(value, schema, rng, path, errors)
        return
    errors.append(LinkMLSubsetError(path, f"range {rng!r} is neither a scalar, enum, nor class in this schema"))
