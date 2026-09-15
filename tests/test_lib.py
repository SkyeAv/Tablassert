from __future__ import annotations

import json
from collections.abc import Callable
from functools import reduce
from itertools import chain
from pathlib import Path
from typing import Any, Self, cast

import polars as pl
import pytest
from pydantic import ValidationError

import tablassert.cli as cli
import tablassert.lib as lib
from tablassert import rs
from tablassert.biolink import ALLOWED_EDGE_FIELDS, EFFECT_TYPE_VALUES, UNSATISFIABLE_EDGE_FIELDS, Categories, validate_kgx
from tablassert.coerce import (
    _EFFECT_TYPE_ALIASES,
    _RULES,
    _map_effect_type_series,
    _map_effect_type_value,
    coerce_study_metadata_columns,
    study_metadata_target,
)
from tablassert.enums import Repositories
from tablassert.fullmap import ResolveSpec
from tablassert.ingests import from_yaml
from tablassert.lib import (
    HEAD_ROWS,
    Tcode,
    clean_numeric,
    coerce_effect_size_columns,
    coerce_effect_type_columns,
    coerce_pvalue_columns,
    coerce_study_size_columns,
    coerced_target,
    drop_low_number_of_cases,
    drop_not_significant,
    drop_zero_effect_size,
    edge_category,
    edge_tables,
    effect_size_target,
    effect_type_target,
    fold_unknown_to_supporting_text,
    format_numeric,
    head,
    idx,
    idxname,
    infores,
    is_neglog10_column,
    numeric_columns,
    parse_edge_name,
    publications,
    pvalue_target,
    retrieval_sources,
    split_list,
    strip_nulls,
    study_size_target,
)
from tablassert.models import DEFAULT_RIG_UI_EXPLANATION


def fake_fullmap_row(term: str, curie: str, name: str, category: str, taxon: int = 0) -> dict[str, object]:
    """Build a fake fullmap lookup row using the Rust extension return schema."""
    return {
        "term": term,
        "CURIE": curie,
        "PREFERRED_NAME": name,
        "CATEGORY_NAME": category,
        "TAXON_ID": taxon,
        "SOURCE_NAME": "TEST",
        "SOURCE_VERSION": "1",
    }


def install_fake_fullmap(monkeypatch: Any, rows: dict[str, list[dict[str, object]]]) -> list[list[str]]:
    """Monkeypatch fullmap lookup and return captured term batches.

    ``rows`` keys (and each row's ``term`` field) must match ``nlp.level_one``
    output exactly -- clean -> lowercase -> Porter2 stem -> dedupe -> sort --
    not the raw source text, because the pipeline queries the normalized form.
    Any change to level-one normalization must re-key these fixtures deliberately.
    """
    calls: list[list[str]] = []

    def fake_lookup(db: Path, terms: list[str], return_format: str = "rows") -> list[dict[str, object]]:
        del db, return_format
        calls.append(terms)
        return [row for term in terms for row in rows.get(term, [])]

    monkeypatch.setattr(rs, "lookup_fullmap_terms", fake_lookup)
    return calls


def write_text_section(tmp_path: Path, name: str, section: dict[str, object], rows: list[str]) -> tuple[Path, Path]:
    """Write a source TSV and matching table YAML for a test section."""
    from tablassert.ingests import to_yaml

    table_path: Path = tmp_path / f"{name}.yaml"
    source_path: Path = tmp_path / f"{name}.tsv"
    source_path.write_text("\n".join(rows) + "\n")
    source_config: object = section.get("source", {})
    source_overrides: dict[str, object] = source_config if isinstance(source_config, dict) else {}
    section["source"] = {"url": [f"https://example.com/{name}.tsv"], "local": str(source_path), "kind": "text", "delimiter": "\t", **source_overrides}
    to_yaml(table_path, section)
    return table_path, source_path


class DummyProgress:
    """Minimal progress reporter for build_pipeline smoke tests."""

    def __init__(self) -> None:
        self.stages: list[str] = []
        self.sections: list[str] = []
        self.sub_steps: list[str] = []

    def stage(self, name: str) -> None:
        self.stages.append(name)

    def section_loop(self, n: int, label: str) -> tuple[Any, Any, Any]:
        del n, label

        def start(name: str) -> None:
            self.sections.append(name)

        def advance() -> None:
            return None

        def sub_step(name: str) -> None:
            self.sub_steps.append(name)

        return start, advance, sub_step


class SyncPool:
    """Synchronous drop-in replacement for multiprocessing.Pool in tests."""

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        del exc_type, exc, traceback

    def map(self, fn: Any, items: list[Any]) -> list[Any]:
        return [fn(item) for item in items]

    def starmap(self, fn: Any, items: object) -> list[Any]:
        return [fn(*item) for item in items]  # pyright: ignore

    def imap_unordered(self, fn: Any, items: object) -> list[Any]:
        # Synchronous stand-in: yields fn(item) for each input (consumed by a for-loop, so a list suffices).
        return [fn(item) for item in items]  # pyright: ignore


def test_idxname_single_letter() -> None:
    """idxname converts single letter columns."""
    assert idxname("A") == "column_1"
    assert idxname("B") == "column_2"
    assert idxname("Z") == "column_26"


def test_idxname_double_letter() -> None:
    """idxname converts double letter columns."""
    assert idxname("AA") == "column_27"
    assert idxname("AB") == "column_28"
    assert idxname("AZ") == "column_52"


def test_idxname_triple_letter() -> None:
    """idxname converts triple letter columns."""
    assert idxname("AAA") == "column_703"


def test_idxname_format() -> None:
    """idxname returns column prefixed string."""
    result: str = idxname("C")
    assert result.startswith("column_")


def test_strip_nulls_removes_empty_string() -> None:
    """strip_nulls removes null like values."""
    r: dict[str, Any] = {"a": "hello", "b": ""}
    result: dict = strip_nulls(r)
    assert "a" in result
    assert "b" not in result


def test_strip_nulls_removes_null_variants() -> None:
    """strip_nulls removes na nan null none."""
    r: dict[str, Any] = {"a": "na", "b": "nan", "c": "null", "d": "none"}
    result: dict = strip_nulls(r)
    assert len(result) == 0


def test_strip_nulls_case_insensitive() -> None:
    """strip_nulls case insensitive."""
    r: dict[str, Any] = {"a": "NA", "b": "NaN", "c": "NULL", "d": "None"}
    result: dict = strip_nulls(r)
    assert len(result) == 0


def test_strip_nulls_preserves_valid() -> None:
    """strip_nulls preserves valid values."""
    r: dict[str, Any] = {"name": "BRCA1", "score": 0.05, "active": True}
    result: dict = strip_nulls(r)
    assert result["name"] == "BRCA1"
    assert result["score"] == 0.05
    assert result["active"] is True


def test_strip_nulls_nested_dict() -> None:
    """strip_nulls handles nested dicts."""
    r: dict[str, Any] = {"outer": {"inner": "na", "keep": "yes"}}
    result: dict = strip_nulls(r)
    assert "keep" in result["outer"]
    assert "inner" not in result["outer"]


def test_strip_nulls_list_of_dicts() -> None:
    """strip_nulls handles lists of dicts."""
    r: dict[str, Any] = {"items": [{"a": "keep", "b": ""}, {"a": "also", "c": "null"}]}
    result: dict = strip_nulls(r)
    assert result["items"][0] == {"a": "keep"}
    assert result["items"][1] == {"a": "also"}


def test_strip_nulls_empty_dict() -> None:
    """strip_nulls handles empty dict."""
    r: dict[str, Any] = {}
    result: dict = strip_nulls(r)
    assert result == {}


def test_strip_nulls_whitespace() -> None:
    """strip_nulls strips whitespace before check."""
    r: dict[str, Any] = {"a": "  ", "b": " na "}
    result: dict = strip_nulls(r)
    assert len(result) == 0


def test_tcode_model_allows_unresolved_value_encoding(fixtures_path: Path) -> None:
    """tcode allows unresolved value encodings during validation."""
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    store: Path = Path("/tmp/sectionhash.parquet")
    data["statement"]["subject"] = {"method": "value", "encoding": "Incertae Sedis XI"}

    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {**data, "config": fixtures_path / "minimal_section.yaml", "store": store}
    )

    assert tcode_model.statement.subject.method == "value"
    assert tcode_model.statement.subject.encoding == "Incertae Sedis XI"


def test_tcode_collect_skips_qc_by_default(fixtures_path: Path) -> None:
    """tcode collect enables QC logging by default."""
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    store: Path = Path("/tmp/sectionhash.parquet")
    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {**data, "config": fixtures_path / "minimal_section.yaml", "store": store}
    )

    collected: list[tuple[Any, tuple[Any]]] = tcode_model.collect(Path("/tmp/fullmap.redb"))  # pyright: ignore
    qc_ops: list[tuple[Any, tuple[Any]]] = [op for op in collected if op[0].__name__ == "fullmap_audit"]

    assert qc_ops == []


def test_tcode_collect_enables_qc_logging(fixtures_path: Path) -> None:
    """tcode collect enables QC logging when graph QC is enabled."""
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    store: Path = Path("/tmp/sectionhash.parquet")
    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {**data, "config": fixtures_path / "minimal_section.yaml", "store": store, "qc": True}
    )

    collected: list[tuple[Any, tuple[Any]]] = tcode_model.collect(Path("/tmp/fullmap.redb"))  # pyright: ignore
    qc_ops: list[tuple[Any, tuple[Any]]] = [op for op in collected if op[0].__name__ == "fullmap_audit"]

    assert len(qc_ops) == 2
    assert qc_ops[0][1] == ("subject", "sectionhash", "minimal_section.yaml", "passed", True)
    assert qc_ops[1][1] == ("object", "sectionhash", "minimal_section.yaml", "passed", True)


# tcode collect orders release-mode filters before resolve_batch
# rows that will be dropped must never reach the expensive fullmap resolve step
def test_tcode_collect_orders_release_filters_before_resolve_when_release(fixtures_path: Path, tmp_path: Path) -> None:
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    store: Path = tmp_path / "sectionhash_release.parquet"
    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {**data, "config": fixtures_path / "minimal_section.yaml", "store": store, "release": True}
    )

    collected: list[tuple[Any, tuple[Any]]] = tcode_model.collect(tmp_path / "fullmap.redb")  # pyright: ignore
    drop_ns_idx: int = next(i for i, op in enumerate(collected) if op[0].__name__ == "drop_not_significant")
    drop_zero_idx: int = next(i for i, op in enumerate(collected) if op[0].__name__ == "drop_zero_effect_size")
    resolve_idx: int = next(i for i, op in enumerate(collected) if op[0].__name__ == "resolve_batch")

    assert drop_ns_idx < resolve_idx
    assert drop_zero_idx < resolve_idx


def test_tcode_collect_omits_release_filters_without_release(fixtures_path: Path, tmp_path: Path) -> None:
    """tcode collect omits release-mode filters without release but keeps sig before resolve_batch."""
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    store: Path = tmp_path / "sectionhash_norelease.parquet"
    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {**data, "config": fixtures_path / "minimal_section.yaml", "store": store}
    )

    collected: list[tuple[Any, tuple[Any]]] = tcode_model.collect(tmp_path / "fullmap.redb")  # pyright: ignore
    names: list[str] = [op[0].__name__ for op in collected]

    assert "drop_not_significant" not in names
    assert "drop_zero_effect_size" not in names
    assert "drop_low_number_of_cases" not in names
    assert names.index("sig") < names.index("resolve_batch")


def test_tcode_collect_includes_drop_low_number_of_cases_for_applied_to_treat_release(fixtures_path: Path, tmp_path: Path) -> None:
    """tcode collect gates drop_low_number_of_cases on release and the applied_to_treat predicate."""
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    data["statement"]["predicate"] = "applied_to_treat"
    store: Path = tmp_path / "sectionhash_number_of_cases.parquet"
    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {**data, "config": fixtures_path / "minimal_section.yaml", "store": store, "release": True}
    )

    collected: list[tuple[Any, tuple[Any]]] = tcode_model.collect(tmp_path / "fullmap.redb")  # pyright: ignore
    names: list[str] = [op[0].__name__ for op in collected]

    assert "drop_low_number_of_cases" in names
    assert names.index("drop_low_number_of_cases") < names.index("resolve_batch")


def test_tcode_collect_omits_drop_low_number_of_cases_for_other_predicates_in_release(fixtures_path: Path, tmp_path: Path) -> None:
    """tcode collect omits drop_low_number_of_cases in release mode when the predicate is not applied_to_treat."""
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    store: Path = tmp_path / "sectionhash_related_to.parquet"
    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {**data, "config": fixtures_path / "minimal_section.yaml", "store": store, "release": True}
    )

    collected: list[tuple[Any, tuple[Any]]] = tcode_model.collect(tmp_path / "fullmap.redb")  # pyright: ignore
    names: list[str] = [op[0].__name__ for op in collected]

    assert "drop_low_number_of_cases" not in names
    assert "drop_not_significant" in names


def test_section_store_path_suffixes_every_build_mode() -> None:
    """_section_store_path gives each build-mode flag combination its own cache file name."""
    assert cli._section_store_path("abc123").name == "abc123.parquet"
    assert cli._section_store_path("abc123", head=True).name == "abc123.head.parquet"
    assert cli._section_store_path("abc123", release=True).name == "abc123.release.parquet"
    assert cli._section_store_path("abc123", qc=True).name == "abc123.qc.parquet"
    # Modes compose instead of overwriting one another's suffix.
    assert cli._section_store_path("abc123", head=True, release=True, qc=True).name == "abc123.head.release.qc.parquet"


def test_tcode_collect_quick_exit_never_serves_another_mode_cached_parquet(monkeypatch: Any, fixtures_path: Path, tmp_path: Path) -> None:
    """A parquet cached by a non-release build never quick-exits a release build (and vice versa)."""
    # STORE is relative to the cwd, so chdir keeps the simulated cache inside tmp_path.
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".tablassert" / "store").mkdir(parents=True)
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    # A previous non-release build cached this section's subgraph (filters skipped).
    pl.DataFrame({"subject": ["cached"]}).write_parquet(cli._section_store_path("sectionhash"))
    store: Path = cli._section_store_path("sectionhash", release=True)
    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {**data, "config": fixtures_path / "minimal_section.yaml", "store": store, "release": True}
    )

    collected: Any = tcode_model.collect(tmp_path / "fullmap.redb")

    # The release store does not exist yet: the non-release parquet must not be reused.
    assert not isinstance(collected, Path)
    # Once the release-mode build has written its own parquet, the quick exit serves it.
    pl.DataFrame({"subject": ["release"]}).write_parquet(store)
    assert tcode_model.collect(tmp_path / "fullmap.redb") == store


def test_tcode_model_validate_rejects_duplicate_qualifier_keys(fixtures_path: Path) -> None:
    """Tcode construction rejects a section whose statement repeats a qualifier key.

    The duplicate would otherwise survive into ``collect`` as two ResolveSpecs for
    one column and crash the second fullmap join pass mid-build; failing at
    ``model_validate`` catches it for every entry point (validate, build_pipeline).
    """
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    data["statement"]["qualifiers"] = [
        {"qualifier": "anatomical_context_qualifier", "method": "value", "encoding": "UBERON:0000061"},
        {"qualifier": "anatomical_context_qualifier", "method": "column", "encoding": "A"},
    ]
    with pytest.raises(ValidationError) as exc_info:
        Tcode.model_validate(  # pyright: ignore
            {**data, "config": fixtures_path / "minimal_section.yaml", "store": Path("/tmp/sectionhash_dup_qualifier.parquet")}
        )
    assert "qualifier-duplicated" in str(exc_info.value)


def test_tcode_collect_emits_single_resolve_batch_for_all_node_columns(fixtures_path: Path) -> None:
    """tcode collect emits exactly one resolve_batch op covering subject/object/qualifiers."""
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    store: Path = Path("/tmp/sectionhash_batch.parquet")
    data["statement"]["subject"]["taxon"] = 9606
    data["statement"]["object"]["prioritize"] = ["Gene"]
    data["statement"]["qualifiers"] = [
        {"qualifier": "disease_context_qualifier", "method": "value", "encoding": "MONDO:0000001", "avoid": ["Gene"]},
        {"qualifier": "anatomical_context_qualifier", "method": "value", "encoding": "UBERON:0000061"},
    ]

    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {**data, "config": fixtures_path / "minimal_section.yaml", "store": store}
    )

    collected: list[tuple[Any, tuple[Any]]] = tcode_model.collect(Path("/tmp/fullmap.redb"))  # pyright: ignore
    batch_ops: list[tuple[Any, tuple[Any]]] = [op for op in collected if op[0].__name__ == "resolve_batch"]

    assert len(batch_ops) == 1
    specs: list[ResolveSpec] = batch_ops[0][1][0]
    assert [spec.col for spec in specs] == ["subject", "object", "disease_context_qualifier", "anatomical_context_qualifier"]
    assert specs[0].taxon == "9606"
    assert specs[1].prioritize == [Categories.GENE]
    assert specs[2].avoid == [Categories.GENE]


def test_tcode_collect_audits_follow_single_resolve_batch_with_qualifiers(fixtures_path: Path) -> None:
    """tcode collect runs every node column's QC audit after the single resolve_batch op."""
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    store: Path = Path("/tmp/sectionhash_batch_qc.parquet")
    data["statement"]["qualifiers"] = [{"qualifier": "anatomical_context_qualifier", "method": "value", "encoding": "UBERON:0000061"}]

    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {**data, "config": fixtures_path / "minimal_section.yaml", "store": store, "qc": True}
    )

    collected: list[tuple[Any, tuple[Any]]] = tcode_model.collect(Path("/tmp/fullmap.redb"))  # pyright: ignore
    batch_idx: int = next(i for i, op in enumerate(collected) if op[0].__name__ == "resolve_batch")
    audit_ops: list[tuple[int, tuple[Any, tuple[Any]]]] = [(i, op) for i, op in enumerate(collected) if op[0].__name__ == "fullmap_audit"]

    assert [op[1][0] for _, op in audit_ops] == ["subject", "object", "anatomical_context_qualifier"]
    assert all(i > batch_idx for i, _ in audit_ops)


def test_tcode_collect_excludes_nullable_qualifier_from_audit(fixtures_path: Path) -> None:
    """QC audit skips a nullable qualifier column (its nulls are expected, not resolution errors)."""
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    store: Path = Path("/tmp/sectionhash_nullable_audit.parquet")
    data["statement"]["qualifiers"] = [
        {"qualifier": "disease_context_qualifier", "method": "column", "encoding": "C", "nullable": True},
        {"qualifier": "anatomical_context_qualifier", "method": "column", "encoding": "D"},
    ]

    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {**data, "config": fixtures_path / "minimal_section.yaml", "store": store, "qc": True}
    )

    collected: list[tuple[Any, tuple[Any]]] = tcode_model.collect(Path("/tmp/fullmap.redb"))  # pyright: ignore
    audit_cols: list[str] = [op[1][0] for op in collected if op[0].__name__ == "fullmap_audit"]

    assert audit_cols == ["subject", "object", "anatomical_context_qualifier"]
    assert "disease_context_qualifier" not in audit_cols


def test_tcode_collect_edge_ops_follow_resolve_batch(fixtures_path: Path) -> None:
    """tcode collect runs predicate/edge_category after the single resolve_batch op."""
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    store: Path = Path("/tmp/sectionhash_edge_order.parquet")
    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {**data, "config": fixtures_path / "minimal_section.yaml", "store": store}
    )

    collected: list[tuple[Any, tuple[Any]]] = tcode_model.collect(Path("/tmp/fullmap.redb"))  # pyright: ignore
    batch_idx: int = next(i for i, op in enumerate(collected) if op[0].__name__ == "resolve_batch")
    predicate_idx: int = next(i for i, op in enumerate(collected) if op[0].__name__ == "value" and op[1][0] == "predicate")
    edge_category_idx: int = next(i for i, op in enumerate(collected) if op[0].__name__ == "edge_category")

    assert batch_idx < predicate_idx
    assert batch_idx < edge_category_idx


def test_tcode_collect_passes_category_override_to_edge_category(fixtures_path: Path) -> None:
    """A statement-level category_override reaches the edge_category op as a biolink: CURIE map."""
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    data["statement"]["category_override"] = {"Disease": "EntityToDiseaseAssociation"}
    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {**data, "config": fixtures_path / "minimal_section.yaml", "store": Path("/tmp/sectionhash_override.parquet")}
    )

    collected: list[tuple[Any, tuple[Any]]] = tcode_model.collect(Path("/tmp/fullmap.redb"))  # pyright: ignore
    op: tuple[Any, tuple[Any]] = next(op for op in collected if op[0].__name__ == "edge_category")

    assert op[1] == ("biolink:related_to", {"Disease": "biolink:EntityToDiseaseAssociation"})


def test_tcode_collect_passes_local_path_to_csv_reader(fixtures_path: Path) -> None:
    """tcode collect passes the local source path through to the csv reader."""
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    store: Path = Path("/tmp/sectionhash.parquet")
    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {**data, "config": fixtures_path / "minimal_section.yaml", "store": store}
    )

    collected: list[tuple[Any, tuple[Any]]] = tcode_model.collect(Path("/tmp/fullmap.redb"))  # pyright: ignore
    csv_ops: list[tuple[Any, tuple[Any]]] = [op for op in collected if op[0].__name__ == "csv"]

    assert csv_ops[0][1] == (tcode_model.source.local, tcode_model.source.delimiter)  # pyright: ignore


def test_head_caps_at_n_rows() -> None:
    """head keeps min(n, height) rows, never more than the source height."""
    small: pl.DataFrame = head(pl.LazyFrame({"a": [1, 2, 3]}), n=HEAD_ROWS).collect()
    big: pl.DataFrame = head(pl.LazyFrame({"a": list(range(10))}), n=HEAD_ROWS).collect()

    assert small.height == 3
    assert big.height == 5


def test_head_samples_subset_of_source() -> None:
    """head returns a random subset of the source rows, never inventing or exceeding them."""
    source: list[int] = list(range(10))
    sampled: pl.DataFrame = head(pl.LazyFrame({"a": source}), n=HEAD_ROWS).collect()

    assert sampled.height == HEAD_ROWS
    assert set(sampled["a"].to_list()) <= set(source)

    # A frame shorter than n is returned whole (never sampled beyond its height).
    short: pl.DataFrame = head(pl.LazyFrame({"a": [7, 8, 9]}), n=HEAD_ROWS).collect()
    assert short.height == 3
    assert set(short["a"].to_list()) == {7, 8, 9}


def test_tcode_collect_omits_head_by_default(fixtures_path: Path) -> None:
    """tcode collect omits the head op unless head mode is requested."""
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    store: Path = Path("/tmp/sectionhash.parquet")
    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {**data, "config": fixtures_path / "minimal_section.yaml", "store": store}
    )

    collected: list[tuple[Any, tuple[Any]]] = tcode_model.collect(Path("/tmp/fullmap.redb"))  # pyright: ignore
    names: list[str] = [op[0].__name__ for op in collected]

    assert "head" not in names


def test_tcode_collect_inserts_head_after_row_filters_before_resolve_when_head(fixtures_path: Path) -> None:
    """head op sits after idx/row-filters and before resolve_batch so resolve only sees HEAD_ROWS rows."""
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    store: Path = Path("/tmp/sectionhash_head.parquet")
    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {**data, "config": fixtures_path / "minimal_section.yaml", "store": store, "head": True}
    )

    collected: list[tuple[Any, tuple[Any]]] = tcode_model.collect(Path("/tmp/fullmap.redb"))  # pyright: ignore
    names: list[str] = [op[0].__name__ for op in collected]

    head_idx: int = names.index("head")
    resolve_idx: int = names.index("resolve_batch")
    idx_idx: int = names.index("idx")
    annotations_idx: int = next(i for i, op in enumerate(collected) if op[0].__name__ == "column" and op[1][0].startswith("original_"))

    assert idx_idx < head_idx < resolve_idx
    assert head_idx < annotations_idx
    assert collected[head_idx][1] == (HEAD_ROWS,)


def test_publication_curie_pmc() -> None:
    """publication_curie uses PMCID namespace for PubMed central."""
    assert lib.publication_curie("PMC", "PMC1234567") == "PMCID:PMC1234567"


def test_publication_curie_pubmed() -> None:
    """publication_curie uses repo namespace for non PMC repositories."""
    assert lib.publication_curie("PMID", "11708054") == "PMID:11708054"


def test_infores_screaming_snake() -> None:
    """infores lower kebab cases a screaming snake graph name with infores prefix."""
    assert infores("MULTIOMICS_KG") == "infores:multiomics-kg"


def test_infores_single_and_tutorial() -> None:
    """infores handles single word and tutorial graph names."""
    assert infores("TUTORIAL_KG") == "infores:tutorial-kg"
    assert infores("CHEMBL") == "infores:chembl"


def test_upstream_resource_ids_pmc() -> None:
    """upstream_resource_ids uses PubMed central InfoRes for PMC repositories."""
    assert lib.upstream_resource_ids(Repositories.PUBMED_CENTRAL) == ["infores:pubmed-central"]


def test_upstream_resource_ids_pubmed() -> None:
    """upstream_resource_ids uses PubMed InfoRes for PMID repositories."""
    assert lib.upstream_resource_ids(Repositories.PUBMED) == ["infores:pubmed"]


def test_tcode_collect_nests_upstream_resource_ids_in_sources(fixtures_path: Path) -> None:
    """Upstream resource IDs reach output inside ``sources``, never flat on the edge.

    ``upstream_resource_ids`` has ``domain: retrieval source`` in Biolink, so a
    top-level column would fail validation with ``extra_forbidden``.
    """
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    store: Path = Path("/tmp/sectionhash.parquet")
    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {**data, "config": fixtures_path / "minimal_section.yaml", "store": store, "name": "GRAPH_KG", "infores": "infores:graph-kg"}
    )

    collected: list[tuple[Any, tuple[Any]]] = tcode_model.collect(Path("/tmp/fullmap.redb"))  # pyright: ignore
    assert [op for op in collected if len(op[1]) > 0 and op[1][0] == "upstream_resource_ids"] == []

    source_ops: list[tuple[Any, tuple[Any]]] = [op for op in collected if op[0] is retrieval_sources]
    assert len(source_ops) == 1
    out: pl.DataFrame = source_ops[0][0](pl.LazyFrame({"subject": ["A"]}), *source_ops[0][1]).collect()
    sources: list[dict[str, Any]] = out["sources"].to_list()[0]
    primary: dict[str, Any] = next(s for s in sources if s["resource_role"] == "primary_knowledge_source")
    assert primary["upstream_resource_ids"] == ["infores:pubmed-central"]
    assert {s["resource_id"] for s in sources if s["resource_role"] == "supporting_data_source"} == {"infores:pubmed-central"}
    assert all("id" not in s for s in sources)


def test_normalize_category_list_with_biolink_prefix() -> None:
    """normalize wraps category in a list and ensures biolink: prefix."""
    edges: pl.LazyFrame = pl.DataFrame(
        {
            "subject": ["CURIE:1", "CURIE:2"],
            "subject_name": ["Gene A", "Protein B"],
            "subject_category": ["Gene", "biolink:Protein"],
            "subject_taxon": ["NCBITaxon:9606", "NCBITaxon:9606"],
            "subject_source": ["HGNC", "HGNC"],
            "subject_source_version": ["1", "1"],
        }
    ).lazy()
    nodes, _ = lib.normalize(edges, "subject")
    result: list[Any] = sorted(nodes.collect()["category"].to_list())
    assert result == [["biolink:Gene"], ["biolink:Protein"]]


def test_normalize_category_null_stays_null() -> None:
    """normalize keeps null categories null for strip_nulls removal."""
    edges: pl.LazyFrame = pl.DataFrame(
        {
            "subject": ["CURIE:1"],
            "subject_name": ["Gene A"],
            "subject_category": pl.Series([None], dtype=pl.String),
            "subject_taxon": ["NCBITaxon:9606"],
            "subject_source": ["HGNC"],
            "subject_source_version": ["1"],
        }
    ).lazy()
    nodes, _ = lib.normalize(edges, "subject")
    result: list[Any] = nodes.collect()["category"].to_list()
    assert result == [None]


def test_tcode_collect_does_not_schedule_species_context_derivation(fixtures_path: Path) -> None:
    """Taxon resolution remains available without creating an edge qualifier operation."""
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {**data, "config": fixtures_path / "minimal_section.yaml", "store": Path("/tmp/no_species_context.parquet")}
    )

    collected: list[tuple[Any, tuple[Any]]] = tcode_model.collect(Path("/tmp/fullmap.redb"))  # pyright: ignore

    assert "derive_species_context" not in [op[0].__name__ for op in collected]


def test_disabled_species_context_is_dropped_before_study_or_supporting_text() -> None:
    """Legacy/direct frames cannot relocate the disabled field into study text or supporting text."""
    lf: pl.LazyFrame = pl.DataFrame({"subject": ["A"], "species_context_qualifier": ["NCBITaxon:9606"]}).lazy()

    study_frame: pl.DataFrame = lib.inline_supporting_study(lf, "study", None).collect()
    folded_frame: pl.DataFrame = fold_unknown_to_supporting_text(lf).collect()

    assert "species_context_qualifier" not in study_frame.columns
    assert "species_context_qualifier" not in json.dumps(study_frame.to_dicts())
    assert "species_context_qualifier" not in folded_frame.columns
    assert "species_context_qualifier" not in json.dumps(folded_frame.to_dicts())


def test_tcode_collect_emits_primary_sources_entry_with_explicit_infores(fixtures_path: Path) -> None:
    """tcode collect routes the explicit graph infores into `sources`; no flat scalar is emitted."""
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    store: Path = Path("/tmp/sectionhash.parquet")
    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {**data, "config": fixtures_path / "minimal_section.yaml", "store": store, "name": "MULTIOMICS_KG", "infores": "infores:multiomics-kg"}
    )

    collected: list[tuple[Any, tuple[Any]]] = tcode_model.collect(Path("/tmp/fullmap.redb"))  # pyright: ignore
    source_ops: list[tuple[Any, tuple[Any]]] = [op for op in collected if op[0] is retrieval_sources]
    flat_ops: list[tuple[Any, tuple[Any]]] = [
        op for op in collected if op[0].__name__ == "value" and len(op[1]) > 0 and op[1][0] == "primary_knowledge_source"
    ]

    assert len(source_ops) == 1
    assert source_ops[0][1][0] == "infores:multiomics-kg"
    # No flat scalar: retrieval provenance lives only in the nested `sources` list.
    assert flat_ops == []


def test_tcode_collect_omits_sources_without_infores(fixtures_path: Path) -> None:
    """tcode collect omits the retrieval_sources op when no infores is configured (validate path)."""
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    store: Path = Path("/tmp/sectionhash.parquet")
    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {**data, "config": fixtures_path / "minimal_section.yaml", "store": store}
    )

    collected: list[tuple[Any, tuple[Any]]] = tcode_model.collect(Path("/tmp/fullmap.redb"))  # pyright: ignore
    source_ops: list[tuple[Any, tuple[Any]]] = [op for op in collected if op[0] is retrieval_sources]

    assert source_ops == []


def test_tcode_collect_manual_provenance_overrides_auto_sources(fixtures_path: Path) -> None:
    """manual provenance overrides upstream/publication/KL/AT; PKS still derives from graph infores."""
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    data["provenance"] = {
        "override": {
            "upstream_resource_ids": ["infores:upstream-source"],
            "publications": ["PMCID:PMC9999999"],
            "knowledge_level": "knowledge_assertion",
            "agent_type": "manual_agent",
        }
    }
    store: Path = Path("/tmp/sectionhash.parquet")
    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {**data, "config": fixtures_path / "minimal_section.yaml", "store": store, "name": "GRAPH_KG", "infores": "infores:graph-source"}
    )

    collected: list[tuple[Any, tuple[Any]]] = tcode_model.collect(Path("/tmp/fullmap.redb"))  # pyright: ignore
    values: dict[str, object] = {str(op[1][0]): op[1][1] for op in collected if op[0].__name__ == "value" and len(op[1]) >= 2}
    pub_ops = [op for op in collected if op[0] is publications]

    # The primary `sources` entry still derives from the graph infores; the override
    # cannot replace it.
    source_args: tuple[Any, ...] = next(op[1] for op in collected if op[0] is retrieval_sources)
    assert source_args[0] == "infores:graph-source"
    # Manual upstream infores reach output nested in `sources`, not flat on the edge.
    assert "upstream_resource_ids" not in values
    assert source_args[1] == ["infores:upstream-source"]
    assert values["knowledge_level"] == "knowledge_assertion"
    assert values["agent_type"] == "manual_agent"
    assert pub_ops[0][1] == (["PMCID:PMC9999999"],)


def test_tcode_collect_uses_graph_infores_when_no_section_override(fixtures_path: Path) -> None:
    """the explicit graph infores is the edge primary knowledge source verbatim."""
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    store: Path = Path("/tmp/sectionhash.parquet")
    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {**data, "config": fixtures_path / "minimal_section.yaml", "store": store, "name": "GRAPH_KG", "infores": "infores:custom-graph"}
    )

    collected: list[tuple[Any, tuple[Any]]] = tcode_model.collect(Path("/tmp/fullmap.redb"))  # pyright: ignore
    source_ops = [op for op in collected if op[0] is retrieval_sources]

    assert source_ops[0][1][0] == "infores:custom-graph"


def test_tcode_collect_nests_source_record_urls_in_sources(fixtures_path: Path) -> None:
    """Source record URLs hang off the primary ``RetrievalSource``, not the edge."""
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    store: Path = Path("/tmp/sectionhash.parquet")
    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {**data, "config": fixtures_path / "minimal_section.yaml", "store": store, "name": "GRAPH_KG", "infores": "infores:graph-kg"}
    )

    collected: list[tuple[Any, tuple[Any]]] = tcode_model.collect(Path("/tmp/fullmap.redb"))  # pyright: ignore
    source_ops: list[tuple[Any, tuple[Any]]] = [op for op in collected if op[0] is retrieval_sources]
    url_ops: list[tuple[Any, tuple[Any]]] = [op for op in collected if op[0].__name__ == "value" and len(op[1]) > 0 and op[1][0] == "url"]
    result: pl.DataFrame = source_ops[0][0](pl.LazyFrame({"subject": ["A"]}), *source_ops[0][1]).collect()

    assert len(source_ops) == 1
    assert url_ops == []
    assert "source_record_urls" not in result.columns
    primary: dict[str, Any] = next(s for s in result["sources"].to_list()[0] if s["resource_role"] == "primary_knowledge_source")
    assert primary["source_record_urls"] == ["https://example.com/test.tsv"]
    assert "id" not in primary


def test_tcode_collect_upstream_source_record_urls_rehome_urls(fixtures_path: Path) -> None:
    """``override.upstream_source_record_urls`` leaves the primary bare and attaches URLs to supporting entries.

    The section's ``source.url`` values serve the RIG only in this mode: the primary
    ``sources`` entry (the transforming resource) emits no ``source_record_urls``,
    and each mapped upstream supporting entry carries its own dataset URLs.
    """
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    data["provenance"] = {
        "override": {
            "upstream_resource_ids": ["infores:upstream-source", "infores:other-source"],
            "upstream_source_record_urls": {"infores:upstream-source": ["https://example.org/dataset"]},
        }
    }
    store: Path = Path("/tmp/sectionhash.parquet")
    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {**data, "config": fixtures_path / "minimal_section.yaml", "store": store, "name": "GRAPH_KG", "infores": "infores:graph-kg"}
    )

    collected: list[tuple[Any, tuple[Any]]] = tcode_model.collect(Path("/tmp/fullmap.redb"))  # pyright: ignore
    source_ops: list[tuple[Any, tuple[Any]]] = [op for op in collected if op[0] is retrieval_sources]
    assert len(source_ops) == 1
    # The mapping is forwarded as the fourth op arg.
    source_args: tuple[Any, ...] = source_ops[0][1]  # pyright: ignore[reportAssignmentType]
    assert source_args[3] == {"infores:upstream-source": ["https://example.org/dataset"]}

    result: pl.DataFrame = source_ops[0][0](pl.LazyFrame({"subject": ["A"]}), *source_ops[0][1]).collect()
    sources: list[dict[str, Any]] = result["sources"].to_list()[0]
    primary: dict[str, Any] = next(s for s in sources if s["resource_role"] == "primary_knowledge_source")
    assert primary["resource_id"] == "infores:graph-kg"
    assert primary["source_record_urls"] is None
    assert primary["upstream_resource_ids"] == ["infores:upstream-source", "infores:other-source"]
    by_resource: dict[str, dict[str, Any]] = {s["resource_id"]: s for s in sources if s["resource_role"] == "supporting_data_source"}
    # The mapped upstream carries its URLs; the unmapped one stays bare.
    assert by_resource["infores:upstream-source"]["source_record_urls"] == ["https://example.org/dataset"]
    assert by_resource["infores:other-source"]["source_record_urls"] is None


def test_tcode_collect_explicit_sources_override_replaces_derivation(fixtures_path: Path) -> None:
    """``override.sources`` is forwarded as the op's ``explicit`` arg and emitted verbatim, in order."""
    template: list[dict[str, Any]] = [
        {
            "resource_id": "infores:multiomics-drugapprovals",
            "resource_role": "aggregator_knowledge_source",
            "upstream_resource_ids": ["infores:dailymed", "infores:faers"],
            "source_record_urls": ["https://db.systemsbiology.net/gestalt/cgi-pub/KGinfo.pl?id={edge_id}"],
        },
        {"resource_id": "infores:faers", "resource_role": "primary_knowledge_source"},
        {"resource_id": "infores:dailymed", "resource_role": "supporting_data_source"},
    ]
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    data["provenance"] = {"override": {"sources": template}}
    store: Path = Path("/tmp/sectionhash.parquet")
    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {**data, "config": fixtures_path / "minimal_section.yaml", "store": store, "name": "MULTIOMICS_KG", "infores": "infores:multiomics-kg"}
    )

    collected: list[tuple[Any, tuple[Any]]] = tcode_model.collect(Path("/tmp/fullmap.redb"))  # pyright: ignore
    source_ops: list[tuple[Any, tuple[Any]]] = [op for op in collected if op[0] is retrieval_sources]
    assert len(source_ops) == 1
    # The template is forwarded (with unset optional fields dropped) as the fifth op arg.
    source_args: tuple[Any, ...] = source_ops[0][1]  # pyright: ignore[reportAssignmentType]
    assert source_args[4] == template

    result: pl.DataFrame = source_ops[0][0](pl.LazyFrame({"subject": ["A"]}), *source_args).collect()
    sources: list[dict[str, Any]] = result["sources"].to_list()[0]
    assert [s["resource_id"] for s in sources] == ["infores:multiomics-drugapprovals", "infores:faers", "infores:dailymed"]
    assert [s["resource_role"] for s in sources] == ["aggregator_knowledge_source", "primary_knowledge_source", "supporting_data_source"]
    # `resource_id` is the sole identifier on each entry (no `id` mirror, #115).
    assert all("id" not in s for s in sources)
    # The `{edge_id}` placeholder stays unresolved at this stage: the edge id is a
    # content hash assigned by the final dedup stage, after subgraphs are written.
    assert sources[0]["source_record_urls"] == ["https://db.systemsbiology.net/gestalt/cgi-pub/KGinfo.pl?id={edge_id}"]
    assert sources[0]["upstream_resource_ids"] == ["infores:dailymed", "infores:faers"]
    # Entries without urls/upstream emit typed nulls, like the default path.
    assert sources[1]["source_record_urls"] is None
    assert sources[1]["upstream_resource_ids"] is None
    assert sources[2]["source_record_urls"] is None


def test_resolve_edge_id_placeholders(tmp_path: Path) -> None:
    """The post-dedup sweep substitutes each record's own ``id`` for ``{edge_id}``, edges file only."""
    edges: Path = tmp_path / "graph.edges.ndjson"
    with_placeholder: dict[str, Any] = {
        "id": "uuid-1",
        "subject": "CURIE:1",
        "sources": [
            {
                "id": "infores:multiomics-drugapprovals",
                "resource_id": "infores:multiomics-drugapprovals",
                "resource_role": "aggregator_knowledge_source",
                "source_record_urls": ["https://db.systemsbiology.net/gestalt/cgi-pub/KGinfo.pl?id={edge_id}", "https://example.org/static"],
            }
        ],
    }
    without_placeholder: dict[str, Any] = {
        "id": "uuid-2",
        "subject": "CURIE:2",
        "sources": [{"id": "infores:faers", "resource_id": "infores:faers", "resource_role": "primary_knowledge_source"}],
    }
    plain_without: str = json.dumps(without_placeholder)
    edges.write_text(json.dumps(with_placeholder) + "\n" + plain_without + "\n", encoding="utf-8")

    lib._resolve_edge_id_placeholders(edges)

    lines: list[str] = edges.read_text(encoding="utf-8").splitlines()
    resolved: dict[str, Any] = json.loads(lines[0])
    assert resolved["sources"][0]["source_record_urls"] == [
        "https://db.systemsbiology.net/gestalt/cgi-pub/KGinfo.pl?id=uuid-1",
        "https://example.org/static",
    ]
    assert resolved["id"] == "uuid-1"
    # Lines without the marker pass through untouched.
    assert lines[1] == plain_without
    assert not (tmp_path / "graph.edges.ndjson.placeholder.tmp").exists()

    # A file without the marker is left byte-identical.
    clean: Path = tmp_path / "clean.edges.ndjson"
    content: str = plain_without + "\n"
    clean.write_text(content, encoding="utf-8")
    lib._resolve_edge_id_placeholders(clean)
    assert clean.read_text(encoding="utf-8") == content


def test_tcode_original_value_before_regex_for_columns(fixtures_path: Path) -> None:
    """tcode captures original value before regex for column encoded nodes."""
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    store: Path = Path("/tmp/sectionhash.parquet")
    data["statement"]["subject"] = {"method": "column", "encoding": "A", "regex": [{"pattern": "\\s+", "replacement": " "}]}
    data["statement"]["object"] = {"method": "column", "encoding": "B"}

    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {**data, "config": fixtures_path / "minimal_section.yaml", "store": store}
    )

    collected: list[tuple[Any, tuple[Any]]] = tcode_model.collect(Path("/tmp/fullmap.redb"))  # pyright: ignore
    targets: list[str] = [op[1][0] for op in collected if op[0].__name__ == "column" and len(op[1]) > 1]
    assert "original_subject" in targets
    assert "original_object" in targets

    lit_idx: int = next(i for i, op in enumerate(collected) if len(op[1]) > 0 and op[1][0] == "original_subject")
    regex_idx: int = next(i for i, op in enumerate(collected) if op[0].__name__ == "regex" and len(op[1]) > 0 and op[1][0] == "subject")
    assert lit_idx < regex_idx


def test_tcode_original_value_present_for_value_encoding(fixtures_path: Path) -> None:
    """tcode emits original value for value encoded nodes."""
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    store: Path = Path("/tmp/sectionhash.parquet")
    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {**data, "config": fixtures_path / "minimal_section.yaml", "store": store}
    )

    collected: list[tuple[Any, tuple[Any]]] = tcode_model.collect(Path("/tmp/fullmap.redb"))  # pyright: ignore
    targets: list[str] = [op[1][0] for op in collected if op[0].__name__ == "column" and len(op[1]) > 1]
    assert "original_subject" in targets
    assert "original_object" in targets


def test_resolve_many_skips_qc(monkeypatch: Any, tmp_path: Path) -> None:
    """resolve_many skips QC when disabled."""
    calls: list[tuple[Any, ...]] = []

    def fake_resolve(lf: pl.LazyFrame, col: str, db: Path, **kwargs: Any) -> pl.LazyFrame:
        calls.append(("resolve", col, db, kwargs))
        return lf

    def fake_qc(lf: pl.LazyFrame, col: str, section_hash: str, config_file: str, out: str = "passed", log: bool = True) -> pl.LazyFrame:
        calls.append(("qc", col, section_hash, config_file, out, log))
        return lf

    monkeypatch.setattr(lib, "resolve", fake_resolve)
    monkeypatch.setattr(lib, "fullmap_audit", fake_qc)

    result: list[dict[str, Any]] = lib.resolve_many("subject", ["BRCA1", "TP53"], tmp_path, qc=False)

    assert len(result) == 2
    assert (
        "resolve",
        "subject",
        tmp_path / "data" / "fullmap.redb",
        {"taxon": None, "prioritize": None, "avoid": None, "column_context": True},
    ) in calls
    assert not any(call[0] == "qc" for call in calls)


def test_resolve_many_runs_qc(monkeypatch: Any, tmp_path: Path) -> None:
    """resolve_many runs QC with logging when enabled."""
    calls: list[tuple[Any, ...]] = []

    def fake_resolve(lf: pl.LazyFrame, col: str, db: Path, **kwargs: Any) -> pl.LazyFrame:
        calls.append(("resolve", col, db, kwargs))
        return lf

    def fake_qc(lf: pl.LazyFrame, col: str, section_hash: str, config_file: str, out: str = "passed", log: bool = True) -> pl.LazyFrame:
        calls.append(("qc", col, section_hash, config_file, out, log))
        return lf.with_columns(pl.lit("YES").alias(out))

    monkeypatch.setattr(lib, "resolve", fake_resolve)
    monkeypatch.setattr(lib, "fullmap_audit", fake_qc)

    result: list[dict[str, Any]] = lib.resolve_many("subject", ["BRCA1"], tmp_path, qc=True)

    assert result == [{"subject": "brca1", "original_subject": "BRCA1", "subject_two": "brca1", "passed": "YES"}]
    assert ("qc", "subject", "", "", "passed", True) in calls


def test_resolve_many_accepts_direct_fullmap_file(monkeypatch: Any, tmp_path: Path) -> None:
    """resolve_many accepts a direct fullmap redb file path."""
    calls: list[Path] = []
    db: Path = tmp_path / "fullmap.redb"
    db.touch()

    def fake_resolve(lf: pl.LazyFrame, col: str, db_path: Path, **kwargs: Any) -> pl.LazyFrame:
        calls.append(db_path)
        return lf

    monkeypatch.setattr(lib, "resolve", fake_resolve)

    result: list[dict[str, Any]] = lib.resolve_many("subject", ["BRCA1"], db, qc=False)

    assert calls == [db]
    assert result == [{"subject": "brca1", "original_subject": "BRCA1", "subject_two": "brca1"}]


def test_sig_prefers_exact_p_value_column() -> None:
    """sig uses exact "p_value" column when present alongside other P-Value columns."""
    lf: pl.LazyFrame = pl.DataFrame({"p_value": [0.01, 0.1], "adjusted_p_value": [0.5, 0.5]}).lazy()
    result: pl.DataFrame = lib.sig(lf).collect()
    assert list(result["statistical_significance_qualifier"]) == ["strongly_significant", "suggestive"]


def test_sig_uses_non_exact_p_value_column() -> None:
    """sig falls back to non-exact P-Value column when no exact match."""
    lf: pl.LazyFrame = pl.DataFrame({"adjusted_p_value": [0.01, 0.1]}).lazy()
    result: pl.DataFrame = lib.sig(lf).collect()
    assert list(result["statistical_significance_qualifier"]) == ["strongly_significant", "suggestive"]


def test_sig_picks_closest_non_exact_match() -> None:
    """sig prefers a raw p-value bucket over an adjusted one when both are non-exact."""
    lf: pl.LazyFrame = pl.DataFrame({"log_p_value": [0.01], "adjusted_p_value_corrected": [0.5]}).lazy()
    result: pl.DataFrame = lib.sig(lf).collect()
    # "log_p_value" -> raw p_value bucket; "adjusted_p_value_corrected" -> adjusted bucket.
    # The raw bucket is preferred, so 0.01 -> strongly_significant (not 0.5 -> not_significant).
    # Before the underscore-anchoring fix "adjusted_p_value_corrected" landed in the RAW bucket
    # too, and this passed only because the shorter name won the fuzzy tiebreak.
    assert list(result["statistical_significance_qualifier"]) == ["strongly_significant"]


# sig omits the qualifier column when no P-Value column exists (biolink class rule)
# edges are retained; the qualifier is simply absent (not set)
def test_sig_omits_qualifier_with_no_p_value_column() -> None:
    lf: pl.LazyFrame = pl.DataFrame({"gene": ["BRCA1", "TP53"]}).lazy()
    result: pl.DataFrame = lib.sig(lf).collect()
    assert "statistical_significance_qualifier" not in result.columns
    assert result.height == 2


def test_sig_marks_null_as_null_qualifier() -> None:
    """sig emits null (not UNSURE) for null P-Values; edges are retained."""
    lf: pl.LazyFrame = pl.DataFrame({"p_value": [None, 0.01, 0.1]}).lazy()
    result: pl.DataFrame = lib.sig(lf).collect()
    assert list(result["statistical_significance_qualifier"]) == [None, "strongly_significant", "suggestive"]


def test_sig_marks_suggestive_band() -> None:
    """sig maps the 0.05 < p <= 0.10 band to suggestive."""
    lf: pl.LazyFrame = pl.DataFrame({"p_value": [0.01, 0.07, 0.1]}).lazy()
    result: pl.DataFrame = lib.sig(lf).collect()
    assert list(result["statistical_significance_qualifier"]) == ["strongly_significant", "suggestive", "suggestive"]


def test_sig_very_strongly_significant_band() -> None:
    """sig maps p <= 0.001 to very_strongly_significant (boundary included)."""
    lf: pl.LazyFrame = pl.DataFrame({"p_value": [1e-8, 0.001, 0.002]}).lazy()
    result: pl.DataFrame = lib.sig(lf).collect()
    assert list(result["statistical_significance_qualifier"]) == ["very_strongly_significant", "very_strongly_significant", "strongly_significant"]


@pytest.mark.parametrize(
    "name",
    [
        "negative log p value",  # mokg-v12 HOYER1 spelling
        "negative log10 p value",
        "-log10(p)",
        "neg log10 q value",
        "Negated Log P Value",
        "-LOG10 P VALUE",
    ],
)
def test_is_neglog10_column_matches_negation_spellings(name: str) -> None:
    assert is_neglog10_column(name) is True


@pytest.mark.parametrize(
    "name",
    [
        "p value",
        "log p value",  # no negation marker: sign convention ambiguous
        "log10 p value",
        "adjusted p value",
        "negatively correlated",
        "regulation",
        "negative log protein",  # [pq] must be a complete token, not a word prefix
        "negative log qwerty",
    ],
)
def test_is_neglog10_column_rejects_unmarked_or_unrelated(name: str) -> None:
    assert is_neglog10_column(name) is False


def test_coerce_pvalue_columns_unlogs_negative_log_p_value() -> None:
    """-log10(p)=8 means p=1e-8: the slot receives the recovered p-value, nulls stay null."""
    lf: pl.LazyFrame = pl.DataFrame({"negative log p value": ["8.0", "0.0522071", None]}).lazy()
    out: pl.DataFrame = coerce_pvalue_columns(lf).collect()
    assert out["p_value"][0] == pytest.approx(1e-8)
    assert out["p_value"][1] == pytest.approx(10**-0.0522071)
    assert out["p_value"][2] is None


def test_coerce_pvalue_columns_unlog_underflows_to_zero() -> None:
    """Observed -log10 scores reach ~864; 10**-864 underflows float64 to 0.0."""
    lf: pl.LazyFrame = pl.DataFrame({"negative log10 p value": [864.066614351]}).lazy()
    out: pl.DataFrame = coerce_pvalue_columns(lf).collect()
    assert out["p_value"][0] == 0.0


def test_coerce_pvalue_columns_prefers_raw_over_neglog10_alias() -> None:
    """A raw p-value column beats a -log10 alias, and the alias is dropped.

    The alias is the same statistic re-expressed, so leaving it would fold
    ``"negative log10 p value: 8.0"`` into supporting_text beside ``p_value`` 0.03 --
    two numbers for one measurement that read as a contradiction.
    """
    lf: pl.LazyFrame = pl.DataFrame({"p value": [0.03], "negative log10 p value": [8.0]}).lazy()
    out: pl.DataFrame = coerce_pvalue_columns(lf).collect()
    assert out.columns == ["p_value"]
    assert out["p_value"][0] == pytest.approx(0.03)


def test_coerce_pvalue_columns_raw_beats_neglog10_despite_short_name() -> None:
    """A short raw name ("P", fuzz-score 0 against "p value") must still beat a
    long -log10 alias (score ~48) — fuzzy ranking never sees the alias."""
    lf: pl.LazyFrame = pl.DataFrame({"P": [0.03], "negative log10 p value": [8.0]}).lazy()
    out: pl.DataFrame = coerce_pvalue_columns(lf).collect()
    assert out.columns == ["p_value"]
    assert out["p_value"][0] == pytest.approx(0.03)


def test_coerce_pvalue_columns_raw_fdr_beats_neglog10_q_alias() -> None:
    """ "FDR" also loses the raw/alias contest against "negative log10 q value"."""
    lf: pl.LazyFrame = pl.DataFrame({"FDR": [0.02], "negative log10 q value": [3.0]}).lazy()
    out: pl.DataFrame = coerce_pvalue_columns(lf).collect()
    assert out.columns == ["adjusted_p_value"]
    assert out["adjusted_p_value"][0] == pytest.approx(0.02)


def test_sig_raw_beats_neglog10_despite_short_name() -> None:
    """sig applies the same raw-beats-alias rule: a bare "P" column wins over a
    -log10 alias, so the band comes from the raw 0.03 (significant)."""
    lf: pl.LazyFrame = pl.DataFrame({"P": [0.03], "negative log10 p value": [8.0]}).lazy()
    result: pl.DataFrame = lib.sig(lf).collect()
    assert list(result["statistical_significance_qualifier"]) == ["significant"]


def test_coerce_pvalue_columns_unlogs_neglog10_q_value_into_adjusted() -> None:
    lf: pl.LazyFrame = pl.DataFrame({"negative log10 q value": [3.0]}).lazy()
    out: pl.DataFrame = coerce_pvalue_columns(lf).collect()
    assert out["adjusted_p_value"][0] == pytest.approx(1e-3)


def test_sig_unlogs_neglog10_source() -> None:
    """Banding un-logs a -log10 score column: 8 -> very strongly significant,
    0 (p=1) -> not significant — the bands no longer invert."""
    lf: pl.LazyFrame = pl.DataFrame({"negative log p value": [8.0, 0.0, 2.0]}).lazy()
    result: pl.DataFrame = lib.sig(lf).collect()
    assert list(result["statistical_significance_qualifier"]) == [
        "very_strongly_significant",
        "not_significant",
        "strongly_significant",  # 10**-2 == 0.01 boundary, inclusive
    ]


def test_sig_significant_band_boundary() -> None:
    """sig maps the 0.01 < p <= 0.05 band to significant (boundary included)."""
    lf: pl.LazyFrame = pl.DataFrame({"p_value": [0.05, 0.06]}).lazy()
    result: pl.DataFrame = lib.sig(lf).collect()
    assert list(result["statistical_significance_qualifier"]) == ["significant", "suggestive"]


def test_sig_not_significant_band() -> None:
    """sig maps p > 0.10 to not_significant."""
    lf: pl.LazyFrame = pl.DataFrame({"p_value": [0.11, 0.5]}).lazy()
    result: pl.DataFrame = lib.sig(lf).collect()
    assert list(result["statistical_significance_qualifier"]) == ["not_significant", "not_significant"]


def test_sig_prefers_raw_p_value_over_adjusted_bucket() -> None:
    """sig derives the qualifier from a raw p-value column, not a co-present adjusted one.

    ``"P"`` classifies as the raw ``p_value`` bucket (bare-P token) and ``"FDR"`` as the
    ``adjusted_p_value`` bucket. The qualifier must follow the raw column: 0.01 maps to
    ``strongly_significant``, whereas the adjusted 0.001 would wrongly yield
    ``very_strongly_significant``.
    """
    lf: pl.LazyFrame = pl.DataFrame({"P": [0.01], "FDR": [0.001]}).lazy()
    result: pl.DataFrame = lib.sig(lf).collect()
    assert list(result["statistical_significance_qualifier"]) == ["strongly_significant"]


def test_sig_canonical_column_wins_over_higher_scoring_alias() -> None:
    """An existing canonical ``p_value`` column wins over a higher-scoring spaced alias.

    Both ``"p_value"`` and ``"p vals"`` land in the raw bucket; the canonical column is
    chosen directly (no fuzzy tiebreak), so banding follows ``p_value``=0.05
    (``significant``) rather than ``p vals``=0.001 (``very_strongly_significant``).
    """
    lf: pl.LazyFrame = pl.DataFrame({"p_value": [0.05], "p vals": [0.001]}).lazy()
    result: pl.DataFrame = lib.sig(lf).collect()
    assert list(result["statistical_significance_qualifier"]) == ["significant"]


def test_sig_excludes_substring_only_non_pvalue_column() -> None:
    """sig uses pvalue_target, not a naive substring, so a look-alike column is ignored.

    ``"xp_value_x"`` contains the literal ``p_value`` substring (the old selector would
    grab it) but ``pvalue_target`` rejects it: the ``p`` is glued to an alphanumeric on
    both sides, so neither the value token nor the bare-P token matches. With no real
    p-value column the qualifier is omitted (Biolink class rule).
    """
    lf: pl.LazyFrame = pl.DataFrame({"xp_value_x": [0.01], "gene": ["BRCA1"]}).lazy()
    result: pl.DataFrame = lib.sig(lf).collect()
    assert "statistical_significance_qualifier" not in result.columns
    assert result.height == 1


def test_drop_not_significant_removes_band_keeps_nulls() -> None:
    """drop_not_significant removes not_significant rows while keeping null qualifiers."""
    lf: pl.LazyFrame = pl.DataFrame(
        {"subject": ["a", "b", "c", "d"], "statistical_significance_qualifier": ["significant", "not_significant", None, "suggestive"]}
    ).lazy()
    result: pl.DataFrame = drop_not_significant(lf).collect()
    assert list(result["subject"]) == ["a", "c", "d"]
    assert "not_significant" not in list(result["statistical_significance_qualifier"])


def test_drop_not_significant_noop_without_column() -> None:
    """drop_not_significant is a no-op when the qualifier column is absent."""
    lf: pl.LazyFrame = pl.DataFrame({"subject": ["a", "b"]}).lazy()
    result: pl.DataFrame = drop_not_significant(lf).collect()
    assert result.shape == (2, 1)
    assert list(result["subject"]) == ["a", "b"]


def test_drop_not_significant_keeps_all_other_bands() -> None:
    """drop_not_significant keeps every band except not_significant."""
    bands: list[str | None] = ["very_strongly_significant", "strongly_significant", "significant", "suggestive", "not_significant", None]
    lf: pl.LazyFrame = pl.DataFrame({"q": bands}).lazy()
    result: pl.DataFrame = drop_not_significant(lf, col="q").collect()
    assert list(result["q"]) == [b for b in bands if b != "not_significant"]


def test_drop_zero_effect_size_removes_zero_keeps_nonzero_and_nulls() -> None:
    """drop_zero_effect_size drops exact zeros while keeping non-zeros and nulls."""
    lf: pl.LazyFrame = pl.DataFrame({"subject": ["a", "b", "c", "d", "e"], "effect_size": [0.0, 1.5, -0.5, None, 0]}).lazy()
    result: pl.DataFrame = drop_zero_effect_size(lf).collect()
    assert list(result["subject"]) == ["b", "c", "d"]
    assert list(result["effect_size"]) == [1.5, -0.5, None]


def test_drop_zero_effect_size_noop_without_column() -> None:
    """drop_zero_effect_size is a no-op when the effect-size column is absent."""
    lf: pl.LazyFrame = pl.DataFrame({"subject": ["a", "b"]}).lazy()
    result: pl.DataFrame = drop_zero_effect_size(lf).collect()
    assert result.shape == (2, 1)
    assert list(result["subject"]) == ["a", "b"]


def test_drop_low_number_of_cases_removes_below_threshold_keeps_at_threshold_and_nulls() -> None:
    """drop_low_number_of_cases drops case counts under 10 while keeping 10+ and nulls."""
    lf: pl.LazyFrame = pl.DataFrame({"subject": ["a", "b", "c", "d", "e"], "number_of_cases": [9, 10, 11, None, 0]}).lazy()
    result: pl.DataFrame = drop_low_number_of_cases(lf).collect()
    assert list(result["subject"]) == ["b", "c", "d"]
    assert list(result["number_of_cases"]) == [10, 11, None]


def test_drop_low_number_of_cases_noop_without_column() -> None:
    """drop_low_number_of_cases is a no-op when the number-of-cases column is absent."""
    lf: pl.LazyFrame = pl.DataFrame({"subject": ["a", "b"]}).lazy()
    result: pl.DataFrame = drop_low_number_of_cases(lf).collect()
    assert result.shape == (2, 1)
    assert list(result["subject"]) == ["a", "b"]


def test_drop_low_number_of_cases_keeps_non_numeric_cells() -> None:
    """drop_low_number_of_cases keeps non-numeric cells (a header row read as data) instead of crashing on a strict cast."""
    # The csv op reads sources with has_header=False, so a TSV's header row flows
    # through as a data row; the strict cast raised InvalidOperationError on it.
    lf: pl.LazyFrame = pl.DataFrame({"subject": ["hdr", "a", "b", "c"], "number_of_cases": ["number_of_cases", "30", "9", None]}).lazy()
    result: pl.DataFrame = drop_low_number_of_cases(lf).collect()
    assert list(result["subject"]) == ["hdr", "a", "c"]


def test_drop_zero_effect_size_keeps_non_numeric_cells() -> None:
    """drop_zero_effect_size keeps non-numeric cells (a header row read as data) instead of crashing on a strict cast."""
    lf: pl.LazyFrame = pl.DataFrame({"subject": ["hdr", "a", "b", "c"], "effect_size": ["effect_size", "0.0", "1.5", None]}).lazy()
    result: pl.DataFrame = drop_zero_effect_size(lf).collect()
    assert list(result["subject"]) == ["hdr", "b", "c"]


def test_numeric_columns_matches_p_value_substring() -> None:
    """numeric_columns matches any column with P value in the name."""
    names: list[str] = ["p_value", "adjusted_p_value", "log_p_value", "subject"]
    result: list[str] = numeric_columns(names)
    assert result == ["p_value", "adjusted_p_value", "log_p_value"]
    assert "subject" not in result


def test_numeric_columns_matches_exact_names() -> None:
    """numeric_columns matches exact effect size, number-of-cases and study size names."""
    names: list[str] = ["effect_size", "number_of_cases", "study_size", "cohort", "sample_size", "relationship_strength"]
    result: list[str] = numeric_columns(names)
    assert "effect_size" in result
    assert "number_of_cases" in result
    assert "study_size" in result
    assert "cohort" not in result
    # Old names are superseded: coercion renames them before clean_numeric/format_numeric run.
    assert "sample_size" not in result
    assert "relationship_strength" not in result
    assert "supporting_study_size" not in result


def test_numeric_columns_case_insensitive() -> None:
    """numeric_columns is case insensitive on the P value substring."""
    names: list[str] = ["P_Value", "P_VALUE"]
    result: list[str] = numeric_columns(names)
    assert result == ["P_Value", "P_VALUE"]


def test_clean_numeric_parses_numeric_and_scientific() -> None:
    """clean_numeric coerces numeric and scientific notation strings to Float64."""
    lf: pl.LazyFrame = pl.DataFrame({"p_value": ["1e-8", "0.05", "450"], "study_size": ["1200", "0.42", "-1.2"]}).lazy()
    result: pl.DataFrame = clean_numeric(lf).collect()
    assert result.schema["p_value"] == pl.Float64
    assert result.schema["study_size"] == pl.Float64
    assert result["p_value"].to_list() == [1e-8, 0.05, 450.0]
    assert result["study_size"].to_list() == [1200.0, 0.42, -1.2]


def test_clean_numeric_nulls_non_numeric() -> None:
    """clean_numeric drops non numeric entries to null."""
    lf: pl.LazyFrame = pl.DataFrame({"p_value": ["1e-8", "N/A", "", "<0.001", "abc"], "effect_size": ["0.85", "n/a", "NULL", "x", "y"]}).lazy()
    result: pl.DataFrame = clean_numeric(lf).collect()
    assert result["p_value"].to_list() == [1e-8, None, None, None, None]
    assert result["effect_size"].to_list() == [0.85, None, None, None, None]


def test_clean_numeric_leaves_non_matching_untouched() -> None:
    """clean_numeric leaves non matching columns untouched."""
    lf: pl.LazyFrame = pl.DataFrame({"subject": ["BRCA1", "TP53"], "assertion_method": ["ANOVA", "t-test"], "p_value": ["0.05", "1e-8"]}).lazy()
    result: pl.DataFrame = clean_numeric(lf).collect()
    assert result.schema["subject"] == pl.String
    assert result.schema["assertion_method"] == pl.String
    assert result.schema["p_value"] == pl.Float64
    assert result["subject"].to_list() == ["BRCA1", "TP53"]
    assert result["assertion_method"].to_list() == ["ANOVA", "t-test"]


def test_clean_numeric_noop_without_numeric_columns() -> None:
    """clean_numeric is a noop when no numeric columns are present."""
    lf: pl.LazyFrame = pl.DataFrame({"subject": ["BRCA1"], "cohort": ["adult"]}).lazy()
    result: pl.DataFrame = clean_numeric(lf).collect()
    assert result.schema["subject"] == pl.String
    assert result.schema["cohort"] == pl.String


def test_clean_numeric_idempotent_on_float64() -> None:
    """clean_numeric is idempotent on already Float64 columns."""
    lf: pl.LazyFrame = pl.DataFrame({"p_value": [1e-8, 0.05]}).lazy()
    once: pl.DataFrame = clean_numeric(lf).collect()
    twice: pl.DataFrame = clean_numeric(once.lazy()).collect()
    assert twice["p_value"].to_list() == [1e-8, 0.05]
    assert twice.schema["p_value"] == pl.Float64


def test_format_numeric_emits_p_values_as_scientific_strings() -> None:
    """P-value columns are emitted as controlled scientific-notation strings.

    Notation is part of the output contract (the tutorial's edge example shows
    ``"p_value":"1.0000e-03"``); Biolink's ``float`` typing is satisfied by Pydantic's
    lax coercion, so the notation control costs no KGX validity. The
    ``numeric_slot_kind`` float short-circuit must never fire for p-value columns.
    """
    lf: pl.LazyFrame = pl.DataFrame({"p_value": ["1e-8", "0.05", "0.001"], "adjusted_p_value": ["0.0001", "0.1", "0.2"]}).lazy()
    result: pl.DataFrame = format_numeric(clean_numeric(lf)).collect()
    assert result["p_value"].to_list() == ["1.0000e-08", "5.0000e-02", "1.0000e-03"]
    assert result["adjusted_p_value"].to_list() == ["1.0000e-04", "1.0000e-01", "2.0000e-01"]
    assert result.schema["p_value"] == pl.String
    assert result.schema["adjusted_p_value"] == pl.String


def test_format_numeric_emits_model_typed_numbers() -> None:
    """format_numeric emits model-typed columns as real JSON numbers.

    biolink-model 4.4.4 types ``effect_size`` ``float`` (PR #1774) and ``study_size``
    ``int`` on the inlined ``Study`` (PR #1770), so both leave the pipeline as real
    numbers -- ``numeric_slot_kind`` reads the typing off the installed model, so no
    controlled-notation string survives for slots the model now owns.
    """
    lf: pl.LazyFrame = pl.DataFrame({"effect_size": ["0.85", "0.42", "0.1234"], "study_size": ["450", "1200", "7"]}).lazy()
    result: pl.DataFrame = format_numeric(clean_numeric(lf)).collect()
    assert result["effect_size"].to_list() == [0.85, 0.42, 0.1234]
    assert result.schema["effect_size"] == pl.Float64
    assert result["study_size"].to_list() == [450, 1200, 7]
    assert result.schema["study_size"] == pl.Int64


def test_format_numeric_nulls_invalid_study_counts() -> None:
    """Fractional, negative, and non-finite study counts become null instead of being rounded."""
    lf: pl.LazyFrame = pl.DataFrame({"study_size": ["0.42", "-1", "1.9", "2", "NaN"]}).lazy()
    result: pl.DataFrame = format_numeric(clean_numeric(lf)).collect()
    assert result["study_size"].to_list() == [None, None, None, 2, None]
    assert result.schema["study_size"] == pl.Int64


def test_format_numeric_emits_number_of_cases_as_int() -> None:
    """number_of_cases leaves the pipeline as a real JSON int, not a TSV-text string.

    biolink-model 4.4.4 types ``number_of_cases`` ``int`` on the disease/phenotype
    ``Association`` classes, and the Rust ``uuid_on_collision: merge`` recompute already
    writes an int union length, so first-wins edges must not ship the raw source string.
    Fractional, negative, non-finite and non-numeric counts become null, like study counts.
    """
    lf: pl.LazyFrame = pl.DataFrame({"number_of_cases": ["1", "25", "0.42", "-3", "abc", None]}).lazy()
    result: pl.DataFrame = format_numeric(clean_numeric(lf)).collect()
    assert result["number_of_cases"].to_list() == [1, 25, None, None, None, None]
    assert result.schema["number_of_cases"] == pl.Int64


def test_format_numeric_preserves_nulls() -> None:
    """format_numeric preserves nulls as null."""
    lf: pl.LazyFrame = pl.DataFrame({"p_value": ["1e-8", "N/A", "0.05"]}).lazy()
    result: pl.DataFrame = format_numeric(clean_numeric(lf)).collect()
    assert result["p_value"].to_list() == ["1.0000e-08", None, "5.0000e-02"]


def test_format_numeric_passes_floats_through() -> None:
    """format_numeric passes model-typed ``float`` columns through as real floats.

    The old ``{:.4g}`` noise-cleaning string branch retired with biolink-model 4.4.4's
    ``float`` typing of ``effect_size``: values ship as JSON numbers verbatim.
    """
    lf: pl.LazyFrame = pl.DataFrame({"effect_size": ["0.85000000001", "0.41999999999"]}).lazy()
    result: pl.DataFrame = format_numeric(clean_numeric(lf)).collect()
    assert result["effect_size"].to_list() == [0.85000000001, 0.41999999999]
    assert result.schema["effect_size"] == pl.Float64


def test_format_numeric_noop_without_numeric_columns() -> None:
    """format_numeric is a noop when no numeric columns are present."""
    lf: pl.LazyFrame = pl.DataFrame({"subject": ["BRCA1"], "cohort": ["adult"]}).lazy()
    result: pl.DataFrame = format_numeric(lf).collect()
    assert result["subject"].to_list() == ["BRCA1"]
    assert result.schema["subject"] == pl.String


def test_format_numeric_nulls_stripped_from_ndjson_rows() -> None:
    """cleaned and formatted null numeric values are stripped from NDJSON rows.

    A zero p-value formats to ``"0.0000e+00"`` — a non-empty string, so it survives
    ``strip_nulls`` instead of vanishing like the pre-#71 bug.
    """
    lf: pl.LazyFrame = pl.DataFrame(
        {"subject": ["BRCA1", "TP53", "EGFR"], "p_value": ["1e-8", "N/A", "0"], "effect_size": ["0.85", "0.42", "1.0"]}
    ).lazy()
    formatted: pl.DataFrame = format_numeric(clean_numeric(lf)).collect()
    rows: list[dict[str, Any]] = [strip_nulls(r) for r in formatted.iter_rows(named=True)]
    assert rows[0] == {"subject": "BRCA1", "p_value": "1.0000e-08", "effect_size": 0.85}
    assert "p_value" not in rows[1]
    assert rows[1]["subject"] == "TP53"
    assert rows[1]["effect_size"] == 0.42
    assert rows[2]["p_value"] == "0.0000e+00"  # zero survives strip_nulls
    assert rows[2]["effect_size"] == 1.0


def test_compile_graph_emits_ndjson(monkeypatch: Any, tmp_path: Path, rig_factory: Any) -> None:
    """compile_graph emits edges and nodes plus a schema-shaped, audited RIG."""
    monkeypatch.chdir(tmp_path)
    sub: Path = tmp_path / "sub.parquet"
    primary: dict[str, Any] = {
        "resource_id": "infores:smoke",
        "resource_role": "primary_knowledge_source",
        "upstream_resource_ids": ["infores:pubmed-central"],
        "source_record_urls": ["https://pmc.ncbi.nlm.nih.gov/bin/table1.xlsx"],
    }
    supporting: dict[str, Any] = {"resource_id": "infores:pubmed-central", "resource_role": "supporting_data_source"}
    pl.DataFrame(
        {
            "subject": ["A", "B"],
            "subject_name": ["Alpha", "Beta"],
            "subject_category": ["gene", "gene"],
            "subject_taxon": [None, None],
            "subject_source": [None, None],
            "subject_source_version": [None, None],
            "subject_pre_resolution": ["A", "B"],
            "object": ["HGNC:1", "HGNC:2"],
            "object_name": ["Xray", "Yankee"],
            "object_category": ["disease", "disease"],
            "object_taxon": [None, None],
            "object_source": [None, None],
            "object_source_version": [None, None],
            "object_pre_resolution": ["X", "Y"],
            "predicate": ["biolink:related_to", "biolink:related_to"],
            "knowledge_level": ["knowledge_assertion", "knowledge_assertion"],
            "agent_type": ["manual_agent", "manual_agent"],
            "primary_knowledge_source": ["infores:smoke", "infores:smoke"],
            "sources": [[primary, supporting], [primary, supporting]],
            "p_value": ["1.0000e-08", "5.0000e-02"],
        }
    ).write_parquet(sub)
    rig = rig_factory(
        tmp_path,
        infores_id="infores:smoke",
        source_info={"description": "Smoke graph"},
        ui_explanation="Custom UI explanation.",
        source_files=["table1.xlsx"],
    )
    lib.compile_graph([sub], "smoke", "1.0.0", rig)
    edges: list[str] = (tmp_path / "smoke_1.0.0.edges.ndjson").read_text().strip().splitlines()
    nodes: list[str] = (tmp_path / "smoke_1.0.0.nodes.ndjson").read_text().strip().splitlines()
    rig_doc: dict[str, Any] = lib.strip_nulls(from_yaml(tmp_path / "smoke_1.0.0.RIG.yaml"))
    assert len(edges) == 2
    assert all('"id"' in line for line in edges)
    flat: str = "\n".join(edges)
    assert '"p_value":1e-8' in flat or '"p_value":"1.0000e-08"' in flat
    # Retrieval provenance is nested under `sources`, never flat on the edge.
    assert '"upstream_resource_ids":["infores:pubmed-central"]' in flat  # inside the sources struct
    # internal pre-resolution snapshot is stripped from final edges
    assert "_pre_resolution" not in flat
    assert len(nodes) >= 1

    # The RIG document: config semantics verbatim, mechanics derived from the build.
    assert rig_doc["name"] == "smoke v1.0.0 Resource Ingest Guide"
    assert rig_doc["source_info"]["infores_id"] == "infores:smoke"  # pyright: ignore
    assert rig_doc["source_info"]["description"] == "Smoke graph"  # pyright: ignore
    assert rig_doc["source_info"]["data_access_locations"] == ["Test source - https://example.org/data"]  # pyright: ignore
    assert rig_doc["provenance_info"]["contributions"] == ["Test author - code author"]  # pyright: ignore

    # Generated artifact entries carry the exact output names at the configured URL base.
    relevant: list[dict[str, Any]] = rig_doc["ingest_info"]["relevant_files"]  # pyright: ignore
    by_name: dict[str, dict[str, Any]] = {entry["file_name"]: entry for entry in relevant}
    assert by_name["smoke_1.0.0.nodes.ndjson"]["location"] == "https://example.org/smoke/smoke_1.0.0.nodes.ndjson"
    assert by_name["smoke_1.0.0.edges.ndjson"]["location"] == "https://example.org/smoke/smoke_1.0.0.edges.ndjson"
    included: list[dict[str, Any]] = rig_doc["ingest_info"]["included_content"]  # pyright: ignore
    assert {entry["file_name"] for entry in included} == {"smoke_1.0.0.nodes.ndjson", "smoke_1.0.0.edges.ndjson"}
    assert all(entry["included_records"] for entry in included)

    # Edge summaries come from the FINAL graph: role-separated sources, list KL/AT,
    # observed properties; source files come from the configured rig.source_files.
    edge_type: dict[str, Any] = rig_doc["target_info"]["edge_type_info"][0]  # pyright: ignore
    assert edge_type["subject_categories"] == ["biolink:gene"]
    assert edge_type["predicates"] == ["biolink:related_to"]
    assert edge_type["object_categories"] == ["biolink:disease"]
    assert edge_type["knowledge_level"] == ["knowledge_assertion"]
    assert edge_type["agent_type"] == ["manual_agent"]
    assert edge_type["primary_knowledge_sources"] == ["infores:smoke"]
    assert edge_type["supporting_data_sources"] == ["infores:pubmed-central"]
    assert "biolink:p_value" in edge_type["edge_properties"]
    assert edge_type["source_files"] == ["table1.xlsx"]
    # Custom UI prefix first, built-in explanation always appended.
    assert edge_type["ui_explanation"].startswith("Custom UI explanation. ")
    assert DEFAULT_RIG_UI_EXPLANATION in edge_type["ui_explanation"]
    node_types: list[dict[str, Any]] = rig_doc["target_info"]["node_type_info"]  # pyright: ignore
    assert {x["node_category"] for x in node_types} == {"biolink:gene", "biolink:disease"}
    assert any(x["source_identifier_types"] == ["HGNC"] for x in node_types)


def test_compile_graph_opens_ndjson_outputs_as_utf8(monkeypatch: Any, tmp_path: Path, rig_factory: Any) -> None:
    """compile_graph passes UTF-8 file handles to Polars NDJSON writers."""
    monkeypatch.chdir(tmp_path)
    original_open: Any = Path.open
    append_encodings: list[tuple[str, str | None]] = []

    def spy_open(
        self: Path, mode: str = "r", buffering: int = -1, encoding: str | None = None, errors: str | None = None, newline: str | None = None
    ) -> Any:
        if mode == "a" and self.name.endswith(".ndjson.tmp"):
            append_encodings.append((self.name, encoding))
            return original_open(self, mode, buffering, encoding or "ascii", errors, newline)
        return original_open(self, mode, buffering, encoding, errors, newline)

    monkeypatch.setattr(Path, "open", spy_open)
    sub: Path = tmp_path / "sub.parquet"
    pl.DataFrame(
        {
            "subject": ["A-é"],
            "subject_name": ["Alpha-é"],
            "subject_category": ["gene"],
            "subject_taxon": [None],
            "subject_source": [None],
            "subject_source_version": [None],
            "subject_pre_resolution": ["A-é"],
            "object": ["HGNC:1"],
            "object_name": ["Xräy"],
            "object_category": ["disease"],
            "object_taxon": [None],
            "object_source": [None],
            "object_source_version": [None],
            "object_pre_resolution": ["Xräy"],
            "predicate": ["biolink:related_to"],
            "knowledge_level": ["knowledge_assertion"],
            "agent_type": ["manual_agent"],
            "primary_knowledge_source": ["infores:utf8-kg"],
            "sources": [
                [
                    {
                        "resource_id": "infores:utf8-kg",
                        "resource_role": "primary_knowledge_source",
                        "source_record_urls": ["https://example.org/utf8.tsv"],
                    }
                ]
            ],
        }
    ).write_parquet(sub)

    lib.compile_graph([sub], "utf8", "1.0.0", rig_factory(tmp_path, infores_id="infores:utf8-kg"))

    assert append_encodings == [("utf8_1.0.0.nodes.ndjson.tmp", "utf-8"), ("utf8_1.0.0.edges.ndjson.tmp", "utf-8")]
    assert "Alpha-é" in (tmp_path / "utf8_1.0.0.nodes.ndjson").read_text(encoding="utf-8")
    assert "A-é" in (tmp_path / "utf8_1.0.0.edges.ndjson").read_text(encoding="utf-8")


def test_compile_graph_progress_callbacks_fire_per_subgraph_and_phase(monkeypatch: Any, tmp_path: Path, rig_factory: Any) -> None:
    """compile_graph threads on_phase/on_subgraph: ordered phases, one tick per subgraph, output unchanged."""
    monkeypatch.chdir(tmp_path)

    def write_sub(p: Path, subj: str, obj: str) -> None:
        pl.DataFrame(
            {
                "subject": [subj],
                "subject_name": [subj],
                "subject_category": ["gene"],
                "subject_taxon": [None],
                "subject_source": [None],
                "subject_source_version": [None],
                "subject_pre_resolution": [subj],
                "object": [obj],
                "object_name": [obj],
                "object_category": ["disease"],
                "object_taxon": [None],
                "object_source": [None],
                "object_source_version": [None],
                "object_pre_resolution": [obj],
                "predicate": ["biolink:related_to"],
                "knowledge_level": ["knowledge_assertion"],
                "agent_type": ["manual_agent"],
                "primary_knowledge_source": ["infores:cb-kg"],
                "sources": [
                    [
                        {
                            "resource_id": "infores:cb-kg",
                            "resource_role": "primary_knowledge_source",
                            "source_record_urls": ["https://example.org/cb.tsv"],
                        }
                    ]
                ],
            }
        ).write_parquet(p)

    sub_a: Path = tmp_path / "a.parquet"
    sub_b: Path = tmp_path / "b.parquet"
    write_sub(sub_a, "A", "X")
    write_sub(sub_b, "B", "Y")

    phases: list[str] = []
    ticks: list[None] = []
    lib.compile_graph(
        [sub_a, sub_b],
        "cb",
        "1.0.0",
        rig_factory(tmp_path, infores_id="infores:cb-kg"),
        on_phase=phases.append,
        on_subgraph=lambda: ticks.append(None),
    )

    # scan/normalize fire once per subgraph, then the shared write phases in order.
    assert phases == ["scan", "normalize", "scan", "normalize", "write-nodes", "write-edges", "dedup", "rig"]
    # on_subgraph ticks exactly once per subgraph (this is what drives the bar total).
    assert len(ticks) == 2

    # Callbacks are pure observation: KGX output is byte-identical to a no-callback run.
    lib.compile_graph([sub_a, sub_b], "cb2", "1.0.0", rig_factory(tmp_path, infores_id="infores:cb-kg"))
    for stem in ("edges.ndjson", "nodes.ndjson"):
        assert (tmp_path / f"cb_1.0.0.{stem}").read_bytes() == (tmp_path / f"cb2_1.0.0.{stem}").read_bytes()


def test_compile_graph_keeps_qualifiers_and_publications_on_edges(monkeypatch: Any, tmp_path: Path, rig_factory: Any) -> None:
    """compile_graph keeps qualifier and publication columns on edges, out of nodes."""
    monkeypatch.chdir(tmp_path)
    sub: Path = tmp_path / "sub.parquet"
    pl.DataFrame(
        {
            "subject": ["A"],
            "subject_name": ["Alpha"],
            "subject_category": ["gene"],
            "subject_taxon": [None],
            "subject_source": [None],
            "subject_source_version": [None],
            "subject_pre_resolution": ["A"],
            "object": ["X"],
            "object_name": ["Xray"],
            "object_category": ["disease"],
            "object_pre_resolution": ["X"],
            "predicate": ["biolink:related_to"],
            "knowledge_level": ["knowledge_assertion"],
            "agent_type": ["manual_agent"],
            "primary_knowledge_source": ["infores:qual-kg"],
            "sources": [
                [
                    {
                        "resource_id": "infores:qual-kg",
                        "resource_role": "primary_knowledge_source",
                        "source_record_urls": ["https://example.org/qual.tsv"],
                    }
                ]
            ],
            "disease_context_qualifier": ["MONDO:0005148"],
            "disease_context_qualifier_pre_resolution": ["MONDO:0005148"],
            "publications": [["PMID:123"]],
        }
    ).write_parquet(sub)
    lib.compile_graph([sub], "qual", "1.0.0", rig_factory(tmp_path, infores_id="infores:qual-kg"))
    edges: str = (tmp_path / "qual_1.0.0.edges.ndjson").read_text()
    nodes: str = (tmp_path / "qual_1.0.0.nodes.ndjson").read_text()
    # qualifier and publications stay on edges
    assert "MONDO:0005148" in edges
    assert "PMID:123" in edges
    # internal pre-resolution snapshots are stripped from final edges
    assert "_pre_resolution" not in edges
    # neither becomes a node
    assert "MONDO:0005148" not in nodes
    assert "PMID:123" not in nodes


def test_compile_graph_preserves_original_columns_by_default(monkeypatch: Any, tmp_path: Path, rig_factory: Any) -> None:
    """compile_graph preserves verbatim ``original_*`` edge columns by default."""

    def write_subgraph(p: Path) -> None:
        pl.DataFrame(
            {
                "subject": ["A"],
                "subject_name": ["Alpha"],
                "subject_category": ["gene"],
                "subject_taxon": [None],
                "subject_source": [None],
                "subject_source_version": [None],
                "subject_pre_resolution": ["A"],
                "original_subject": ["ALPHA"],
                "object": ["X"],
                "object_name": ["Xray"],
                "object_category": ["disease"],
                "object_pre_resolution": ["X"],
                "original_object": ["X-RAY"],
                "predicate": ["biolink:related_to"],
                "knowledge_level": ["knowledge_assertion"],
                "agent_type": ["manual_agent"],
                "primary_knowledge_source": ["infores:no-orig-kg"],
                "sources": [
                    [
                        {
                            "resource_id": "infores:no-orig-kg",
                            "resource_role": "primary_knowledge_source",
                            "source_record_urls": ["https://example.org/no-orig.tsv"],
                        }
                    ]
                ],
            }
        ).write_parquet(p)

    monkeypatch.chdir(tmp_path)
    default_sub: Path = tmp_path / "default.parquet"
    write_subgraph(default_sub)
    lib.compile_graph([default_sub], "orig", "1.0.0", rig_factory(tmp_path, infores_id="infores:no-orig-kg"))
    default_edges: str = (tmp_path / "orig_1.0.0.edges.ndjson").read_text()
    assert '"original_subject":"ALPHA"' in default_edges
    assert '"original_object":"X-RAY"' in default_edges


def test_dedup_stream_nodes(tmp_path: Path) -> None:
    """dedup_stream deduplicates and strips null like values from node streams."""
    p_in: Path = tmp_path / "nodes.ndjson.tmp"
    p_in.write_text('{"id":"A","drop":"NA"}\n{"id":"A","drop":"NA"}\n{"id":"B"}\n')

    lib.dedup_stream(p_in, is_edges=False)

    assert not p_in.exists()  # temp input is removed
    lines: list[str] = (tmp_path / "nodes.ndjson").read_text().strip().splitlines()
    assert lines == ['{"id":"A"}', '{"id":"B"}']


def test_dedup_stream_edges(tmp_path: Path) -> None:
    """dedup_stream labels edges with UUID shaped ids and deduplicates."""
    import json

    p_in: Path = tmp_path / "edges.ndjson.tmp"
    p_in.write_text('{"subject":"A","object":"B","predicate":"r"}\n{"subject":"A","object":"B","predicate":"r"}\n')

    lib.dedup_stream(p_in, is_edges=True)

    assert not p_in.exists()
    lines: list[str] = (tmp_path / "edges.ndjson").read_text().strip().splitlines()
    assert len(lines) == 1  # duplicate edges collapse to one
    row: dict = json.loads(lines[0])
    assert "id" in row


def test_sig_works_on_cleaned_float64() -> None:
    """sig computes significance on a cleaned Float64 P value column."""
    lf: pl.LazyFrame = pl.DataFrame({"p_value": ["1e-8", "0.5", "N/A"]}).lazy()
    cleaned: pl.LazyFrame = clean_numeric(lf)
    result: pl.DataFrame = lib.sig(cleaned).collect()
    assert result["statistical_significance_qualifier"].to_list() == ["very_strongly_significant", "not_significant", None]


def test_edge_category_chemical_to_disease() -> None:
    """edge_category maps SmallMolecule + disease to ChemicalEntityToDiseaseAssociation."""
    lf: pl.LazyFrame = pl.LazyFrame({"subject category": ["biolink:SmallMolecule"], "object category": ["biolink:Disease"]})
    result: pl.DataFrame = edge_category(lf).collect()
    assert result["category"].to_list()[0] == ["biolink:ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation"]


def test_edge_category_gene_to_disease() -> None:
    """edge_category maps gene + disease to GeneToDiseaseAssociation."""
    lf: pl.LazyFrame = pl.LazyFrame({"subject category": ["biolink:Gene"], "object category": ["biolink:Disease"]})
    result: pl.DataFrame = edge_category(lf).collect()
    assert result["category"].to_list()[0] == ["biolink:GeneToDiseaseAssociation"]


def test_edge_category_protein_to_disease() -> None:
    """edge_category bridges protein to gene (no ProteinTo* associations in biolink)."""
    lf: pl.LazyFrame = pl.LazyFrame({"subject category": ["biolink:Protein"], "object category": ["biolink:Disease"]})
    result: pl.DataFrame = edge_category(lf).collect()
    assert result["category"].to_list()[0] == ["biolink:GeneToDiseaseAssociation"]


def test_edge_category_unmapped_falls_back() -> None:
    """edge_category falls back to generic association for unmapped pairs."""
    lf: pl.LazyFrame = pl.LazyFrame({"subject category": ["biolink:Publication"], "object category": ["biolink:Pathway"]})
    result: pl.DataFrame = edge_category(lf).collect()
    assert result["category"].to_list()[0] == ["biolink:Association"]


def test_edge_category_drug_to_disease() -> None:
    """edge_category maps drug + disease through ChemicalEntity hierarchy."""
    lf: pl.LazyFrame = pl.LazyFrame({"subject category": ["biolink:Drug"], "object category": ["biolink:Disease"]})
    result: pl.DataFrame = edge_category(lf).collect()
    assert result["category"].to_list()[0] == ["biolink:ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation"]


def test_edge_category_variant_to_disease() -> None:
    """edge_category maps SequenceVariant + disease through variant role."""
    lf: pl.LazyFrame = pl.LazyFrame({"subject category": ["biolink:SequenceVariant"], "object category": ["biolink:Disease"]})
    result: pl.DataFrame = edge_category(lf).collect()
    assert result["category"].to_list()[0] == ["biolink:VariantToDiseaseAssociation"]


def test_edge_category_override_resolves_per_object_category() -> None:
    """category_override pins the association class per row from the raw object category.

    Disease and PhenotypicFeature roll up to one (subject, object) pair key, so the
    pair lookup alone can never split them; the override keys on the raw category
    before that rollup. Rows whose object category is absent from the map fall back
    to the pair lookup.
    """
    lf: pl.LazyFrame = pl.LazyFrame(
        {"subject category": ["biolink:ChemicalEntity"] * 3, "object category": ["biolink:Disease", "biolink:PhenotypicFeature", "biolink:Gene"]}
    )
    override: dict[str, str] = {"Disease": "biolink:EntityToDiseaseAssociation", "PhenotypicFeature": "biolink:EntityToPhenotypicFeatureAssociation"}
    result: pl.DataFrame = edge_category(lf, "biolink:associated_with", override).collect()
    assert result["category"].to_list() == [
        ["biolink:EntityToDiseaseAssociation"],
        ["biolink:EntityToPhenotypicFeatureAssociation"],
        ["biolink:Association"],
    ]


def test_edge_category_override_still_reconciled_against_predicate() -> None:
    """A pinned class that rejects the predicate walks up its hierarchy like any other."""
    lf: pl.LazyFrame = pl.LazyFrame({"subject category": ["biolink:Gene"], "object category": ["biolink:Disease"]})
    override: dict[str, str] = {"Disease": "biolink:GeneToDiseaseAssociation"}
    result: pl.DataFrame = edge_category(lf, "biolink:gene_associated_with_condition", override).collect()
    assert result["category"].to_list()[0] == ["biolink:Association"]


def test_prune_to_class_keeps_override_only_slots() -> None:
    """Canonical approval annotations split into arrays and survive only granted classes.

    The canonical approval slot is granted to ``EntityToDiseaseAssociation`` /
    ``EntityToPhenotypicFeatureAssociation`` but not to the ungranted fallback class.
    This exercises the real annotation ``split_by`` operation before the category
    override and verifies rejected evidence remains observable in the pruned path.
    """
    from tablassert.lib import PRUNED_COLUMN, prune_to_class

    lf: pl.LazyFrame = pl.LazyFrame(
        {
            "subject category": ["biolink:ChemicalEntity"] * 3,
            "object category": ["biolink:Disease", "biolink:PhenotypicFeature", "biolink:Gene"],
            "regulatory_approvals": ["011111|022222", "033333", "044444"],
            "number_of_cases": [42, 7, 3],
        }
    )
    override: dict[str, str] = {"Disease": "biolink:EntityToDiseaseAssociation", "PhenotypicFeature": "biolink:EntityToPhenotypicFeatureAssociation"}
    annotated: pl.LazyFrame = split_list(lf, "regulatory_approvals", "|")
    out: pl.DataFrame = prune_to_class(edge_category(annotated, "biolink:associated_with", override)).collect()
    assert out["regulatory_approvals"].to_list() == [["011111", "022222"], ["033333"], None]
    assert out["number_of_cases"].to_list() == [42, 7, None]
    assert out[PRUNED_COLUMN].to_list() == [[], [], ["regulatory_approvals=044444", "number_of_cases=3"]]

    control: pl.DataFrame = prune_to_class(edge_category(annotated, "biolink:associated_with")).collect()
    assert control["regulatory_approvals"].to_list() == [None, None, None]
    assert all(any("regulatory_approvals=" in s for s in v) for v in control[PRUNED_COLUMN].to_list())


def test_prune_to_class_keeps_class_field_override_grants() -> None:
    """Slots granted to a class by CLASS_FIELD_OVERRIDES survive prune_to_class.

    ``disease_context_qualifier`` is declared only on the
    ``ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation`` lineage, but the policy
    grant keeps it on ``EntityToDiseaseAssociation`` /
    ``EntityToPhenotypicFeatureAssociation`` rows so a pinned edge can carry it
    alongside ``regulatory_approvals`` -- and the same holds for DAKP's sparse
    qualifier stack (``anatomical_context_qualifier``, ``sex_qualifier``,
    ``population_context_qualifier``, ``frequency_qualifier``,
    ``temporal_context_qualifier``), none of which the pinned classes declare.
    Classes without the grant still prune them.
    """
    from tablassert.lib import PRUNED_COLUMN, prune_to_class

    lf: pl.LazyFrame = pl.LazyFrame(
        {
            "category": [
                ["biolink:EntityToDiseaseAssociation"],
                ["biolink:GeneToDiseaseAssociation"],
                ["biolink:EntityToPhenotypicFeatureAssociation"],
            ],
            "disease_context_qualifier": ["MONDO:0005148", "MONDO:0005148", "MONDO:0005015"],
            "regulatory_approvals": ["FDA:1", "FDA:2", "FDA:3"],
            "anatomical_context_qualifier": ["UBERON:0001557", "UBERON:0001557", "UBERON:0001557"],
            "sex_qualifier": ["PATO:0000383", "PATO:0000383", "PATO:0000383"],
            "population_context_qualifier": ["NCIT:C25667", "NCIT:C25667", "NCIT:C25667"],
            "frequency_qualifier": ["HP:0012823", "HP:0012823", "HP:0012823"],
            "temporal_context_qualifier": ["TIME:1", "TIME:1", "TIME:1"],
        }
    )
    out: pl.DataFrame = prune_to_class(lf).collect()
    assert out["disease_context_qualifier"].to_list() == ["MONDO:0005148", None, "MONDO:0005015"]
    assert out["regulatory_approvals"].to_list() == ["FDA:1", None, "FDA:3"]
    for col in ("anatomical_context_qualifier", "sex_qualifier", "population_context_qualifier", "frequency_qualifier", "temporal_context_qualifier"):
        assert out[col].to_list() == [out[col].to_list()[0], None, out[col].to_list()[2]], col
    pruned_second: list[str] = out[PRUNED_COLUMN].to_list()[1]
    for col in (
        "disease_context_qualifier",
        "regulatory_approvals",
        "anatomical_context_qualifier",
        "sex_qualifier",
        "population_context_qualifier",
        "frequency_qualifier",
        "temporal_context_qualifier",
    ):
        assert any(entry.startswith(f"{col}=") for entry in pruned_second), col
    assert out[PRUNED_COLUMN].to_list()[0] == []
    assert out[PRUNED_COLUMN].to_list()[2] == []


def test_supporting_case_ids_survives_prune_and_fold_to_dedup_input() -> None:
    """The build-internal ``supporting_case_ids`` carrier survives to the dedup input.

    ``merge_records`` recomputes ``number_of_cases`` from the union of these lists, so
    the column must reach the final edge frames as a real ``list[str]``: allow-listed,
    it is not folded into ``supporting_text``, and declared by no association class, it
    hits ``prune_to_class``'s no-declaring-class path and is left untouched on ANY
    category -- pinned or pair-derived -- without a ``CLASS_FIELD_OVERRIDES`` grant.
    """
    from tablassert.lib import PRUNED_COLUMN, prune_to_class

    lf: pl.LazyFrame = pl.LazyFrame(
        {
            "subject category": ["biolink:ChemicalEntity"] * 2,
            "object category": ["biolink:Disease", "biolink:PhenotypicFeature"],
            "supporting_case_ids": [["FAERS:1", "FAERS:2"], ["FAERS:3"]],
        }
    )
    override: dict[str, str] = {"Disease": "biolink:EntityToDiseaseAssociation", "PhenotypicFeature": "biolink:EntityToPhenotypicFeatureAssociation"}
    for categorized in (edge_category(lf, "biolink:associated_with", override), edge_category(lf, "biolink:associated_with")):
        pruned: pl.DataFrame = prune_to_class(categorized).collect()
        assert pruned.schema["supporting_case_ids"] == pl.List(pl.String)
        assert pruned["supporting_case_ids"].to_list() == [["FAERS:1", "FAERS:2"], ["FAERS:3"]]
        assert PRUNED_COLUMN not in pruned.columns
        # The ``<col> category`` scaffolding columns are consumed by node derivation in
        # the real pipeline; fold runs on what remains.
        folded: pl.DataFrame = fold_unknown_to_supporting_text(pruned.drop("subject category", "object category").lazy()).collect()
        assert "supporting_text" not in folded.columns
        assert folded["supporting_case_ids"].to_list() == [["FAERS:1", "FAERS:2"], ["FAERS:3"]]


def test_parse_edge_name_standard() -> None:
    """parse_edge_name parses standard name."""
    assert parse_edge_name("GeneToDiseaseAssociation") == ("Gene", ["Disease"])


def test_parse_edge_name_multi_object() -> None:
    """parse_edge_name splits multi-object names on or."""
    assert parse_edge_name("ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation") == ("ChemicalEntity", ["Disease", "PhenotypicFeature"])


def test_parse_edge_name_no_to() -> None:
    """parse_edge_name returns none for non-standard names."""
    assert parse_edge_name("ChemicalGeneInteractionAssociation") is None


def test_parse_edge_name_embedded_or_not_split() -> None:
    """parse_edge_name does not split on "Or" embedded inside a role name."""
    assert parse_edge_name("OrganismTaxonToOrganismTaxonAssociation") == ("OrganismTaxon", ["OrganismTaxon"])


def test_edge_tables_cached() -> None:
    """edge_tables returns same object on repeat calls (cached)."""
    first: tuple[dict[str, str], dict[str, str]] = edge_tables()
    second: tuple[dict[str, str], dict[str, str]] = edge_tables()
    assert first is second


def test_pvalue_target_matches_common_spellings() -> None:
    """pvalue_target matches common P value spellings."""
    names: list[str] = ["p value", "p-value", "p.value", "pvalue", "P VALUE", "p vals", "p-values", "P"]
    for n in names:
        assert pvalue_target(n) == "p_value", n


def test_pvalue_target_matches_bare_p_and_padj_conventions() -> None:
    """pvalue_target matches bare P and padj style conventions found in real GWAS/DESeq2 data."""
    plain: list[str] = ["p SMR", "smr p", "p eQTL", "eqtl p", "gwas p", "fisher combined p", "log p"]
    for n in plain:
        assert pvalue_target(n) == "p_value", n

    adjusted: list[str] = ["padj", "p.adj", "adj.P.Val"]
    for n in adjusted:
        assert pvalue_target(n) == "adjusted_p_value", n


def test_pvalue_target_detects_adjusted_variants() -> None:
    """pvalue_target detects adjusted P value variants."""
    names: list[str] = ["adjusted p value", "adjusted-p-value", "adj p value"]
    for n in names:
        assert pvalue_target(n) == "adjusted_p_value", n


def test_pvalue_target_detects_broader_adjustment_synonyms() -> None:
    """pvalue_target detects broader adjustment synonyms."""
    names: list[str] = [
        "FDR",
        "Bonferroni",
        "Holm",
        "false discovery rate",
        "q value",
        "q-value",
        "bonferroni pval",
        "corrected p value",
        "corrected q value",
    ]
    for n in names:
        assert pvalue_target(n) == "adjusted_p_value", n


def test_pvalue_target_detects_underscore_glued_adjusted_spellings() -> None:
    """Underscore-glued adjustment words still mark a P value adjusted.

    Regression: the pattern anchored on ``\\b``, but ``_`` is a word character, so ``\\b`` never
    fired between ``adjusted`` and ``_p_value``. Every underscore-glued spelling classified as a
    RAW ``p_value`` -- a column literally named ``adjusted_p_value`` was renamed to ``p_value``
    and ``sig`` banded an FDR-adjusted number as a raw one. ``p_adjust`` / ``p_adjustment`` are
    the same DESeq2-family spelling as ``padj`` and were missing from the vocabulary outright.
    """
    names: list[str] = [
        "adjusted_p_value",
        "adj_p_value",
        "corrected_p_value",
        "p_adjusted",
        "p_adjust",
        "p_adjustment",
        "adjusted_p_value_analysis1",
        "adjusted_p_value_corrected",
    ]
    for n in names:
        assert pvalue_target(n) == "adjusted_p_value", n


def test_pvalue_target_underscore_anchoring_keeps_non_pvalue_columns_out() -> None:
    """The looser anchors must not drag non-P-value columns in.

    ``adjusted``/``corrected`` are generic words that also modify hazard and odds ratios, so they
    only count alongside a P/Q value token -- underscore-glued or not.
    """
    for n in ("adjusted_hazard_ratio", "corrected_expression", "adjustment_notes", "batch_corrected_counts"):
        assert pvalue_target(n) is None, n


def test_pvalue_target_bare_corrected_is_not_treated_as_adjusted() -> None:
    """pvalue_target does not treat bare corrected as adjusted without a P/Q value token."""
    assert pvalue_target("corrected age") is None
    assert pvalue_target("batch corrected expression") is None


# pvalue_target does not treat bare adjusted as adjusted P value without a P/Q value token
# regression for a real false positive found auditing production KGX output: "fully adjusted HR"
# is an adjusted hazard ratio, not a P value
def test_pvalue_target_bare_adjusted_without_pvalue_context_is_not_treated_as_adjusted() -> None:
    assert pvalue_target("fully adjusted HR") is None
    assert pvalue_target("adjusted odds ratio") is None


# pvalue_target excludes significance flag columns
# regression for a real false positive found auditing production KGX output: "bonferroni significance"
# is a categorical flag like sig()'s own "statistical_significance_qualifier" column, not the numeric value
def test_pvalue_target_excludes_significance_flag_columns() -> None:
    names: list[str] = [
        "bonferroni significance",
        "nominal significance",
        "age significance flag",
        "significant",
        "statistical_significance_qualifier",
        "statistical significance qualifier",
    ]
    for n in names:
        assert pvalue_target(n) is None, n


# pvalue_target excludes bare Q and Q statistic columns
# bare "Q" is deliberately not treated as Q value like since real data also uses it for
# cochran's Q test statistic, unrelated to storey's Q value
def test_pvalue_target_excludes_bare_q_and_q_statistic_columns() -> None:
    names: list[str] = ["Q degrees of freedom", "heterogeneity statistic Q", "Cochran Q statistic"]
    for n in names:
        assert pvalue_target(n) is None, n


def test_pvalue_target_excludes_unrelated_columns() -> None:
    """pvalue_target excludes unrelated columns."""
    names: list[str] = [
        "sample size",
        "relationship strength",
        "subject",
        "cohort",
        "top value",
        "group value",
        "hazard ratio",
        "odds ratio",
        "probe id",
        "beta",
        "standard error",
    ]
    for n in names:
        assert pvalue_target(n) is None, n


def test_pvalue_target_unadjusted_prefix_not_treated_as_adjusted() -> None:
    """pvalue_target does not conflate unadjusted with adjusted."""
    assert pvalue_target("unadjusted p value") == "p_value"
    assert pvalue_target("unadjusted HR") is None


def test_pvalue_target_matches_separator_glued_bare_p() -> None:
    """pvalue_target treats a bare P glued by underscores/dots/hyphens as a P value.

    Real GWAS outputs glue qualifiers onto a standalone P with underscores
    ("raw_p", "snp_p", "p_nominal"); these are the underscore-delimited siblings
    of the already-supported space-delimited forms ("gwas p", "log p").
    """
    plain: list[str] = [
        "raw_p",
        "nominal_p",
        "unadjusted_p",
        "p_nominal",
        "p_two_tailed",
        "two_sided_p_value",
        "wald_p",
        "p_wald",
        "snp_p",
        "assoc_p",
        "meta_p",
        "empirical_p",
        "perm_p",
        "permutation_p",
    ]
    for n in plain:
        assert pvalue_target(n) == "p_value", n


def test_pvalue_target_matches_numbered_pvalue_conventions() -> None:
    """pvalue_target matches numbered P/Q value columns from multi-phenotype outputs."""
    plain: list[str] = ["pvalue1", "p_value_1", "pvalue2"]
    for n in plain:
        assert pvalue_target(n) == "p_value", n

    adjusted: list[str] = ["padj_1", "qvalue1"]
    for n in adjusted:
        assert pvalue_target(n) == "adjusted_p_value", n


def test_pvalue_target_excludes_alnum_glued_p_and_near_misses() -> None:
    """pvalue_target excludes P glued to a letter/digit and value-like near misses.

    Guards the separator-glued bare-P expansion: gene/protein and chemistry names
    ("p53", "p16", "pH", "protein", "phosphate") and words merely ending in P
    ("peak value", "probability") must not be read as P values.
    """
    names: list[str] = ["pH", "protein", "phosphate", "p53", "p16", "peak value", "probability", "q statistic", "adjusted hazard ratio"]
    for n in names:
        assert pvalue_target(n) is None, n


def test_coerce_pvalue_columns_renames_single_p_value_column() -> None:
    """coerce_pvalue_columns renames a single P value column."""
    lf: pl.LazyFrame = pl.DataFrame({"p value": [0.01, 0.05]}).lazy()
    result: pl.DataFrame = coerce_pvalue_columns(lf).collect()
    assert "p_value" in result.columns
    assert "p value" not in result.columns
    assert result["p_value"].to_list() == [0.01, 0.05]


def test_coerce_pvalue_columns_renames_both_p_value_and_adjusted() -> None:
    """coerce_pvalue_columns renames both P value and adjusted P value columns together."""
    lf: pl.LazyFrame = pl.DataFrame({"p value": [0.01], "adjusted p value": [0.2]}).lazy()
    result: pl.DataFrame = coerce_pvalue_columns(lf).collect()
    assert result["p_value"].to_list() == [0.01]
    assert result["adjusted_p_value"].to_list() == [0.2]


def test_coerce_pvalue_columns_picks_best_fuzzy_match_among_multiple_candidates() -> None:
    """coerce_pvalue_columns picks the best fuzzy match and drops the losing alias."""
    lf: pl.LazyFrame = pl.DataFrame({"log p value": [0.9], "p value": [0.01]}).lazy()
    result: pl.DataFrame = coerce_pvalue_columns(lf).collect()
    assert result.columns == ["p_value"]
    assert result["p_value"].to_list() == [0.01]


def test_coerce_pvalue_columns_noop_without_pvalue_columns() -> None:
    """coerce_pvalue_columns is a noop without P value like columns."""
    lf: pl.LazyFrame = pl.DataFrame({"subject": ["BRCA1"], "cohort": ["adult"]}).lazy()
    result: pl.DataFrame = coerce_pvalue_columns(lf).collect()
    assert result.columns == ["subject", "cohort"]


def test_coerce_pvalue_columns_noop_when_already_canonical() -> None:
    """coerce_pvalue_columns is a noop when already canonically named."""
    lf: pl.LazyFrame = pl.DataFrame({"p_value": [0.01]}).lazy()
    result: pl.DataFrame = coerce_pvalue_columns(lf).collect()
    assert result.columns == ["p_value"]
    assert result["p_value"].to_list() == [0.01]


def test_coerce_pvalue_columns_keeps_existing_canonical_over_alias() -> None:
    """An existing canonical column wins over a spaced alias, which is then dropped."""
    lf: pl.LazyFrame = pl.DataFrame({"p_value": [0.01], "p value": [0.02]}).lazy()
    result: pl.DataFrame = coerce_pvalue_columns(lf).collect()
    assert result.columns == ["p_value"]
    assert result["p_value"].to_list() == [0.01]


def test_study_size_target_matches_common_spellings() -> None:
    """study_size_target matches common study size spellings."""
    names: list[str] = ["n", "N", "sample_size", "sample size", "sample-size", "sample.size", "samplesize", "study size", "cohort size"]
    for n in names:
        assert study_size_target(n) == "study_size", n


def test_study_size_target_matches_count_synonyms() -> None:
    """study_size_target matches count synonyms with explicit sample/study context."""
    names: list[str] = [
        "sample_count",
        "sample count",
        "number of samples",
        "num_samples",
        "n_samples",
        "samples_n",
        "total_n",
        "total n",
        "participant_count",
        "participants",
        "enrollment",
        "enrollment_count",
    ]
    for n in names:
        assert study_size_target(n) == "study_size", n


def test_study_size_target_excludes_false_positives() -> None:
    """study_size_target excludes false positives without explicit study size meaning."""
    names: list[str] = [
        "sample_id",
        "sample_name",
        "sample_type",
        "gene",
        "mean",
        "normalized_count",
        "nucleotide",
        "cohort",
        "population",
        "subject",
        "object",
        "predicate",
    ]
    for n in names:
        assert study_size_target(n) is None, n


def test_study_size_target_matches_cohort_total_and_study_n_variants() -> None:
    """study_size_target matches cohort counts, mirrored total-n, and study-n labels."""
    names: list[str] = [
        "cohort_n",
        "cohort n",
        "cohort_count",
        "cohort count",
        "n_cohort",
        "total_cohort",
        "n_total",
        "n total",
        "study_n",
        "study n",
    ]
    for n in names:
        assert study_size_target(n) == "study_size", n


def test_study_size_target_matches_enrolled_variants() -> None:
    """study_size_target treats 'enrolled' as an 'enrollment' study-size variant."""
    names: list[str] = ["enrolled", "enrolled_count", "enrolled n"]
    for n in names:
        assert study_size_target(n) == "study_size", n


def test_study_size_target_excludes_expanded_near_misses() -> None:
    """study_size_target keeps near-misses out after the cohort/enrolled expansion.

    Guards the expansion: adding cohort/enrolled as units must not pull in
    identifier/date columns ("cohort_id", "enrolled_date") or unrelated n-words
    ("nucleotide_variation", "normalization"), and "case"/"subject" units must not
    match "case_control"/"subject_id".
    """
    names: list[str] = [
        "cohort_id",
        "cohort_name",
        "case_control",
        "subject_id",
        "study_id",
        "nucleotide_variation",
        "normalization",
        "sample_size_estimate",
        "enrollment_date",
        "enrolled_date",
        "participant_id",
    ]
    for n in names:
        assert study_size_target(n) is None, n


def test_study_size_target_leaves_number_of_cases_alone() -> None:
    """study_size_target never touches the Biolink ``number_of_cases`` slot.

    ``number_of_cases`` counts cases carrying the phenotype/disease, not the
    study population; coercing it to ``study_size`` destroys a legitimate edge
    field.
    """
    names: list[str] = ["number_of_cases", "Number of Cases", "number-of-cases", "numberofcases"]
    for n in names:
        assert study_size_target(n) is None, n

    lf: pl.LazyFrame = pl.DataFrame({"number_of_cases": [42, 7]}).lazy()
    result: pl.DataFrame = coerce_study_size_columns(lf).collect()
    assert result.columns == ["number_of_cases"]


def test_coerce_study_size_columns_renames_n_column() -> None:
    """coerce_study_size_columns renames bare N to study_size."""
    lf: pl.LazyFrame = pl.DataFrame({"n": [120, 450]}).lazy()
    result: pl.DataFrame = coerce_study_size_columns(lf).collect()
    assert "study_size" in result.columns
    assert "n" not in result.columns
    assert result["study_size"].to_list() == [120, 450]


def test_coerce_study_size_columns_renames_sample_size_column() -> None:
    """coerce_study_size_columns renames sample_size to study_size."""
    lf: pl.LazyFrame = pl.DataFrame({"sample_size": [1200]}).lazy()
    result: pl.DataFrame = coerce_study_size_columns(lf).collect()
    assert result.columns == ["study_size"]
    assert result["study_size"].to_list() == [1200]


def test_coerce_study_size_columns_renames_legacy_supporting_study_size() -> None:
    """The deprecated ``supporting_study_size`` name is an alias for ``study_size``."""
    lf: pl.LazyFrame = pl.DataFrame({"supporting_study_size": [1200]}).lazy()
    result: pl.DataFrame = coerce_study_size_columns(lf).collect()
    assert result.columns == ["study_size"]
    assert result["study_size"].to_list() == [1200]


def test_coerce_study_size_columns_picks_best_candidate_and_drops_aliases() -> None:
    """The winning study-size synonym is canonicalized and losing synonyms cannot leak downstream."""
    lf: pl.LazyFrame = pl.DataFrame({"n": [9], "sample size": [1200], "participants": [1250]}).lazy()
    result: pl.DataFrame = coerce_study_size_columns(lf).collect()
    assert result["study_size"].to_list() == [1200]
    assert "n" not in result.columns
    assert "participants" not in result.columns


def test_coerce_study_size_columns_drops_alias_when_already_canonical() -> None:
    """An existing canonical study-size column wins and consumes its synonym aliases."""
    lf: pl.LazyFrame = pl.DataFrame({"study_size": [1200], "sample_size": [999]}).lazy()
    result: pl.DataFrame = coerce_study_size_columns(lf).collect()
    assert result.columns == ["study_size"]
    assert result["study_size"].to_list() == [1200]


def test_coerce_study_size_columns_drops_spaced_alias_when_canonical_exists() -> None:
    """A canonical value wins over a spaced duplicate without leaving the duplicate as context."""
    lf: pl.LazyFrame = pl.DataFrame({"study_size": [1200], "supporting study size": [999]}).lazy()
    result: pl.DataFrame = coerce_study_size_columns(lf).collect()
    assert result.columns == ["study_size"]
    assert result["study_size"].to_list() == [1200]


def test_coerced_target_matches_the_clean_phase_rename() -> None:
    """coerced_target reports the canonical name each clean-phase coercion renames a column to."""
    expected: dict[str, str] = {
        "padj": "adjusted_p_value",
        "p value": "p_value",
        "sample size": "study_size",
        "supporting_study_size": "study_size",
        "supporting_study_cohort": "study_cohort",
        "supporting_study_method_types": "study_method_types",
        "odds ratio": "effect_size",
        "relationship_strength": "effect_size",
        "effect type": "effect_type",
        # Claimed by no coercion -- returned unchanged.
        "cohort": "cohort",
    }
    for name, target in expected.items():
        assert coerced_target(name) == target, name


def test_coercion_rules_do_not_claim_each_others_targets() -> None:
    """Drift guard: no rule claims a canonical target that belongs to another rule.

    ``coerce_columns`` plans every rule against the ORIGINAL schema in one pass instead of
    re-reading the schema after each rename. That is only equivalent to running the five
    coercions in sequence because the classifiers are disjoint on the canonical targets --
    if a later rule could claim a name an earlier rule renamed into existence, the fused
    pass and the sequential chain would diverge. This pins that property.
    """
    owners: dict[str, Callable[[str], str | None]] = {
        "p_value": pvalue_target,
        "adjusted_p_value": pvalue_target,
        "study_size": study_size_target,
        "study_cohort": study_metadata_target,
        "study_context": study_metadata_target,
        "study_date_range": study_metadata_target,
        "study_method_description": study_metadata_target,
        "study_method_types": study_metadata_target,
        "effect_size": effect_size_target,
        "effect_type": effect_type_target,
    }
    for target, owner in owners.items():
        for rule in _RULES:
            claimed: str | None = rule.classify(target)
            if rule.classify is owner:
                continue
            assert claimed is None, f"{rule.classify.__name__} claims {target!r} as {claimed!r}"


def test_coerce_columns_equals_the_five_coercions_in_sequence() -> None:
    """The fused clean-phase op is exactly the five per-slot coercions applied in order.

    Every recognized alias family is present at once, including the cross-rule dependency
    (``effect type``'s Biolink class rule reads the ``effect_size`` that the effect-size
    rule renamed into existence).
    """
    frame: pl.LazyFrame = pl.DataFrame(
        {
            "subject": ["A"],
            "p value": [0.01],
            "negative log10 q value": [3.0],
            "sample size": [1200],
            "n": [9],
            "supporting_study_cohort": ["FINNGEN"],
            "supporting study context": ["EU ancestry"],
            "effect size": [0.85],
            "odds ratio": [0.4],
            "effect type": ["Spearman"],
            "notes": ["free text"],
        }
    ).lazy()
    sequential: pl.DataFrame = reduce(
        lambda lf, op: op(lf),
        (coerce_pvalue_columns, coerce_study_size_columns, coerce_study_metadata_columns, coerce_effect_size_columns, coerce_effect_type_columns),
        frame,
    ).collect()
    assert lib.coerce_columns(frame).collect().equals(sequential)
    # And the fused result is the canonical shape, with every losing alias gone.
    assert sorted(sequential.columns) == [
        "adjusted_p_value",
        "effect_size",
        "effect_type",
        "notes",
        "p_value",
        "study_cohort",
        "study_context",
        "study_size",
        "subject",
    ]


def test_map_effect_type_series_handles_degenerate_and_high_cardinality_columns() -> None:
    """The distinct-value mapper survives empty vocabularies and scales past the old cache.

    ``replace_strict`` is handed an EMPTY mapping when nothing resolves, which must still
    produce an all-null column rather than raise; and resolution is bounded by distinct
    values, so a column with far more distinct values than the old 4096-entry cache held
    is mapped in one pass.
    """
    assert _map_effect_type_series(pl.Series("x", [None, None], dtype=pl.String)).to_list() == [None, None]
    assert _map_effect_type_series(pl.Series("x", ["", "   "])).to_list() == [None, None]
    assert _map_effect_type_series(pl.Series("x", [], dtype=pl.String)).to_list() == []

    noise: list[str] = [f"unmatchable-metric-{i}" for i in range(5000)]
    mapped: list[str | None] = _map_effect_type_series(pl.Series("x", [*noise, "Cohen's d", "OR"])).to_list()
    assert mapped[-2:] == ["cohens_d", "odds_ratio"]
    assert set(mapped[:-2]) == {None}


def test_study_metadata_target_maps_deprecated_slots_to_study_properties() -> None:
    """study_metadata_target maps each deprecated supporting-study slot to its Study property."""
    expected: dict[str, str] = {
        "supporting_study_cohort": "study_cohort",
        "supporting_study_context": "study_context",
        "supporting_study_date_range": "study_date_range",
        "supporting_study_method_description": "study_method_description",
        "supporting_study_method_types": "study_method_types",
        "supporting_study_size": "study_size",
        # Separator variants are anchored too.
        "supporting study cohort": "study_cohort",
        "supporting-study-date-range": "study_date_range",
    }
    for name, target in expected.items():
        assert study_metadata_target(name) == target, name


def test_study_metadata_target_ignores_non_metadata_names() -> None:
    """study_metadata_target ignores names that are not deprecated supporting-study slots."""
    for name in ("study_size", "study_cohort", "sample_size", "cohort", "supporting_text"):
        assert study_metadata_target(name) is None, name


def test_coerce_study_metadata_columns_renames_deprecated_slots() -> None:
    """coerce_study_metadata_columns renames deprecated slots onto the current Study properties."""
    lf: pl.LazyFrame = pl.DataFrame(
        {"supporting_study_cohort": ["FINNGEN"], "supporting_study_context": ["European ancestry"], "subject": ["A"]}
    ).lazy()
    result: pl.DataFrame = coerce_study_metadata_columns(lf).collect()
    assert sorted(result.columns) == ["study_cohort", "study_context", "subject"]
    assert result["study_cohort"].to_list() == ["FINNGEN"]


def test_coerce_study_metadata_columns_drops_existing_canonical_sibling() -> None:
    """A declared canonical study_* column wins and consumes a deprecated duplicate."""
    lf: pl.LazyFrame = pl.DataFrame({"study_size": [10], "supporting_study_size": [99]}).lazy()
    result: pl.DataFrame = coerce_study_metadata_columns(lf).collect()
    assert result.columns == ["study_size"]
    assert result["study_size"].to_list() == [10]


# --- Effect-size / effect-type coercion (Biolink PR #1774) --------------------------------------


def test_effect_size_target_matches_common_spellings() -> None:
    """effect_size_target matches common effect size spellings including the old name."""
    names: list[str] = [
        "effect size",
        "effectsize",
        "effect_size",
        "ES",
        "es",
        "relationship_strength",
        "relationship strength",
        "beta",
        "beta coefficient",
        "log2FC",
        "log2 fold change",
        "odds ratio",
        "OR",
        "hazard ratio",
        "HR",
        "risk ratio",
        "correlation",
        "rho",
        "r",
    ]
    for n in names:
        assert effect_size_target(n) == "effect_size", n


def test_effect_size_target_matches_qualified_statistic_names() -> None:
    """effect_size_target matches qualified numeric-statistic column names."""
    names: list[str] = [
        "spearman rho",
        "Spearman's rho",
        "spearman_rho",
        "pearson_r",
        "kendall_tau",
        "correlation coefficient",
        "adjusted odds ratio",
        "effect size estimate",
        "log2_FC",
    ]
    for n in names:
        assert effect_size_target(n) == "effect_size", n


def test_effect_size_target_excludes_false_positives() -> None:
    """effect_size_target excludes identifiers, unrelated columns, and effect-type labels."""
    names: list[str] = [
        "correlation_id",
        "subject",
        "gene",
        "p_value",
        "sample_id",
        "beta_actin",
        "or_value",
        "hr_status",
        "order",
        "effect type",
        "effect metric",
        "statistic type",
        "metric",
    ]
    for n in names:
        assert effect_size_target(n) is None, n


def test_effect_type_target_matches_common_spellings() -> None:
    """effect_type_target matches effect-type/metric label spellings."""
    names: list[str] = [
        "effect type",
        "effect_type",
        "effect metric",
        "statistic type",
        "statistical type",
        "metric type",
        "metric",
        "effect size type",
    ]
    for n in names:
        assert effect_type_target(n) == "effect_type", n


def test_effect_type_target_excludes_unrelated_columns() -> None:
    """effect_type_target excludes effect-size names and unrelated columns."""
    names: list[str] = ["effect_size", "effect size", "metric_value", "subject", "effect", "correlation"]
    for n in names:
        assert effect_type_target(n) is None, n


def test_coerce_effect_size_columns_renames_relationship_strength() -> None:
    """coerce_effect_size_columns renames the old relationship_strength name forward to effect_size."""
    lf: pl.LazyFrame = pl.DataFrame({"relationship_strength": [0.85, 0.42]}).lazy()
    result: pl.DataFrame = coerce_effect_size_columns(lf).collect()
    assert "effect_size" in result.columns
    assert "relationship_strength" not in result.columns
    assert result["effect_size"].to_list() == [0.85, 0.42]


def test_coerce_effect_size_columns_renames_effect_size_like_column() -> None:
    """coerce_effect_size_columns renames an effect-size-like column."""
    lf: pl.LazyFrame = pl.DataFrame({"spearman rho": [0.85]}).lazy()
    result: pl.DataFrame = coerce_effect_size_columns(lf).collect()
    assert result.columns == ["effect_size"]
    assert result["effect_size"].to_list() == [0.85]


def test_coerce_effect_size_columns_picks_best_candidate() -> None:
    """coerce_effect_size_columns picks the best candidate and drops the losers.

    ``beta`` and ``effect size estimate`` may well be DIFFERENT statistics rather than
    synonyms, and dropping them loses those numbers. That is the accepted cost of a
    uniform alias policy: a column that must survive needs a name the effect-size
    classifier does not claim.
    """
    lf: pl.LazyFrame = pl.DataFrame({"effect size estimate": [0.1], "effect size": [0.85], "beta": [0.3]}).lazy()
    result: pl.DataFrame = coerce_effect_size_columns(lf).collect()
    assert result.columns == ["effect_size"]
    assert result["effect_size"].to_list() == [0.85]


def test_coerce_effect_size_columns_noop_without_candidates() -> None:
    """coerce_effect_size_columns is a noop without effect-size-like columns."""
    lf: pl.LazyFrame = pl.DataFrame({"subject": ["BRCA1"], "cohort": ["adult"]}).lazy()
    result: pl.DataFrame = coerce_effect_size_columns(lf).collect()
    assert result.columns == ["subject", "cohort"]


def test_coerce_effect_size_columns_noop_when_already_canonical() -> None:
    """An already-canonical effect_size wins, and its aliases are consumed."""
    lf: pl.LazyFrame = pl.DataFrame({"effect_size": [0.85], "beta": [0.3]}).lazy()
    result: pl.DataFrame = coerce_effect_size_columns(lf).collect()
    assert result.columns == ["effect_size"]
    assert result["effect_size"].to_list() == [0.85]


def test_coerce_effect_size_columns_keeps_existing_canonical_over_alias() -> None:
    """An existing canonical column wins over a spaced alias, which is then dropped."""
    lf: pl.LazyFrame = pl.DataFrame({"effect_size": [0.85], "effect size": [0.99]}).lazy()
    result: pl.DataFrame = coerce_effect_size_columns(lf).collect()
    assert result.columns == ["effect_size"]
    assert result["effect_size"].to_list() == [0.85]


def test_coerce_effect_type_columns_renames_and_maps_alias_values() -> None:
    """coerce_effect_type_columns renames the column and maps alias values to canonical enum values."""
    lf: pl.LazyFrame = pl.DataFrame({"effect size": [0.85, 1.2, 0.4], "effect type": ["Spearman", "odds ratio", "beta"]}).lazy()
    result: pl.DataFrame = coerce_effect_type_columns(coerce_effect_size_columns(lf)).collect()
    assert "effect_type" in result.columns
    assert "effect type" not in result.columns
    assert result["effect_type"].to_list() == ["spearmans_rho", "odds_ratio", "regression_coefficient"]


def test_coerce_effect_type_columns_maps_canonical_and_case_variants() -> None:
    """coerce_effect_type_columns passes canonical values through case/separator-insensitively."""
    lf: pl.LazyFrame = pl.DataFrame({"effect_size": [0.5, 0.6, 0.7], "effect_type": ["COHENS_D", "Hedges' g", "eta-squared"]}).lazy()
    result: pl.DataFrame = coerce_effect_type_columns(lf).collect()
    assert result["effect_type"].to_list() == ["cohens_d", "hedges_g", "eta_squared"]


def test_coerce_effect_type_columns_fuzzy_fallback_and_unmatched_null() -> None:
    """coerce_effect_type_columns fuzzy-matches close spellings and nulls values matching nothing."""
    lf: pl.LazyFrame = pl.DataFrame(
        {"effect_size": [0.5, 0.6, 0.7], "effect_type": ["spearmans", "pearsons", "totally unrelated garbage xyz"]}
    ).lazy()
    result: pl.DataFrame = coerce_effect_type_columns(lf).collect()
    assert result["effect_type"].to_list() == ["spearmans_rho", "pearsons_r", None]


def test_map_effect_type_value_direct() -> None:
    """_map_effect_type_value handles null input directly (map_elements skips nulls itself)."""
    assert _map_effect_type_value(None) is None
    assert _map_effect_type_value("   ") is None
    assert _map_effect_type_value("Cohen's d") == "cohens_d"
    assert _map_effect_type_value("spearmans") == "spearmans_rho"
    assert _map_effect_type_value("totally unrelated garbage xyz") is None


def test_effect_type_aliases_only_map_to_permissible_values() -> None:
    """Drift guard: every alias canonical is a permissible value, and canonical values round-trip."""
    assert {canonical for _, canonical in _EFFECT_TYPE_ALIASES} <= set(EFFECT_TYPE_VALUES)
    for value in EFFECT_TYPE_VALUES:
        assert _map_effect_type_value(value) == value


def test_coerce_effect_type_columns_nulls_blank_and_null_values() -> None:
    """coerce_effect_type_columns maps null and blank values to null."""
    lf: pl.LazyFrame = pl.DataFrame({"effect_size": [0.1, 0.2], "effect_type": [None, "  "]}).lazy()
    result: pl.DataFrame = coerce_effect_type_columns(lf).collect()
    assert result["effect_type"].to_list() == [None, None]


def test_coerce_effect_type_columns_nulls_where_effect_size_null() -> None:
    """effect_type is nulled on rows whose effect_size is null or non-numeric (Biolink class rule)."""
    lf: pl.LazyFrame = pl.DataFrame({"effect size": ["0.85", None, "n/a"], "effect type": ["Spearman", "OR", "beta"]}).lazy()
    result: pl.DataFrame = coerce_effect_type_columns(coerce_effect_size_columns(lf)).collect()
    assert result["effect_size"].to_list() == ["0.85", None, "n/a"]
    assert result["effect_type"].to_list() == ["spearmans_rho", None, None]


def test_coerce_effect_type_columns_nulls_entirely_without_effect_size() -> None:
    """effect_type is nulled entirely when no effect_size column is present (Biolink class rule)."""
    lf: pl.LazyFrame = pl.DataFrame({"metric": ["OR", "pearson r"]}).lazy()
    result: pl.DataFrame = coerce_effect_type_columns(lf).collect()
    assert result.columns == ["effect_type"]
    assert result["effect_type"].to_list() == [None, None]


def test_coerce_effect_type_columns_picks_best_candidate() -> None:
    """coerce_effect_type_columns picks the best candidate column and drops the losers."""
    lf: pl.LazyFrame = pl.DataFrame({"effect_size": [0.85, 0.2], "effect size type": ["Spearman", "OR"], "effect type": ["pearson r", "beta"]}).lazy()
    result: pl.DataFrame = coerce_effect_type_columns(lf).collect()
    assert result.columns == ["effect_size", "effect_type"]
    assert result["effect_type"].to_list() == ["pearsons_r", "regression_coefficient"]


def test_coerce_effect_type_columns_keeps_existing_canonical_over_alias() -> None:
    """An existing canonical column wins over a higher-scoring spaced alias (no duplicate rename)."""
    lf: pl.LazyFrame = pl.DataFrame(
        {"effect_size": [0.85, 0.2], "effect_type": ["odds_ratio", "cohens_d"], "effect type": ["pearson r", "beta"]}
    ).lazy()
    result: pl.DataFrame = coerce_effect_type_columns(lf).collect()
    assert result.columns == ["effect_size", "effect_type"]
    assert result["effect_type"].to_list() == ["odds_ratio", "cohens_d"]


def test_coerce_effect_type_columns_noop_without_candidates() -> None:
    """coerce_effect_type_columns is a noop without effect-type-like columns."""
    lf: pl.LazyFrame = pl.DataFrame({"subject": ["BRCA1"], "effect_size": [0.85]}).lazy()
    result: pl.DataFrame = coerce_effect_type_columns(lf).collect()
    assert result.columns == ["subject", "effect_size"]


def test_coerced_effect_size_alias_survives_unknown_folding() -> None:
    """The old relationship_strength name becomes a top-level effect_size field before unknown folding."""
    lf: pl.LazyFrame = pl.DataFrame(
        {"subject": ["A"], "object": ["B"], "predicate": ["related_to"], "relationship_strength": ["0.85"], "miscellaneous_notes": ["note"]}
    ).lazy()
    out: pl.DataFrame = fold_unknown_to_supporting_text(coerce_effect_size_columns(lf)).collect()
    assert out["effect_size"].to_list() == ["0.85"]
    assert "relationship_strength" not in out.columns
    assert out["supporting_text"].to_list() == [["miscellaneous_notes: note"]]


def test_unpicked_relationship_strength_is_dropped_not_folded() -> None:
    """A superseded effect-size alias is dropped outright, never folded into supporting_text.

    Before the alias policy was unified, the loser survived coercion and the unknown-folding
    sweep turned it into ``"relationship_strength: 0.42"`` -- a second number for the slot
    ``effect_size`` had already claimed. Dropping it at coercion time is what stops the edge
    carrying one measurement twice.
    """
    lf: pl.LazyFrame = pl.DataFrame(
        {"subject": ["A"], "object": ["B"], "predicate": ["related_to"], "effect size": ["0.85"], "relationship_strength": ["0.42"]}
    ).lazy()
    out: pl.DataFrame = fold_unknown_to_supporting_text(coerce_effect_size_columns(lf)).collect()
    assert out["effect_size"].to_list() == ["0.85"]
    assert "relationship_strength" not in out.columns
    assert "supporting_text" not in out.columns


def test_coerced_effect_type_survives_unknown_folding() -> None:
    """Coerced effect_type values stay top-level edge fields after unknown folding."""
    lf: pl.LazyFrame = pl.DataFrame(
        {
            "subject": ["A"],
            "object": ["B"],
            "predicate": ["related_to"],
            "effect_size": ["0.85"],
            "effect type": ["Spearman"],
            "miscellaneous_notes": ["note"],
        }
    ).lazy()
    out: pl.DataFrame = fold_unknown_to_supporting_text(coerce_effect_type_columns(lf)).collect()
    assert out["effect_size"].to_list() == ["0.85"]
    assert out["effect_type"].to_list() == ["spearmans_rho"]
    assert out["supporting_text"].to_list() == [["miscellaneous_notes: note"]]


# tcode coerces P value columns after annotations and before clean_numeric
# so downstream numeric_columns/sig/format_numeric see already canonical p_value/adjusted_p_value names
def test_tcode_collect_coerces_pvalue_before_clean_numeric(fixtures_path: Path) -> None:
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    store: Path = Path("/tmp/sectionhash.parquet")
    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {**data, "config": fixtures_path / "minimal_section.yaml", "store": store}
    )

    collected: list[tuple[Any, tuple[Any]]] = tcode_model.collect(Path("/tmp/fullmap.redb"))  # pyright: ignore
    coerce_idx: int = next(i for i, op in enumerate(collected) if op[0].__name__ == "coerce_columns")
    clean_idx: int = next(i for i, op in enumerate(collected) if op[0].__name__ == "clean_numeric")

    assert coerce_idx < clean_idx


# tcode coerces study size columns after annotations and before clean_numeric
# so downstream numeric_columns/format_numeric see already canonical study_size names
def test_tcode_collect_coerces_study_size_before_clean_numeric(fixtures_path: Path) -> None:
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    store: Path = Path("/tmp/sectionhash.parquet")
    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {**data, "config": fixtures_path / "minimal_section.yaml", "store": store}
    )

    collected: list[tuple[Any, tuple[Any]]] = tcode_model.collect(Path("/tmp/fullmap.redb"))  # pyright: ignore
    coerce_idx: int = next(i for i, op in enumerate(collected) if op[0].__name__ == "coerce_columns")
    clean_idx: int = next(i for i, op in enumerate(collected) if op[0].__name__ == "clean_numeric")

    assert coerce_idx < clean_idx


# tcode coerces effect size and effect type columns after annotations and before clean_numeric
# so downstream numeric_columns/format_numeric see already canonical effect_size names.
# The five coercions are ONE op now (`coerce_columns`); their relative order is an internal
# property of `coerce._RULES`, asserted directly rather than through the op list.
def test_tcode_collect_coerces_effect_columns_before_clean_numeric(fixtures_path: Path) -> None:
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    store: Path = Path("/tmp/sectionhash.parquet")
    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {**data, "config": fixtures_path / "minimal_section.yaml", "store": store}
    )

    collected: list[tuple[Any, tuple[Any]]] = tcode_model.collect(Path("/tmp/fullmap.redb"))  # pyright: ignore
    coerce_idx: int = next(i for i, op in enumerate(collected) if op[0].__name__ == "coerce_columns")
    clean_idx: int = next(i for i, op in enumerate(collected) if op[0].__name__ == "clean_numeric")

    assert coerce_idx < clean_idx
    # The within-op precedence the fused op applies, and the precedence `coerced_target` reports.
    assert [rule.classify.__name__ for rule in _RULES] == [
        "pvalue_target",
        "study_size_target",
        "study_metadata_target",
        "effect_size_target",
        "effect_type_target",
    ]


def test_coerced_study_size_alias_reaches_the_inlined_study_not_supporting_text() -> None:
    """Study-size aliases land on Study and never in supporting_text or StudyResult.description.

    Coercion renames ``sample_size`` to the canonical ``study_size`` (the current Biolink
    Study property, biolink-model#1770), and ``inline_supporting_study`` consumes it onto
    the Study BEFORE the unknown-folding sweep runs -- so the value never becomes a
    ``"study_size: …"`` supporting_text entry.
    """
    lf: pl.LazyFrame = pl.DataFrame(
        {"subject": ["A"], "object": ["B"], "predicate": ["related_to"], "sample_size": [12000], "miscellaneous_notes": ["note"]}
    ).lazy()
    studied: pl.LazyFrame = lib.inline_supporting_study(coerce_study_size_columns(lf), "PMID:1", "data.tsv", True)
    out: pl.DataFrame = fold_unknown_to_supporting_text(studied).collect()
    assert "sample_size" not in out.columns
    assert "study_size" not in out.columns
    assert "study_size: 12000" not in out["supporting_text"].to_list()[0]
    study: dict[str, Any] = out["has_supporting_studies"].to_list()[0]["PMID:1"]
    assert study["study_size"] == 12000
    assert "miscellaneous_notes: note" in out["supporting_text"].to_list()[0]


def test_duplicate_study_aliases_do_not_leak_into_provenance_text() -> None:
    """Canonical study metadata wins over duplicate legacy aliases at the finalization boundary."""
    lf: pl.LazyFrame = pl.DataFrame(
        {
            "subject": ["A"],
            "object": ["B"],
            "predicate": ["related_to"],
            "extracted_from_row_number": [7],
            "study_size": [1200],
            "sample_size": [999],
            "supporting_study_size": [888],
            "study_cohort": ["canonical"],
            "supporting_study_cohort": ["legacy"],
        }
    ).lazy()
    coerced: pl.LazyFrame = coerce_study_metadata_columns(coerce_study_size_columns(lf))
    out: pl.DataFrame = fold_unknown_to_supporting_text(lib.inline_supporting_study(coerced, "PMID:1", "data.tsv", True)).collect()
    edge_text: str = json.dumps(out.to_dicts()[0])
    assert "sample_size" not in edge_text
    assert "supporting_study_size" not in edge_text
    assert "supporting_study_cohort" not in edge_text
    study: dict[str, Any] = out["has_supporting_studies"].to_list()[0]["PMID:1"]
    assert study["study_size"] == 1200
    assert study["study_cohort"] == "canonical"
    assert study["has_study_results"] == [{"id": "row:7"}]


def test_publications_wraps_curie_as_list() -> None:
    """publications() wraps a CURIE literal as a single element list[str] column."""
    lf: pl.LazyFrame = pl.DataFrame({"subject": ["A"]}).lazy()
    out: pl.DataFrame = publications(lf, "PMID:42").collect()
    assert out.schema["publications"] == pl.List(pl.String)
    assert out["publications"].to_list() == [["PMID:42"]]


def test_idx_emits_one_based_extracted_from_row_number() -> None:
    """idx emits a 1-Based column named extracted_from_row_number."""
    lf: pl.LazyFrame = pl.LazyFrame({"a": ["x", "y", "z"]})
    out: pl.DataFrame = idx(lf).collect()
    assert "extracted_from_row_number" in out.columns
    assert out["extracted_from_row_number"].to_list() == [1, 2, 3]


def test_fold_unknown_noop_when_all_allowed() -> None:
    """fold_unknown_to_supporting_text is a noop when every column is on the allow list."""
    lf: pl.LazyFrame = pl.DataFrame(
        {
            "subject": ["A"],
            "object": ["B"],
            "predicate": ["related_to"],
            "p_value": [0.01],
            "disease_context_qualifier": ["MONDO:0005148"],
            "publications": [["PMID:1"]],
            # Canonical class-scoped slot: it remains an edge field, never folded into
            # supporting_text.
            "regulatory_approvals": ["011111|022222"],
        }
    ).lazy()
    out: pl.DataFrame = fold_unknown_to_supporting_text(lf).collect()
    # nothing folded, no supporting_text column created
    assert "supporting_text" not in out.columns
    assert set(out.columns) == {"subject", "object", "predicate", "p_value", "disease_context_qualifier", "publications", "regulatory_approvals"}
    assert out["regulatory_approvals"].to_list() == ["011111|022222"]


def test_fold_unknown_single_column() -> None:
    """fold_unknown_to_supporting_text folds a single unknown column as "col: value"."""
    lf: pl.LazyFrame = pl.DataFrame(
        {"subject": ["A"], "object": ["B"], "predicate": ["related_to"], "miscellaneous_notes": ["see smith et al"]}
    ).lazy()
    out: pl.DataFrame = fold_unknown_to_supporting_text(lf).collect()
    assert "miscellaneous_notes" not in out.columns
    assert out.schema["supporting_text"] == pl.List(pl.String)
    assert out["supporting_text"].to_list() == [["miscellaneous_notes: see smith et al"]]


def test_fold_unknown_multiple_columns_sorted() -> None:
    """fold_unknown_to_supporting_text folds multiple columns in deterministic sorted order."""
    lf: pl.LazyFrame = pl.DataFrame(
        {
            "subject": ["A"],
            "object": ["B"],
            "predicate": ["related_to"],
            # deliberately listed out of sort order to verify output is sorted by column name
            "extracted_from_row_number": ["7"],
            "sheet_name": ["Sheet1"],
            "miscellaneous_flag": ["yes"],
        }
    ).lazy()
    out: pl.DataFrame = fold_unknown_to_supporting_text(lf).collect()
    assert out["supporting_text"].to_list() == [["extracted_from_row_number: 7", "miscellaneous_flag: yes", "sheet_name: Sheet1"]]


def test_fold_unknown_skips_null_and_blank() -> None:
    """fold_unknown_to_supporting_text skips null and empty string values."""
    lf: pl.LazyFrame = pl.DataFrame(
        {
            "subject": ["A", "B", "C"],
            "object": ["X", "Y", "Z"],
            "predicate": ["related_to", "related_to", "related_to"],
            "miscellaneous_notes": ["present", None, "   "],
        }
    ).lazy()
    out: pl.DataFrame = fold_unknown_to_supporting_text(lf).collect()
    rows: list[list[str | None]] = out["supporting_text"].to_list()
    assert rows[0] == ["miscellaneous_notes: present"]
    # null and whitespace only both yield an empty list
    assert rows[1] == []
    assert rows[2] == []


def test_fold_unknown_appends_to_existing_list_supporting_text() -> None:
    """fold_unknown_to_supporting_text appends to existing list[str] supporting_text."""
    lf: pl.LazyFrame = pl.DataFrame(
        {
            "subject": ["A"],
            "object": ["B"],
            "predicate": ["related_to"],
            "supporting_text": [["method: fisher exact"]],
            "miscellaneous_notes": ["see smith et al"],
        }
    ).lazy()
    out: pl.DataFrame = fold_unknown_to_supporting_text(lf).collect()
    assert out["supporting_text"].to_list() == [["method: fisher exact", "miscellaneous_notes: see smith et al"]]


def test_fold_unknown_coerces_scalar_supporting_text() -> None:
    """fold_unknown_to_supporting_text coerces scalar supporting_text to list[str] then appends."""
    lf: pl.LazyFrame = pl.DataFrame(
        {"subject": ["A"], "object": ["B"], "predicate": ["related_to"], "supporting_text": ["plain summary"], "miscellaneous_notes": ["extra"]}
    ).lazy()
    out: pl.DataFrame = fold_unknown_to_supporting_text(lf).collect()
    assert out.schema["supporting_text"] == pl.List(pl.String)
    assert out["supporting_text"].to_list() == [["plain summary", "miscellaneous_notes: extra"]]


def test_fold_unknown_preserves_qualifier_columns() -> None:
    """fold_unknown_to_supporting_text never folds known qualifier columns."""
    lf: pl.LazyFrame = pl.DataFrame(
        {
            "subject": ["A"],
            "object": ["B"],
            "predicate": ["related_to"],
            "disease_context_qualifier": ["MONDO:0005148"],
            "anatomical_context_qualifier": ["UBERON:0000061"],
        }
    ).lazy()
    out: pl.DataFrame = fold_unknown_to_supporting_text(lf).collect()
    # no supporting_text column materialized because nothing was foldable
    assert "supporting_text" not in out.columns
    assert "disease_context_qualifier" in out.columns
    assert "anatomical_context_qualifier" in out.columns


SUPPORTING_STUDY_SLOTS: tuple[str, ...] = (
    "supporting_study_method_types",
    "supporting_study_method_description",
    "supporting_study_size",
    "supporting_study_cohort",
    "supporting_study_date_range",
    "supporting_study_context",
)


def test_fold_unknown_tracks_supporting_study_slots_of_installed_model() -> None:
    """The ``supporting_study_*`` slots are folded iff the installed model can hold them.

    ``biolink/biolink-model#1770`` attaches these six to root ``association``. Until it
    ships they are declared in the LinkML schema but on no Pydantic class, so emitting
    them flat produces an unvalidatable edge. The behaviour must be derived from the
    installed model rather than pinned to either state.
    """
    lf: pl.LazyFrame = pl.DataFrame(
        {
            "subject": ["A"],
            "object": ["B"],
            "predicate": ["related_to"],
            "has_supporting_studies": [["PMID:1"]],
            "supporting_study_method_types": [["case-control"]],
            "supporting_study_method_description": ["linear regression adjusted for age"],
            "supporting_study_size": [12000],
            "supporting_study_cohort": ["FINNGEN"],
            "supporting_study_date_range": ["2018-2023"],
            "supporting_study_context": ["European ancestry"],
            "miscellaneous_notes": ["see smith et al"],
        }
    ).lazy()
    out: pl.DataFrame = fold_unknown_to_supporting_text(lf).collect()
    # `has_supporting_studies` is a real Association slot in every supported version.
    assert "has_supporting_studies" in out.columns
    for col in SUPPORTING_STUDY_SLOTS:
        assert (col in out.columns) is (col not in UNSATISFIABLE_EDGE_FIELDS), col
    assert "miscellaneous_notes: see smith et al" in out["supporting_text"].to_list()[0]


def test_allowed_edge_fields_covers_tablassert_pipeline_columns() -> None:
    """ALLOWED_EDGE_FIELDS covers intentional tablassert output columns."""
    for col in ("publications", "sources", "p_value", "supporting_text", "has_supporting_studies"):
        assert col in ALLOWED_EDGE_FIELDS


def test_allowed_edge_fields_tracks_supporting_study_slots_of_installed_model() -> None:
    """``supporting_study_*`` membership follows the installed biolink-model exactly."""
    for col in SUPPORTING_STUDY_SLOTS:
        assert (col in ALLOWED_EDGE_FIELDS) is (col not in UNSATISFIABLE_EDGE_FIELDS), col


def test_compile_graph_folds_unknown_annotations_into_supporting_text(monkeypatch: Any, tmp_path: Path, rig_factory: Any) -> None:
    """compile_graph folds non allow list annotation columns into supporting_text on edges."""
    monkeypatch.chdir(tmp_path)
    sub: Path = tmp_path / "sub.parquet"
    pl.DataFrame(
        {
            "subject": ["A"],
            "subject_category": ["gene"],
            "subject_pre_resolution": ["A"],
            "object": ["X"],
            "object_category": ["disease"],
            "object_pre_resolution": ["X"],
            "predicate": ["biolink:related_to"],
            "knowledge_level": ["knowledge_assertion"],
            "agent_type": ["manual_agent"],
            "primary_knowledge_source": ["infores:fold-kg"],
            "sources": [
                [
                    {
                        "resource_id": "infores:fold-kg",
                        "resource_role": "primary_knowledge_source",
                        "source_record_urls": ["https://example.org/fold.tsv"],
                    }
                ]
            ],
            "miscellaneous_notes": ["see smith et al"],
            "extracted_from_row_number": ["7"],
            "p_value": [0.01],
            "publications": [["PMID:1"]],
        }
    ).write_parquet(sub)
    lib.compile_graph([sub], "fold", "1.0.0", rig_factory(tmp_path, infores_id="infores:fold-kg"))
    edges: str = (tmp_path / "fold_1.0.0.edges.ndjson").read_text()
    # folded columns no longer appear as top level JSON keys on the edge object
    assert '"miscellaneous_notes":' not in edges
    assert '"extracted_from_row_number":' not in edges
    # but their values survive inside supporting_text
    assert "miscellaneous_notes: see smith et al" in edges
    assert "extracted_from_row_number: 7" in edges
    # real biolist fields survive as top level fields
    assert '"p_value":0.01' in edges or '"p_value": 0.01' in edges
    assert "PMID:1" in edges


def test_compile_subgraph_e2e_value_encoded_nodes(monkeypatch: Any, tmp_path: Path) -> None:
    """compile_subgraph resolves value-encoded subject/object nodes into parquet output.

    The fake fullmap returns the legacy flat-row shape (no ``records`` key) for every call
    regardless of ``return_format``, so ``lookup_rows`` takes its legacy-compat path: after the
    ``return_format="pairs"`` query it re-queries the FULL term set (hardening so partially-warm
    caches do not drop already-cached terms). The subject and object terms are therefore fetched
    in two identical FULLY-BATCHED calls — never split per-column — which is the batching
    guarantee this test asserts.
    """
    rows: dict[str, list[dict[str, object]]] = {
        "brca1": [fake_fullmap_row("brca1", "HGNC:1100", "BRCA1", "Gene", 9606)],
        "tp53": [fake_fullmap_row("tp53", "HGNC:11998", "TP53", "Gene", 9606)],
    }
    calls: list[list[str]] = install_fake_fullmap(monkeypatch, rows)
    table_path, _ = write_text_section(
        tmp_path,
        "value_nodes",
        {
            "statement": {"subject": {"method": "value", "encoding": "BRCA1"}, "object": {"method": "value", "encoding": "TP53"}},
            "provenance": {"repo": "PMC", "publication": "PMC0000000"},
        },
        ["ignored"],
    )
    data: Any = from_yaml(table_path)
    store: Path = tmp_path / "value_nodes.parquet"
    tcode_model: Tcode = Tcode.model_validate({**data, "config": table_path, "store": store, "name": "TEST_KG", "infores": "infores:test-kg"})  # pyright: ignore

    result_path: Path = lib.compile_subgraph(tcode_model.collect(tmp_path / "fullmap.redb"))  # pyright: ignore
    result: dict[str, Any] = pl.read_parquet(result_path).row(0, named=True)

    assert calls == [["brca1", "tp53"], ["brca1", "tp53"]]  # legacy-shape re-query; both fully batched
    assert result_path == store
    assert result["subject"] == "HGNC:1100"
    assert result["subject_name"] == "BRCA1"
    assert result["subject_category"] == "biolink:Gene"
    assert result["object"] == "HGNC:11998"
    assert result["object_name"] == "TP53"
    assert result["predicate"] == "biolink:related_to"
    assert result["publications"] == ["PMCID:PMC0000000"]
    primary: dict[str, Any] = next(s for s in result["sources"] if s["resource_role"] == "primary_knowledge_source")
    assert primary["resource_id"] == "infores:test-kg"
    assert "primary_knowledge_source" not in result


def test_compile_subgraph_e2e_column_cleanup_and_numeric_annotations(monkeypatch: Any, tmp_path: Path) -> None:
    """compile_subgraph applies column encodings, regex cleanup, aliases, and numeric formatting."""
    rows: dict[str, list[dict[str, object]]] = {
        "brca1": [fake_fullmap_row("brca1", "HGNC:1100", "BRCA1", "Gene", 9606)],
        "tp53": [fake_fullmap_row("tp53", "HGNC:11998", "TP53", "Gene", 9606)],
    }
    install_fake_fullmap(monkeypatch, rows)
    table_path, source_path = write_text_section(
        tmp_path,
        "column_cleanup",
        {
            "statement": {
                "subject": {"method": "column", "encoding": "A", "regex": [{"pattern": "\\s+", "replacement": ""}], "remove": ["-", "\\[.*\\]"]},
                "object": {"method": "column", "encoding": "B", "remove": ["\\s+"]},
            },
            "annotations": [
                {"annotation": "P Value", "method": "column", "encoding": "C"},
                {"annotation": "sample size", "method": "column", "encoding": "D"},
                {"annotation": "miscellaneous_notes", "method": "column", "encoding": "E"},
            ],
            "provenance": {"repo": "PMID", "publication": "12345"},
        },
        ["BRCA-1 [alias]\tTP 53\t1e-8\t1200\tkept note"],
    )
    data: Any = from_yaml(table_path)
    store: Path = tmp_path / "column_cleanup.parquet"
    tcode_model: Tcode = Tcode.model_validate({**data, "config": table_path, "store": store})  # pyright: ignore

    result_path: Path = lib.compile_subgraph(tcode_model.collect(tmp_path / "fullmap.redb"))  # pyright: ignore
    result: dict[str, Any] = pl.read_parquet(result_path).row(0, named=True)

    assert result["subject"] == "HGNC:1100"
    assert result["original_subject"] == "BRCA-1 [alias]"
    assert result["object"] == "HGNC:11998"
    assert result["original_object"] == "TP 53"
    assert result["p_value"] == "1.0000e-08"
    # biolink-model 4.4.4 attaches ``statistical_significance_qualifier`` to every
    # association class, so the derived band rides the edge as a bare enum token.
    assert result["statistical_significance_qualifier"] == "very_strongly_significant"
    # The ``sample size`` alias is coerced to ``study_size`` and lands as a real int
    # field on the inlined Study; the StudyResult only anchors the source row.
    study: dict[str, Any] = result["has_supporting_studies"]["PMID:12345"]
    assert study["id"] == "PMID:12345"
    assert study["name"] == source_path.name
    assert study["study_size"] == 1200
    # The result only anchors the row; the parquet-level null description is stripped
    # by the NDJSON writer before emission.
    assert [r["id"] for r in study["has_study_results"]] == ["row:1"]
    assert not any(r.get("description") for r in study["has_study_results"])
    assert result["miscellaneous_notes"] == "kept note"
    assert result["publications"] == ["PMID:12345"]


def test_compile_subgraph_e2e_release_drops_rows_before_fullmap_lookup(monkeypatch: Any, tmp_path: Path) -> None:
    """release-mode subgraph compilation drops not-significant rows before resolution."""
    # Keys are the level_one form of the source row below ("KeptGene" -> "keptgen", Porter2 stem).
    # Hardcoded on purpose: deriving them through nlp.level_one would let normalization drift pass silently.
    rows: dict[str, list[dict[str, object]]] = {
        "keptgen": [fake_fullmap_row("keptgen", "HGNC:1", "KEPTGENE", "Gene", 9606)],
        "keptdiseas": [fake_fullmap_row("keptdiseas", "MONDO:1", "Kept disease", "Disease", 0)],
        "droppedgen": [fake_fullmap_row("droppedgen", "HGNC:2", "DROPPEDGENE", "Gene", 9606)],
        "droppeddiseas": [fake_fullmap_row("droppeddiseas", "MONDO:2", "Dropped disease", "Disease", 0)],
    }
    calls: list[list[str]] = install_fake_fullmap(monkeypatch, rows)
    table_path, source_path = write_text_section(
        tmp_path,
        "release_drop",
        {
            "statement": {"subject": {"method": "column", "encoding": "A"}, "object": {"method": "column", "encoding": "B"}},
            "annotations": [{"annotation": "p_value", "method": "column", "encoding": "C"}],
            "provenance": {"repo": "PMC", "publication": "PMC0000000"},
        },
        ["DroppedGene\tDroppedDisease\t0.5", "KeptGene\tKeptDisease\t0.01"],
    )
    data: Any = from_yaml(table_path)
    store: Path = tmp_path / "release_drop.parquet"
    tcode_model: Tcode = Tcode.model_validate({**data, "config": table_path, "store": store, "release": True})  # pyright: ignore

    result_path: Path = lib.compile_subgraph(tcode_model.collect(tmp_path / "fullmap.redb"))  # pyright: ignore
    result: pl.DataFrame = pl.read_parquet(result_path)
    looked_up: set[str] = set(calls[0])

    assert result.height == 1
    assert result["subject"].to_list() == ["HGNC:1"]
    assert result["object"].to_list() == ["MONDO:1"]
    # The derived band is a real edge field in biolink-model 4.4.4: bare enum token.
    assert result["statistical_significance_qualifier"].to_list() == ["strongly_significant"]
    # Published wrapper with no study metadata remains, but an empty rescue column
    # cannot justify a row-only StudyResult.
    study: dict[str, Any] = result["has_supporting_studies"].to_list()[0]["PMCID:PMC0000000"]
    assert study["name"] == source_path.name
    assert not study.get("has_study_results")
    assert "droppedgen" not in looked_up
    assert "droppeddiseas" not in looked_up


def test_compile_subgraph_e2e_release_drops_zero_effect_size_before_fullmap_lookup(monkeypatch: Any, tmp_path: Path) -> None:
    """release-mode subgraph compilation drops zero effect-size rows before resolution."""
    # Keys are the level_one form of the source row below ("KeptGene" -> "keptgen", Porter2 stem).
    # Hardcoded on purpose: deriving them through nlp.level_one would let normalization drift pass silently.
    rows: dict[str, list[dict[str, object]]] = {
        "keptgen": [fake_fullmap_row("keptgen", "HGNC:1", "KEPTGENE", "Gene", 9606)],
        "keptdiseas": [fake_fullmap_row("keptdiseas", "MONDO:1", "Kept disease", "Disease", 0)],
        "droppedgen": [fake_fullmap_row("droppedgen", "HGNC:2", "DROPPEDGENE", "Gene", 9606)],
        "droppeddiseas": [fake_fullmap_row("droppeddiseas", "MONDO:2", "Dropped disease", "Disease", 0)],
    }
    calls: list[list[str]] = install_fake_fullmap(monkeypatch, rows)
    table_path, _ = write_text_section(
        tmp_path,
        "release_drop_zero_effect",
        {
            "statement": {"subject": {"method": "column", "encoding": "A"}, "object": {"method": "column", "encoding": "B"}},
            "annotations": [
                {"annotation": "p_value", "method": "column", "encoding": "C"},
                {"annotation": "effect_size", "method": "column", "encoding": "D"},
                {"annotation": "effect_type", "method": "value", "encoding": "spearmans_rho"},
            ],
            "provenance": {"repo": "PMC", "publication": "PMC0000000"},
        },
        ["DroppedGene\tDroppedDisease\t0.05\t0.0", "KeptGene\tKeptDisease\t0.01\t1.5"],
    )
    data: Any = from_yaml(table_path)
    store: Path = tmp_path / "release_drop_zero_effect.parquet"
    tcode_model: Tcode = Tcode.model_validate({**data, "config": table_path, "store": store, "release": True})  # pyright: ignore

    result_path: Path = lib.compile_subgraph(tcode_model.collect(tmp_path / "fullmap.redb"))  # pyright: ignore
    result: pl.DataFrame = pl.read_parquet(result_path)
    looked_up: set[str] = set(calls[0])

    assert result.height == 1
    assert result["subject"].to_list() == ["HGNC:1"]
    assert result["object"].to_list() == ["MONDO:1"]
    # biolink-model 4.4.4 types ``effect_size`` float (PR #1774): a real JSON number.
    assert result["effect_size"].to_list() == [1.5]
    assert "droppedgen" not in looked_up
    assert "droppeddiseas" not in looked_up


def test_compile_subgraph_e2e_head_caps_rows_to_five(monkeypatch: Any, tmp_path: Path) -> None:
    """--head randomly samples 5 of 8 rows before fullmap resolution (never more than available)."""
    rows: dict[str, list[dict[str, object]]] = {
        **{f"gene{i}": [fake_fullmap_row(f"gene{i}", f"HGNC:{i}", f"GENE{i}", "Gene", 9606)] for i in range(1, 9)},
        **{f"disease{i}": [fake_fullmap_row(f"disease{i}", f"MONDO:{i}", f"Disease{i}", "Disease", 0)] for i in range(1, 9)},
    }
    calls: list[list[str]] = install_fake_fullmap(monkeypatch, rows)
    table_path, _ = write_text_section(
        tmp_path,
        "head_cap",
        {
            "statement": {"subject": {"method": "column", "encoding": "A"}, "object": {"method": "column", "encoding": "B"}},
            "provenance": {"repo": "PMC", "publication": "PMC0000000"},
        },
        [f"gene{i}\tdisease{i}" for i in range(1, 9)],
    )
    data: Any = from_yaml(table_path)
    store: Path = tmp_path / "head_cap.parquet"
    tcode_model: Tcode = Tcode.model_validate({**data, "config": table_path, "store": store, "head": True})  # pyright: ignore

    result_path: Path = lib.compile_subgraph(tcode_model.collect(tmp_path / "fullmap.redb"))  # pyright: ignore
    result: pl.DataFrame = pl.read_parquet(result_path)
    looked_up: set[str] = set(chain.from_iterable(calls))
    genes: set[str] = {t for t in looked_up if t.startswith("gene")}
    diseases: set[str] = {t for t in looked_up if t.startswith("disease")}

    assert result.height == 5
    assert len(genes) == 5
    assert len(diseases) == 5
    assert genes <= {f"gene{i}" for i in range(1, 9)}
    assert diseases <= {f"disease{i}" for i in range(1, 9)}


def test_compile_subgraph_and_graph_e2e_does_not_emit_species_context(monkeypatch: Any, tmp_path: Path, rig_factory: Any) -> None:
    """Species context is absent from section, study, and final edge output."""
    monkeypatch.chdir(tmp_path)
    rows: dict[str, list[dict[str, object]]] = {
        "brca1": [fake_fullmap_row("brca1", "HGNC:1100", "BRCA1", "Gene", 9606)],
        # level_one keys: "Disease X" -> "diseas x", "Homo sapiens" -> "homo sapien" (Porter2 stems).
        "diseas x": [fake_fullmap_row("diseas x", "MONDO:0000001", "Disease X", "Disease", 0)],
        "homo sapien": [fake_fullmap_row("homo sapien", "NCBITaxon:9606", "Homo sapiens", "OrganismTaxon", 9606)],
    }
    install_fake_fullmap(monkeypatch, rows)
    table_path, _ = write_text_section(
        tmp_path,
        "qualifier",
        {
            "statement": {
                "subject": {"method": "value", "encoding": "BRCA1"},
                "object": {"method": "value", "encoding": "Disease X"},
                "qualifiers": [],
            },
            "provenance": {"repo": "PMC", "publication": "PMC0000000"},
        },
        ["ignored"],
    )
    data: Any = from_yaml(table_path)
    store: Path = tmp_path / "qualifier.parquet"
    tcode_model: Tcode = Tcode.model_validate({**data, "config": table_path, "store": store, "name": "QUAL_KG", "infores": "infores:qual-kg"})  # pyright: ignore

    subgraph: Path = lib.compile_subgraph(tcode_model.collect(tmp_path / "fullmap.redb"))  # pyright: ignore
    frame: pl.DataFrame = pl.read_parquet(subgraph)
    assert "species_context_qualifier" not in frame.columns
    assert "species_context_qualifier" not in json.dumps(frame["has_supporting_studies"].to_list())

    lib.compile_graph([subgraph], "qual", "1.0.0", rig_factory(tmp_path, infores_id="infores:qual-kg"))
    edges: list[dict[str, Any]] = [json.loads(line) for line in (tmp_path / "qual_1.0.0.edges.ndjson").read_text().splitlines()]
    nodes: list[dict[str, Any]] = [json.loads(line) for line in (tmp_path / "qual_1.0.0.nodes.ndjson").read_text().splitlines()]

    assert "species_context_qualifier" not in json.dumps(edges[0])
    assert all("species_context_qualifier_pre_resolution" not in edge for edge in edges)
    assert {node["id"] for node in nodes} == {"HGNC:1100", "MONDO:0000001"}
    assert "NCBITaxon:9606" not in {node["id"] for node in nodes}


def test_node_output_reflects_disease_taxon(monkeypatch: Any, tmp_path: Path, rig_factory: Any) -> None:
    """Taxon-bearing disease nodes surface their taxon in KGX nodes NDJSON."""
    monkeypatch.chdir(tmp_path)
    rows: dict[str, list[dict[str, object]]] = {
        # level_one key: "Disease Taxon" -> "diseas taxon" (Porter2 stem of "disease").
        "diseas taxon": [fake_fullmap_row("diseas taxon", "MONDO:50", "Taxon-bearing disease", "Disease", 9606)],
        "brca1": [fake_fullmap_row("brca1", "HGNC:1100", "BRCA1", "Gene", 9606)],
    }
    install_fake_fullmap(monkeypatch, rows)
    table_path, _ = write_text_section(
        tmp_path,
        "disease_taxon_node",
        {
            "statement": {"subject": {"method": "value", "encoding": "Disease Taxon"}, "object": {"method": "value", "encoding": "BRCA1"}},
            "provenance": {"repo": "PMC", "publication": "PMC0000000"},
        },
        ["ignored"],
    )
    data: Any = from_yaml(table_path)
    store: Path = tmp_path / "disease_taxon_node.parquet"
    tcode_model: Tcode = Tcode.model_validate(
        {**data, "config": table_path, "store": store, "name": "DISEASE_TAXON_KG", "infores": "infores:disease-taxon-kg"}
    )  # pyright: ignore

    subgraph: Path = lib.compile_subgraph(tcode_model.collect(tmp_path / "fullmap.redb"))  # pyright: ignore
    lib.compile_graph([subgraph], "disease_taxon", "1.0.0", rig_factory(tmp_path, infores_id="infores:disease-taxon-kg"))
    nodes: list[dict[str, Any]] = [json.loads(line) for line in (tmp_path / "disease_taxon_1.0.0.nodes.ndjson").read_text().splitlines()]

    disease_node: dict[str, Any] = next(node for node in nodes if node["id"] == "MONDO:50")
    assert disease_node["in_taxon"] == ["NCBITaxon:9606"]


def test_build_pipeline_e2e_smoke_with_monkeypatched_fullmap(monkeypatch: Any, tmp_path: Path, rig_factory: Any) -> None:
    """build_pipeline runs all six stages and emits KGX/RIG using a monkeypatched fullmap DB."""
    from tablassert.ingests import to_yaml

    monkeypatch.chdir(tmp_path)
    (tmp_path / ".tablassert" / "store").mkdir(parents=True)
    monkeypatch.setattr(cli, "Pool", SyncPool)
    rows: dict[str, list[dict[str, object]]] = {
        "brca1": [fake_fullmap_row("brca1", "HGNC:1100", "BRCA1", "Gene", 9606)],
        "tp53": [fake_fullmap_row("tp53", "HGNC:11998", "TP53", "Gene", 9606)],
    }
    install_fake_fullmap(monkeypatch, rows)
    table_path, _ = write_text_section(
        tmp_path,
        "pipeline_table",
        {
            "statement": {"subject": {"method": "value", "encoding": "BRCA1"}, "object": {"method": "value", "encoding": "TP53"}},
            "provenance": {"repo": "PMC", "publication": "PMC0000000"},
        },
        ["ignored"],
    )
    table_data: Any = from_yaml(table_path)
    to_yaml(table_path, {"template": table_data})
    graph_path: Path = tmp_path / "graph.yaml"
    to_yaml(
        graph_path,
        {
            "name": "PIPELINE_KG",
            "version": "0.1.0",
            "tables": [str(table_path)],
            "fullmap": str(tmp_path / "fullmap.redb"),
            "rig": rig_factory(
                tmp_path, infores_id="infores:pipeline-kg", source_info={"description": "Pipeline smoke graph."}, source_files=["pipeline_table.tsv"]
            ),
        },
    )
    progress: DummyProgress = DummyProgress()

    cli.build_pipeline(graph_path, cast(Any, progress), release=False, qc=False, log=False)
    edge_rows: list[dict[str, Any]] = [json.loads(line) for line in (tmp_path / "PIPELINE_KG_0.1.0.edges.ndjson").read_text().splitlines()]
    node_rows: list[dict[str, Any]] = [json.loads(line) for line in (tmp_path / "PIPELINE_KG_0.1.0.nodes.ndjson").read_text().splitlines()]
    rig: dict[str, Any] = from_yaml(tmp_path / "PIPELINE_KG_0.1.0.RIG.yaml")  # pyright: ignore

    assert progress.stages == [
        "Loading Tables",
        "Extracting Sections",
        "Building TCode",
        "Collecting Instructions",
        "Building Subgraphs",
        "Compiling Graph",
    ]
    assert len(edge_rows) == 1
    assert edge_rows[0]["subject"] == "HGNC:1100"
    assert edge_rows[0]["object"] == "HGNC:11998"
    primary_source: dict[str, Any] = next(x for x in edge_rows[0]["sources"] if x["resource_role"] == "primary_knowledge_source")
    assert primary_source["resource_id"] == "infores:pipeline-kg"
    assert primary_source["upstream_resource_ids"] == ["infores:pubmed-central"]
    # Source provenance uses `resource_id` without a duplicate Biolink `id`.
    assert "id" not in primary_source
    assert "primary_knowledge_source" not in edge_rows[0]
    assert {row["id"] for row in node_rows} == {"HGNC:1100", "HGNC:11998"}
    assert rig["name"] == "PIPELINE_KG v0.1.0 Resource Ingest Guide"
    assert rig["source_info"]["infores_id"] == "infores:pipeline-kg"  # pyright: ignore
    # Generated artifacts are documented at the configured URL base.
    locations: list[str] = [entry["location"] for entry in rig["ingest_info"]["relevant_files"]]  # pyright: ignore
    assert "https://example.org/pipeline-kg/PIPELINE_KG_0.1.0.nodes.ndjson" in locations
    assert "https://example.org/pipeline-kg/PIPELINE_KG_0.1.0.edges.ndjson" in locations
    edge_type: dict[str, Any] = rig["target_info"]["edge_type_info"][0]  # pyright: ignore
    # Role separation: the graph infores is the primary source; PMC is supporting data.
    assert edge_type["primary_knowledge_sources"] == ["infores:pipeline-kg"]
    assert edge_type["supporting_data_sources"] == ["infores:pubmed-central"]
    # Source files come from the configured rig.source_files, not scraped from edges.
    assert edge_type["source_files"] == ["pipeline_table.tsv"]

    # The gate: every emitted record must construct as its own Biolink class. Without
    # this, a build can (and previously did) ship files where no record validated.
    report: dict[str, Any] = validate_kgx(tmp_path / "PIPELINE_KG_0.1.0.nodes.ndjson", tmp_path / "PIPELINE_KG_0.1.0.edges.ndjson")
    assert report["ok"], report
    assert report["edges"]["valid"] == report["edges"]["total"] == 1
    assert report["nodes"]["valid"] == report["nodes"]["total"] == 2


def test_build_pipeline_head_mode_isolates_store_and_caps_rows(monkeypatch: Any, tmp_path: Path, rig_factory: Any) -> None:
    """--head caches subgraphs to .head.parquet (never clobbering a full build) and caps to 5 rows."""
    from tablassert.ingests import to_yaml

    monkeypatch.chdir(tmp_path)
    (tmp_path / ".tablassert" / "store").mkdir(parents=True)
    monkeypatch.setattr(cli, "Pool", SyncPool)
    rows: dict[str, list[dict[str, object]]] = {
        **{f"gene{i}": [fake_fullmap_row(f"gene{i}", f"HGNC:{i}", f"GENE{i}", "Gene", 9606)] for i in range(1, 9)},
        **{f"disease{i}": [fake_fullmap_row(f"disease{i}", f"MONDO:{i}", f"Disease{i}", "Disease", 0)] for i in range(1, 9)},
    }
    install_fake_fullmap(monkeypatch, rows)
    table_path, _ = write_text_section(
        tmp_path,
        "head_store",
        {
            "statement": {"subject": {"method": "column", "encoding": "A"}, "object": {"method": "column", "encoding": "B"}},
            "provenance": {"repo": "PMC", "publication": "PMC0000000"},
        },
        [f"gene{i}\tdisease{i}" for i in range(1, 9)],
    )
    table_data: Any = from_yaml(table_path)
    to_yaml(table_path, {"template": table_data})
    graph_path: Path = tmp_path / "graph.yaml"
    to_yaml(
        graph_path,
        {
            "name": "HEAD_KG",
            "version": "0.1.0",
            "tables": [str(table_path)],
            "fullmap": str(tmp_path / "fullmap.redb"),
            "rig": rig_factory(tmp_path, infores_id="infores:head-kg", source_info={"description": "Head preview graph."}),
        },
    )
    progress: DummyProgress = DummyProgress()

    cli.build_pipeline(graph_path, cast(Any, progress), release=False, qc=False, log=False, head=True)

    edge_rows: list[dict[str, Any]] = [json.loads(line) for line in (tmp_path / "HEAD_KG_0.1.0.edges.ndjson").read_text().splitlines()]
    store_files: list[Path] = list((tmp_path / ".tablassert" / "store").glob("*.parquet"))

    assert len(edge_rows) == 5
    assert store_files, "expected a cached subgraph parquet"
    assert all(f.name.endswith(".head.parquet") for f in store_files)


def test_compile_subgraph_threads_fine_phases_into_resolve_and_qc(monkeypatch: Any, tmp_path: Path) -> None:
    """compile_subgraph forwards on_phase into resolve_batch/fullmap_audit so fine sub-phases fire in order."""
    rows: dict[str, list[dict[str, object]]] = {
        "brca1": [fake_fullmap_row("brca1", "HGNC:1100", "BRCA1", "Gene", 9606)],
        "tp53": [fake_fullmap_row("tp53", "HGNC:11998", "TP53", "Gene", 9606)],
    }
    install_fake_fullmap(monkeypatch, rows)

    # Safety net: this data passes QC at the exact stage, so SapBERT must never run (keeps the test offline).
    class DummySapBERT:
        def encode(self, values: list[str]) -> object:
            raise AssertionError("Stage 4 (SapBERT) must not run for exact-match QC data")

    monkeypatch.setattr("tablassert.qc.get_sapbert", lambda: DummySapBERT())

    table_path, _ = write_text_section(
        tmp_path,
        "fine_phases",
        {
            "statement": {"subject": {"method": "value", "encoding": "BRCA1"}, "object": {"method": "value", "encoding": "TP53"}},
            "provenance": {"repo": "PMC", "publication": "PMC0000000"},
        },
        ["ignored"],
    )
    data: Any = from_yaml(table_path)
    store: Path = tmp_path / "fine_phases.parquet"
    tcode_model: Tcode = Tcode.model_validate({**data, "config": table_path, "store": store, "qc": True})  # pyright: ignore

    phases: list[str] = []
    result_path: Path = lib.compile_subgraph(tcode_model.collect(tmp_path / "fullmap.redb"), on_phase=phases.append)  # pyright: ignore

    assert result_path == store
    # Per-column resolve sub-phases fire in spec order, before any QC sub-phase.
    assert phases.index("resolve:subject") < phases.index("resolve:object")
    assert phases.index("resolve:object") < phases.index("qc:exact")
    # QC sub-phases fire for the audits: exact then fuzzy; abbrev/sapbert never (exact-match quick exit).
    assert phases.index("qc:exact") < phases.index("qc:fuzzy")
    assert "qc:abbrev" not in phases
    assert "qc:sapbert" not in phases


def test_predicate_options_answers_which_predicates_keep_the_class() -> None:
    """The authoring-time helper: which predicates does a (subject, object) pair actually permit?

    Before this existed there was no way to ask, which is how 723,595 edges shipped with
    ``gene_associated_with_condition`` on ``GeneToDiseaseAssociation`` -- a predicate that class
    forbids, so every one of them silently demoted to bare ``biolink:Association``.
    """
    from tablassert.lib import derived_edge_category, predicate_options

    assert derived_edge_category("biolink:Gene", "biolink:Disease") == "biolink:GeneToDiseaseAssociation"
    options = predicate_options("biolink:Gene", "biolink:Disease")
    assert options is not None
    assert options == {"biolink:affects", "biolink:associated_with", "biolink:contributes_to"}
    assert "biolink:gene_associated_with_condition" not in options

    # The bare name works identically (configs are written without the prefix).
    assert predicate_options("Gene", "Disease") == options
    # Roles roll up through CATEGORY_PARENT: Protein is a Gene-role subject.
    assert predicate_options("biolink:Protein", "biolink:Disease") == options
    # gene_associated_with_condition IS legal -- on the variant~gene pair, not gene~disease.
    assert "biolink:gene_associated_with_condition" in (predicate_options("SequenceVariant", "Gene") or set())
    # A pair with no specific association class leaves `predicate` open: nothing to demote.
    assert predicate_options("biolink:OrganismTaxon", "biolink:ChemicalEntity") is None


def test_prune_to_class_wraps_scalar_for_uniformly_multivalued_slot() -> None:
    """A scalar bound for a slot every declaring class types multivalued becomes a one-element list.

    Regression: the wrap used to be built per row (``when(class-multivalued).then(concat_list)``),
    which asks one column for two dtypes and dies in ``strict_cast`` at collect with
    ``cannot cast List type (inner: 'String', to: 'String')``.
    """
    from tablassert.lib import prune_to_class

    lf: pl.LazyFrame = pl.DataFrame({"category": [["biolink:Association"], ["biolink:Association"]], "has_evidence": ["ECO:0000001", None]}).lazy()
    out: pl.DataFrame = prune_to_class(lf).collect()
    assert out["has_evidence"].to_list() == [["ECO:0000001"], None]


def test_prune_to_class_mixed_class_qualifier_does_not_crash_and_rescues() -> None:
    """AVUTHU1 regression: a scalar qualifier a row's class refuses is nulled and rescued, not crashed.

    ``anatomical_context_qualifier`` is declared by only some association classes (multivalued
    on all of them); ``biolink:Association`` is not one. The refused value is preserved in
    ``_pruned_by_class`` for the inlined StudyResult description; a declaring class keeps the
    value, wrapped because the slot is multivalued everywhere it is declared.
    """
    from tablassert.lib import PRUNED_COLUMN, prune_to_class

    lf: pl.LazyFrame = pl.DataFrame(
        {
            "category": [["biolink:Association"], ["biolink:ChemicalAffectsGeneAssociation"]],
            "anatomical_context_qualifier": ["UBERON:0001555", "UBERON:0001556"],
        }
    ).lazy()
    out: pl.DataFrame = prune_to_class(lf).collect()
    assert out["anatomical_context_qualifier"].to_list() == [None, ["UBERON:0001556"]]
    assert out[PRUNED_COLUMN].to_list() == [["anatomical_context_qualifier=UBERON:0001555"], []]


def test_inline_supporting_study_skips_a_study_with_no_identity_and_nothing_to_carry() -> None:
    """An unpublished section with nothing to carry emits no ``has_supporting_studies``.

    ``study_id`` would be the config stem and the struct would assert a Study that never
    existed on every edge. The row and sheet columns are still consumed -- they are never
    meant to reach the edge.
    """
    from tablassert.lib import inline_supporting_study

    lf: pl.LazyFrame = pl.DataFrame({"subject": ["A"], "object": ["B"], "extracted_from_row_number": [6848], "sheet_name": ["Table_S7"]}).lazy()
    out: pl.DataFrame = inline_supporting_study(lf, "my_table", "my_table.tsv", False).collect()
    assert "has_supporting_studies" not in out.columns
    assert "extracted_from_row_number" not in out.columns
    assert "sheet_name" not in out.columns
    assert out["subject"].to_list() == ["A"]


def test_inline_supporting_study_keeps_an_unidentified_study_that_carries_values() -> None:
    """Without a publication the struct still survives when it has values to preserve.

    The rescue path is the whole reason the fallback id exists: a class-refused value is
    real evidence, and losing it would be worse than keying its carrier by the config
    stem. The id (config stem) and name (source filename) stay disjoint, the StudyResult
    is identified by its scoped ``row:<N>`` CURIE, and the rescued value rides the
    description.
    """
    from tablassert.lib import PRUNED_COLUMN, inline_supporting_study

    lf: pl.LazyFrame = pl.DataFrame(
        {"subject": ["A"], "extracted_from_row_number": [12], PRUNED_COLUMN: [["species_context_qualifier=NCBITaxon:9606"]]}
    ).lazy()
    out: pl.DataFrame = inline_supporting_study(lf, "my_table", "my_table.tsv", False).collect()
    study: dict[str, Any] = out["has_supporting_studies"].to_list()[0]["my_table"]
    assert study["id"] == "my_table"
    assert study["name"] == "my_table.tsv"
    assert study["has_study_results"] == [{"id": "row:12", "description": "species_context_qualifier=NCBITaxon:9606"}]
    assert PRUNED_COLUMN not in out.columns


def test_inline_supporting_study_keeps_a_published_wrapper_with_no_content() -> None:
    """A real publication earns the wrapper on its own -- id and name stay disjoint fields.

    With no study metadata and nothing rescued there is no StudyResult to anchor: the
    struct is the bare ``{id, name}`` study wrapper (``has_study_results`` omitted).
    """
    from tablassert.lib import inline_supporting_study

    lf: pl.LazyFrame = pl.DataFrame({"subject": ["A"], "extracted_from_row_number": [12]}).lazy()
    out: pl.DataFrame = inline_supporting_study(lf, "PMID:123", "Table_S7", True).collect()
    study: dict[str, Any] = out["has_supporting_studies"].to_list()[0]["PMID:123"]
    assert study == {"id": "PMID:123", "name": "Table_S7"}


def test_inline_supporting_study_puts_metadata_on_the_study_and_row_on_the_result() -> None:
    """Study metadata lands as typed Study fields; the StudyResult only anchors the row.

    ``study_size`` / ``study_context`` are the current Biolink Study properties
    (biolink-model#1770): they are emitted on the Study itself, never duplicated into a
    description, and the single StudyResult carries the ``row:<N>`` id with no name and
    no description.
    """
    from tablassert.lib import inline_supporting_study

    lf: pl.LazyFrame = pl.DataFrame(
        {
            "subject": ["A"],
            "extracted_from_row_number": [42],
            "study_size": [9],
            "study_context": ["  European ancestry  "],
            "study_method_types": ["case-control"],
        }
    ).lazy()
    out: pl.DataFrame = inline_supporting_study(lf, "PMCID:PMC1", "all correlations", True).collect()
    study: dict[str, Any] = out["has_supporting_studies"].to_list()[0]["PMCID:PMC1"]
    assert study["study_size"] == 9
    assert study["study_context"] == "European ancestry"
    assert study["study_method_types"] == ["case-control"]
    assert study["has_study_results"] == [{"id": "row:42"}]
    assert "study_size" not in out.columns
    assert "study_context" not in out.columns


def test_inline_supporting_study_omits_a_name_equal_to_the_id() -> None:
    """A study name duplicating the id is omitted -- id and name never carry the same info."""
    from tablassert.lib import inline_supporting_study

    lf: pl.LazyFrame = pl.DataFrame({"subject": ["A"], "extracted_from_row_number": [1]}).lazy()
    out: pl.DataFrame = inline_supporting_study(lf, "PMID:1", "PMID:1", True).collect()
    study: dict[str, Any] = out["has_supporting_studies"].to_list()[0]["PMID:1"]
    assert study["id"] == "PMID:1"
    assert study["name"] is None


def test_inline_supporting_study_skips_null_or_blank_unpublished_metadata() -> None:
    """An unpublished fallback study is null when its metadata carries no row value."""
    from tablassert.lib import inline_supporting_study

    lf: pl.LazyFrame = pl.DataFrame({"extracted_from_row_number": [1, 2], "study_cohort": [None, "   "]}).lazy()
    out: pl.DataFrame = inline_supporting_study(lf, "table", "data.tsv", False).collect()
    assert out["has_supporting_studies"].to_list() == [None, None]


def test_inline_supporting_study_skips_null_like_method_type_items() -> None:
    """Blank and null-like list items do not justify an unpublished fallback Study."""
    from tablassert.lib import inline_supporting_study

    lf: pl.LazyFrame = pl.DataFrame({"extracted_from_row_number": [1, 2], "study_method_types": [[None, " ", "NA"], ["null", "none", ""]]}).lazy()
    out: pl.DataFrame = inline_supporting_study(lf, "table", "data.tsv", False).collect()
    assert out["has_supporting_studies"].to_list() == [None, None]


def test_inline_supporting_study_skips_invalid_unpublished_numeric_metadata() -> None:
    """An invalid numeric metadata value does not justify an unpublished fallback Study."""
    from tablassert.lib import inline_supporting_study

    lf: pl.LazyFrame = pl.DataFrame({"extracted_from_row_number": [1, 2, 3], "study_size": ["not-a-number", "-5", "1.9"]}).lazy()
    out: pl.DataFrame = inline_supporting_study(lf, "table", "data.tsv", False).collect()
    assert out["has_supporting_studies"].to_list() == [None, None, None]


def test_inline_supporting_study_routes_unsatisfiable_values_without_pruned_values() -> None:
    """An unattached qualifier is preserved in a routed StudyResult description."""
    from tablassert.lib import inline_supporting_study

    lf: pl.LazyFrame = pl.DataFrame({"extracted_from_row_number": [7], "aspect_qualifier": ["increased"]}).lazy()
    out: pl.DataFrame = inline_supporting_study(lf, "table", "data.tsv", False).collect()
    study: dict[str, Any] = out["has_supporting_studies"].to_list()[0]["table"]
    assert study["has_study_results"] == [{"id": "row:7", "description": "aspect_qualifier=increased"}]


def test_inline_supporting_study_routes_unsatisfiable_values_with_pruned_values() -> None:
    """Routed and class-pruned values share one deterministic StudyResult description."""
    from tablassert.lib import PRUNED_COLUMN, inline_supporting_study

    lf: pl.LazyFrame = pl.DataFrame(
        {"extracted_from_row_number": [7], "aspect_qualifier": ["increased"], PRUNED_COLUMN: [["severity_qualifier=high"]]}
    ).lazy()
    out: pl.DataFrame = inline_supporting_study(lf, "table", "data.tsv", False).collect()
    study: dict[str, Any] = out["has_supporting_studies"].to_list()[0]["table"]
    assert study["has_study_results"] == [{"id": "row:7", "description": "aspect_qualifier=increased; severity_qualifier=high"}]


def test_inline_supporting_study_rejects_routed_values_without_row_provenance() -> None:
    """A routed value without a row cannot receive a valid row:<N> StudyResult id."""
    from tablassert.lib import inline_supporting_study

    lf: pl.LazyFrame = pl.DataFrame({"aspect_qualifier": ["increased"]}).lazy()
    with pytest.raises(ValueError, match="extracted_from_row_number"):
        inline_supporting_study(lf, "table", "data.tsv", False).collect()


def test_inline_supporting_study_metadata_without_row_has_no_result_fallback() -> None:
    """A direct metadata frame does not invent the invalid static StudyResult id ``result``."""
    from tablassert.lib import inline_supporting_study

    lf: pl.LazyFrame = pl.DataFrame({"study_size": [9]}).lazy()
    out: pl.DataFrame = inline_supporting_study(lf, "table", "data.tsv", False).collect()
    study: dict[str, Any] = out["has_supporting_studies"].to_list()[0]["table"]
    assert study == {"id": "table", "name": "data.tsv", "study_size": 9}


def test_dedup_stream_edges_declared_uuid_fields_survive_attribute_edits(tmp_path: Path) -> None:
    """declared `uuid_fields` hold the edge id still when only an attribute changes.

    This is the point of the feature: a corrected p_value must leave the id alone so
    downstream sees one edge updated, not one retired and one created.
    """
    import json

    fields: list[str] = ["subject", "object", "predicate"]

    def build(name: str, body: str) -> str:
        p_in: Path = tmp_path / f"{name}.ndjson.tmp"
        p_in.write_text(body)
        lib.dedup_stream(p_in, is_edges=True, domain="infores:test-kg", uuid_fields=fields)
        line: str = (tmp_path / f"{name}.ndjson").read_text().strip()
        return json.loads(line)["id"]

    before: str = build("before", '{"subject":"A","object":"B","predicate":"r","p_value":"0.01"}\n')
    after: str = build("after", '{"subject":"A","object":"B","predicate":"r","p_value":"0.99","effect_size":1.5}\n')
    assert before == after
    # ...and the whole-record default still drifts, so the opt-in is what changed things.
    drifted: str = build("drifted", '{"subject":"A","object":"B","predicate":"r","p_value":"0.99","effect_size":1.5}\n')
    p_in: Path = tmp_path / "plain.ndjson.tmp"
    p_in.write_text('{"subject":"A","object":"B","predicate":"r","p_value":"0.01"}\n')
    lib.dedup_stream(p_in, is_edges=True)
    plain: str = json.loads((tmp_path / "plain.ndjson").read_text().strip())["id"]
    assert plain != drifted


def test_dedup_stream_edges_reject_uuid_fields_that_are_not_a_key(tmp_path: Path) -> None:
    """two different edges deriving one id abort the build instead of shipping a duplicate."""
    p_in: Path = tmp_path / "edges.ndjson.tmp"
    p_in.write_text('{"subject":"A","object":"B","predicate":"r","p_value":"0.01"}\n{"subject":"A","object":"B","predicate":"r","p_value":"0.99"}\n')

    with pytest.raises(RuntimeError) as exc_info:
        lib.dedup_stream(p_in, is_edges=True, domain="infores:test-kg", uuid_fields=["subject", "object", "predicate"])

    message: str = str(exc_info.value)
    assert "uuid-fields-not-a-key" in message
    # The diagnostic must name the field that would disambiguate, or it is unactionable.
    assert "p_value" in message


def test_dedup_stream_edges_domain_separates_graphs(tmp_path: Path) -> None:
    """the same triple in two graphs derives two ids, so a narrow key set stays safe."""
    import json

    def build(name: str, domain: str) -> str:
        p_in: Path = tmp_path / f"{name}.ndjson.tmp"
        p_in.write_text('{"subject":"A","object":"B","predicate":"r"}\n')
        lib.dedup_stream(p_in, is_edges=True, domain=domain, uuid_fields=["subject", "object", "predicate"])
        return json.loads((tmp_path / f"{name}.ndjson").read_text().strip())["id"]

    assert build("left", "infores:left-kg") != build("right", "infores:right-kg")


def test_dedup_stream_edges_merge_folds_divergent_records_into_one_edge(tmp_path: Path) -> None:
    """`on_collision="merge"` unions divergent same-id edges instead of aborting.

    The DAKP case: two rows with different raw mention spellings resolve to the same
    CURIE, so subject/predicate/object derive one id. Merge folds them into a single
    edge with unioned evidence rather than failing the build.
    """
    import json

    p_in: Path = tmp_path / "edges.ndjson.tmp"
    p_in.write_text(
        '{"subject":"UMLS:C4721779","object":"B","predicate":"r","p_value":"0.01","publications":["PMID:2","PMID:1"]}\n'
        '{"subject":"UMLS:C4721779","object":"B","predicate":"r","p_value":"0.99","publications":["PMID:3","PMID:1"]}\n'
    )
    lib.dedup_stream(p_in, is_edges=True, domain="infores:test-kg", uuid_fields=["subject", "object", "predicate"], on_collision="merge")

    lines: list[str] = (tmp_path / "edges.ndjson").read_text().strip().splitlines()
    assert len(lines) == 1
    edge: dict[str, Any] = json.loads(lines[0])
    # List fields union, dedup, and sort; conflicting scalars keep the first value.
    assert edge["publications"] == ["PMID:1", "PMID:2", "PMID:3"]
    assert edge["p_value"] == "0.01"


def test_dedup_stream_edges_merge_output_is_order_independent(tmp_path: Path) -> None:
    """merged output must not depend on which source row arrived first."""
    p_left: Path = tmp_path / "left.ndjson.tmp"
    p_left.write_text(
        '{"subject":"A","object":"B","predicate":"r","publications":["PMID:2"]}\n'
        '{"subject":"A","object":"B","predicate":"r","publications":["PMID:1","PMID:3"]}\n'
    )
    lib.dedup_stream(p_left, is_edges=True, domain="infores:test-kg", uuid_fields=["subject", "object", "predicate"], on_collision="merge")

    p_right: Path = tmp_path / "right.ndjson.tmp"
    p_right.write_text(
        '{"subject":"A","object":"B","predicate":"r","publications":["PMID:1","PMID:3"]}\n'
        '{"subject":"A","object":"B","predicate":"r","publications":["PMID:2"]}\n'
    )
    lib.dedup_stream(p_right, is_edges=True, domain="infores:test-kg", uuid_fields=["subject", "object", "predicate"], on_collision="merge")

    assert (tmp_path / "left.ndjson").read_text() == (tmp_path / "right.ndjson").read_text()


def test_dedup_stream_edges_merge_recomputes_number_of_cases_from_case_id_union(tmp_path: Path) -> None:
    """`supporting_case_ids` turns `number_of_cases` into the exact union size, then is stripped.

    The DAKP case: two builds of one edge (e.g. different FAERS quarters) each know
    their own case count and case IDs. First-wins under-reports and summing
    double-counts the shared case, so merge mode recomputes the count as the union
    length and never ships the build-internal carrier.
    """
    import json

    p_in: Path = tmp_path / "edges.ndjson.tmp"
    p_in.write_text(
        '{"subject":"A","object":"B","predicate":"r","number_of_cases":2,"supporting_case_ids":["case:1","case:2"]}\n'
        '{"subject":"A","object":"B","predicate":"r","number_of_cases":5,"supporting_case_ids":["case:2","case:3"]}\n'
    )
    lib.dedup_stream(p_in, is_edges=True, domain="infores:test-kg", uuid_fields=["subject", "object", "predicate"], on_collision="merge")

    lines: list[str] = (tmp_path / "edges.ndjson").read_text().strip().splitlines()
    assert len(lines) == 1
    edge: dict[str, Any] = json.loads(lines[0])
    assert edge["number_of_cases"] == 3
    assert "supporting_case_ids" not in edge


def test_dedup_stream_edges_strip_supporting_case_ids_without_merge(tmp_path: Path) -> None:
    """The default streaming path strips the carrier too, so it never ships."""
    import json

    p_in: Path = tmp_path / "edges.ndjson.tmp"
    p_in.write_text('{"subject":"A","object":"B","predicate":"r","number_of_cases":2,"supporting_case_ids":["case:1","case:2"]}\n')
    lib.dedup_stream(p_in, is_edges=True, domain="infores:test-kg", uuid_fields=["subject", "object", "predicate"])

    edge: dict[str, Any] = json.loads((tmp_path / "edges.ndjson").read_text().strip())
    assert edge["number_of_cases"] == 2
    assert "supporting_case_ids" not in edge


def test_dedup_stream_edges_default_mode_still_aborts_on_divergence(tmp_path: Path) -> None:
    """leaving `on_collision` at its default keeps the uuid-fields-not-a-key abort."""
    p_in: Path = tmp_path / "edges.ndjson.tmp"
    p_in.write_text('{"subject":"A","object":"B","predicate":"r","p_value":"0.01"}\n{"subject":"A","object":"B","predicate":"r","p_value":"0.99"}\n')
    with pytest.raises(RuntimeError, match="uuid-fields-not-a-key"):
        lib.dedup_stream(p_in, is_edges=True, domain="infores:test-kg", uuid_fields=["subject", "object", "predicate"], on_collision="error")
