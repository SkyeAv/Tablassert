"""Generic-label exclusions must use the same normalization as fullmap probe keys."""

from __future__ import annotations

import polars as pl
import pytest

from tablassert.fullmap import distinct
from tablassert.nlp import level_one, level_two


@pytest.mark.parametrize("label", ["Expression", "Variables", "Exposure", "P value", "Not applicable"])
def test_generic_labels_are_dropped_after_normalization(label: str) -> None:
    """Stemming and token sorting must not bypass the existing generic-label blocklist."""
    normalized = level_two(level_one(pl.LazyFrame({"term": [label]}), "term"), "term")

    assert distinct(normalized, "term", "term_two").collect().is_empty()


def test_useful_terms_and_lowest_probe_level_are_preserved() -> None:
    """Repairing the existing blocklist must not silently add new blocked labels or change tiering."""
    labels = ["BRCA1", "TNF-alpha", "Control", "Status", "Expression", "p_value", "Result", "symbol", "123", ""]
    normalized = level_two(level_one(pl.LazyFrame({"term": labels}), "term"), "term")
    found = dict(distinct(normalized, "term", "term_two").collect().iter_rows())

    assert found == {"brca1": 1, "tnf-alpha": 1, "tnfalpha": 2, "control": 1, "status": 1}


def test_existing_unstemmed_blocklist_keys_remain_excluded() -> None:
    """Programmatic callers that already supplied unstemmed probe columns keep their old behavior."""
    labels = ["expression", "variable", "exposure", "not applicable", "unknown", "na", "nan", "none", "null"]
    probes = pl.LazyFrame({"first": labels, "second": labels})

    assert distinct(probes, "first", "second").collect().is_empty()


def test_null_probes_stay_dropped() -> None:
    """The set-membership filter must keep the old regex behavior for null probes."""
    probes = pl.LazyFrame({"first": [None, "BRCA1"], "second": [None, "brca1"]})

    found = distinct(probes, "first", "second").collect()

    assert sorted(found["term"].to_list()) == ["BRCA1", "brca1"]
