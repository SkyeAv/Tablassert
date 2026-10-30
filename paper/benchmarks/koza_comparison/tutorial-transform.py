"""Koza 2.7.0 transform for the Tablassert tutorial table.

Entity resolution calls ``tablassert.lib.resolve_many`` with the recipe's column
priorities, against the fullmap configured in ``tutorial-graph.yaml``.
"""

from dataclasses import dataclass, field
from pathlib import Path

import koza

from tablassert.biolink import Categories
from tablassert.lib import resolve_many

FULLMAP = Path("/local_raid1/sgoetz/DBSTORE/FULLMAP/fullmap")
ASSOCIATION = "biolink:associated_with"


@dataclass
class NamedThing:
    id: str
    name: str
    category: list[str] = field(default_factory=list)


@dataclass
class Association:
    subject: str
    predicate: str
    object: str
    primary_knowledge_source: str
    publications: list[str] = field(default_factory=list)
    p_value: float | None = None
    sample_size: int | None = None


def resolve(col: str, values: list[str], category: Categories) -> dict[str, list[dict]]:
    matches: dict[str, dict[str, dict]] = {}
    for row in resolve_many(col, values, FULLMAP, taxon="9606", prioritize=[category]):
        matches.setdefault(row[f"original_{col}"], {})[row[col]] = row
    return {term: list(rows.values()) for term, rows in matches.items()}


@koza.transform()
def transform_rows(koza, data):
    records = list(data)
    genes = resolve("subject", [r["gene_symbol"] for r in records], Categories.GENE)
    diseases = resolve("object", [r["disease_name"] for r in records], Categories.DISEASE)
    for record in records:
        for gene in genes.get(record["gene_symbol"], []):
            for disease in diseases.get(record["disease_name"], []):
                koza.write(
                    NamedThing(id=gene["subject"], name=gene["subject_name"], category=[gene["subject_category"]]),
                    NamedThing(id=disease["object"], name=disease["object_name"], category=[disease["object_category"]]),
                    Association(
                        subject=gene["subject"],
                        predicate=ASSOCIATION,
                        object=disease["object"],
                        primary_knowledge_source="infores:tutorial-kg",
                        publications=["PMID:12345678"],
                        p_value=float(record["p_value"]),
                        sample_size=int(record["sample_size"]),
                    ),
                )
