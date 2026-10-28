"""Koza 2.7.0 transform equivalent of the Tablassert tutorial example.

Emits one associated_with edge per row plus its two nodes. Entity resolution is
NOT included: Koza provides no resolver, so identifiers must already be CURIEs
or be mapped by hand (see README for what this comparison measures).
"""

from dataclasses import dataclass, field

import koza

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


@koza.transform_record()
def transform_row(koza, record):
    subject = NamedThing(id=record["gene_symbol"], name=record["gene_symbol"], category=["biolink:Gene"])
    obj = NamedThing(id=record["disease_name"], name=record["disease_name"], category=["biolink:Disease"])
    edge = Association(
        subject=record["gene_symbol"],
        predicate=ASSOCIATION,
        object=record["disease_name"],
        primary_knowledge_source="infores:tutorial-kg",
        publications=["PMID:12345678"],
        p_value=float(record["p_value"]),
        sample_size=int(record["sample_size"]),
    )
    koza.write(subject, obj, edge)
