"""Tablassert configuration enums (non-Biolink).

These enums describe Tablassert's own configuration vocabulary (source kinds,
comparison operators, encoding/fill methods, contribution labels, and so on).

The Biolink Model vocabulary -- ``Categories``, ``Predicates``, ``Qualifiers``,
``KnowledgeLevels``, ``AgentTypes``, ``EdgeCategories``, and
``ALLOWED_EDGE_FIELDS`` -- is derived from the ``biolink-model`` package and lives
in :mod:`tablassert.biolink`. Import those from there.
"""

from __future__ import annotations

from enum import Enum


class Tokens(str, Enum):
    AUTO = "auto"
    VALUES = "values"


class Repositories(str, Enum):
    PUBMED_CENTRAL = "PMC"
    PUBMED = "PMID"


class InformationResources(str, Enum):
    PUBMED = "infores:pubmed"
    PUBMED_CENTRAL = "infores:pubmed-central"


class Contributions(str, Enum):
    CURATION = "curation"
    VALIDATION = "validation"
    TOOL = "tool"


class Comparisons(str, Enum):
    GT = "gt"
    GE = "ge"
    LT = "lt"
    LE = "le"
    EQ = "eq"
    NE = "ne"


class Functions(str, Enum):
    COPYSIGN = "copysign"
    POW = "pow"


class Files(str, Enum):
    TEXT = "text"
    EXCEL = "excel"
    PARQUET = "parquet"


class EncodingMethods(str, Enum):
    VALUE = "value"
    COLUMN = "column"


class FillMethods(str, Enum):
    FORWARD = "forward"
    BACKWARD = "backward"
    MIN = "min"
    MAX = "max"
    MEAN = "mean"
    ZERO = "zero"
    ONE = "one"


# --- Resource Ingest Guide vocabularies ------------------------------------ #
# These mirror the enums declared by the released RIG schema
# (biolink/resource-ingest-guide-schema), so a generated `.RIG.yaml` can only
# carry values the upstream validator accepts.


class SourceStatuses(str, Enum):
    MAINTAINED_REGULAR_UPDATES = "maintained_regular_updates"
    MAINTAINED_AS_NEEDED_UPDATES = "maintained_as_needed_updates"
    NOT_MAINTAINED = "not_maintained"
    UNKNOWN = "unknown"


class ProvisionMechanisms(str, Enum):
    FILE_DOWNLOAD = "file_download"
    API_ENDPOINT = "api_endpoint"
    DATABASE_DUMP = "database_dump"
    OTHER = "other"


class DataFormats(str, Enum):
    TSV = "tsv"
    XML = "xml"
    CSV = "csv"
    JSON = "json"
    YAML = "yaml"
    OBO = "obo"
    PROTOBUFF = "protobuff"
    KGX = "kgx"
    MYSQL = "mysql"
    POSTGRESQL = "postgresql"
    SQLITE = "sqlite"
    OTHER = "other"


class IngestCategories(str, Enum):
    PRIMARY_KNOWLEDGE_PROVIDER = "primary_knowledge_provider"
    AGGREGATION_PROVIDER = "aggregation_provider"
    AGGREGATION_INTERPRETER = "aggregation_interpreter"
    SUPPORTING_DATA_PROVIDER = "supporting_data_provider"
    TRANSLATOR_KNOWLEDGE_CREATOR = "translator_knowledge_creator"
    ONTOLOGY_PROVIDER = "ontology_provider"
    NODE_PROPERTY_ONLY_PROVIDER = "node_property_only_provider"
    OTHER = "other"


class ContentCategories(str, Enum):
    EDGE_CONTENT = "edge_content"
    NODE_PROPERTY_CONTENT = "node_property_content"
    EDGE_PROPERTY_CONTENT = "edge_property_content"
    OTHER = "other"


class ModelingCategories(str, Enum):
    SPOQ_PATTERN = "spoq_pattern"
    PREDICATES = "predicates"
    QUALIFIERS = "qualifiers"
    EDGE_PROPERTIES = "edge_properties"
    NODE_PROPERTIES = "node_properties"
    OTHER = "other"
