# ruff: noqa: B018
"""Vulture whitelist: real symbols whose usage vulture cannot see statically.

Scanned alongside the code via ``[tool.vulture] paths`` in pyproject.toml. Each
entry below is exercised dynamically at runtime:

- Vocabulary ``Enum`` members are parsed from configuration strings by value
  (``FillMethods("backward")``), so most members never appear as a name in code.
  They are schema, not dead code: removing one would reject valid configs.
- ``models`` RIG fields are populated by pydantic from YAML graph configs and
  consumed through serialization into the generated ``.RIG.yaml``, not through
  attribute access in this repo.
- The ``_``-receiver entries are attribute/parameter names fixed by the protocol
  being imitated (PyYAML's emitter, loguru's Logger shape).

Entries cover only the names vulture flags today, so a newly added unreferenced
member still trips the gate and forces a deliberate decision here. Remove an
entry when the name gains a static reference.
"""

from typing import Any

from tablassert.cli import draft
from tablassert.enums import ContentCategories, DataFormats, FillMethods, IngestCategories, ModelingCategories, ProvisionMechanisms, SourceStatuses
from tablassert.models import (
    RIGFilteredContent,
    RIGFutureContentConsideration,
    RIGFutureModelingConsideration,
    RIGIncludedContent,
    RIGProvenanceInfo,
    RIGSourceInfo,
    RIGSupportingDataSourceInfo,
)

# Stand-in receiver for names that are not importable symbols (parameters and
# instance attributes); vulture only needs to see the name used.
_: Any = None

# ingests.py: parameter name fixed by yaml.Emitter.increase_indent, which PyYAML
# itself calls as increase_indent(flow=False, indentless=indentless).
_.indentless

# log.py: _NullLogger imitates the loguru Logger API; these names are fixed by
# loguru's interface, not by callers in this repo.
_._category
_.sink

# enums.py: configuration vocabulary members parsed by string value.
FillMethods.BACKWARD
FillMethods.MIN
FillMethods.MAX
FillMethods.MEAN
FillMethods.ONE
SourceStatuses.MAINTAINED_REGULAR_UPDATES
SourceStatuses.MAINTAINED_AS_NEEDED_UPDATES
SourceStatuses.NOT_MAINTAINED
SourceStatuses.UNKNOWN
ProvisionMechanisms.FILE_DOWNLOAD
ProvisionMechanisms.API_ENDPOINT
ProvisionMechanisms.DATABASE_DUMP
ProvisionMechanisms.OTHER
DataFormats.TSV
DataFormats.XML
DataFormats.CSV
DataFormats.JSON
DataFormats.YAML
DataFormats.OBO
DataFormats.PROTOBUFF
DataFormats.KGX
DataFormats.MYSQL
DataFormats.POSTGRESQL
DataFormats.SQLITE
DataFormats.OTHER
IngestCategories.PRIMARY_KNOWLEDGE_PROVIDER
IngestCategories.AGGREGATION_PROVIDER
IngestCategories.AGGREGATION_INTERPRETER
IngestCategories.SUPPORTING_DATA_PROVIDER
IngestCategories.ONTOLOGY_PROVIDER
IngestCategories.NODE_PROPERTY_ONLY_PROVIDER
IngestCategories.OTHER
ContentCategories.EDGE_CONTENT
ContentCategories.NODE_PROPERTY_CONTENT
ContentCategories.EDGE_PROPERTY_CONTENT
ContentCategories.OTHER
ModelingCategories.SPOQ_PATTERN
ModelingCategories.PREDICATES
ModelingCategories.QUALIFIERS
ModelingCategories.EDGE_PROPERTIES
ModelingCategories.NODE_PROPERTIES
ModelingCategories.OTHER

# models.py: RIG schema fields set from YAML configs and read back through
# pydantic serialization, never as attributes in repo code.
RIGSourceInfo.citations
RIGSourceInfo.terms_of_use_info
RIGSourceInfo.data_access_locations
RIGSourceInfo.data_provision_mechanisms
RIGSourceInfo.data_formats
RIGSourceInfo.data_versioning_and_releases
RIGSourceInfo.source_status
RIGIncludedContent.included_records
RIGIncludedContent.fields_used
RIGFilteredContent.filtered_records
RIGFilteredContent.rationale
RIGFutureContentConsideration.consideration
RIGFutureModelingConsideration.consideration
RIGSupportingDataSourceInfo.terms_of_use_info
RIGProvenanceInfo.artifacts

# cli.py: Cyclopts @APP.command callbacks registered dynamically at import time.
draft
