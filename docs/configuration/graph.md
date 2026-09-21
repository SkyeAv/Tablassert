# Graph Configuration Reference

Graph configurations orchestrate one or more [table configurations](table.md) into a single knowledge-graph build: author one to produce KGX output with `tablassert build-kg` (see the [CLI reference](../cli.md#build-kg)). To check a single table config on its own, use `tablassert validate <table.yaml> --schema table`.

## Purpose

A graph configuration file specifies:

- Output knowledge graph name and version
- List of table configurations to process
- Database location for entity resolution
- The **required `rig:` section**: all Resource Ingest Guide (RIG) metadata emitted as `<name>_<version>.RIG.yaml`

QC auditing and verbose logging are controlled at build time via the `build-kg --qc` and `build-kg --log` flags: they are **not** graph-config fields.

## Schema

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| `name` | String | Knowledge graph name (used in output filenames and the RIG) |
| `version` | String | Knowledge graph version (used in output filenames) |
| `tables` | List[Path] | Paths to table configuration YAML files |
| `fullmap` | Path | Path to the fullmap redb file, or a base directory containing it |
| `rig` | RIGConfig | Resource Ingest Guide metadata (see below) |

The legacy top-level RIG fields (`description`, `contributions`, `ui_explanation`, `infores`) are **rejected** with a migration pointer; they now live under `rig:`.

### Optional Fields

| Field | Type | Description |
|-------|------|-------------|
| `uuid_fields` | List[str] | Edge fields that constitute edge identity. Only these feed the derived edge `id` (see [Stable edge ids](#stable-edge-ids)) |
| `uuid_domain` | String | Explicit UUID namespace. Defaults to `rig.source_info.infores_id` when `uuid_fields` is set, `TABLASSERT` otherwise |
| `uuid_on_collision` | `error` \| `merge` | What to do when two different edges derive one id. `error` (default) aborts; `merge` folds them into one edge. Requires `uuid_fields` (see [Merging collisions instead](#merging-collisions-instead)) |
| `qc_similarity_threshold` | Float 0–1 | SapBERT cosine threshold used by QC |
| `qc_fuzzy_ratio_threshold` | Float 0–100 | RapidFuzz ratio threshold used by QC |
| `qc_fuzzy_partial_threshold` | Float 0–100 | RapidFuzz partial/token-set threshold used by QC |
| `qc_aliases` | Mapping[String, String] | Source-text aliases applied before QC similarity stages |

## Stable edge ids

Every edge gets a deterministic `id`: a UUID v3 derived from the edge itself. By default it is
derived from the **whole record**, which makes it maximally brittle — a corrected `p_value`, a new
`supporting_text` entry, a reordered source row, or a Biolink release that renames a slot all mint a
brand-new id. Downstream Translator consumers then see a new edge where they should see the same
edge with updated attributes.

`uuid_fields` fixes that by naming the fields that actually identify an edge:

```yaml
uuid_fields: [subject, predicate, object, publications, has_supporting_studies]
```

Everything else is then free to change without moving the id.

### Choosing a field set

Start from what identifies an assertion, and what each entry buys you:

- **`subject` / `predicate` / `object`** — the assertion itself.
- **`publications`** — the evidence it rests on.
- **`has_supporting_studies`** — carries `has_study_results[].id` (`row:<N>`), the **row
  discriminator**. Include it whenever one table contributes several rows that share a subject,
  predicate, and object. It also carries the `study_*` metadata, but those come from per-section
  config and are far more stable than `p_value` or `effect_size`.

Add qualifiers (`object_direction_qualifier`, `anatomical_context_qualifier`, …) when they
*distinguish* assertions rather than merely describe them.

Leave out anything that is an observation *about* the edge rather than the edge's identity:
`p_value`, `effect_size`, `effect_type`, `original_subject` / `original_object`, `supporting_text`,
`sources`, `category`, `knowledge_level`, `agent_type`.

**Then build, and let the failure tell you what is missing.** That list is a starting point, not an
answer — whether it is a key depends on your data, and the only way to find out is to run it. On a
real 1.27M-edge graph the set above left 2,476 collisions (0.2% of edges): pairs whose subject,
predicate, object, publication and row were identical, differing only in the NLP level recorded in
`supporting_text` because two raw strings had resolved onto the same CURIE. Adding
`supporting_text` made it a key.

Expect to iterate once or twice. Each failure names the id, the fields that differ, and one
offending edge, so each round is mechanical.

### What it buys

On that same 1.27M-edge graph, re-analysing the statistics (new `p_value` and `effect_size` on
every row) and rebuilding:

| | edge ids that changed |
|---|---|
| no `uuid_fields` (whole-record hash) | 930,081 of 1,265,355 — **73%** |
| `uuid_fields` declared | 0 of 1,265,355 — **none** |

### The field set must be a key

Narrowing what feeds the hash means two different edges can derive the same id. Tablassert refuses
to ship duplicate edge ids, so that is a build failure, not a silent collapse:

```
uuid-fields-not-a-key: declared uuid_fields are not a key for this graph.
  id 83ade536-9b07-34ec-a2f9-abf0fb5b6a2f is claimed by 2 different edges.
  they differ in: effect_size, p_value
  declared uuid_fields: subject, predicate, object
  offending edge: subject=A predicate=r object=B
Add a discriminating field to `uuid_fields` (...).
```

The fix is whatever the message names: add the qualifier that separates them,
`has_supporting_studies` for the source row, or the statistic that genuinely differs. Two rows with
an identical subject, predicate, and object that differ only in `p_value` are exactly this case —
and were previously producing two ids for what config claimed was one assertion.

An exact duplicate is *not* a violation: identical edges collapse, as they always have.

### Merging collisions instead

Sometimes a collision is not a config mistake but the data working as intended: two rows with
different raw mention spellings resolve to the same CURIE, so they derive one id — and the right
answer is one edge with the combined evidence, not a failed build. Opt in with:

```yaml
uuid_fields: [subject, predicate, object]
uuid_on_collision: merge
```

Under `merge`, a divergent same-id record is folded into the first record that claimed the id:

- **list fields** (`publications`, `sources`, `source_record_urls`, `supporting_text`, `category`,
  `upstream_resource_ids`, `has_supporting_studies`, …) are unioned, deduplicated, and **sorted**,
  so the merged edge is identical regardless of which row arrived first. Object entries such as
  `sources[]` dedup by content — key order alone never keeps two copies.
- **scalar fields** keep the lexicographically smallest canonical value; each conflict is counted and reported in a
  build-log summary.
- fields only the later record carries are copied over — first-wins arbitrates *conflicts*, not
  additions.
- **one scalar is exempt from first-wins:** when the merged record carries the build-internal
  `supporting_case_ids` list — the case IDs behind a `number_of_cases` count — the merged count
  becomes the length of the unioned list, so a case ID shared by both records counts once where
  first-wins would under-report and summing would double-count. The superseded divergence is not
  reported as a scalar conflict, and the carrier list is stripped from every edge before write in
  both modes, so it never ships in the final NDJSON.

`merge` requires `uuid_fields` (under the whole-record hash every field is an identity field, so
two different records can never share an id — there would be nothing to merge) and is rejected
without it as `uuid-merge-without-fields`.

The trade-off is memory: merge mode buffers one full record per unique id and writes edges only at
end-of-stream, where the default path streams and holds 24 bytes per edge. That is why it is
opt-in. The default `error` behavior is unchanged in every respect.

### Namespacing

Because a narrow field set no longer distinguishes graphs by accident, declaring `uuid_fields`
moves the UUID namespace onto the graph's own `rig.source_info.infores_id`. Two graphs asserting
the same triple from the same publication then still derive different ids, structurally.

Set `uuid_domain` only when graphs must deliberately **share** an id space — a KG compiled in
shards, or one renamed across versions that has to keep its published ids:

```yaml
uuid_domain: infores:multiomicskg
```

### Migration

Adding `uuid_fields` to an existing graph **changes every edge id in it, once**. That is the cost of
switching identity models; ids are stable from then on. Plan it as a deliberate version bump and
tell your consumers.

### The `rig:` section

The `rig:` section carries every human-authored RIG fact. Its shape mirrors the released [RIG schema](https://github.com/biolink/resource-ingest-guide-schema), so the generated `.RIG.yaml` is always schema-shaped. The generator derives only mechanical facts from the build (generated artifact file entries, observed edge/node type summaries) and **validates the complete document before writing anything**, so a build never leaves behind an invalid or incomplete RIG.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `rig.name` | String | No | RIG display name; defaults to `<name> v<version> Resource Ingest Guide` |
| `rig.source_info` | Object | Yes | Information about the source being ingested (see below) |
| `rig.ingest_info` | Object | Yes | Rationale and scope of the ingest (see below) |
| `rig.target_info` | Object | No | Target-level `future_considerations` and `additional_notes` (edge/node type summaries are always generated) |
| `rig.ui_explanation` | String | No | Per-edge-type UI explanation **prefix**; the built-in Tablassert explanation is always appended after it |
| `rig.source_files` | List[String] | No | Upstream source file names/URLs listed verbatim as `source_files` on every generated `edge_type_info` entry; never scraped from edge `source_record_urls` |
| `rig.provenance_info` | Object | Yes | Contributor statements and provenance artifacts |
| `rig.supporting_data_source_info` | List[Object] | No | Upstream data sources for data-derived graphs (each needs `infores_id`, `terms_of_use_info`, and `relevant_files`) |
| `rig.artifact_base_url` | String | Yes | Public URL prefix for the generated KGX artifacts; each `.nodes.ndjson`/`.edges.ndjson` name is appended to build RIG `relevant_files` locations |
| `rig.artifact_base_path` | Path | Yes | Output directory the generated artifacts (and the RIG) are written into; created when missing |

#### `rig.source_info`

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `infores_id` | `infores:` CURIE | Yes | Infores of the source this graph ingests; also emitted as the edge `primary_knowledge_source` and node `provided_by`. There is no implicit derivation from the graph name |
| `name` | String | No | Human-readable source name |
| `description` | String | No | What the source contains and how its knowledge is produced |
| `citations` | List[String] | No | PMIDs, DOIs, URLs, or free-text citations |
| `terms_of_use_info` | Object | Yes | At least one of `terms_of_use_url`, `terms_of_use_description`, `license_name`, `license_url` must carry a real assessment |
| `data_access_locations` | List[String] | Yes | Where the upstream source data is accessed; each entry must contain an http(s) or file URL |
| `data_provision_mechanisms` | List[Enum] | No | `file_download`, `api_endpoint`, `database_dump`, `other` |
| `data_formats` | List[Enum] | No | `tsv`, `csv`, `xml`, `json`, `yaml`, `obo`, `protobuff`, `kgx`, `mysql`, `postgresql`, `sqlite`, `other` |
| `data_versioning_and_releases` | String | No | How the source versions/releases its data |
| `source_status` | Enum | Yes | `maintained_regular_updates`, `maintained_as_needed_updates`, `not_maintained`, `unknown` |
| `additional_notes` | List[String] | No | Anything not captured by dedicated fields |

#### `rig.ingest_info`

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `ingest_categories` | List[Enum] | No | Defaults to `[translator_knowledge_creator]`; also `primary_knowledge_provider`, `aggregation_provider`, `aggregation_interpreter`, `supporting_data_provider`, `ontology_provider`, `node_property_only_provider`, `other` |
| `utility` | String | Yes | Why the source is ingested and its utility for Translator use cases |
| `scope` | String | Yes | High-level narrative of what is included and excluded |
| `relevant_files` | List[Object] | No | **Upstream** source files: `file_name`, `location` (URL), optional `description`. Entries are cross-checked against the table configs' source URLs/local files, and an entry matching no configured source fails the build |
| `included_content` | List[Object] | No | Upstream `file_name` / `included_records` / optional `fields_used` entries |
| `filtered_content` | List[Object] | No | `file_name` / `filtered_records` / `rationale` entries |
| `future_considerations` | List[Object] | No | `category` (`edge_content`, `node_property_content`, `edge_property_content`, `other`), `consideration`, optional `relevant_files` |
| `additional_notes` | List[String] | No | Extra ingest notes |

The generator **prepends** two `relevant_files` entries and two `included_content` entries for the generated artifacts (`<name>_<version>.nodes.ndjson` and `.edges.ndjson`) with their exact output names, the composed `artifact_base_url` locations, and observed record counts/fields.

#### `rig.provenance_info`

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `contributions` | List[String] | Yes | Who contributed and how (e.g. `"Name - code author, data modeling"`) |
| `artifacts` | List[String] | No | Links/descriptions of external provenance artifacts (tickets, surveys, repos) |

### What the generator derives

Everything under `target_info.edge_type_info` and `target_info.node_type_info` is computed from the **final emitted KGX files** after deduplication:

- **Edge types** (one per observed predicate): subject/object categories resolved from the emitted nodes, list-valued `knowledge_level`/`agent_type`, role-separated `primary_knowledge_sources` / `supporting_data_sources` / `aggregator_knowledge_sources` from each edge's `sources` retrieval provenance, observed `edge_properties`, qualifier shapes (enumerated literal values or identifier prefixes for CURIE-valued qualifiers), and `source_files` from the configured `rig.source_files` (never scraped from edge `source_record_urls`).
- **Node types**: observed categories and the identifier prefixes actually emitted (`source_identifier_types`); categories with prefix-less identifiers get a factual free-text entry.
- **UI explanation**: `rig.ui_explanation` (when set) followed by the built-in Tablassert explanation; the default text is always present.

### Built-in RIG validation

Before the `.RIG.yaml` is written, the build audits the assembled document and fails with `[rig-validation-failed]` (writing nothing) when:

- any required field is missing/empty or outside the RIG schema's enums;
- the generated artifact entries are missing or their locations disagree with `artifact_base_url`;
- edge/node summaries disagree with the observed graph (categories, predicates, KL/AT values, infores CURIEs);
- a configured upstream `relevant_files` entry matches no table source file or URL.

This is what makes every emitted RIG PR-worthy by construction: placeholders and silent gaps fail the build instead of shipping.

## Minimal Example

```yaml
name: MY_GRAPH
version: 1.0.0
tables:
  - ./my-table.yaml
fullmap: /data/fullmap
rig:
  source_info:
    infores_id: infores:my-graph
    terms_of_use_info:
      terms_of_use_url: https://example.org/terms
    data_access_locations:
      - My source downloads - https://example.org/downloads
    source_status: maintained_regular_updates
  ingest_info:
    utility: Why this content matters for Translator queries.
    scope: What this graph includes and excludes.
  provenance_info:
    contributions:
      - "Author Name - code author, data modeling"
  artifact_base_url: https://example.org/my-graph
  artifact_base_path: ./published/my-graph
```

## Multi-Table Example

```yaml
name: MULTIOMICS_KG
version: UNSTABLE
tables:
  - /configs/gene-disease-associations.yaml
  - /configs/drug-targets.yaml
  - /configs/protein-interactions.yaml
fullmap: /databases/fullmap
rig:
  source_info:
    infores_id: infores:multiomics-kg
    name: Multiomics supplementary tables
    description: Multi-omics associations mined from curated supplementary tables.
    citations:
      - https://doi.org/10.3389/fsysb.2025.1544432
    terms_of_use_info:
      terms_of_use_url: https://pmc.ncbi.nlm.nih.gov/about/copyright/
      terms_of_use_description: PubMed Central open-access subset; individual article licenses apply.
    data_access_locations:
      - PubMed Central - https://pmc.ncbi.nlm.nih.gov/
    data_provision_mechanisms:
      - file_download
    data_formats:
      - kgx
    source_status: maintained_as_needed_updates
  ingest_info:
    ingest_categories:
      - translator_knowledge_creator
    utility: Statistical associations supporting hypothesis generation for gene-disease and drug-target queries.
    scope: Gene-disease, drug-target, and protein-interaction assertions from the three configured tables.
    relevant_files:
      - file_name: gene-disease.tsv
        location: https://pmc.ncbi.nlm.nih.gov/articles/instance/example/bin/gene-disease.tsv
        description: Gene-disease association table.
  ui_explanation: Microbiome-host associations derived from multi-omics supplementary tables.
  provenance_info:
    contributions:
      - "Author Name - code author, data modeling"
    artifacts:
      - "Ingest ticket: https://github.com/NCATSTranslator/Data-Ingest-Coordination-Working-Group/issues/1"
  artifact_base_url: https://example.org/multiomics-kg
  artifact_base_path: /published/multiomics-kg
```

## Processing Flow

When you run `tablassert build-kg graph.yaml`:

1. **Load graph configuration** - Parse YAML, validate schema (including the full `rig:` section)
2. **Load table configurations** - Parse each YAML in `tables`
3. **Extract sections** - Expand templates into per-section `Tcode` instances
4. **Collect instructions (per section):**
   - Read the source file from disk (`source.local`)
   - Apply transformations and resolve entities using `fullmap`
   - Validate with the QC audit when `build-kg --qc` is passed
5. **Build subgraphs** - Compile each section's resolved data into a parquet file
6. **Compile graph** - Aggregate all subgraph parquets, export `{name}_{version}.nodes.ndjson` / `.edges.ndjson` into `rig.artifact_base_path`, summarize the final graph, audit the RIG document, and write `{name}_{version}.RIG.yaml`

## Output Files

Given this configuration:

```yaml
name: EXAMPLE_KG
version: 2.0.0
rig:
  artifact_base_path: ./published/example-kg
  # ...
```

Produces (inside `./published/example-kg/`):

- `EXAMPLE_KG_2.0.0.nodes.ndjson`
- `EXAMPLE_KG_2.0.0.edges.ndjson`
- `EXAMPLE_KG_2.0.0.RIG.yaml`

## PR-readiness for Translator Ingests

When the generated RIG will back a PR to [`NCATSTranslator/translator-ingests`](https://github.com/NCATSTranslator/translator-ingests):

- Use an **infores that is registered** (or being registered) in the [information resource registry](https://github.com/biolink/information-resource-registry).
- Replace any `file://` artifact base with the **public https location** where the KGX files will be served.
- Fill `terms_of_use_info` with the source's actual license/terms assessment, and `data_versioning_and_releases` with how the upstream source releases data.
- Describe upstream source files in `rig.ingest_info.relevant_files` / `included_content` / `filtered_content`: the generator cross-checks them against your table configs but the semantics are yours.
- Give every edge type's provenance real contributors under `rig.provenance_info.contributions`.

## Next Steps

- **[Table Configuration](table.md)** - Learn how to define table transformations
- **[Fullmap](../fullmap.md)** - Entity-resolution database build and schema
- **[Tutorial](../tutorial.md)** - Complete example walkthrough
