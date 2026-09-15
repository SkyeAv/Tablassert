# Table Configuration Reference

Table configurations define how Tablassert transforms tabular data (Excel, CSV, TSV) into knowledge-graph assertions: author one per source table to declare its source, triple mappings, entity-resolution rules, provenance, and optional edge annotations.

## Template vs Sections

Table configurations support two patterns:

### Pattern 1: Template Only

Use when processing a single table with one output.

```yaml
template:
  source: {...}
  statement: {...}
  provenance: {...}
```

### Pattern 2: Template + Sections

Use when processing variations of the same data (different columns, predicates, etc.) while sharing common configuration.

```yaml
template:
  source: {...}  # Shared by all sections
  provenance: {...}  # Shared by all sections

sections:
  - statement:  # Section 1: Gene-Disease
      subject: {method: column, encoding: A}
      predicate: associated_with
      object: {method: column, encoding: B}

  - statement:  # Section 2: Gene-Pathway
      subject: {method: column, encoding: A}
      predicate: participates_in
      object: {method: column, encoding: C}
```

### Merge Behavior (fastmerge)

Sections inherit from template and override specific fields:

**Dictionaries:** Recursive merge, section overrides template keys
```yaml
template:
  statement:
    subject: {encoding: A}
    predicate: related_to

sections:
  - statement:
      predicate: associated_with  # Overrides, subject stays "A"
```

**Lists:** Concatenation (extends)
```yaml
template:
  statement:
    subject:
      prioritize: [Gene]

sections:
  - statement:
      subject:
        prioritize: [Protein]  # Result: [Gene, Protein]
```

**Scalars:** Section replaces template
```yaml
template:
  statement:
    predicate: related_to

sections:
  - statement:
      predicate: treats  # Replaces the template predicate
```

## Configuration Schema

### Source

Defines the data file location and format.

#### Excel Source

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `kind` | String | No | Source kind. Model default is `"excel"`, but specify it explicitly in configs. |
| `local` | Path | Yes | Local file path the source is read from. The file must already exist here; Tablassert does not download it. |
| `url` | List[URL] | Yes | One or more source URLs recorded as provenance (emitted as the primary `sources` entry's `source_record_urls` list and in the RIG; when `provenance.override.upstream_source_record_urls` is set, RIG only; the per-upstream mapping determines edge placement). At least one URL is required; supply multiple to back a single section with several links. Format-validated only; not fetched. |
| `sheet` | String | No | Sheet name. Defaults to `"Sheet1"`. |
| `row_slice` | List[PositiveInt\|"auto"] | No | Two-value zero-based crop bounds: `[start, stop]`. Each value may be a positive integer or `"auto"`. Mutually exclusive with `rows`. |
| `rows` | List[PositiveInt] | No | Zero-based row indices to keep after any `row_slice` crop. Mutually exclusive with `row_slice`. |
| `reindex` | List[Reindex] | No | Conditional row filtering |

**Example:**
```yaml
source:
  kind: excel
  local: ./data/mydata.xlsx
  url:
    - https://example.com/data.xlsx
  sheet: "Sheet1"
  row_slice: [1, auto]  # Start at the second physical row, read to end
```

> **Specify `kind` explicitly.** Tablassert selects the reader purely from the declared `kind`: `excel` reads a workbook (`sheet`), `text` scans delimited text (`delimiter`); the file on disk is never inspected to infer its format. Because `kind` carries a default, a source whose `kind` is omitted or does not match the actual file is still accepted and fed to the wrong reader, surfacing only later as a read error or garbled rows. Stating `kind` explicitly makes a mis-declared source fail fast.

#### Text Source (CSV/TSV)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `kind` | String | No | Source kind. Model default is `"text"`, but specify it explicitly in configs. |
| `local` | Path | Yes | Local file path the source is read from. The file must already exist here; Tablassert does not download it. |
| `url` | List[URL] | Yes | One or more source URLs recorded as provenance (emitted as the primary `sources` entry's `source_record_urls` list and in the RIG; when `provenance.override.upstream_source_record_urls` is set, RIG only; the per-upstream mapping determines edge placement). At least one URL is required; supply multiple to back a single section with several links. Format-validated only; not fetched. |
| `delimiter` | String | No | Field delimiter. Defaults to `","`. |
| `row_slice` | List[PositiveInt\|"auto"] | No | Two-value zero-based crop bounds: `[start, stop]`. Each value may be a positive integer or `"auto"`. Mutually exclusive with `rows`. |
| `rows` | List[PositiveInt] | No | Zero-based row indices to keep after any `row_slice` crop. Mutually exclusive with `row_slice`. |
| `reindex` | List[Reindex] | No | Conditional filtering |

**Example:**
```yaml
source:
  kind: text
  local: ./data/mydata.tsv
  url:
    - https://example.com/data.tsv
  delimiter: "\t"
  row_slice: [1, auto]
```

#### Reindexing (Conditional Filtering)

Filter rows based on column values.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `column` | String | Yes | Source column letters to evaluate; constrained to `^[A-Z]{1,3}$` (`A`-`ZZZ`). |
| `comparison` | String | No | Operator. Defaults to `"ne"`; allowed values are `"eq"`, `"ne"`, `"lt"`, `"le"`, `"gt"`, `"ge"`. |
| `comparator` | String\|Int\|Float | Yes | Value to compare against. Must be a string for `"eq"`/`"ne"`, or a number for `"lt"`/`"le"`/`"gt"`/`"ge"`. |

**Example:**
```yaml
reindex:
  - {column: C, comparison: lt, comparator: 0.05}  # Keep rows where column C < 0.05
```

### Statement (Triple Definition)

Defines subject-predicate-object relationships.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `subject` | NodeEncoding | Yes | Subject entity configuration |
| `predicate` | String | No | Biolink predicate. Defaults to `"related_to"`. |
| `object` | NodeEncoding | Yes | Object entity configuration |
| `qualifiers` | List[Qualifier] | No | Edge qualifiers (context) |
| `category_override` | Map[Categories, EdgeCategories] | No | Pin the association class per resolved object category (bare names, no `biolink:` prefix), replacing the derived (subject, object) pair lookup for those rows. Rows whose object category is absent from the map derive as before. Pinned classes are still reconciled against the section predicate (a class whose `predicate` slot rejects it is walked up the association hierarchy, with a warning at config time). Use it when one section mixes object categories the pair lookup merges — e.g. `Disease` and `PhenotypicFeature` both derive to `ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation`, but only `EntityToDiseaseAssociation` / `EntityToPhenotypicFeatureAssociation` receive the class-scoped `regulatory_approvals` grant and declare slots like `number_of_cases`. Rows pinned to either of those classes may additionally carry `disease_context_qualifier` plus DAKP's sparse qualifier stack (`anatomical_context_qualifier`, `sex_qualifier`, `population_context_qualifier`, `frequency_qualifier`, `temporal_context_qualifier`) under deliberate Tablassert policy grants. Deliberately ungranted: `species_context_qualifier` (disabled -- never emittable) plus `temporal_interval_qualifier` and `severity_qualifier` (unsatisfiable -- attached to no class, so a grant could never validate). |

**Example:**
```yaml
statement:
  subject:
    method: column
    encoding: A
    prioritize: [ChemicalEntity]
  predicate: associated_with
  object:
    method: column
    encoding: B
  category_override:
    Disease: EntityToDiseaseAssociation
    PhenotypicFeature: EntityToPhenotypicFeatureAssociation
```

**Example:**
```yaml
statement:
  subject:
    method: column
    encoding: A
    prioritize: [Gene]
  predicate: treats
  object:
    method: column
    encoding: B
    prioritize: [Disease]
```

### NodeEncoding

Defines how to extract and resolve entities.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `method` | String | No | `"value"` (literal) or `"column"` (source column letters). Defaults to `"value"`. |
| `encoding` | String\|Int\|Float | Yes | Literal value or source column letters, depending on `method` |
| `taxon` | PositiveInt | No | NCBI Taxon ID for filtering (e.g., `9606` for human) |
| `prioritize` | List[String] | No | Preferred Biolink categories (must be valid `Categories` enum values such as `Gene`, `Protein`) |
| `avoid` | List[String] | No | Excluded Biolink categories (must be valid `Categories` enum values). Setting `avoid` makes the column an allow-list by complement: categories the `Categories` enum cannot name (fullmap-emitted Biolink mixins; see `biolink.CATEGORY_OVERRIDES`) are dropped as well, so the guard has no silent hole |
| `exclude_prefixes` | List[String] | No | CURIE namespace prefixes excluded during entity resolution: the prefix is the text before the first `:` of a resolved CURIE, and every candidate whose prefix is listed is dropped. Matching is exact and case-sensitive (`OMIM` drops `OMIM:100100` but not `OMIMPS:100` or `omim:100100`). Optional; defaults to null (no prefix filtering). See [Resolution Filters](#resolution-filters). |
| `exclude_regex` | List[String] | No | Case-sensitive regex patterns; any resolved CURIE matching one is dropped during entity resolution. Polars-compatible patterns only (no backreferences or lookarounds); an empty or whitespace-only pattern is rejected at config time (`regex-bad-pattern`) because an empty pattern would match every CURIE and silently drop all candidates. Optional; defaults to null (no regex filtering). See [Resolution Filters](#resolution-filters). |
| `regex` | List[Regex] | No | Pattern replacements |
| `fill` | String | No | Null-filling strategy: `"forward"`, `"backward"`, `"min"`, `"max"`, `"mean"`, `"zero"`, `"one"` |
| `remove` | List[Int\|Float\|String] | No | Regex patterns to remove (replaced with empty string) |
| `prefix` | String | No | Add prefix to values |
| `suffix` | String | No | Add suffix to values |
| `explode_by` | String | No | Delimiter to split multi-value cells |
| `transformations` | List[Math] | No | Mathematical transformations |

#### Method: Value and Column

**`method: value`** - Use a literal value

```yaml
subject:
  method: value
  encoding: CHEBI:41774  # All rows get this CURIE
```

**`method: column`** - Reference a source column (Excel-style letters, since sources are read without headers)

```yaml
subject:
  method: column
  encoding: A  # Read from column A
```

At runtime those letters are converted internally to Polars column names such as `column_1`, but those internal names are not valid configuration values.

#### `split_by`

**`split_by`**: annotations only; splits each cell of a `method: column` encoding into a real JSON array.

```yaml
annotations:
  - annotation: has_evidence
    method: column
    encoding: D        # cells like "EFO:0001,EFO:0002"
    split_by: ","      # -> ["EFO:0001", "EFO:0002"], per row
```

The separator is a property of the data, not of the slot. Inspect the table's cells and set `split_by` to the separator the cells actually use: `","` for comma-joined ids like `"EFO:0001,EFO:0002"` above, `";"` for `"EFO:0001;EFO:0002"`, `"|"` only if the cells happen to be pipe-joined.

For the canonical `regulatory_approvals` annotation, a pipe-delimited source column uses `split_by: "|"` and emits a real per-row array such as `["011111", "022222"]`; combine it with a class-scoped `category_override` when only the intended association classes receive that grant.

`split_by` is the one multivalued encoding: every row's cell becomes its own JSON array, so an array that differs per row, the shape a literal can never express, is declared directly. Values are trimmed and blanks dropped; a null cell stays null.

Single-value cells need no `split_by` at all: `prune_to_class` wraps a scalar bound for a uniformly multivalued slot into a one-element list, so a lone `EFO:0001` cell already emits as `has_evidence: ["EFO:0001"]`. Reach for it when the cells actually join multiple values. Leave such a column without `split_by` and the joined cell stays a scalar: the same wrapping yields a one-element list holding the whole string (`has_evidence: ["EFO:0001;EFO:0002"]`), structurally valid Biolink that hands consumers one unusable blob instead of two ids.

`split_by` requires `method: column` and rejects an empty separator, which would split into individual characters. It is unrelated to the `source.delimiter` CSV/TSV field separator. (The earlier annotation `delimiter` field, unrelated to the `source.delimiter` CSV/TSV separator, was replaced by `split_by`.)

**`split_by` and `explode_by` are the same split, with different destinations.** Both read a delimited cell through one shared primitive, with items trimmed, blanks dropped (so `"a;b;"` and `"a;;b"` yield two items, not three), and a null cell left null; they then differ only in what they do with the items:

| | Destination | Use for |
|---|---|---|
| `explode_by` | one **row** per item | node encodings: each item is its own entity, producing its own edge |
| `split_by` | one **array** on the row | annotations: the items are one multivalued slot on a single edge |

So `explode_by` is not an alternative to `split_by` for a multivalued annotation: it multiplies edges rather than filling one edge's array.

#### Taxonomic Filtering

**`taxon: int`** - Filter entities by organism

```yaml
subject:
  method: column
  encoding: A
  taxon: 9606  # Only human genes (Homo sapiens)
```

Common taxon IDs:
- `9606` - Homo sapiens (human)
- `10090` - Mus musculus (mouse)
- `7227` - Drosophila melanogaster (fruit fly)

#### Category Prioritization

**`prioritize: list[category]`** - Prefer specific Biolink categories

```yaml
subject:
  encoding: A
  prioritize: [Gene, Protein]
```

If "TP53" maps to both Gene and Protein, prefer Gene.

**`avoid: list[category]`** - Exclude specific categories

```yaml
subject:
  method: column
  encoding: A
  prioritize: [OrganismTaxon]
  avoid: [Gene]
```

Prevents misclassifying organism names as genes. Because `avoid` is an allow-list by complement, it also drops any fullmap category the `Categories` enum cannot name, keeping the guard hard against vocabulary drift.

#### Resolution Filters

**`exclude_prefixes` / `exclude_regex`** - Drop resolved CURIEs after lookup

```yaml
subject:
  method: column
  encoding: A
  exclude_prefixes: [OMIM]          # Drop candidates resolving into the OMIM namespace
  exclude_regex: ["^OMIM:\\d+$"]    # Drop resolved CURIEs matching the pattern
```

Unlike `regex` and `remove`, which rewrite the cell text before resolution, `exclude_prefixes` and `exclude_regex` filter resolved CURIEs after the lookup has run: they shrink the candidate set during entity resolution, and a cell whose candidates are all dropped is left unresolved. What an unresolved cell costs depends on the node it belongs to: an unresolved subject, object, or strict qualifier (`nullable: false`, the default) drops the row, exactly like any other resolution failure, whereas a qualifier declared `nullable: true` keeps the edge and omits the qualifier key for the row (see [Qualifiers](#qualifiers)).

`exclude_prefixes` matches on the namespace prefix — the text before the first `:` of a resolved CURIE (a prefix-less CURIE counts as its own prefix) — and drops every candidate whose prefix is listed. The comparison is a membership test against the listed strings, so matching is **exact and case-sensitive**: `OMIM` drops `OMIM:100100`, but leaves both `OMIMPS:100` (a different prefix, not a prefix match) and `omim:100100` (different case) in the candidate set. `exclude_regex` drops every candidate whose CURIE matches any pattern; matching is likewise case-sensitive and unanchored (Polars `str.contains`), so `^omim:` matches nothing in `OMIM:100100` — use the casing the CURIEs actually carry, and anchor with `^`/`$` to pin a whole CURIE. The Polars/Rust regex dialect constraints documented for `regex` apply to `exclude_regex` too, and an empty or whitespace-only `exclude_regex` entry is rejected at config time with `regex-bad-pattern` because an empty pattern would match every CURIE and silently drop all candidates.

#### Text Transformations

**`regex: list[{pattern: Int|Float|String, replacement: Int|Float|String}]`** - Pattern-based replacements

```yaml
subject:
  encoding: A
  regex:
    - {pattern: ".*g__", replacement: ""}   # Remove genus prefix
    - {pattern: ";s__", replacement: " "}   # Replace species separator
```

Executed in order.

> **Regex dialect:** Patterns are passed directly to Polars `str.replace_all()`, which uses the Rust [`regex`](https://docs.rs/regex/) crate. Only features supported by that engine work: in particular, **backreferences (`\1`, `\2`, …) and lookarounds (`(?=...)`, `(?<=...)`, `(?!...)`, `(?<!...)`) are not supported** and will raise an error at parse time. Plain groups `(...)` and non-capturing groups `(?:...)` *are* supported. Stick to character classes, anchors (`^`, `$`), quantifiers, alternation (`a|b`), and grouping if needed. If a transformation is too complex to express, prefer chaining several simple substitutions or capturing the residual context in a `miscellaneous notes` annotation instead.

**`remove: list[regex]`** - Regex patterns to remove

```yaml
subject:
  encoding: A
  remove: ["^NA "]  # Strip leading "NA " prefix from cell text
```

Each entry is applied as a regex replace-with-empty-string on the cell text in place (rows are not dropped). Same regex constraints apply as the `regex` field: Polars-compatible patterns only, no backreferences or lookarounds.

**`prefix` / `suffix`** - Add text

```yaml
object:
  encoding: identifier
  prefix: "CUSTOM:"  # "123" → "CUSTOM:123"
```

**Output columns: `original_<col>` vs `<col>_pre_resolution`** - For every subject/object/qualifier node, the pipeline snapshots the cell value into two columns at different stages:

- `original_<col>` - the **pristine source value**, captured immediately after the column is read (or the literal is set for `method: value`) and *before* any `fill`, `explode_by`, `regex`, `remove`, `prefix`, `suffix`, or `transformations`. Emitted for both `method: value` and `method: column`. Present in final edge output.
- `<col>_pre_resolution` - the **fully-transformed value**, captured *after* all of the above, i.e. the same text that is then normalized and resolved to a CURIE. **Internal only**: used by QC (`fullmap_audit`) and as the node marker in `compile_graph`; stripped from final edge output.

Example: with `method: column`, `encoding: A`, `remove: ["^NA "]` over a cell `"NA BRCA1"`, `original_subject` is `"NA BRCA1"` while `subject_pre_resolution` is `"BRCA1"`. Annotations never emit either column.

#### Null Handling

**`fill: string`** - Fill null values using a strategy

Available strategies:
- `"forward"` - Fill nulls with previous non-null value
- `"backward"` - Fill nulls with next non-null value
- `"min"` - Fill with column minimum
- `"max"` - Fill with column maximum
- `"mean"` - Fill with column mean
- `"zero"` - Fill with 0
- `"one"` - Fill with 1

```yaml
subject:
  method: column
  encoding: A
  fill: forward  # Propagate values down through null rows
```

```yaml
annotations:
  - annotation: expression_level
    method: column
    encoding: C
    fill: mean  # Replace nulls with column average
```

#### Multi-Value Handling

**`explode_by: string`** - Split delimited values into multiple rows

```yaml
object:
  method: column
  encoding: B
  explode_by: ";"  # "P1;P2;P3" → 3 separate edges
```

#### Mathematical Transformations

**`transformations: list[{function, arguments}]`**

Available functions: `copysign`, `pow`

Use the `"values"` token to reference column values in transformations.

### Qualifiers

Add context to edges (anatomical location, disease context, etc.).
`species_context_qualifier` is intentionally disabled: it is neither derived from
node taxon metadata nor accepted as a qualifier or annotation. Taxon metadata is
still emitted on resolved nodes.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `qualifier` | String | Yes | Biolink qualifier from the `Qualifiers` enum (e.g., `"anatomical_context_qualifier"`). Each qualifier key may be declared at most once per statement (`qualifier-duplicated`). |
| `nullable` | Boolean | No | Default `false`. When `true`, a blank or unresolvable `method: column` cell **keeps the edge** and omits the qualifier for that row; when `false` (the default) such a row is dropped, exactly like an unresolved subject/object. Only valid with `method: column`; a literal qualifier can never be null (`qualifier-nullable-literal`). |
| (inherits NodeEncoding) | | | All NodeEncoding fields available |

A qualifier inherits the full NodeEncoding surface, so it accepts the resolution filters `exclude_prefixes` and `exclude_regex` with the same matching rules subject/object encodings use: they filter the resolved CURIEs of the qualifiers that go through entity resolution (CURIE-ranged qualifiers; enum-ranged ones pass through verbatim). What differs is the consequence when filtering removes every candidate. A strict qualifier (`nullable: false`, the default) drops the row like an unresolved subject/object, while a `nullable: true` qualifier keeps the edge and omits the qualifier key for that row. See [Resolution Filters](#resolution-filters).

**Example: required qualifier (dense column, default)**
```yaml
qualifiers:
  - {qualifier: anatomical_context_qualifier, method: value, encoding: UBERON:0000061}
```

**Example: optional qualifier (sparse column, `nullable: true`)**

A qualifier is a node encoding resolved through the fullmap alongside subject/object,
so by default a blank or unresolved qualifier cell drops the whole edge. That is the
right default (an edge should not silently lose a declared context), but it forbids
per-edge *optional* qualifiers, e.g. a `disease_context_qualifier` on conditional
contraindications where most rows carry no context. Set `nullable: true` on a
`method: column` qualifier to keep such rows and omit the qualifier key for
them (the null value is stripped from the edge):

```yaml
qualifiers:
  - {qualifier: disease_context_qualifier, method: column, encoding: F, nullable: true, prioritize: [Disease]}
```

### Provenance

Required metadata about data source.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `repo` | String | No | Repository. Defaults to `"PMC"`; allowed values are `"PMC"`, `"PMID"`. |
| `publication` | String | Yes, unless `override` is set | Repository-local identifier emitted as a CURIE. For `repo: PMC` the value **must** start with `PMC` followed by digits (e.g., `"PMC11708054"`, `"PMC123"`) and is emitted under the `PMCID:` namespace as `PMCID:PMC...`; for `repo: PMID` it is emitted as `PMID:<publication>` (e.g., `"11708054"` → `PMID:11708054`). |
| `knowledge_level` | String | No | Biolink KL/AT knowledge level of produced edges. Defaults to `"statistical_association"`; allowed values are the `KnowledgeLevels` enum (e.g., `knowledge_assertion`, `logical_entailment`, `prediction`, `statistical_association`, `text_co_occurrence`, `observation`, `not_provided`). |
| `agent_type` | String | No | Biolink KL/AT agent type responsible for produced edges. Defaults to `"data_analysis_pipeline"`; allowed values are the `AgentTypes` enum (e.g., `manual_agent`, `automated_agent`, `data_analysis_pipeline`, `computational_model`, `text_mining_agent`, `image_processing_agent`, `manual_validation_of_automated_agent`, `not_provided`). |
| `override` | Object | No | Manual provenance for non-PMC/PMID sources. When set, it replaces repo/publication-derived provenance and `publication` must be omitted. |

**Example:**
```yaml
provenance:
  repo: PMC
  publication: "PMC11708054"
  knowledge_level: statistical_association
  agent_type: data_analysis_pipeline
```

> **Default provenance trio.** When omitted, `repo` / `knowledge_level` / `agent_type` default to `PMC` / `statistical_association` / `data_analysis_pipeline`, so every produced edge is asserted as a *statistical association* generated by an automated *data analysis pipeline* under the PubMed Central namespace. That is a sensible default for GWAS/omics tables mined from a pipeline, but it is a claim about your data's semantics: if your rows are manually curated assertions, predictions, or observations, override `knowledge_level` and `agent_type` (and `repo` / `publication`) so the emitted Biolink KL/AT provenance is accurate.

#### Manual provenance override

Use `provenance.override` when a table comes from another knowledge graph or source system whose Translator provenance cannot be derived from a PMC/PMID publication. The override is wired like the other Tablassert model classes and wins over the repo/publication auto-generation for that section's upstream sources, publications, and KL/AT. The primary entry of the edge `sources` list (`resource_role: primary_knowledge_source`) is **not** re-targetable per section via `upstream_resource_ids`; it derives from the graph-level `infores` (see [Graph](graph.md)) unless an explicit [`sources` template](#explicit-sources-template) is given. Put manual infores CURIEs in `upstream_resource_ids`.

```yaml
provenance:
  override:
    upstream_resource_ids:
      - infores:external-source
    publications:
      - PMCID:PMC1234567
    knowledge_level: knowledge_assertion
    agent_type: manual_agent
```

Override fields:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `sources` | List[Object] | No | Explicit retrieval-`sources` entry templates replacing the derived primary/upstream emission entirely. Mutually exclusive with `upstream_resource_ids` and `upstream_source_record_urls` (the template subsumes both). See [Explicit sources template](#explicit-sources-template). |
| `upstream_resource_ids` | List[String] | No | Manual upstream source infores CURIEs replacing the repo-derived `PMC`/`PMID` source map, the sanctioned place for manual infores. Each entry must start with `infores:`. |
| `upstream_source_record_urls` | Map[String, List[URL]] | No | Per-upstream source record URLs keyed by infores CURIE; every key must appear in `upstream_resource_ids`. When set, the section's `source.url` values serve the RIG only and are NOT emitted on the primary `sources` entry; each listed upstream supporting entry carries its own `source_record_urls` instead. |
| `publications` | List[String] | No | Manual publication CURIEs. Entries must currently start with `PMCID:`; PMID compatibility for manual overrides is intentionally deferred. |
| `knowledge_level` | String | No | Override-specific KL value. Defaults to `statistical_association`. |
| `agent_type` | String | No | Override-specific AT value. Defaults to `data_analysis_pipeline`. |

Tablassert emits the graph-level infores (or `infores:<graph-name>` when unset) as the primary entry of the Biolink `sources` list on each edge, `{resource_id: "infores:multiomics-kg", resource_role: "primary_knowledge_source", upstream_resource_ids: [...], source_record_urls: [...]}`, with one additional `supporting_data_source` entry per upstream. When `override.upstream_source_record_urls` is set, the primary entry emits no `source_record_urls` and each mapped supporting entry carries its own instead. When `override.sources` is set, that derivation is skipped entirely and the template entries are emitted verbatim, in order. No flat `primary_knowledge_source` scalar is emitted: current translator-ingests practice carries retrieval provenance only in `sources`, and the Biolink `RetrievalSource` class is where `resource_id` / `upstream_resource_ids` / `source_record_urls` are defined. Each retrieval-source entry uses `resource_id` as its sole identifier. Without `override.sources`, the override cannot set a per-section primary source; manual infores CURIEs belong in `upstream_resource_ids`. Older flat `resource_id` / `primary_knowledge_source` output has been removed so generated KGX matches the Biolink edge contract.

#### Explicit sources template

`override.sources` replaces the derived `sources` list with an explicit, ordered template: one Biolink `RetrievalSource` struct per entry. Use it when the emitted provenance must differ structurally from the "graph infores as primary + one supporting entry per upstream" shape, e.g. to mark the graph as an `aggregator_knowledge_source` with the real primary elsewhere.

```yaml
provenance:
  override:
    sources:
      - resource_id: infores:multiomics-drugapprovals
        resource_role: aggregator_knowledge_source
        upstream_resource_ids: [infores:dailymed, infores:faers]
        source_record_urls:
          - "https://db.systemsbiology.net/gestalt/cgi-pub/KGinfo.pl?id={edge_id}"
      - resource_id: infores:faers
        resource_role: primary_knowledge_source
      - resource_id: infores:dailymed
        resource_role: supporting_data_source
```

Entry fields:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `resource_id` | String | Yes | Infores CURIE of this entry (its sole identifier on output). Must start with `infores:`, and must be unique within the template. |
| `resource_role` | String | Yes | One of the Biolink `ResourceRoleEnum` values: `primary_knowledge_source`, `aggregator_knowledge_source`, or `supporting_data_source`. Anything else fails KGX validation downstream, so it is rejected at config time. |
| `upstream_resource_ids` | List[String] | No | Upstream infores CURIEs carried by this entry; each must start with `infores:`. |
| `source_record_urls` | List[String] | No | Source record URLs carried by this entry. Each entry must be an absolute `http(s)://` URL, optionally containing the `{edge_id}` placeholder (see below). |

Validation rules (all reported with error code `override-bad-sources`):

- `sources` is **mutually exclusive** with `upstream_resource_ids` and `upstream_source_record_urls`; the template subsumes both, so combining them fails loudly.
- When set, the list must be **non-empty**, `resource_id` values must be **unique**, and at least one entry must carry role `primary_knowledge_source` or `aggregator_knowledge_source`.

**The `{edge_id}` placeholder.** A `source_record_urls` entry may embed the literal string `{edge_id}` to build per-edge URLs (e.g. a drill-down page keyed by edge id). The edge `id` is a deterministic content hash assigned at the final dedup stage, *after* subgraphs are written, so the placeholder cannot be interpolated during the table build. Instead, the literal placeholder is emitted into the subgraph (and is what the content hash covers, keeping ids deterministic), and a post-dedup sweep of the final `*.edges.ndjson` replaces `{edge_id}` with each record's own `id` in every `sources[].source_record_urls` string. Files without the marker are left byte-identical.

### Annotations

Optional edge attributes (statistical metadata, notes, etc.).

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `annotation` | String | Yes | Attribute name (e.g., `"p_value"`, `"effect_size"`). Lowercased and trimmed of leading/trailing whitespace at parse time; underscores are preserved (use snake_case). |
| `split_by` | String | No | Separator splitting each cell of a `method: column` encoding into a real JSON array. See [`split_by`](#split_by). |
| (inherits Encoding) | | | All Encoding fields available (method, encoding, regex, etc.) |

Multivalued Biolink slots such as `has_evidence` or `regulatory_approvals`, whose consumers iterate the value, must emit a real JSON array rather than a scalar. [`split_by`](#split_by) is the multivalued encoding: point it at a column whose cells join multiple values, set it to the separator the cells actually use, and each cell's delimited text splits into a per-row array. A single-value column needs no `split_by`; the scalar is wrapped into a one-element array.

**Example:**
```yaml
annotations:
  - {annotation: p_value, method: column, encoding: C}                 # Read from column C
  - {annotation: adjusted_p_value, method: column, encoding: D}        # A real Association slot -> emitted on the edge
  - {annotation: study_size, method: value, encoding: 450}            # Study metadata -> inlined supporting Study (see below)
  - {annotation: multiple_testing_correction_method, method: value, encoding: "Benjamini Hochberg"}
  - {annotation: has_evidence, method: column, encoding: E, split_by: ","}       # cells like "EFO:0001,EFO:0002" -> a per-row JSON array

  # Descriptive name of your choice: folded into `supporting_text` on output.
  - annotation: log2fc_relative_to_vehicle_control
    method: value
    encoding: "Values are log2 fold-change relative to vehicle control; n=3 biological replicates per arm"
```

#### Allow-list and auto-folding

Annotation names fall into three groups at build time:

- **Allowed edge fields:** names on the edge allow-list: [Biolink Association](https://biolink.github.io/biolink-model/) slots, qualifier slots, and curated KGX/Tablassert edge fields (e.g. `p_value`, `adjusted_p_value`, `knowledge_level`, `supporting_text`, `publications`, `effect_size`, `effect_type`, `statistical_significance_qualifier`, qualifier slots like `severity_qualifier` / `disease_context_qualifier`) are written to edges verbatim. `effect_size` and `effect_type` became real Association slots in biolink-model 4.4.4 (PR #1774) and emit as real JSON numbers and enum tokens. Annotation names are matched case-insensitively against the allow-list and emitted under the canonical slot spelling. The class-scoped `regulatory_approvals` grant is available on `EntityToDiseaseAssociation` and `EntityToPhenotypicFeatureAssociation`; it is a multivalued slot, so declare [`split_by`](#split_by) for pipe-joined cells like `011111|022222`. The same two classes also carry class-scoped grants for DAKP's sparse qualifier stack (`anatomical_context_qualifier`, `sex_qualifier`, `population_context_qualifier`, `frequency_qualifier`, `temporal_context_qualifier`) plus `disease_context_qualifier`; without a grant `prune_to_class` nulls those qualifiers off the pinned rows. `species_context_qualifier` stays disabled, and `temporal_interval_qualifier` / `severity_qualifier` stay unsatisfiable, so none of the three is granted.
- **Study metadata:** `study_size`, `study_cohort`, `study_context`, `study_date_range`, `study_method_description`, and `study_method_types` are current Biolink `Study` node properties ([biolink-model PR #1770](https://github.com/biolink/biolink-model/pull/1770)). They describe the study itself, not an association, so their values are carried on the edge's **inlined supporting Study** (`has_supporting_studies` to `Study`, the COHD/ICEES pattern) rather than emitted as edge fields. Deprecated `supporting_study_*` spellings and `sample_size` are accepted as aliases and renamed onto canonical `study_*` names by coercion. They never appear in final JSON. Declaring any of them is legal and emits a `BiolinkRelocationWarning` naming where the value went.
- **Unsatisfiable slots:** names the Biolink LinkML schema declares but attaches to **no** Pydantic class, derived from the installed `biolink-model`. A record carrying one could never validate, so its value is preserved in the inlined supporting study's `StudyResult.description`. A slot leaves the set automatically once a release attaches it.

- **Tablassert pipeline fields**: `upstream_resource_ids`, `source_record_urls`.

Any other annotation name is treated as **supporting context**. At the end of `compile_graph`, tablassert sweeps the edge columns: for each non-allow-listed name it emits `"name: value"` entries into the edge's `supporting_text` (a `list[str]`), then drops the original column. Behavior worth knowing:

- **Pick descriptive names.** Whatever string you choose becomes the prefix in `supporting_text`, so `log2fc_relative_to_vehicle_control` reads as `"log2fc_relative_to_vehicle_control: 1.4"` on the edge. Avoid generic names like `notes` or `value`.
- **Null and blank cells produce no entry.** Whitespace-only values are treated as blank.
- **Existing `supporting_text` is preserved.** If an annotation named `supporting_text` is already on the edge (Biolink-native slot), folded entries are appended to it rather than replacing it. Scalar values are coerced to a single-element list first.
- **Ordering is stable.** Folded entries are sorted alphabetically by column name.

This means nothing in your source data is silently dropped: context that doesn't map to a structured Biolink slot travels along inside `supporting_text` instead.

In addition to user-declared annotations, every edge automatically carries `extracted_from_row_number`, a 1-based index into the original source table (matching Excel-style row numbering). It is not declared as an annotation. Tablassert emits it internally so each edge always carries its source-row provenance, and the inlined supporting study consumes it: the `StudyResult` that anchors the study's row is identified by the scoped CURIE `row:<N>`. The row and sheet columns are never folded into `supporting_text`.

The supporting study's identity is disjoint by construction: `Study.id` is the section's publication CURIE (`PMID:123`, `PMCID:PMC11708054`); `Study.name` is a human label, the worksheet name for spreadsheet sources and the source filename otherwise. It is never a composition of the two. A section declaring **no `publications`** falls back to the config stem (the YAML filename without its extension) as the id, and the struct is then emitted **only when it carries something to preserve**, such as study metadata, routed unsatisfiable values, or class-pruned values. Biolink defines `has supporting studies` as "studies that produced information used as evidence", so a contentless unpublished section emits no `has_supporting_studies` at all; a section with a real publication always keeps the wrapper:

```json
"has_supporting_studies": {
  "PMCID:PMC11708054": {
    "id": "PMCID:PMC11708054",
    "name": "all correlations",
    "study_size": 9,
    "has_study_results": [{"id": "row:42"}]
  }
}
```

`StudyResult` carries no name and no row description. The `row:<N>` id is the provenance, and its `description` is reserved for values with no structured home, such as routed unsatisfiable values and class-pruned qualifiers.

#### Automatic column coercion

Before the allow-list sweep runs, tablassert renames statistical columns to their canonical Biolink names so source headers do not have to match exactly. Recognition is delimiter-anchored (spaces, `_`, `-`, `.` are interchangeable) and the **best fuzzy match per target wins, with an existing canonical column always preferred** over a higher-scoring spaced alias.

**Losing candidates are dropped, for every target.** A loser is another spelling of the value that already reached the canonical slot, and anything left on the frame would be folded into `supporting_text` — emitting the same measurement twice, once as a typed Biolink slot and once as free text. This has a cost worth knowing: if a table's headers are genuinely *different* statistics that all recognize onto one slot (`beta` beside `odds ratio`, or a two-analysis header set like `p_value_analysis1` / `p_value_analysis2`), **only the winner survives**. Give such columns names the table below does not recognize when every one of them must reach the edge.

| Recognized as | Canonical name | Typical source spellings |
|---|---|---|
| P value (raw) | `p_value` | `p value`, `p-value`, `pvalue`, `P`, `gwas p`, `raw_p`, `pvalue1` |
| Adjusted P value | `adjusted_p_value` | `padj`, `p.adj`, `adj.P.Val`, `adjusted_p_value`, `adj_p_value`, `corrected_p_value`, `p_adjust`, `FDR`, `Bonferroni`, `Holm`, `q value` |
| Negative-log P value | `p_value` / `adjusted_p_value`, un-logged | `-log10(p)`, `negative log p value`, `neg log10 q value`, `neg-log10-pvalue` |
| Study size | `study_size` | `n`, `sample_size`, `study size`, `cohort_size`, `participants_n`, `enrollment`, the deprecated `supporting_study_size` |
| Effect size | `effect_size` | `effect size`, `odds ratio`, `hazard ratio`, `beta`, `log2FC`, `correlation`, `rho` (and the legacy `relationship_strength`) |
| Effect type | `effect_type` | `effect type`, `effect metric`, `statistic type`, `metric` |

- **Negative-log p-value columns are un-logged, not shipped verbatim.** A header carrying an explicit negation marker (`negative`, `negated`, `neg`, or a leading `-`) before a `log` / `log10` p-or-q token reports a `-log10` score rather than a probability, so the value is converted back to its original scale (`p = 10 ** -x`) as the column is renamed onto `p_value` / `adjusted_p_value`, and again when the significance band is derived. Left alone, a `-log10(p)` of 8 would emit `p_value` 8.0 and band as `not_significant`, the exact inverse of the truth. The trailing token has to be a *complete* p/q-value token (or a bare delimited `P`), so a word that merely starts with p or q (`negative log protein`) is not matched. A plain `log10 p value` with no negation marker is deliberately left verbatim, because the sign convention is ambiguous there. When a raw p/q column and a `-log10` alias of the same statistic both appear, the raw column always wins selection and the alias is dropped, so a short raw name (`P`, `FDR`) cannot lose to a long alias it would then be un-logged over — and the edge never carries `p_value` 0.03 beside a `"negative log10 p value: 8.0"` supporting-text entry, which reads as a contradiction rather than a duplicate. Scores extreme enough to underflow Float64 are floored at `0.0`, which is indistinguishable from `p ~ 0` in practice and lands in the same band; nulls stay null.
- **`effect_type` values are also coerced.** Each cell is matched case/separator-insensitively against an alias table (e.g. `"OR"` → `odds_ratio`, `"Cohen's d"` → `cohens_d`, `"Spearman"` → `spearmans_rho`), then by `rapidfuzz` fallback against the 25 permissible `EffectTypes` values; anything matching nothing is dropped to `null` rather than carried through (the Biolink range is the enum).
- **`statistical_significance_qualifier` is auto-derived** from the p-value column into five bands: `very_strongly_significant` (p ≤ 0.001), `strongly_significant` (≤ 0.01), `significant` (≤ 0.05), `suggestive` (≤ 0.10), and `not_significant` (> 0.10). The values are bare `StatisticalSignificanceQualifierEnum` tokens. Biolink-model 4.4.4 attaches the slot to `Association` with the enum as its range, so the band rides the edge as a real field with no `biolink:` prefix. The same rigorous selection picks the source column: a raw `p_value` column is preferred, `adjusted_p_value` is the fallback, and the qualifier is omitted entirely when no p-value column is present.
- **`effect_size` and `effect_type` travel as a pair.** A section declaring one without the other does not fail validation: the unpaired annotation is **dropped** from the section with an `UnpairedEffectAnnotationWarning` naming what was dropped and from where, and the section's edges are kept; neither half carries evidence alone. A bare effect size is uninterpretable (0.85 of *what*, an odds ratio or a Spearman rho?), and Biolink PR #1774 only populates `effect_type` alongside a numeric `effect_size`, so the build nulls an unpaired type anyway. Declare both together to retain the full evidence. Alias spellings count: `odds ratio` and the legacy `relationship_strength` coerce to `effect_size`, so both still need a sibling `effect_type`. Use `method: value` when every row shares one statistic and `method: column` when the table provides it; in a `template` + `sections` config the two lists are concatenated, so a constant `effect_type` declared once on the template pairs with each section's own `effect_size` column.
- **Biolink class rules are enforced.** `effect_type` is nulled on every row where `effect_size` is null (and nulled entirely when no `effect_size` column exists); `statistical_significance_qualifier` is only set when `p_value`/`adjusted_p_value` is populated, and null p-values yield a null qualifier.

This is why declaring an annotation like `{annotation: p value, method: column, encoding: E}` still produces a top-level `p_value` edge field: the header is normalized to the Biolink name before folding is considered.

## Next Steps

- **[Advanced Example](advanced-example.md)** - Real-world configuration with complex transformations
- **[Graph Configuration](graph.md)** - How to orchestrate multiple tables
- **[Tutorial](../tutorial.md)** - Step-by-step walkthrough
