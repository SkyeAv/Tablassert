# Tutorial: Your First Knowledge Graph

**By the end of this tutorial you will have built a KGX-compliant knowledge graph from a CSV of
gene-disease associations**: nodes and edges with standardized CURIEs, biolink categories, provenance,
and statistical annotations, ready for NCATS Translator. You'll learn the complete workflow: creating
configurations, running Tablassert, and examining the output.

The input files ship under `docs/examples/` and are validated against the live schema by the test suite;
the configurations below reproduce them exactly.

**Time:** 5-10 minutes

## Prerequisites

- Tablassert installed (see [Installation](installation.md))
- Required database: fullmap
- Basic familiarity with YAML

## The Data

The input is a CSV of gene-disease associations, shipped at `docs/examples/tutorial-data.csv`:

```csv
gene_symbol,disease_name,p_value,sample_size
TP53,lung cancer,0.001,450
BRCA1,breast cancer,0.0001,1200
EGFR,colorectal cancer,0.005,680
KRAS,pancreatic cancer,0.002,320
```

**Goal:** Transform this into KGX-compliant nodes and edges.

## Step 1: Create the Data File

If you're following along outside the repository, save the CSV from [The Data](#the-data) above as `tutorial-data.csv`.

## Step 2: Create Table Configuration

Create `tutorial-table.yaml`. This config is shipped, test-validated, at
`docs/examples/tutorial-table.yaml` (checked against the live schema by the test suite); the listing
below matches it exactly:

```yaml
template:
  source:
    kind: text
    local: ./docs/examples/tutorial-data.csv
    url:
      - https://example.com/data.csv
    row_slice:
      - 1
      - auto
    delimiter: ","
  statement:
    subject:
      method: column
      encoding: A
      prioritize:
        - Gene
    predicate: associated_with
    object:
      method: column
      encoding: B
      prioritize:
        - Disease
  provenance:
    repo: PMID
    publication: "12345678"
  annotations:
    - annotation: p_value
      method: column
      encoding: C
    - annotation: study_size
      method: column
      encoding: D
```

!!! note "Paths"
    The shipped fixture reads the CSV from `docs/examples/tutorial-data.csv` (repo-root-relative). If you
    saved your own `tutorial-data.csv` in Step 1, set `source.local: ./tutorial-data.csv` instead.

**What this does:**
- **source**: Reads the CSV, skipping the header row (`row_slice` starts at 1)
- **statement**: Creates edges where genes (subject) are `associated_with` diseases (object)
- **subject/object**: Uses `column` method to read from columns A (gene symbol) and B (disease name)
- **prioritize**: Tells entity resolution to prefer Gene/Disease categories
- **annotations**: Adds p-value (column C) as an edge attribute and study size (column D) as metadata on the inlined supporting study

## Step 3: Create Graph Configuration

Create `tutorial-graph.yaml`. The shipped, test-validated version lives at
`docs/examples/tutorial-graph.yaml`; the listing below matches it exactly:

```yaml
name: TUTORIAL_KG
version: 1.0.0
tables:
  - ./docs/examples/tutorial-table.yaml
fullmap: /path/to/fullmap
rig:
  source_info:
    infores_id: infores:tutorial-kg
    name: Tutorial tabular source
    description: Example source data used by the Tablassert tutorial graph.
    terms_of_use_info:
      license_name: CC0 1.0 Universal
      license_url: https://creativecommons.org/publicdomain/zero/1.0/
    data_access_locations:
      - Tutorial data - https://example.com/data.csv
    source_status: unknown
  ingest_info:
    utility: Demonstrates how Tablassert turns configured tabular records into Translator-ready KGX.
    scope: Gene-disease associations from the tutorial table, one edge per configured row.
  provenance_info:
    contributions:
      - "Tutorial author: config author, data modeling"
  artifact_base_url: https://example.com/tutorial-kg
  artifact_base_path: ./tutorial-output
```

**Important:** Replace `fullmap` with the path to your fullmap redb (and adjust `tables` if your table
config lives elsewhere). The `rig:` section is required; see the
[Graph configuration reference](configuration/graph.md) for every field.

**What this does:**
- **name/version**: Output files will be `TUTORIAL_KG_1.0.0.nodes.ndjson`, `TUTORIAL_KG_1.0.0.edges.ndjson`, and `TUTORIAL_KG_1.0.0.RIG.yaml` (written into `rig.artifact_base_path`, here `./tutorial-output/`)
- **rig**: Resource Ingest Guide metadata (source info, utility/scope, provenance, artifact bases) emitted as the `.RIG.yaml`
- **tables**: List of table configurations to process
- **fullmap**: Path to the fullmap redb file (or base directory) for entity resolution

## Step 4: Run Tablassert

```bash
tablassert build-kg tutorial-graph.yaml
```

To also run the quality-control audit stage, add `--qc`; add `--log` for verbose per-section logging when the `[log]` extra is installed:

```bash
tablassert build-kg tutorial-graph.yaml --qc --log
```

**What happens:**
1. Loads graph configuration
2. For each table config:
   - Reads the source file from disk
   - Applies row slicing
   - Resolves entities (genes and diseases)
   - Validates with the QC pipeline when `--qc` is passed (exact → fuzzy → abbreviation → SapBERT)
   - Creates subgraph parquet file
3. Aggregates all subgraphs
4. Exports NDJSON files and the RIG

## Step 5: Examine Output

**Nodes file:**

```bash
head -n 3 tutorial-output/TUTORIAL_KG_1.0.0.nodes.ndjson
```

Example output:
```json
{"id":"HGNC:11998","name":"TP53","category":["biolink:Gene"],"taxon":"NCBITaxon:9606"}
{"id":"MONDO:0008903","name":"lung cancer","category":["biolink:Disease"]}
{"id":"HGNC:1100","name":"BRCA1","category":["biolink:Gene"],"taxon":"NCBITaxon:9606"}
```

**Edges file:**

```bash
head -n 2 tutorial-output/TUTORIAL_KG_1.0.0.edges.ndjson
```

Example output (p-values are emitted as controlled scientific-notation strings and the derived
significance band as a bare enum token; the study size is `Study.study_size` metadata on the
inlined supporting study, anchored to the source row by the `row:<N>` StudyResult):
```json
{"id":"2cfea591-0f8f-33af-a7df-03da531d3359","subject":"HGNC:11998","predicate":"biolink:associated_with","object":"MONDO:0008903","p_value":"1.0000e-03","statistical_significance_qualifier":"strongly_significant","has_supporting_studies":{"PMID:12345678":{"id":"PMID:12345678","name":"tutorial-data.csv","study_size":450,"has_study_results":[{"id":"row:2"}]}},"publications":["PMID:12345678"]}
{"id":"7b1c9d02-5e8a-4f3b-9c1d-2a6e8f0b4d7c","subject":"HGNC:1100","predicate":"biolink:associated_with","object":"MONDO:0005041","p_value":"1.0000e-04","statistical_significance_qualifier":"very_strongly_significant","has_supporting_studies":{"PMID:12345678":{"id":"PMID:12345678","name":"tutorial-data.csv","study_size":1200,"has_study_results":[{"id":"row:3"}]}},"publications":["PMID:12345678"]}
```

**RIG file:**

```bash
cat tutorial-output/TUTORIAL_KG_1.0.0.RIG.yaml
```

The Resource Ingest Guide records the graph's source metadata and terms of use (`rig.source_info`), ingest utility and scope (`rig.ingest_info`), provenance (`rig.provenance_info`), the generated artifact locations, and a summary of the emitted node and edge types, all validated in memory before the file is written, for NCATS Translator registration.

## Understanding the Transformation

Entity resolution maps text to CURIEs ("TP53" → `HGNC:11998` Gene, "lung cancer" → `MONDO:0008903` Disease); when `--qc` is passed, the QC pipeline validates each mapping across four stages (exact → fuzzy → abbreviation → SapBERT). The output is KGX-compliant nodes and edges with standardized CURIEs, Biolink categories and predicates, provenance, and edge annotations.

## Next Steps

- **[Configuration Reference](configuration/table.md)** - Learn all configuration options
- **[Advanced Example](configuration/advanced-example.md)** - See real-world usage with complex transformations
- **[API Reference](api/fullmap.md)** - Understand entity resolution internals
