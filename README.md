# Tablassert

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![PyPI version](https://badge.fury.io/py/tablassert.svg)](https://badge.fury.io/py/tablassert)
[![Downloads](https://img.shields.io/pypi/dm/tablassert.svg)](https://pypi.org/project/tablassert/)
[![CI](https://github.com/SkyeAv/Tablassert/actions/workflows/ci.yml/badge.svg)](https://github.com/SkyeAv/Tablassert/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/github/deployments/SkyeAv/Tablassert/github-pages?label=docs)](https://skyeav.github.io/Tablassert/)
[![GitHub stars](https://img.shields.io/github/stars/SkyeAv/Tablassert.svg)](https://github.com/SkyeAv/Tablassert/stargazers)

> Extract knowledge assertions from tabular data into NCATS Translator-compliant KGX NDJSON,
> declaratively, with entity resolution built in and optional quality control.

Tablassert turns biomedical spreadsheets (Excel, CSV, TSV) into knowledge graphs ready for NCATS
Translator. Declare how your columns map to subject-predicate-object statements in YAML; Tablassert
resolves free text to standard CURIEs, attaches provenance and statistical annotations, and emits
KGX-compliant nodes and edges.

**[Full Documentation](https://skyeav.github.io/Tablassert/)**: installation guides, tutorial,
configuration reference, and API docs.

## Quick Start

```bash
pip install tablassert
```

Given a CSV of gene-disease associations with p-values and sample sizes, declare the mapping in a
table config (`table.yaml`):

```yaml
template:
  source:
    kind: text
    local: ./gene-disease.csv
    url: [https://example.com/data.csv]
    row_slice: [1, auto]
    delimiter: ","
  statement:
    subject: { method: column, encoding: A, prioritize: [Gene] }
    predicate: associated_with
    object: { method: column, encoding: B, prioritize: [Disease] }
  provenance: { repo: PMID, publication: "12345678" }
  annotations:
    - { annotation: p_value, method: column, encoding: C }
    - { annotation: study_size, method: column, encoding: D }
```

Wrap it in a graph config (`graph.yaml`) pointing at your fullmap entity-resolution database
and carrying the required `rig:` metadata for the generated Resource Ingest Guide:

```yaml
name: MY_KG
version: 1.0.0
tables:
  - ./table.yaml
fullmap: /path/to/fullmap
rig:
  source_info:
    infores_id: infores:my-kg
    terms_of_use_info:
      terms_of_use_url: https://example.org/terms
    data_access_locations:
      - My source downloads - https://example.org/downloads
    source_status: maintained_regular_updates
  ingest_info:
    utility: Gene-disease associations support Translator disease-mechanism queries.
    scope: Gene-disease associations extracted from tabular sources.
  provenance_info:
    contributions:
      - "Author Name - code author, data modeling"
  artifact_base_url: https://example.org/my-kg
  artifact_base_path: ./published/my-kg
```

Build the knowledge graph:

```bash
tablassert build-kg graph.yaml
```

Output is one JSON object per line: nodes with Biolink categories, edges with annotations.

```json
{"id":"HGNC:11998","name":"TP53","category":["biolink:Gene"],"taxon":"NCBITaxon:9606"}
{"id":"MONDO:0008903","name":"lung cancer","category":["biolink:Disease"]}
```

```json
{"subject":"HGNC:11998","predicate":"biolink:associated_with","object":"MONDO:0008903","p_value":"1.0000e-03","has_supporting_studies":{"PMID:12345678":{"id":"PMID:12345678","name":"gene-disease.csv","study_size":450,"has_study_results":[{"id":"row:2"}]}}}
```

See the [Tutorial](https://skyeav.github.io/Tablassert/tutorial/) for the full walkthrough.

## Key Features

- **Declarative YAML configuration**: define data transformations without writing code
- **Built-in entity resolution**: map free text to genes, diseases, and chemicals with standard
  CURIEs, taxonomic filtering, and provenance, backed by an embedded redb database
- **Optional quality control**: a four-stage audit (exact → fuzzy → abbreviation → SapBERT embeddings) flags
  low-confidence mappings
- **KGX compliance**: emits NCATS Translator-compatible node/edge NDJSON with Biolink categories
  and predicates
- **Autonomous agent**: `tablassert agent` derives, builds, and refines configs for whole papers
- **Performance & reproducibility**: lazy Polars pipelines and a deterministic UV-based
  development environment

## Installation

```bash
pip install tablassert
```

Or with uv: `uv tool install "tablassert[cli]"`. The base install provides the Python API;
install `[cli]` to use the `tablassert` command and optional extras for additional runtime and pipeline capabilities:

| Extra | Adds | Install |
| ----- | ---- | ------- |
| `cli` | `tablassert` command and rich terminal progress | `pip install "tablassert[cli]"` |
| `rt` | CPU-compatible Polars runtime | `pip install "tablassert[rt]"` |
| `aria2` | bundled aria2c downloader, used automatically by `build-fullmap` when installed (Linux/Windows wheels only) | `pip install "tablassert[aria2]"` |
| `qc` | four-stage QC audit (exact → fuzzy → abbreviation → SapBERT embeddings) | `pip install "tablassert[qc]"` |
| `agent` | autonomous agent (smolagents, litellm, article/table context) | `pip install "tablassert[agent]"` |
| `optimize` | GEPA prompt optimization for `agent --optimize` (dspy) | `pip install "tablassert[optimize]"` |
| `distill` | distillation dataset export (`tablassert distill-export`, HF `datasets`) | `pip install "tablassert[distill]"` |
| `log` | loguru-backed file/progress logging (rotation, enqueue) | `pip install "tablassert[log]"` |

The `tablassert` command requires `[cli]`; without it, the console launcher reports the exact install command. QC is opt-in at build time (`build-kg --qc`). Reaching a feature whose extra is not installed never
produces a bare `ModuleNotFoundError`: the failure names the missing package and the exact install
command, and for `build-kg --qc` and `tablassert agent` it arrives before the run starts rather than
partway through. Logging is the exception: without the `log` extra Tablassert produces no logs
instead of failing. See the
[Installation guide](https://skyeav.github.io/Tablassert/installation/) for the full matrix and the
[CLI Reference](https://skyeav.github.io/Tablassert/cli/) for every flag.

## Entity Resolution API

```python
from pathlib import Path
from tablassert.lib import resolve_many

results = resolve_many(
    col="gene",
    entities=["TP53", "BRCA1"],
    fullmap=Path("/path/to/fullmap"),
    taxon="9606",
)
# [{"original_gene": "TP53", "gene": "HGNC:11998", "gene_name": "TP53", ...}, ...]
```

Point `resolve_many()` at a fullmap database to resolve any iterable of entity strings to CURIEs,
no LazyFrame setup or NLP preprocessing required. See the
[Batch Resolution API](https://skyeav.github.io/Tablassert/api/lib/) for the full reference.

## Documentation

- **[Installation](https://skyeav.github.io/Tablassert/installation/)**: install methods, extras, and development setup
- **[Tutorial](https://skyeav.github.io/Tablassert/tutorial/)**: step-by-step example with synthetic data
- **[CLI Reference](https://skyeav.github.io/Tablassert/cli/)**: complete command-line flag reference
- **[Use Case Gallery](https://skyeav.github.io/Tablassert/examples/)**: real-world configuration patterns
- **[Fullmap](https://skyeav.github.io/Tablassert/fullmap/)**: building and querying the entity-resolution database
- **[Configuration](https://skyeav.github.io/Tablassert/configuration/graph/)**: graph and table configuration reference
- **[Agent](https://skyeav.github.io/Tablassert/agent/)**: the autonomous agent pipeline
- **[API Reference](https://skyeav.github.io/Tablassert/api/fullmap/)**: core functions documentation
- **[Development](https://skyeav.github.io/Tablassert/development/)**: dev environment setup and contributor workflow
- **[Changelog](https://skyeav.github.io/Tablassert/changelog/)**: release history

## Developing

```bash
uv sync --group dev --extra cli --extra qc --extra log
uv run maturin develop --manifest-path rust/Cargo.toml
make check
```

See **[CONTRIBUTING.md](CONTRIBUTING.md)** for the full development loop, quality gates, and pull
request guidelines.

## Citation

If you use Tablassert, please cite it as described in [CITATION.cff](CITATION.cff). The approach is
described in:

> Skye Lane Goetz, Alex K. Glen, and Gwênlyn Glusman. “MicrobiomeKG: bridging microbiome research
> and host health through knowledge graphs.” *Frontiers in Systems Biology* 5 (2025).
> [doi:10.3389/fsysb.2025.1544432](https://doi.org/10.3389/fsysb.2025.1544432)

## License

[Apache License 2.0](LICENSE)

## Contributors

- [Skye Lane Goetz](mailto:sgoetz@isbscience.org), Institute for Systems Biology
- [Gwênlyn Glusman](mailto:gglusman@isbscience.org), Institute for Systems Biology
- Jared C. Roach, Institute for Systems Biology
