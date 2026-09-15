# Tablassert

Tablassert turns biomedical tables (Excel, CSV, TSV) into NCATS Translator-compliant KGX knowledge
graphs, declaratively, with entity resolution built in and optional quality control. **Installing?**
See [Installation](installation.md). **First build?** Follow the [Tutorial](tutorial.md). **Automating?**
Use the [CLI](cli.md) or the autonomous [Agent](agent.md).

## Why Tablassert

- **Declarative YAML configuration**: define data transformations without code
- **Entity resolution**: map free text to biological entities (genes, diseases, chemicals) with
  taxonomic filtering and provenance, backed by an embedded redb database
- **Optional quality control**: four-stage audit (exact → fuzzy → abbreviation → SapBERT embeddings) flags
  low-confidence mappings
- **KGX compliance**: emits NCATS Translator-compatible node/edge NDJSON with Biolink categories and
  predicates
- **Performance & reproducibility**: lazy Polars pipelines and a UV-based, deterministic development
  environment

## Quick Start

```bash
pip install "tablassert[cli]"   # or: uv tool install "tablassert[cli]"
tablassert build-kg config.yaml
```

Quality control and runtime-compatible Polars are opt-in extras. See [Installation](installation.md) for
the full install matrix, extras, and development setup.

## Documentation Sections

- **[Installation](installation.md)**: install methods, extras, and development setup
- **[CLI Reference](cli.md)**: complete command-line flag reference
- **[Tutorial](tutorial.md)**: step-by-step example with synthetic data
- **[Use Case Gallery](examples.md)**: real-world configuration patterns
- **[Fullmap](fullmap.md)**: building and querying the entity-resolution database
- **[Configuration](configuration/graph.md)**: graph and table configuration reference
- **[Agent](agent.md)**: the autonomous agent pipeline
- **[API Reference](api/fullmap.md)**: core functions documentation
- **[Development](development.md)**: dev environment setup and contributor workflow
- **[Changelog](changelog.md)**: release history

## Authors

- **[Skye Lane Goetz](mailto:sgoetz@isbscience.org)**: Institute for Systems Biology
- **[Gwênlyn Glusman](mailto:gglusman@isbscience.org)**: Institute for Systems Biology
- **Jared C. Roach**: Institute for Systems Biology

## License

See repository for license information.
