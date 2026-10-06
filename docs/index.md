# Tablassert

Tablassert turns biomedical tables (Excel, CSV, TSV) into NCATS Translator-compliant KGX knowledge
graphs, declaratively, with entity resolution built in and optional quality control.

**Installing?** See [Installation](installation.md). **First build?** Follow the
[Tutorial](tutorial.md). **Automating?** Use the [CLI](cli.md) or the autonomous
[Agent](agent.md).

## Who this is for

Translator ingest authors and knowledge-graph data engineers holding tabular biomedical sources:
association tables, assay results, curated gene-disease lists. Tablassert turns those rows into
KGX nodes and edges from a declarative YAML mapping, resolving free text to standard CURIEs through
an embedded database built from RENCI BABEL exports, so a build needs no hosted resolution service
and no per-source Python transform. It also emits the Resource Ingest Guide (RIG) metadata that
[`NCATSTranslator/translator-ingests`](https://github.com/NCATSTranslator/translator-ingests)
submissions require.

**Questions or bugs?** Open an issue at
[github.com/SkyeAv/Tablassert/issues](https://github.com/SkyeAv/Tablassert/issues); see
[CONTRIBUTING.md](https://github.com/SkyeAv/Tablassert/blob/main/CONTRIBUTING.md) to contribute and the
[Code of Conduct](https://github.com/SkyeAv/Tablassert/blob/main/CODE_OF_CONDUCT.md) for community
expectations.

## Documentation Sections

**Start**

- **[Installation](installation.md)**: install methods, extras, and development setup
- **[Tutorial](tutorial.md)**: build your first knowledge graph from a CSV, step by step
- **[Use Case Gallery](examples.md)**: real-world configuration patterns to copy and adapt

**How-to guides**

- **[CLI Reference](cli.md)**: every command and flag: `build-kg`, `build-fullmap`, `validate`,
  `validate-kgx`, `quick-map`, `agent`, `distill-export`, `distill-weigh`
- **[Fullmap](fullmap.md)**: obtain and build the entity-resolution database
- **[Agent](agent.md)**: run the autonomous PMC-to-graph pipeline, with checkpoints and tooling
  (experimental: API may change)

**Reference**

- **[Configuration](configuration/graph.md)**: graph-level schema, the required `rig:` section, and stable edge ids
- **[Table Configuration](configuration/table.md)**: sources, statements, entity-resolution rules, provenance, annotations
- **[Advanced Example](configuration/advanced-example.md)**: a fully-annotated real-world configuration
- **[API Reference](api/fullmap.md)**: `resolve()` and `quick_map()`
- **[Batch Resolution](api/lib.md)**: `resolve_many()` for scripts and notebooks
- **[Quality Control](api/qc.md)**: the four-stage `fullmap_audit()` pipeline
- **[Utilities](api/utils.md)**: hashing, store keys, and deterministic edge UUIDs

**Contribute**

- **[Development](development.md)**: dev environment setup, the daily loop, and the docs gate
- **[Changelog](changelog.md)**: release history

## Authors

- **[Skye Lane Goetz](mailto:sgoetz@isbscience.org)**: Institute for Systems Biology
- **[Gwênlyn Glusman](mailto:gglusman@isbscience.org)**: Institute for Systems Biology
- **[Jared C. Roach](mailto:jroach@isbscience.org)**: Institute for Systems Biology

## License

Apache License 2.0 (see [LICENSE](https://github.com/SkyeAv/Tablassert/blob/main/LICENSE)).
