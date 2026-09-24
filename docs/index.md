# Tablassert

Tablassert turns biomedical tables (Excel, CSV, TSV) into NCATS Translator-compliant KGX knowledge
graphs, declaratively, with entity resolution built in and optional quality control.

**Installing?** See [Installation](installation.md). **First build?** Follow the
[Tutorial](tutorial.md). **Automating?** Use the [CLI](cli.md) or the autonomous
[Agent](agent.md).

## Documentation Map

### Learn

Start here if this is your first time with Tablassert.

- **[Tutorial](tutorial.md)**: build your first knowledge graph from a CSV, step by step
- **[Use Case Gallery](examples.md)**: real-world configuration patterns to copy and adapt

### How-to Guides

Task-driven guides for getting things done.

- **[CLI Reference](cli.md)**: every command and flag: `build-kg`, `build-fullmap`, `validate`,
  `validate-kgx`, `quick-map`, `agent`, `distill-export`, `distill-weigh`
- **[Fullmap](fullmap.md)**: obtain and build the entity-resolution database
- **[Agent](agent.md)**: run the autonomous PMC-to-graph pipeline, with checkpoints and tooling

### Reference

Look up exact fields, flags, and function signatures.

- **[Graph Configuration](configuration/graph.md)**: graph-level schema, the required `rig:` section, and stable edge ids
- **[Table Configuration](configuration/table.md)**: sources, statements, entity-resolution rules, provenance, annotations
- **[Advanced Example](configuration/advanced-example.md)**: a fully-annotated real-world configuration
- **[Entity Resolution API](api/fullmap.md)**: `resolve()` and `quick_map()`
- **[Batch Resolution API](api/lib.md)**: `resolve_many()` for scripts and notebooks
- **[Quality Control API](api/qc.md)**: the four-stage `fullmap_audit()` pipeline
- **[Utilities API](api/utils.md)**: hashing, store keys, and deterministic edge UUIDs

### Contribute

- **[Development](development.md)**: dev environment setup, the daily loop, and the docs gate
- **[Changelog](changelog.md)**: release history

## Authors

- **[Skye Lane Goetz](mailto:sgoetz@isbscience.org)**: Institute for Systems Biology
- **[Gwênlyn Glusman](mailto:gglusman@isbscience.org)**: Institute for Systems Biology
- **Jared C. Roach**: Institute for Systems Biology

## License

Apache License 2.0 (see [LICENSE](https://github.com/SkyeAv/Tablassert/blob/main/LICENSE)).
