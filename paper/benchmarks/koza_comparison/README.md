# Koza comparison: configuration size and verbosity

This directory compares two mappings for the same four-row tutorial CSV
(`tutorial-data.csv`). It does not benchmark runtime or additional tables.

- Tablassert: `tutorial-table.yaml` maps columns to statements, provenance, and
  annotations. `tutorial-graph.yaml` supplies the fullmap and Resource Ingest Guide
  (RIG) configuration.
- Koza: `tutorial-koza.yaml` configures input/output. `tutorial-transform.py`
  calls `tablassert.lib.resolve_many` once per whole column, with the recipe's
  human taxon default (`9606`), Gene/Disease priorities, and column context enabled.
  It uses the same fullmap path and retains all tied CURIEs, deduplicating repeated
  aliases per input label before constructing the Cartesian product of matches.

The Koza example intentionally depends on Tablassert for identical resolution;
this is not an independent resolver comparison or a benchmark of Koza alone.

## Counting

```bash
python count_loc.py --out loc.json
```

`count_loc.py` excludes blank lines, comment-only lines, and Python docstrings.
Verbosity is non-whitespace characters on the counted lines; indentation style
does not affect it. Tablassert's table and graph wrapper are shown separately.

| Component | Lines | Non-whitespace characters |
| --- | ---: | ---: |
| Tablassert table recipe | 32 | 385 |
| Tablassert graph wrapper | 24 | 767 |
| Tablassert total | 56 | 1,152 |
| Koza YAML | 25 | 392 |
| Koza Python | 47 | 1,589 |
| Koza total | 72 | 1,981 |

Shared installed library implementations are not counted on either side. Counts
illustrate where logic lives in this example, not general complexity or capability.

## Resolution equivalence and output differences

Verified on wenceslaus on 2026-10-30 using Tablassert 19.7.0, Koza 2.7.0, and the
`2026jul22` fullmap. `output-comparison.json` records exact subject/object pair
multiset equality and equality of unique node IDs, preferred names, and categories.
Both produce five edges and nine unique nodes from four input rows: pancreatic
cancer retains two tied CURIEs. The full graph records are not identical:

- Tablassert emits nine deduplicated KGX NDJSON nodes and a populated RIG with
  source, ingest, provenance, artifact locations, emitted fields, and record counts.
- Tablassert edges include category, knowledge level, agent type, structured
  knowledge sources with source URLs, original labels, supporting studies with
  study size and row references, and statistical-significance qualifiers.
- This Koza example emits KGX TSV with ten node rows (nine unique nodes), flat
  knowledge-source/publication fields, p-value, and sample size. It does not
  implement a RIG or the additional graph-level metadata above. Koza could emit
  richer records with further code/configuration; their absence is scoped to this
  example, not a limitation of the framework.

Reproduce on the build host (both environments need Tablassert and its fullmap;
the Koza environment additionally needs Koza):

```bash
/users/sgoetz/Code/dakp/.venv/bin/tablassert build-kg tutorial-graph.yaml
./koza-venv/bin/koza transform tutorial-koza.yaml -o koza-output
python compare_outputs.py --tablassert tutorial-output --koza koza-output \
    --out output-comparison.json
```

Offline tests: `pytest test_bench_koza.py`.
