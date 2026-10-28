# Koza comparison: tutorial example as Tablassert recipe vs Koza transform

Same input table (`tutorial-data.csv`, 4 rows; `tutorial-data-10k.csv` is the same
rows replicated to 10,000), same mapping task: each row becomes one
`associated_with` edge plus its two nodes, with p-value and sample-size
annotations and citation provenance.

- Tablassert side: `tutorial-table.yaml` (the recipe) + `tutorial-graph.yaml`
  (graph wrapper pointing at the fullmap). Recipe lines: 32.
- Koza side: `tutorial-koza.yaml` (reader/writer config) + `tutorial-transform.py`
  (the transform). Combined lines: 29 + 50 = 79.

## Results (wenceslaus, 2026-10-28; tablassert 19.7.0, koza 2.7.0, Python 3.12)

10,000-row input, 3 runs each, wall clock in seconds (medians in bold):

| Tool | Run 1 | Run 2 | Run 3 | Median | Output |
| ----------- | ------- | ------- | ------- | -------- | ---------------------------------------------- |
| Tablassert | 7.21 | 6.85 | 6.94 | 6.94 | KGX NDJSON, resolved CURIEs, categories, taxon-filtered, provenance, RIG |
| Koza | 5.47 | 5.41 | 5.48 | 5.47 | KGX TSV, raw cell text as node ids, no resolution |

`timing.json` holds the raw measurements. For the 10k timing runs both configs
were pointed at `tutorial-data-10k.csv` (the `local:`/`files:` entries), otherwise
unmodified.

## What the comparison shows

- Comparable wall time at this scale, but Tablassert is doing strictly more work:
  every node is resolved against the 24 GB fullmap (taxon-filtered, category-ranked)
  while Koza emits the raw strings as identifiers.
- The Koza equivalent costs about 2.5x the lines because the mapping logic lives in
  Python code, not configuration.
- Koza has no entity resolution stage: getting from "TP53" and "lung cancer" to
  CURIEs requires either pre-mapped inputs, hand-built maps, or calling an external
  resolver per row -- none of which Koza provides.
- Edge properties (knowledge source, publications) are hardcoded in the transform
  Python; in Tablassert they are declared provenance fields validated with the recipe.
