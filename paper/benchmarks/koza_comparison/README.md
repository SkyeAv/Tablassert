# Koza comparison: configuration size and verbosity

This directory compares the size of two mappings for the same four-row tutorial
CSV (`tutorial-data.csv`). It does not benchmark runtime.

- Tablassert: `tutorial-table.yaml` maps columns to statements, provenance, and
  annotations. `tutorial-graph.yaml` is the separate graph wrapper with fullmap
  and RIG metadata.
- Koza: `tutorial-koza.yaml` configures input and output, and
  `tutorial-transform.py` implements row-to-node and row-to-edge mapping.

The Koza example emits raw labels as node identifiers. It does not implement entity
resolution, whereas the Tablassert recipe uses fullmap resolution. The comparison
therefore shows where mapping logic is expressed and how much text each example
requires; it is not an equivalent end-to-end workflow or general complexity metric.

## Counting

```bash
python count_loc.py --out loc.json
```

`count_loc.py` excludes blank lines, comment-only lines, and Python docstrings.
Verbosity is the number of non-whitespace characters on the counted lines, so
indentation style does not affect it. The Tablassert table recipe and graph wrapper
are reported separately and as a total.

Offline tests: `pytest test_bench_koza.py`.
