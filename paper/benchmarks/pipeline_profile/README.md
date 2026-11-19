# Pipeline Profiling and Rust Hot-Path Port (#267)

## Environment and Workload

- **Build**: release PyO3 extension (`make build`, `tablassert-22.3.3-cp313-cp313-macosx_11_0_arm64`)
- **Platform**: macOS arm64, Python 3.13.12
- **Workload**: 50,000-row gene-disease TSV with `p_value` + `study_size` coercion, 500-entity real `fullmap.redb`, and `uuid_fields: [subject, predicate, object]` with `uuid_on_collision: merge`

## Top Cost Centers (`cProfile`, `build_graph_pipeline` warm process)

Total wall time: **9.897s** (50,000 rows).

| Phase / Function | Time | Share | Notes |
| --- | ---: | ---: | --- |
| Stage 1 + Stage 2 (`multiprocessing.Pool` spawn + worker imports) | 7.92s | 80.0% | Fixed process-spawn overhead for 10 workers |
| `tablassert.rs.dedup_ndjson` | 0.980s | 9.9% | Already in Rust (`rust/src/ndjson.rs`) |
| `lib.compile_subgraph` (Polars + fullmap + formatting) | 0.783s | 7.9% | Includes per-row `lib.format_numeric` (`0.066s` at 50k) |
| `rig.rig_edge_type_info` / `runcache.canonical` / `biolink.class_fields` | 0.042s | 0.4% | Metadata and cache keying |

## Ported Hot Path: `format_scientific_4e`

`lib.format_numeric()` formatted `p_value` columns via a per-row Python comprehension (`[None if v is None else f"{v:.4e}" for v in df[c].to_list()]`).
Moving this formatting loop into `tablassert.rs.format_scientific_4e` (single pre-sized buffer per element + `rayon` parallelization via `py.detach` for batches >= 10,000 rows) preserves byte-identical Python `{:.4e}` output (0 diffs across 10,014 edge and random values).

### Before / After Benchmark (100,000 `p_value` floats, 9 runs)

| Implementation | Median | Stdev | Speedup |
| --- | ---: | ---: | ---: |
| Python `[f"{v:.4e}" ...]` | 22.80 ms | 0.41 ms | 1.00x |
| Rust `rs.format_scientific_4e` | 5.39 ms | 0.38 ms | **4.23x** |
