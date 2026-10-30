# Benchmarks

Benchmarks backing the claims in the manuscript. Each subdirectory is self-contained:
scripts, inputs, and recorded results with the environment they were measured in.

- `name_resolution/`: Tablassert's embedded fullmap resolution versus the hosted
  Translator flow (`name-resolution-sri` `/lookup` plus batched NodeNormalization
  `get_normalized_nodes`) on real ingest vocabulary, with per-term canonical
  records and three-level coverage on both sides.
- `koza_comparison/`: line and non-whitespace-character counts for the
  Tablassert tutorial recipe and a mapping-only Koza transform. No runtime is
  benchmarked.

Name-resolution runs were executed on the `wenceslaus` build host (80 hardware
threads, 187 GiB RAM, Ubuntu Linux); environment details are recorded in
`name_resolution/results.json`. Numbers in the manuscript come from these
files and nowhere else. Re-run instructions are in each subdirectory's README.
