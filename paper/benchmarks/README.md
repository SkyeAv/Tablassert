# Benchmarks

Benchmarks backing the claims in the manuscript. Each subdirectory is self-contained:
scripts, inputs, and recorded results with the environment they were measured in.

- `name_resolution/`: Tablassert's embedded fullmap resolution versus the hosted
  Translator flow (`name-resolution-sri` `/lookup` plus batched NodeNormalization
  `get_normalized_nodes`) on real ingest vocabulary, with per-term canonical
  records and three-level coverage on both sides.
- `koza_comparison/`: Tablassert local resolution, Koza mapping only, and Koza
  with hosted NameResolution plus NodeNormalization over the same table. The
  executable harness records cold and warm runtime, output counts, resolution
  coverage, and the policies that make the resulting graphs non-equivalent.

All runs were executed on the `wenceslaus` build host (80 cores, 187 GB RAM,
Ubuntu Linux); per-run environment details (versions, dates, repeat counts) are
recorded inside each `results.json`. Numbers in the manuscript come from these
files and nowhere else. Re-run instructions are in each subdirectory's README.
