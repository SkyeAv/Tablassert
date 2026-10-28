# Benchmarks

Benchmarks backing the claims in the manuscript. Each subdirectory is self-contained:
scripts, inputs, and recorded results with the environment they were measured in.

- `name_resolution/`: Tablassert's embedded fullmap resolution versus the hosted SRI
  name resolution service, on real ingest vocabulary.
- `koza_comparison/`: the Tablassert tutorial recipe versus an equivalent Koza
  transform of the same table -- lines of code, runtime, and what each tool does and
  does not do with the data.

All runs were executed on the `wenceslaus` build host (80 cores, 187 GB RAM,
Ubuntu Linux); per-run environment details (versions, dates, repeat counts) are
recorded inside each `results.json`. Numbers in the manuscript come from these
files and nowhere else. Re-run instructions are in each subdirectory's README.
