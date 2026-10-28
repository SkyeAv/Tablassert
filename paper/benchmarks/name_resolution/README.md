# Name resolution: local fullmap vs hosted SRI service

Compares Tablassert's embedded fullmap database (batched local lookups through
`tablassert.fullmap.resolve_batch`, the exact path a build runs) against the hosted
Translator SRI name resolution service (<https://name-resolution-sri.renci.org>) on
the same 113 terms. Terms are drawn from the DAKP production audit's most frequent
free-text subject/object pairs plus common ingest vocabulary; the exact list ships
in `terms.txt`.

Hosted flows measured:

- Single-hop: `GET /lookup?string=<term>` -- what a build would call per term to get
  candidate CURIEs.
- Multi-hop identifier-record workflow: `GET /lookup` for the string, then
  `POST /synonyms` on the top CURIE to fetch the canonical identifier's preferred
  name and categories. This is the two-request flow needed to obtain what one
  fullmap fetch returns per term (canonical CURIE, preferred name, category, taxon,
  source).

## Results (wenceslaus, 2026-10-28; tablassert 19.7.0; fullmap source 2026jul22, 24 GB)

See `results.json` for full detail; medians of 3 local repeats, hosted at
concurrency 4 with retry/backoff:

| Metric | Local fullmap | Hosted (single-hop) | Hosted (multi-hop) |
| ------------------------------------- | -------------------------- | ----------------------------- | ----------------------------- |
| Throughput | 171 unique terms/s | 113 terms in 19.9 s (~5.7/s at concurrency 4) | 113 terms in 17.4 s (~6.5/s at concurrency 4) |
| Median latency per term | ~5 ms | 0.553 s | 0.576 s |
| Requests per term for full record | 0 (one batched fetch) | 2 | 2 |
| Coverage (terms with >= 1 hit) | 136 resolved rows from 113 terms (ties multi-resolve; misses dropped) | 113/113 | 113/113 (108 canonical records) |
| Offline reproducibility | yes | no | no |

Notes:

- `resolve_batch` deduplicates terms before the redb fetch, so throughput is
  reported over unique terms; replicating rows (1130-row run, `terms_10x.txt`)
  added no measurable lookup cost.
- The hosted service occasionally rate-limits bursts; the harness retries with
  backoff, and hosted runs use modest concurrency (<= 6).
- Qualitative difference (not timed): the hosted API returns ranked hits with no
  taxon constraint, category policy, prefix/regex exclusion, or per-column context;
  the fullmap applies all of these per node column during the same batched fetch.
  Worked example: the hosted top hit for "TP53" is the pig ortholog
  (NCBITaxon:9823); a Tablassert subject column with `taxon: 9606` filters that
  hit by construction.

## Re-running

```bash
python bench_name_res.py --terms terms.txt --fullmap /path/to/fullmap \
    --python /path/to/python-with-tablassert --out results.json --repeats 3
```

Requires a tablassert installation importable by `--python` and a built (or
downloaded) fullmap directory.
