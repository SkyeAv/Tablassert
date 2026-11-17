# Name resolution: local fullmap vs hosted SRI NameResolution + NodeNormalization

Compares Tablassert's embedded fullmap database (batched local lookups through the
installed API: `level_one`/`level_two` normalization + `tablassert.fullmap.resolve_batch`)
against the hosted Translator flow on the same deterministic unique term list from
`terms.txt` (113 hand-selected input entries, 104 unique terms, deduplicated and
sorted). This is a convenience sample from ingest vocabulary, not a random or
representative sample; coverage and timing must not be generalized to accuracy or
to all biomedical terms:

1. `GET https://name-resolution-sri.renci.org/lookup?string=<term>&limit=5&only_taxa=NCBITaxon:9606`
   per term for candidate CURIEs (concurrency <= 4, request budget <= 300 calls).
   The matched run passes the pipe-separated `only_taxa=NCBITaxon:9606` filter on
   every lookup; identical handling of taxon-free candidates is not established.
2. `POST https://nodenormalization-sri.renci.org/get_normalized_nodes` batched
   (100 CURIEs per request) for canonical id, preferred name, and categories.
   The response is keyed by the requested (input) CURIEs, not canonical ids, and
   the harness groups results by requested candidate, preserving the /lookup
   ranking. The `/synonyms` endpoint is NOT called; canonical records come from
   NodeNormalization, the same service a downstream consumer would use.

## What each run records

- Per-term canonical record (id, preferred name, category, taxon) for BOTH sides;
  local rows keep their input term via the `input_term` column, and ties appear as
  multiple rows per input term.
- Coverage at three distinct levels, never collapsed into one number:
  `http_ok` (HTTP 200), `nonempty_candidates` (200 AND >= 1 candidate), and
  `complete_record` (TOP-RANKED candidate has canonical id + preferred name + category).
  Failed requests become error records; there is no fake coverage.
- `api_spec` block: endpoints, methods, request shapes, batch size, retry and
  concurrency parameters as actually called.

## Documented mismatches (explicit, not silently papered over)

- Taxon: `/lookup` DOES support server-side taxon filtering via the pipe-separated
  `only_taxa` query parameter; the matched run passes `only_taxa=NCBITaxon:9606`.
  Fullmap explicitly retains taxon-free rows; equivalence of the hosted service's
  unknown-taxon handling is not established. Separately, the harness records
  `human_pref`, a post-hoc preference for candidates whose NodeNormalization `taxa`
  include NCBITaxon:9606 (taxa-free entries stay eligible), clearly labeled
  post-filtering and kept distinct from `canonical` (the TOP-RANKED candidate's
  record, never the first complete record in arbitrary order).
- Category policy: the hosted API supports Biolink-type filters, but those are not
  equivalent to fullmap's per-column prioritize/avoid ranking and context rules.
  This run enables no category filter or priority on either side.
- No column context: the local side runs with `column_context=False`, so neither
  side sees table context in the per-term lookup.

## Cold vs warm, repeats

- Local cold: ONE fresh `local_runner` process per repeat (fresh interpreter,
  empty term cache, fresh redb handles). The timer surrounds only
  `resolve_batch(...).collect()`; interpreter startup, imports, and input parsing
  are excluded. The OS file cache is NOT cleared, so this is not a cold-page-cache
  measurement.
- Local warm: repeats back to back in one process. Both are reported separately
  and never pooled; at least 3 repeats each.
- Hosted: one complete sweep, bounded request budget (default 300 HTTP attempts,
  including retries), concurrency 4, retry with backoff. Logical calls and actual
  HTTP attempts are reported separately; latency includes any retry/backoff. Lookup and batched-normalization times are recorded
  separately and summed for end-to-end time; hosted latency is not a multi-run
  median. Throughput includes both stages.

`results-2026-10-28-v1.json` is the recorded run of the previous harness version
(per-term `/lookup` + `/synonyms`, no NodeNormalization stage, pooled coverage);
it is kept for provenance and is not comparable to the current harness.

## Re-running

```bash
python bench_name_res.py --terms terms.txt --fullmap /path/to/fullmap \
    --python /path/to/python-with-tablassert --out results.json \
    --local-repeats 3 --hosted-concurrency 4 --hosted-max-requests 300 --limit 5
```

Offline tests (mocked network, no services contacted): `pytest test_bench_name_res.py`.
