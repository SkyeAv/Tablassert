# Evidence for manuscript additions

Checked 2026-10-29. These observations support the normalization, hardware, QC,
and research-impact descriptions; they are not additional benchmark trials.

## Normalization and parallelism

Source inspected at main commit `9a72ad4`:

- `src/tablassert/nlp.py`, `level_one` and `level_two`: whitespace/quote cleaning,
  Unicode lowercase, Porter2 stemming of ASCII alphabetic tokens, token deduplication,
  byte-wise sorting and joining; level two removes non-word characters (`\W+`).
- `rust/src/nlp.rs`, `normalize_terms`: `py.detach` releases the Python GIL and
  `into_par_iter` processes terms using Rayon.
- `rust/src/fullmap.rs`: GIL-released term lookups and parallel shard reads.
- `pyproject.toml`: Python >=3.11 supported. These workers do not require a
  free-threaded Python interpreter.

No controlled Python-only-versus-current benchmark was run here. The manuscript
identifies removed bottlenecks, rather than asserting an unmeasured speedup factor.

## Benchmark host

`lscpu`, `free`, `/etc/os-release`, `/proc/mdstat`, and `/sys/block` on wenceslaus:

- Two Intel Xeon Gold 6230 processors, 20 cores/socket, two threads/core:
  40 physical cores, 80 hardware threads.
- 187 GiB RAM reported by `free -g`.
- Ubuntu 24.04.4 LTS.
- `/local_raid1` on `/dev/md127`, RAID1 over two non-rotating devices (SSDs).

This is the host used for the recorded 2026-10-28 benchmark runs. Hardware was
inspected the following day, not captured contemporaneously by the harness.

## DAKP mapping-QC rejection

Source log on wenceslaus:

```text
/local_raid1/sgoetz/CODE/DAKP/tmp/airflow/logs/dag_id=dakp_pipeline/run_id=manual__2026-10-22T21:34:56.310983+00:00/task_id=run_tablassert/attempt=1.log
```

Joined stderr fragments from the QC rejection:

```text
QC rejected 'HMDB:HMDB0000607': original='factor ii' preferred='Cyanocobalamin' col=subject fuzz=37.5 config=faers_applied_to_treat.yaml hash=01e7437cc5b2706c.release.qc sapbert=0.4442202150821686
```

The BABEL `2026jul22` file
`/local_raid1/sgoetz/DBSTORE/FULLMAP/downloads/synonyms/drugchemicalconflated.txt.gz`
contains `HMDB:HMDB0000607`, preferred name `Cyanocobalamin`, and the synonym
`Factor II`. The hosted `/synonyms` endpoint independently returns that alias:

```bash
curl -s https://name-resolution-sri.renci.org/synonyms \
  -H 'Content-Type: application/json' \
  -d '{"preferred_curies":["HMDB:HMDB0000607"]}'
```

UniProt's reviewed human entry P00734 identifies prothrombin (gene F2), with
alternative name "Coagulation factor II":
<https://www.uniprot.org/uniprotkb/P00734/entry>

This is an ambiguous synonym resolving to a vitamin rather than the coagulation
protein in a drug-treatment context. It demonstrates one useful rejection, not
an estimate of QC precision or recall. Other rejected aliases may be valid;
automatic rejection alone does not establish a mapping error.

The earlier 1,661,094-row figure came from a separate DAKP off-label classification
audit, not Tablassert mapping QC, and has been removed from the manuscript.

## Published graph sizes

Dataset cards accessed 2026-10-29:

- Multiomics 3.1.3: 222,994 nodes, 18,206,965 edges.
  <https://huggingface.co/datasets/SkyeAv/multiomics-kp>
- DAKP 1.26.0: 16,257 nodes, 135,263 edges.
  <https://huggingface.co/datasets/SkyeAv/drug-approvals-kp>

Multiomics publication count: streamed the published edges file and counted distinct
`PMCID:PMC...` values in each record's `publications` list: 2,136 publications over
18,206,965 edges. The file's SHA-256 matches the dataset card:
`148a73057d8f9ffe72d2bc68d441eb514394e6a5dceb923cb8f36215f829f45c`.
Local path:
`/local_raid1/sgoetz/MOKG-REBUILD-19.7.0/out/published-3.1.3/MULTIOMICS_KP_3.1.3.edges.ndjson`.

Counts describe separate versioned graphs, not their deduplicated union.
