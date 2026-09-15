# Fullmap

Build a fullmap once and every `resolve()` / `resolve_many()` call maps free text to the right
biological CURIE; the completeness and freshness of this database directly sets how many of your
entities resolve correctly and how trustworthy the resulting graph is.

Fullmap is Tablassert's embedded entity-resolution database: a small set of [redb](https://github.com/cberner/redb)
files containing biological synonyms, CURIEs, Biolink categories, taxon IDs, and source provenance,
built from NCATS Translator BABEL export files. It comprises a primary file holding the dimensions,
CURIEs, and schema metadata, plus hash-sharded RECORDS files (`fullmap.s0.redb` … `fullmap.s15.redb`
by default) holding the term→postings index.

Fullmap is built entirely in-process by Tablassert's own Rust extension: no external tool or install step required by default (this is an in-process redb shard scheme, not the older external DuckDB shards). When the optional `[aria2]` extra is installed (`pip install "tablassert[aria2]"`; Linux/Windows wheels only), both download stages — the prebuilt `fullmap.tar.zst` archive and, for a from-scratch build, the BABEL class and synonym files — use the bundled aria2c binary automatically; without it, Tablassert's Python downloader is used.

## Build Command

```bash
# Default: download the prebuilt fullmap.tar.zst for this version and extract it (fast)
tablassert build-fullmap

# Force a from-scratch build from BABEL outputs (skips the prebuilt download)
tablassert build-fullmap --force

# Optional: with `pip install "tablassert[aria2]"`, the same command downloads through the bundled aria2c automatically (the multi-GB prebuilt is the ideal aria2 use case)
tablassert build-fullmap
```

The taxon allowlist is always on; there is no flag. Every database `build-fullmap` installs uses the
checked-in `src/tablassert/data/experimental_taxa.yaml` list and filters non-`OrganismTaxon` synonym
rows with valid taxon metadata before interning. Rows with no valid taxon metadata and every
`OrganismTaxon` row are retained; a row with multiple taxa is kept when any taxon is allowlisted. The
first taxon continues to be stored for existing hydration and lookup behavior. The built database
records its allowlist identity in `META.taxon_allowlist`, and that identity gates both reuse paths:

- A prebuilt archive whose recorded identity does not match the current allowlist fails validation
  during extraction, so it is never installed; the command falls back to a filtered source build.
  Note the operational consequence: until a filtered archive is published for a Tablassert version,
  the default invocation downloads that version's unfiltered archive once, fails the identity check,
  and then performs the full BABEL download + build.
- A database already present at `--output` is reused only when its recorded identity matches;
  otherwise it is rebuilt.

See the [CLI Reference → build-fullmap](cli.md#build-fullmap) for the complete flag table (output path,
cache directory, BABEL snapshot version, and the `--force` / `-f` rebuild flag), their defaults, and
more examples.

By default, `build-fullmap` first downloads a **prebuilt** database published for this Tablassert
version, a `fullmap.tar.zst` under `.../fullmap/<tablassert-version>/` (the version directory is the
installed package version, never hardcoded), verified against a co-published `sha256sum.txt` and
extracted beside `--output` entirely in the Rust extension: it streams the archive through zstd → tar
(the multi-GB decompressed tar is never materialized on disk) with the GIL released, extracts into a
temp directory on the output's filesystem, and, before renaming anything into place, validates the
bundle against the same contract a `--force` build must satisfy: the `meta` schema tag is exactly
`tablassert.fullmap.v5`, a `build_id` is recorded, the shard files are exactly the set the primary
advertises (no gaps, no extras), every shard's `build_id` equals the primary's, and the primary's
`META.taxon_allowlist` identity equals the one the built-in allowlist would record. Only a bundle that
passes is atomically renamed into place (primary → `--output`, shards beside it); any failure raises
and the command falls back to the from-scratch build below. If no prebuilt is published for this
version it falls back the same way; `--force` / `-f` skips the prebuilt attempt and always builds.
When the optional `[aria2]` extra is installed, the bundled aria2c from that extra accelerates
**either** download automatically — the multi-GB prebuilt archive is the ideal aria2 use case — and
the Python downloader is used otherwise.

Two facts matter most when planning a build:

- The BABEL **version** flag selects a RENCI BABEL snapshot date (default `2026jul22`), *not*
  Tablassert's package version. Bumping it fetches a different snapshot and requires rebuilding; the
  value used is recorded in the primary's `meta` table (`source_version`).
- The build **parallelizes automatically** across all available CPU threads: the Rust build caps
  workers at `min(available_CPUs, MemAvailable_GB / 2)` on Linux (reading `MemAvailable:` from
  `/proc/meminfo`, each worker budgeting ~2 GB of local buffers) and falls back to ~90% of CPUs
  elsewhere, so a large build stays within a fixed memory budget. There is no flag to tune.

### Data Pipeline

The build is a parallel, **memory-bounded** pipeline executed by the Rust extension:

1. **Download**: fetch BABEL class and synonym files from RENCI into the cache (resumable, reused). This uses Tablassert's Python downloader, or the bundled `aria2c` binary automatically when the `[aria2]` extra is installed, preserving aria2 resume control files across dropped downloads and failing loud on a non-zero aria2c exit instead of being silently rescued by the Python downloader.
2. **Equivalents index**: parse class files into sorted on-disk runs, then k-way merge them into a
   memory-mapped index mapping each primary CURIE to its equivalents.
3. **Synonym pass**: a producer/consumer pool streams byte-bounded line-chunks; workers dedup CURIEs,
   accumulate normalized-term → (CURIE, source) postings, and spill per-shard runs to disk so peak RAM
   stays flat regardless of input size.
4. **Write**: one redb transaction writes the primary tables, then `shard_count` independent k-way
   merges write the shard `records` files in parallel (one thread per shard).

??? note "Pipeline details"
    Heavy intermediate state spills to a temporary directory (`<output>.spill.d`, removed on success)
    rather than RAM, so a full BABEL build (hundreds of millions of CURIEs) completes within a fixed
    memory budget; the [mimalloc](https://github.com/microsoft/mimalloc) allocator keeps heavy
    multi-threaded allocation from bloating resident memory.

    - **Download**: files come from `https://stars.renci.org/var/babel_outputs` via resumable,
      range-request downloads; cached files are reused. When the `[aria2]` extra is installed, only
      this stage of the from-scratch pipeline uses the bundled `aria2c` binary from that extra (the
      prebuilt archive download above uses it too), with aria2's segmented HTTP downloads and
      retry/resume control files while suppressing aria2's own progress UI so Tablassert's progress
      bar stays clean.
      The progress detail remains file-level (`aria2c downloading`) rather than byte-level in that mode.
    - **Equivalents index**: class files parse in parallel into sorted on-disk runs, k-way merged into a
      single memory-mapped CURIE→equivalents index; only a compact `(hash, offset)` index lives in RAM,
      the string data is mmap'd.
    - **Synonym pass**: uses **intra-file parallelism**: a small pool of producer threads
      decompresses/reads the synonym files and pushes byte-bounded line-chunks through a bounded channel,
      and every worker draws from one shared queue, so the few very large files
      (protein/smallmolecule/gene/drugchemicalconflated) are processed by **all** workers, not one thread
      each. Per row, the build collects dimension sets (CURIE prefixes, Biolink categories, sources),
      assigns compact integer CURIE IDs via a hash-keyed dedup map (`xxh3_128(curie) → id`), and
      accumulates normalized-term → (CURIE, source) postings. Each worker drains its per-CURIE rows and
      term postings to bounded on-disk spill runs once its buffer fills, the term postings partitioned
      per-shard at spill time (each `run_s{shard}_{id}.bin` holds only terms with
      `xxh64(term) & (shards-1)` matching that shard), which keeps peak RAM flat regardless of input size
      and lets the write phase merge each shard independently. Dead terms (purely numeric, or generic
      labels like `none`/`nan`/`null`) are skipped, since they can never be queried.
    - **Write**: a single redb write transaction in the primary file emits the dimension tables
      (`prefixes`, `categories`, `sources`), the `curies` table (streamed from its spill runs), and the
      `meta` schema tag (recording the shard count). The `records` table is then written **in parallel
      across the shard files**: building on the per-shard spill partitioning, the write phase runs
      `shard_count` independent k-way merges, one thread per shard, each merging only its own shard's
      runs and inserting the merged term groups inline into that shard's redb file (one database per
      shard, since redb allows a single writer per file) in hash-sorted batches for near-sequential B-tree
      appends (each batch is appended through redb's end-of-table cursor API, the faster ascending
      bulk-load path; the engine's ascending-key page optimization also lets a key-order-loaded shard
      occupy about half as many pages, so current builds write ~50% smaller shard files at the same
      schema). Because every term's postings already live in its own shard's runs, each merge groups a
      term completely with no cross-shard coordination; as the merge+insert is the bottleneck,
      `shard_count` writers deliver ~N× single-threaded write throughput.

### Build Tunables (environment)

Advanced tuning for the build's memory/speed trade-offs. Defaults are safe for a typical large build;
override only when targeting an unusual machine.

| Variable | Default | Description |
|----------|---------|-------------|
| `TABLASSERT_FULLMAP_EXCLUDE_PREFIXES` | *(empty)* | Comma-separated CURIE prefixes to drop at build time (e.g. `Publication`). Excluding prefixes you never resolve dramatically cuts build time, peak memory, and database size. InChIKey terms are retained unless excluded explicitly. |
| `TABLASSERT_FULLMAP_CHUNK_BYTES` | `8388608` (8 MiB) | Byte budget per producer→worker line-chunk. Bounded by bytes (not line count) so chunk memory is fixed even for large synonym records. |
| `TABLASSERT_FULLMAP_PRODUCERS` | `clamp(workers/4, 4, #files)` | Number of producer (decompressor) threads. Decompression far outpaces parallel processing, so a handful keeps all workers fed. |
| `TABLASSERT_FULLMAP_LOCAL_SPILL_ENTRIES` | `1000000` | Per-worker term-posting buffer size before spilling a sorted run to disk. Lower → less RAM, more run files. |
| `TABLASSERT_FULLMAP_CURIE_SPILL_ENTRIES` | `250000` | Per-worker CURIE-row buffer size before spilling to disk. Lower → less RAM, more run files. |
| `TABLASSERT_FULLMAP_EQUIV_SPILL_ENTRIES` | `2000000` | Per-thread equivalents buffer size before spilling during the equivalents-index build. |
| `TABLASSERT_FULLMAP_INSERT_BATCH` | `2000000` | Records buffered per hash-sorted batch during the redb write. Larger → faster writes, modestly more RAM. |
| `TABLASSERT_FULLMAP_REDB_CACHE_BYTES` | `2147483648` (2 GiB) | redb write-cache size. |
| `TABLASSERT_FULLMAP_SPILL_DIR` | `<output>.spill.d` | Directory for intermediate spill runs (removed on success). |

## Output Artifact

A primary redb file (default `./fullmap/data/fullmap.redb`) plus its sibling RECORDS shard files
(`fullmap.s0.redb` … `fullmap.s15.redb`, one per shard, named after the output
file stem in the same directory). The shard count is fixed at 16 (the read path
still honors the count recorded in an existing database's `meta` table). Together
they hold six tables (see `rust/src/fullmap.rs`):

| Table | Description |
|-------|-------------|
| `records` | Normalized term (xxhash `u64`) → serialized list of resolution postings (CURIE id, source id). Hash-partitioned across the shard files (`fullmap.s0..s<N-1>.redb`) by `xxh64(term) & (shards-1)`; a term lives in exactly one shard. |
| `prefixes` | Compact `u16` id → CURIE prefix string (primary file) |
| `categories` | Compact `u16` id → Biolink category string (primary file) |
| `sources` | Compact `u8` id → source metadata (name/version) (primary file) |
| `curies` | Compact `u32` id → CURIE record (CURIE, preferred name, category, taxon, source) (primary file) |
| `meta` | Schema version tag (`tablassert.fullmap.v5`), the shard count (`shards`), the BABEL `source_version` used to build the file, and the allowlist identity (`taxon_allowlist`) recorded for every `build-fullmap` output; absent only in databases built unfiltered through the lower-level surfaces (the Rust API, or `build_fullmap_pipeline`'s `taxon_allowlist=None` default) (primary file) |

The shard files must remain alongside the primary file: lookups discover them as siblings of the
resolved primary path.

Lookups (`lookup_fullmap_terms`) check the primary's `meta` schema tag before reading `records`, read the
`shards` count to open exactly that many shard files, and fan the query terms out across the shards in
parallel (releasing the GIL, one reader per non-empty shard, re-merged into input order); a mismatched or
missing tag raises rather than silently reading incompatible data. Databases built under the older
`v1`/`v2`/`v3`/`v4` schemas are rejected: there is no automatic schema migration, so a schema bump
(including the v3→v4 move to sharded files and the v4→v5 move to the redb 4 engine) requires rebuilding
via `tablassert build-fullmap`.

Readers open every fullmap file READ-ONLY with a SHARED file lock (redb ≥ 3 `ReadOnlyDatabase`), so any
number of processes can run lookups against the same fullmap concurrently; only a `build-fullmap` rebuild
(an exclusive-lock writer) briefly blocks readers. Each lookup pins one primary-plus-shards file
generation, cached handles are validated against the file's `(dev, ino)` on every use, so a reader
follows a rebuild on the next lookup.

## Usage in Graph Config

The `fullmap:` field in a graph configuration points at either the redb file directly or a base
directory. Tablassert resolves it via `fullmap_db_path()`:

- If the path is a file or already ends in `.redb`, it's used as-is.
- Else if `<path>/fullmap.redb` exists, that's used.
- Else it falls back to `<path>/data/fullmap.redb` (the `build-fullmap` default layout).

```yaml
# graph-config.yaml
name: my-graph
version: "1.0"
fullmap: /path/to/fullmap/   # directory containing data/fullmap.redb, or a direct .redb file
tables:
  - ./TABLE/my-table.yaml
rig:
  source_info:
    infores_id: infores:my-graph
    terms_of_use_info:
      terms_of_use_url: https://example.org/terms
    data_access_locations:
      - My source downloads - https://example.org/downloads
    source_status: unknown
  ingest_info:
    utility: Example graph backed by a fullmap entity-resolution database.
    scope: Example scope for the graph.
  provenance_info:
    contributions:
      - "Author Name - code author"
  artifact_base_url: https://example.org/my-graph
  artifact_base_path: ./published/my-graph
```

## Programmatic Usage

Pass the fullmap path (file or base directory) as the `fullmap` argument to `resolve_many()`; see
[Batch Resolution](api/lib.md) for the full reference and example, and
[Entity Resolution](api/fullmap.md) for the lower-level `resolve()` API.
