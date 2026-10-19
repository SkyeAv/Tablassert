# CLI Reference

Tablassert extracts knowledge assertions from tabular data into KGX NDJSON. The `tablassert` app
requires the `[cli]` extra (`pip install "tablassert[cli]"` or `uv tool install "tablassert[cli]"`) and
exposes **five subcommands**: `build-fullmap`, `build-kg`, `quick-map`, `validate`,
and `validate-kgx`, plus an app-level `--version` flag. Run `tablassert --help` (or `<command> --help`)
for the live surface.

## Command index

| Command | Use this to... |
| --- | --- |
| [`build-fullmap`](#build-fullmap) | Build the embedded fullmap redb used for entity resolution |
| [`build-kg`](#build-kg) | Build a KGX NDJSON knowledge graph from a YAML configuration |
| [`quick-map`](#quick-map) | Show what fullmap entity resolution does with one or more terms |
| [`validate`](#validate) | Validate a graph or table configuration without executing it |
| [`validate-kgx`](#validate-kgx) | Validate built KGX NDJSON against the Biolink Model and the Translator registry |

## App flags

These are flags on the root `tablassert` command, **not** subcommands.

| Flag | Description |
| --- | --- |
| `--version` | Print the installed package version as `tablassert <version>` (e.g. `tablassert 21.0.0`) and exit |
| `--help`, `-h` | Show help for the app or a subcommand |

!!! warning "Two different versions"
    The app `--version` prints **Tablassert's package version**. The [`build-fullmap --babel-version`](#build-fullmap)
    flag is unrelated: it selects a **RENCI BABEL snapshot date** (default `2026jul22`). It is deliberately
    not named `--version`, because the app-level `--version` flag shadowed that long form: `build-fullmap
    --version <snapshot>` printed the app version and exited 0 without running the command.

---

## build-fullmap

Use this to obtain the embedded `fullmap.redb` entity-resolution database. Every database this
command installs is filtered by the built-in **top-100 experimental-taxon allowlist**; there is no
flag for it. By default it first tries to **download a prebuilt database** published for this
Tablassert version; `--force` skips that and builds from RENCI BABEL exports instead (download class

+ synonym files, then build a single redb).

```bash
tablassert build-fullmap [ARGS]
```

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `--output`, `-o` | Path | No | `./fullmap/data/fullmap.redb` | Path to write the redb file (prebuilt extraction or build output) |
| `--cache`, `-c` | Path | No | `./fullmap/downloads` | Directory for downloaded BABEL files when building from scratch (`classes/`, `synonyms/`) |
| `--babel-version`, `-v` | str | No | `2026jul22` | BABEL snapshot date to fetch (a RENCI stamp, **not** Tablassert's version) |
| `--force`, `-f` | Flag | No | `False` | Skip the prebuilt download and always rebuild from BABEL outputs |

```bash
# Default: download the prebuilt fullmap.tar.zst for this version and extract it (fast)
tablassert build-fullmap --output /data/fullmap/fullmap.redb
# Force a from-scratch rebuild from BABEL outputs (e.g. after a BABEL snapshot bump)
tablassert build-fullmap --force --output /data/fullmap/fullmap.redb
# With the [aria2] extra installed, the bundled aria2c is used automatically for either download
pip install "tablassert[aria2]"
tablassert build-fullmap --output /data/fullmap/fullmap.redb
```

Every one of those invocations produces a database filtered by the checked-in
`src/tablassert/data/experimental_taxa.yaml` allowlist and records that filter as a
`META.taxon_allowlist` identity.

By default `build-fullmap` looks for a prebuilt `fullmap.tar.zst` at
`https://stars.renci.org/var/babel_outputs/<babel-version>/fullmap/<tablassert-version>/` (the version
directory is the **installed Tablassert package version**, never hardcoded). It verifies the archive
against the published `sha256sum.txt` when that file is present and parseable, and otherwise warns and
proceeds unverified, then extracts it beside `--output` in the Rust extension, streaming the
archive through zstd decompression and untarring it with the GIL released (the decompressed tar never
touches disk), then validating the extracted
primary + shards against the force-build contract (exact `v6` schema, a recorded `build_id`, the exact
shard set, per-shard `build_id` equality, and a `META.taxon_allowlist` identity matching the built-in
allowlist) before atomically renaming them into place. If no prebuilt exists for this version (or the
download, extraction, or identity check fails), it falls back to a from-scratch BABEL build and logs a
warning: an archive published without the current filter is never installed.

The downloader is chosen automatically; there is no flag to pass. With the optional `[aria2]` extra
installed (`pip install "tablassert[aria2]"`; Linux/Windows wheels only), both the prebuilt archive
and the BABEL files download through the bundled aria2c binary with resumable segmented downloads;
without the extra, Tablassert's Python downloader is used. The extra ships no macOS wheels, so a
normal macOS install resolves to the Python downloader. The choice is resolved once per run, after
the reuse short-circuit, and is announced on stderr as well as logged either way (a run that reuses
an existing database downloads nothing and stays silent); a failing aria2c download fails loud
rather than being silently rescued by the Python downloader.

A database already present at `--output` is reused only when it carries that same allowlist identity;
an unfiltered (or differently filtered) leftover from an older Tablassert is rebuilt, with a warning.
Pass `--force` to rebuild unconditionally.

A from-scratch build parallelizes automatically. On Linux the worker count is
`min(available_CPUs, MemAvailable_GB / 2)`, read from `/proc/meminfo`, so each worker is budgeted 2 GB
of addressable memory and a 16 GB machine caps at 8 workers; elsewhere it falls back to 90 percent of
the CPU count. There is no flag to tune.

See [Fullmap](fullmap.md) for the data pipeline, output schema, and graph-config usage.

---

## quick-map

Use this to see what fullmap entity resolution will do with one or more terms: the rows a `build-kg`
run would emit for a cell holding that term under the filters shown. Each term runs through the exact
build path (level-one/level-two normalization, probe-key extraction, one redb fetch, then filter, rank,
and dedup). Matched terms print one `rich` table each, in input order, with the term's normalized probe
keys in the title so a miss is diagnosable. The whole input is one batched redb round trip, never one
per term. A term with no matches prints a `no matches` line and still exits 0. Only a bad flag or a
missing fullmap exits 2; a fullmap that fails to open surfaces its own error.

```bash
tablassert quick-map TERMS... --fullmap ./fullmap/data/fullmap.redb [OPTIONS]
```

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `TERMS` (`--terms`) | list[str] | Yes | n/a | One or more terms to resolve (positional); any casing or whitespace, and a CURIE string works too because fullmap indexes CURIEs and their equivalent identifiers as terms |
| `--fullmap`, `-f` | Path | Yes | n/a | Fullmap redb file, or a directory holding one (resolved like the `Graph.fullmap` config field: `<dir>/fullmap.redb` then `<dir>/data/fullmap.redb`) |
| `--taxon`, `-t` | int | No | `9606` | NCBI taxon id constraining taxon-bearing matches; `0` disables the filter like `taxon: null` does |
| `--prioritize`, `-p` | list[str] | No | `None` | Biolink categories ranked higher (see the vocabulary note below) |
| `--avoid`, `-a` | list[str] | No | `None` | Biolink categories dropped entirely (see the vocabulary note below) |
| `--exclude-prefixes`, `-ep` | list[str] | No | `None` | CURIE namespace prefixes (text before the first `:`) to drop |
| `--exclude-regex`, `-er` | list[str] | No | `None` | Regex patterns; any resolved CURIE matching one is dropped |

The flags mirror the `NodeEncoding` config fields one-to-one, so a debugging invocation can copy
the `taxon` / `prioritize` / `avoid` / `exclude_prefixes` / `exclude_regex` values straight from the
section config whose resolution you are checking (see the
[Table Configuration Reference](configuration/table.md)).

```bash
# One term against the default build location
tablassert quick-map BRCA1 -f ./fullmap/data/fullmap.redb
# Several terms (mixed hits and misses are fine) in one batched lookup
tablassert quick-map BRCA1 "breast cancer 1" MONDO:0005148 nonsense -f ./fullmap/data/fullmap.redb
# Reproduce a node column's exact resolution settings
tablassert quick-map "TNF-alpha" -f ./fullmap -t 9606 -p Gene -a Disease -ep OMIM -er '^OMIM:\\d+$'
```

!!! note "The `--prioritize` / `--avoid` vocabulary"
    These flags accept the **live Biolink entity category names** (the same vocabulary the
    `prioritize` / `avoid` config keys accept), validated at run time rather than enumerated in
    `--help`, because the `Categories` enum is built dynamically from the installed biolink-model.
    An invalid value exits 2 naming the nearest valid names.

!!! note "`-f` means `--fullmap` here"
    On `build-kg` and `validate`, `-f` is `--configuration-file`; on `build-fullmap` it is
    `--force`. Aliases are per-command, and `quick-map` takes no config and has no force mode.
    `-f` is its one required input path.

---

## build-kg

Use this to build a KGX NDJSON knowledge graph (nodes, edges, and a Resource Ingest Guide) from a
YAML configuration file.

```bash
tablassert build-kg GRAPH-CONFIGURATION-FILE [ARGS]
```

The positional `GRAPH-CONFIGURATION-FILE` (also `--configuration-file`, `-f`) is a **graph** YAML.

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `GRAPH-CONFIGURATION-FILE` (`--configuration-file`, `-f`) | Path | Yes | n/a | Graph YAML |
| `--release`, `-r` | Flag | No | `False` | Emit a slim, significant-only graph (drops `not_significant` edges before resolution) |
| `--qc`, `-q` | Flag | No | `False` | Audit resolved mappings (exact match, fuzzy match, abbreviation expansion, then SapBERT) so low-confidence edges are flagged; requires the `[qc]` extra, checked before the build starts. Also runs a final study stage that asserts over the emitted NDJSON: no duplicate node ids, every node has a non-empty `id` and `name`, every edge has a non-empty `subject`, `predicate`, and `object`, no undeclared or isolated nodes, no malformed lines, no null or empty values in any field (checked recursively), no stray whitespace (verbatim `original_*` fields excepted from the whitespace check, since they are faithful source copies), and, when any section declares a `category_override`, no edge demoted to bare `biolink:Association` (a row escaped its pin and shipped without the class-specific slots `prune_to_class` nulled into `has_supporting_studies`; the assertion is graph-wide, so one pinned section also gates edges from unpinned sections) and fails the build (non-zero exit) on any violation |
| `--log`, `-l` | Flag | No | `False` | Enable verbose per-section logging; requires the `[log]` extra, and without it Tablassert produces no logs |
| `--head`, `-hd` | Flag | No | `False` | Fast output-shape preview: <=5 random rows/section, cached under a `.head` store suffix, never clobbers a full build |
| `--linkouts`, `-lo` | Flag | No | `False` | Append the EdgeLinkouts per-edge record URL (`https://linkouts.skyelanegoetz.com/edges/{edge_id}`, resolved to the final edge id after dedup) to the graph infores entry (`rig.source_info.infores_id`) in every emitted edge's `sources` list, so linkouts.skyelanegoetz.com can deep-link each edge; sections declaring `provenance.override` are untouched; cached under a `.linkouts` store suffix, never clobbers a full build |

```bash
tablassert build-kg graph.yaml --qc --log
```

The parallel fullmap reads behind entity resolution are automatic: large lookup batches (>= 1024
terms) fan out across the 16 record-shard files, one thread per shard (redb readers share-lock, so they
never contend), while smaller batches stay serial. There is no flag to tune, and results are identical
at any worker count.

Output is written to `rig.artifact_base_path` (created when missing) as `{name}_{version}.nodes.ndjson`,
`{name}_{version}.edges.ndjson`, and `{name}_{version}.RIG.yaml`; intermediate parquet lands in
`.tablassert/store/`. Build-time store keys incorporate the source-file content, so editing a source
file rebuilds exactly the sections that read the changed content. The first build after this key-format
upgrade rebuilds every section once because the keys have changed; orphaned old parquet files are not
automatically deleted. The RIG document is audited in memory before it is written: an invalid or
incomplete RIG fails the build with `[rig-validation-failed]` and nothing is emitted. See
[Graph Configuration](configuration/graph.md).

??? info "Build progress & stages"
    The build runs six stages in sequence (Loading Tables, Extracting Sections, Building TCode,
    Collecting Instructions, Building Subgraphs, Compiling Graph) plus a seventh, Studying Graph,
    only when `--qc` is passed. They render under a three-row live progress block (stage header;
    section bar with count/elapsed/ETA; in-flight item detail). Each completed stage prints a green
    confirmation line above the block, naming the stage number, its uppercased name, and its elapsed
    time. During Building Subgraphs the detail line also shows the per-section phase (`load`,
    `filter`, `clean`, `encode`, `resolve`, `qc`, `edge`, `provenance`, `significance`, `finalize`,
    `write`, plus `cache` for a shared run-scoped prefix and `transform` for an unlabeled op).

### Automatic run-scoped TCode cache

`build-kg` automatically caches only expensive TCode instruction prefixes shared by at least two
sections. Worth-caching guards exclude trivial prefixes, and runs with no qualifying sharing create
no cache. Snapshot keys use the bundled Rust XXH64 digest. Snapshots live in a temporary directory
that is deleted when the build ends, including on failure; there is no persistent cache and no new
CLI flag to control it.

The `--head` option remains a debug/shape preview. When sections share a cached prefix, consumers
reuse the producer's random sample, so identical samples for identical prefixes are intentional.
With `--log`, resolve/audit lines from a shared label-free prefix are attributed to the producer
section; other section-local operations retain their own section labels.

---

## validate

Use this to validate a configuration against a schema without running the build; it is what CI and
pre-commit hooks call. The required `--schema` flag selects which schema to validate against (the kind is
no longer sniffed from the YAML).

```bash
tablassert validate CONFIGURATION-FILE --schema graph
tablassert validate -f CONFIGURATION-FILE --schema table
```

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `CONFIGURATION-FILE` (`--configuration-file`, `-f`) | Path | Yes | n/a | Configuration file to validate |
| `--schema`, `-s` | `graph` \| `table` | Yes | n/a | Schema to validate against: `graph` validates the `Graph` model **and** every referenced table; `table` validates section syntax only |

Exits non-zero on any schema error. See [Table Configuration](configuration/table.md) and
[Graph Configuration](configuration/graph.md).

```bash
tablassert validate table-config.yaml --schema table
tablassert validate graph.yaml --schema graph
```

---

## validate-kgx

Use this to check that a completed build is actually Biolink-compliant and that the knowledge-source
identifiers it emits are real. Where
[`validate`](#validate) checks your *configuration*, `validate-kgx` checks the *output*: every node
and edge is constructed as the Biolink Pydantic class named by its own `category`, the same classes
[`NCATSTranslator/translator-ingests`](https://github.com/NCATSTranslator/translator-ingests) builds
when it ingests your files, and every emitted `infores:` CURIE is classified against the NCATS
Translator registry (see [Checking knowledge-source identity](#checking-knowledge-source-identity)).

```bash
tablassert validate-kgx --nodes MY_KG_1.0.0.nodes.ndjson --edges MY_KG_1.0.0.edges.ndjson
```

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `--nodes`, `-n` | Path | Yes | n/a | Built `*.nodes.ndjson` file to validate |
| `--edges`, `-e` | Path | Yes | n/a | Built `*.edges.ndjson` file to validate |
| `--limit` | int | No | `20` | Maximum example failures to retain per file (structural and identity examples alike) |
| `--prune`, `-p` | Flag | No | `False` | Rewrite the edges file in place, keeping only edges that validate strictly (pending carryovers are dropped too); nodes are never touched, and a clean file is not rewritten |
| `--rig` | Path | No | n/a | RIG yaml to include in the identity scan (its `source_info.infores_id` and target `primary_knowledge_sources` are checked) |
| `--allow-infores` | list[str] | No | n/a | Registry-exempt CURIE, repeatable; use for the locally-minted graph identifier and other deliberate locals |
| `--registry` | snapshot\|refresh\|off | No | `snapshot` | `snapshot` validates offline against the bundled copy; `refresh` fetches the live upstream catalog (network) and caches it under the user cache dir; `off` skips the identity half entirely |
| `--strict` | Flag | No | `False` | Exit non-zero while any unregistered CURIE remains; without it the identity half is advisory |

Failures are grouped by field and error type, so a systematic modelling problem shows up as one line
per problem rather than one per record:

```text
biolink-model 4.4.4
nodes: 424141/424141 valid (0 failures)
edges: 2000085/2000085 valid (0 failures)
KGX output is Biolink-compliant.
```

Exits non-zero when any record fails, so it can gate a release in CI. A missing or misspelled path is
reported as `file not found` and also exits non-zero: a file that was never read must never count as
a pass.

Some edges carry fields the installed model declares on no association: the KGX denormalized
carryovers (`synonym`, `xref`, `relation`, `provided_by`, ...). Tablassert emits them on purpose,
so they are counted separately as *pending* rather than treated as defects. The set is derived
from the installed package, so a field leaves it the moment a `biolink-model` release declares
it, as `effect_size` / `effect_type` did when 4.4.4 shipped
[#1774](https://github.com/biolink/biolink-model/pull/1774):

```text
edges: 1200000/2000085 valid (800085 failures; 800085 pending biolink-model support)
```

The strict count is what `ok` and the exit code use; the pending count is reported separately, so a
deliberate gap never reads as a modelling error.

### Checking knowledge-source identity

Alongside structure, `validate-kgx` checks *identity*: every `infores:` CURIE on your edges
(`primary_knowledge_source`, `sources[].resource_id`, `sources[].upstream_resource_ids`), nodes
(`provided_by`), and, with `--rig`, the RIG document, is classified against the NCATS Translator
registry ([`biolink/information-resource-registry`](https://github.com/biolink/information-resource-registry)'s
`infores_catalog.yaml`). The registry ships as a bundled snapshot (the check is offline by default);
`--registry refresh` fetches the live catalog instead, and `--registry off` skips the identity half
entirely. Values without the `infores:` prefix are reported as malformed, not unregistered.

```text
registry: .../infores_catalog.yaml (508 entries)
curies: 3 registered, 1 allowed, 1 unregistered, 0 malformed
  e.g. infores:totally-made-up (edges.primary_knowledge_source): unregistered
infores warnings present; pass --strict to fail on them.
```

The default posture is advisory and exits 0: Tablassert legitimately mints graph-local CURIEs (the
graph's own `infores:` id from the `rig:` section's name) that will never sit in the registry. Allow
those with repeatable `--allow-infores`, and pass `--strict` in CI to exit non-zero while any
unregistered CURIE remains. A missing or misspelled path is reported as `file not found` and exits
non-zero in every mode: a file that was never read must never count as a pass.

### Repairing a graph with `--prune`

`--prune` (`-p`) repairs an already-emitted graph instead of only scoring it: the edges file is
rewritten in place and every edge that does not validate strictly is removed. That includes real
defects (non-coercible values, extras the model will never declare, rejected predicates), the
deliberate pending carryovers (`synonym`, `xref`, `relation`, `provided_by`, ...), and lines that
are not JSON at all (counted separately as malformed). On a real graph the carryovers dominate the
failure list: a DAKP-style build prunes 800085 of 2000085 records (about 40 percent), so `--prune`
trades them for a strictly-compliant graph. Nodes are validated and reported, never pruned: dropping
one would strand its references.

```bash
tablassert validate-kgx -n MY_KG_1.0.0.nodes.ndjson -e MY_KG_1.0.0.edges.ndjson --prune
```

The rewrite is atomic (written to a sibling temp file, then swapped in) and kept lines stay
byte-identical, so a pruned graph differs from the original only by the removed records. A clean
file is not rewritten at all, and a missing edges file is reported without ever being pruned.
After pruning, every kept edge is strictly valid, so the exit code goes green as soon as the
nodes are clean too. The identity half runs after the rewrite and classifies the pruned file:
only what ships is judged.

```text
edges: pruned 12/424159 non-compliant records
          9  p_value: float_parsing
  e.g. DISEASE_EDGE_000017: p_value: float_parsing
edges: 424147/424159 valid (0 failures)
KGX output is Biolink-compliant.
```

Retrieval-source entries are the mirror case. Tablassert emits `resource_id` as each `sources` entry's
sole identifier, while the pinned model still requires the inherited `Entity.id` on `RetrievalSource`.
The validator supplies that `id` to its own in-memory copy of the record whenever the installed model
requires it, so compliant output validates without the duplicate identifier ever being written to disk.
The decoded record and the built NDJSON are untouched, and the alias stops being applied on its own
once a model release drops the requirement.

---

## Typical workflow

1. Author a table config, then a graph config that references it.
2. `tablassert validate graph.yaml --schema graph`: fail fast on schema errors.
3. `tablassert build-kg graph.yaml`: produce KGX NDJSON + RIG (add `--qc` to audit mappings).
4. `tablassert quick-map TERM... -f <fullmap>`: spot-check how a term resolves under a column's
   exact filters when a mapping needs debugging.
5. `tablassert validate-kgx -n MY_KG_1.0.0.nodes.ndjson -e MY_KG_1.0.0.edges.ndjson`: confirm the
   output validates against the Biolink Model and its knowledge-source CURIEs against the
   Translator registry before shipping it downstream.

## Next Steps

+ **[Tutorial](tutorial.md)**: complete example walkthrough
+ **[Configuration Guide](configuration/graph.md)**: YAML configuration reference
+ **[Fullmap](fullmap.md)**: entity-resolution database build and schema
