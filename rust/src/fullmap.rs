use flate2::read::GzDecoder;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyList};
use rayon::prelude::*;
use redb::{
    Database, Durability, ReadOnlyDatabase, ReadableDatabase, ReadableTable, TableDefinition,
};
use rustc_hash::FxHasher;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};
use std::fs::File;
use std::hash::{BuildHasher, BuildHasherDefault};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::thread;
use tar::EntryType;
use xxhash_rust::xxh3::xxh3_128;
use xxhash_rust::xxh64::xxh64;

const RECORDS: TableDefinition<u64, &[u8]> = TableDefinition::new("records");
const PREFIXES: TableDefinition<u16, &str> = TableDefinition::new("prefixes");
const CATEGORIES: TableDefinition<u16, &str> = TableDefinition::new("categories");
const SOURCES: TableDefinition<u8, &[u8]> = TableDefinition::new("sources");
const CURIES: TableDefinition<u32, &[u8]> = TableDefinition::new("curies");
const META: TableDefinition<&str, &str> = TableDefinition::new("meta");
/// On-disk schema tag.  Bumped to v6 when the build started deriving level-one
/// keys through `nlp::normalize_l1` (stemmed + deduped + byte-sorted tokens):
/// the RECORDS key SPACE changed, so a v5 database is not merely an older layout
/// of the same data — its keys can never be hit by a normalized query.  Every
/// older tag is rejected at lookup with the rebuild instruction.
const SCHEMA_VERSION: &str = "tablassert.fullmap.v6";
const SCHEMA_VERSION_V5: &str = "tablassert.fullmap.v5";
const SCHEMA_VERSION_V4: &str = "tablassert.fullmap.v4";
const SCHEMA_VERSION_V3: &str = "tablassert.fullmap.v3";
const SCHEMA_VERSION_V2: &str = "tablassert.fullmap.v2";
const SCHEMA_VERSION_V1: &str = "tablassert.fullmap.v1";
const FULLMAP_SOURCE_VERSION: &str = "2026jul22";
/// Number of on-disk redb shard files the RECORDS table is hash-partitioned
/// across.  The public build entry point always writes exactly this many shards
/// (no longer environment-configurable); the read path still honors the
/// per-database META `shards` value so databases built before the count was
/// pinned keep working.  Must be a power of two so `term_shard` can route with a
/// bitmask.  Distinct from the in-memory `SHARD_COUNT` used by the concurrent
/// build maps.
const SHARD_COUNT_SHARDS: usize = 16;
/// A normalized term grouped with its deduplicated `(curie_id, source_id)` pairs.
type TermPairs = (String, Vec<(u32, u8)>);
type PairRecords = Vec<TermPairs>;
/// One k-way-merge heap entry: `(xxh64(term), term, run index, pairs)`,
/// min-ordered by `(hash, term)`.  Ordering by the hash first makes the common
/// heap comparison a single `u64` cmp; the term string is compared only on the
/// rare hash collision.  Spill runs are written sorted by the same `(hash, term)`
/// key (see `spill_run`), so the k-way merge still groups equal terms correctly.
type MergeItem = (Reverse<u64>, Reverse<String>, usize, Vec<(u32, u8)>);
/// A read-path fan-out job: the `(input_index, term)` bucket routed to one shard
/// plus a clone of that shard's handle, so a worker thread owns both outright
/// (no shared receiver or borrow).
type ShardJob = (Vec<(usize, String)>, Arc<ReadOnlyDatabase>);
/// Fast non-cryptographic hash map for the build-hot paths (per-worker term
/// aggregation, dimension/CURIE interning).  `FxHasher` (rustc's own hasher) is
/// a single multiply-XOR pass — ~3-5x faster than std's SipHash for these short
/// string/integer keys.  Collision behavior is irrelevant to build correctness:
/// these maps are internal, and the on-disk RECORDS key is xxh64 (not FxHash).
type FxHashMap<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher>>;
/// A producer->worker channel payload: the source id tagging a chunk of raw
/// NDJSON line bytes (one `Vec<u8>` per non-empty line).  Workers parse each line
/// straight from the bytes (`serde_json::from_slice`), so no per-line `String` is
/// ever allocated on the read path.
type LineChunk = (u8, Vec<Vec<u8>>);

/// Read-path database cache keyed by canonical path, generation-checked.
///
/// Handles are `ReadOnlyDatabase` (redb ≥ 3), which take a SHARED file lock:
/// any number of processes/threads may hold them concurrently, and they only
/// conflict with a writer (`build-fullmap`'s exclusive lock).  Each entry also
/// records the `(st_dev, st_ino)` generation and META `build_id` of the file it
/// was opened from: a rebuild replaces the files via unlink + recreate — in
/// practice on FRESH inodes (an `unlink` does not need the lock) — so the path
/// alone cannot distinguish the old data from the new.  Every cache hit
/// therefore re-stats the canonical path and evicts the entry when `(dev, ino)`
/// no longer matches, so a lookup always reads the generation currently living
/// at the path — in-process after `build_fullmap_db`, or cross-process after
/// another process rebuilt.  A stale evicted handle simply keeps reading the
/// unlinked old file as a consistent snapshot; it can never block the rebuild
/// or observe a torn file.  Each lookup pins ONE primary-plus-shards build:
/// `open_cached_shards` records how every bundle member was served (`Served`)
/// and accepts the bundle only when every member has the same `build_id` AND is
/// either ALL current with a stable primary generation or ALL stale because
/// every path is absent (the documented mid-rebuild snapshot).  A MIXED bundle
/// (e.g. the new primary committed while the shard files are still absent), or
/// an all-stale cache with diverged build IDs, is retried whole and finally
/// raises an exhaustion error the Python side retries like lock contention.  The
/// next lookup after a rebuild opens the replacement.  Read-only opens never
/// write the file, so the mtime-keyed Python-side caches stay stable under
/// lookups.
static DB_CACHE: OnceLock<RwLock<HashMap<PathBuf, CachedDatabase>>> = OnceLock::new();

/// One cached read-only handle plus the file generation and build token it was
/// opened from, so every cache hit can detect an in-place rebuild and bundle
/// opens can reject torn primary/shard sets.
struct CachedDatabase {
    database: Arc<ReadOnlyDatabase>,
    generation: FileGeneration,
    build_id: u64,
}

/// On-disk identity of the file at a path: its `(st_dev, st_ino)` pair.  A
/// rebuild replaces each file on a fresh inode, so this pair distinguishes the
/// generations living at one path over time.
type FileGeneration = (u64, u64);

fn generation_of(path: &Path) -> std::io::Result<FileGeneration> {
    let meta = std::fs::metadata(path)?;
    Ok((meta.dev(), meta.ino()))
}

fn new_build_id() -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let seq = BUILD_ID_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
    let id = now.as_secs().rotate_left(32)
        ^ u64::from(now.subsec_nanos())
        ^ u64::from(std::process::id()).rotate_left(17)
        ^ seq;
    if id == 0 {
        seq
    } else {
        id
    }
}

fn read_build_id(database: &ReadOnlyDatabase) -> PyResult<u64> {
    let read = database.begin_read().map_err(py_err)?;
    let meta = read.open_table(META).map_err(py_err)?;
    let raw = meta.get("build_id").map_err(py_err)?.ok_or_else(|| {
        PyRuntimeError::new_err(
            "fullmap DB is missing META.build_id; rebuild with 'tablassert build-fullmap'",
        )
    })?;
    raw.value().parse::<u64>().map_err(|_| {
        PyRuntimeError::new_err(
            "fullmap DB has invalid META.build_id; rebuild with 'tablassert build-fullmap'",
        )
    })
}

static BUILD_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Number of shards for concurrent maps (power of two for mask routing).
const SHARD_COUNT: usize = 64;
const SHARD_MASK: usize = SHARD_COUNT - 1;

// Build tunables (overridable via environment for benchmarking / target tuning).
const DEFAULT_LOCAL_SPILL_ENTRIES: usize = 1_000_000;
const DEFAULT_EQUIV_SPILL_ENTRIES: usize = 2_000_000;
const DEFAULT_REDB_CACHE_BYTES: usize = 2 * 1024 * 1024 * 1024;
const DEFAULT_INSERT_BATCH: usize = 2_000_000;
const DEFAULT_CURIE_SPILL_ENTRIES: usize = 250_000;
/// Byte budget per producer->worker chunk. Bounding by bytes (not line count)
/// keeps each chunk's memory fixed even when synonym lines are large (protein /
/// PUBCHEM records can be ~1-2 KB), so the in-flight line buffer stays bounded
/// regardless of record size while still balancing load across workers.
const DEFAULT_CHUNK_BYTES: usize = 8 * 1024 * 1024;

/// Round `n` DOWN to the nearest power of two (6->4, 3->2, 5->4); powers of two
/// are unchanged and any `n < 1` floors to 1.  The shard mask routing in
/// `term_shard` requires a power-of-two shard count, so the shard count read from
/// a database's META is normalized through this before use.
fn round_down_pow2(n: usize) -> usize {
    if n <= 1 {
        return 1;
    }
    let p = n.next_power_of_two();
    if p == n {
        n
    } else {
        p / 2
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
struct FullmapRecord {
    curie: String,
    preferred_name: String,
    category_name: String,
    taxon_id: i64,
    source_name: String,
    source_version: String,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
struct CurieRow {
    prefix_id: u16,
    local_id: String,
    preferred_name: String,
    category_id: u16,
    taxon_id: i32,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
struct SourceRow {
    source_name: String,
}

fn py_err<E: std::fmt::Display>(err: E) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}

/// Open a fullmap file with a SHARED (read-only) lock and map open errors to
/// actionable fullmap errors.
///
/// A read-only open can neither upgrade an outdated file format (redb ≥ 3
/// dropped the v2 format that redb 2.x builds wrote) nor repair a crash-damaged
/// file — by design — so `UpgradeRequired`/`RepairAborted` surface as the same
/// rebuild hint `validate_schema` produces for an outdated schema tag, instead
/// of a cryptic redb error.  Every other error passes through verbatim.
fn open_read_only(path: &Path) -> PyResult<ReadOnlyDatabase> {
    ReadOnlyDatabase::open(path).map_err(|err| match err {
        redb::DatabaseError::UpgradeRequired(_) | redb::DatabaseError::RepairAborted => {
            PyRuntimeError::new_err(
                "fullmap DB is outdated or needs repair; rebuild with 'tablassert build-fullmap'",
            )
        }
        other => py_err(other),
    })
}

/// Narrow `value` to its cleaned fixed point — repeated trim + matching /
/// duplicate quote stripping — returning a sub-slice of `value` (zero
/// allocation).  Cleaning only ever narrows to a sub-slice, so the result always
/// borrows from the input.
fn clean_slice(value: &str) -> &str {
    let mut s = value;
    loop {
        let trimmed = s.trim();
        let bytes = trimmed.as_bytes();
        let matching_quotes = bytes.len() >= 2
            && ((bytes[0] == b'\'' && bytes[bytes.len() - 1] == b'\'')
                || (bytes[0] == b'"' && bytes[bytes.len() - 1] == b'"'));
        let duplicate_start = bytes.len() >= 2
            && ((bytes[0] == b'\'' && bytes[1] == b'\'') || (bytes[0] == b'"' && bytes[1] == b'"'));

        let next = if duplicate_start {
            &trimmed[1..]
        } else if matching_quotes {
            &trimmed[1..trimmed.len() - 1]
        } else {
            trimmed
        };

        if next == s {
            return next;
        }
        s = next;
    }
}

/// Clean a value (trim + strip matching/duplicate quotes).  Returns a `Cow` that
/// borrows a sub-slice of the input — zero allocation in every case.
fn clean(value: &str) -> Cow<'_, str> {
    Cow::Borrowed(clean_slice(value))
}

/// Clean AND lowercase in one step (the emit_term hot path): clean to a borrowed
/// sub-slice, then ASCII-fast-path lowercase via `level_one`.  The `Cow` is
/// borrowed in the common case (already-clean, already-lowercase ASCII) and only
/// allocates when the term actually needs lowercasing.
///
/// `pub(crate)`: also the cleaning+lowercasing first stage of
/// `nlp::normalize_l1`, so the normalization pipeline and the fullmap emit
/// path share ONE cleaning implementation and can never drift apart.
pub(crate) fn clean_and_lower(value: &str) -> Cow<'_, str> {
    level_one(clean_slice(value))
}

/// QC a NORMALIZED (level-one / lowercase) term.  Callers must pass an
/// already-lowercased form (emit_term feeds it the cleaned+lowercased term and
/// the level-two form), so the banned-token checks compare lowercase needles
/// directly and skip a per-term `to_lowercase` heap allocation on the hot path.
/// The banned needles are LITERAL SPELLINGS, so this must run BEFORE stemming
/// (Porter2 turns "hypothetical" into "hypothet" and the substring check would
/// silently stop matching).  Behavior is unchanged:
/// lowercasing is case-only and never affects the empty/tab/newline guards, so
/// QC-ing the lowercase form is exactly equivalent to QC-ing the original value
/// and lowercasing internally.
fn token_qc(value: &str) -> bool {
    // Tripwire (debug builds only; zero release cost): the banned-token checks
    // are case-sensitive against lowercase needles, so a mixed-case caller would
    // silently miss them.  Catch that contract violation early.
    debug_assert_eq!(value, value.to_lowercase());
    !value.is_empty()
        && !value.contains('\t')
        && !value.contains('\n')
        && !value.contains('\r')
        && !value.contains("uncharacterized")
        && !value.contains("hypothetical")
}

fn level_one(value: &str) -> Cow<'_, str> {
    if value.is_ascii() {
        // ASCII fast path: lowercasing is a byte operation.  Borrow unchanged if
        // already lowercase; otherwise lowercase into a fresh String.
        if value.bytes().all(|b| !b.is_ascii_uppercase()) {
            Cow::Borrowed(value)
        } else {
            let mut lowered = value.to_string();
            lowered.make_ascii_lowercase();
            Cow::Owned(lowered)
        }
    } else {
        Cow::Owned(value.to_lowercase())
    }
}

fn level_two(value: &str) -> Cow<'_, str> {
    // Fast path: level-two keeps [A-Za-z0-9_]; for an already-lowercase ASCII
    // input that is exactly [a-z0-9_], so when every byte qualifies the filter is
    // a no-op and we borrow unchanged (the common case post-level_one).  Any
    // uppercase or non-ASCII byte falls through to the filtering slow path.
    if value
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    {
        Cow::Borrowed(value)
    } else {
        Cow::Owned(
            value
                .chars()
                .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect(),
        )
    }
}

/// Terms the lookup path can never query: the fullmap `distinct()` bad-regex
/// `^\d+$|^(none|nan|na|null|unknown|not applicable|p_value|variable|result|`
/// `exposure|expression|symbol)$|^$` drops these from the query-term set before
/// lookup, so storing them in the DB is dead weight.  `term` is already a
/// normalized (cleaned level-one, `nlp::normalize_l1`, or level-two) form here.
/// Skipping them is provably safe: a stored term matching this can never meet a
/// surviving query term.  The needles are LITERAL SPELLINGS, so `emit_term`
/// applies this to the pre-stemming cleaned form (where the spellings still
/// match) AND to the normalized key it is about to store (the exact string
/// `distinct()` filters the query side on).
fn is_dead_term(term: &str) -> bool {
    if term.is_empty() {
        return true;
    }
    if term.bytes().all(|b| b.is_ascii_digit()) {
        return true;
    }
    matches!(
        term,
        "none"
            | "nan"
            | "na"
            | "null"
            | "unknown"
            | "not applicable"
            | "p_value"
            | "variable"
            | "result"
            | "exposure"
            | "expression"
            | "symbol"
    )
}

fn open_reader(path: &Path) -> PyResult<Box<dyn Read>> {
    let file = File::open(path).map_err(py_err)?;
    if path.extension().and_then(|x| x.to_str()) == Some("gz") {
        return Ok(Box::new(GzDecoder::new(file)));
    }
    Ok(Box::new(file))
}

/// A synonym NDJSON row, deserialized with borrowed fields so the hot synonym
/// path never builds a `serde_json::Value` DOM (~7 field allocations per row).
/// `Cow` handles JSON strings that need unescaping (`\uXXXX`) while still
/// borrowing the common plain-ASCII case straight from the line buffer.  Field
/// aliases mirror the legacy key fallbacks (curie|id, types|categories,
/// taxa|taxon, preferred_name|name).
#[derive(Deserialize)]
struct SynonymRow<'a> {
    #[serde(rename = "curie", alias = "id", default, borrow)]
    curie: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    names: Vec<Cow<'a, str>>,
    #[serde(rename = "types", alias = "categories", default, borrow)]
    types: Vec<Cow<'a, str>>,
    #[serde(rename = "taxa", alias = "taxon", default, borrow)]
    taxa: Vec<Cow<'a, str>>,
    #[serde(rename = "preferred_name", alias = "name", default, borrow)]
    preferred_name: Option<Cow<'a, str>>,
}

/// A class NDJSON row (the equivalent-identifiers source), deserialized with a
/// borrowed `id`.  `equivalent_identifiers` elements are heterogeneously either
/// a bare CURIE string or an object carrying one under identifier|id|curie, so
/// they decode via the untagged `EquivId` (owned — untagged decoding buffers its
/// content, so borrowing buys nothing there and the equiv lists are materialized
/// as owned strings anyway).
#[derive(Deserialize)]
struct ClassRow<'a> {
    #[serde(rename = "id", alias = "curie", default, borrow)]
    id: Option<Cow<'a, str>>,
    #[serde(default)]
    equivalent_identifiers: Vec<EquivId>,
}

/// One `equivalent_identifiers` element: a bare CURIE string or an object with
/// the CURIE under `identifier`/`id`/`curie` (untagged to accept both shapes).
#[derive(Deserialize)]
#[serde(untagged)]
enum EquivId {
    String(String),
    Object(EquivIdObject),
}

#[derive(Deserialize)]
struct EquivIdObject {
    #[serde(rename = "identifier", alias = "id", alias = "curie", default)]
    identifier: Option<String>,
}

/// Stream a class NDJSON file (gz or plain) line-by-line into a reusable byte
/// buffer, decoding each non-empty line into a borrowed `ClassRow` and handing it
/// to `visit`.  Zero per-line `String` allocation (the row borrows from the line
/// buffer); replaces the old `Value`-DOM `for_json_lines`.
fn for_class_lines(
    path: &Path,
    mut visit: impl FnMut(&ClassRow<'_>) -> PyResult<()>,
) -> PyResult<()> {
    let mut reader = BufReader::new(open_reader(path)?);
    let mut line: Vec<u8> = Vec::with_capacity(8192);
    loop {
        line.clear();
        let n = reader.read_until(b'\n', &mut line).map_err(py_err)?;
        if n == 0 {
            break; // clean EOF
        }
        while line.last() == Some(&b'\n') || line.last() == Some(&b'\r') {
            line.pop();
        }
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let row: ClassRow = serde_json::from_slice(&line).map_err(py_err)?;
        visit(&row)?;
    }
    Ok(())
}

fn first_category(row: &SynonymRow<'_>) -> String {
    row.types
        .first()
        .map(Cow::as_ref)
        .unwrap_or("NamedThing")
        .trim_start_matches("biolink:")
        .to_string()
}

fn is_organism_taxon(row: &SynonymRow<'_>) -> bool {
    row.types.iter().any(|category| {
        category
            .trim()
            .trim_start_matches("biolink:")
            .eq_ignore_ascii_case("OrganismTaxon")
    })
}

fn first_taxon(row: &SynonymRow<'_>) -> i32 {
    row.taxa
        .first()
        .map(Cow::as_ref)
        .unwrap_or_default()
        .trim_start_matches("NCBITaxon:")
        .parse::<i32>()
        .unwrap_or(0)
}

fn parsed_taxa(row: &SynonymRow<'_>) -> Vec<i32> {
    row.taxa
        .iter()
        .filter_map(|taxon| {
            let taxon = taxon.trim();
            let local = taxon
                .split_once(':')
                .filter(|(prefix, _)| prefix.eq_ignore_ascii_case("NCBITaxon"))
                .map(|(_, local)| local)
                .unwrap_or(taxon);
            local.parse::<i32>().ok().filter(|id| *id > 0)
        })
        .collect()
}

fn allowlist_identity(taxa: &HashSet<i32>) -> String {
    let mut ids: Vec<i32> = taxa.iter().copied().collect();
    ids.sort_unstable();
    let encoded = ids
        .iter()
        .map(i32::to_string)
        .collect::<Vec<String>>()
        .join(",");
    format!(
        "count={};xxh64={:016x}",
        ids.len(),
        xxh64(encoded.as_bytes(), 0)
    )
}

/// The positive-ID set an allowlist argument describes.  Shared by the build
/// path and every identity probe so a recorded `META.taxon_allowlist` and the
/// identity computed for the same input list always agree (a non-positive id
/// never filters anything, so it must never change the identity either).
fn allowlist_set(taxon_allowlist: Vec<i32>) -> HashSet<i32> {
    taxon_allowlist.into_iter().filter(|id| *id > 0).collect()
}

/// The `META.taxon_allowlist` identity recorded in a primary DB, or `None` for
/// an unfiltered build — the key is written only when an allowlist was applied,
/// so its absence IS the unfiltered marker.
fn read_taxon_allowlist_identity(database: &ReadOnlyDatabase) -> PyResult<Option<String>> {
    let read = database.begin_read().map_err(py_err)?;
    let meta = read.open_table(META).map_err(py_err)?;
    let identity = meta
        .get("taxon_allowlist")
        .map_err(py_err)?
        .map(|value| value.value().to_string());
    Ok(identity)
}

fn retain_for_taxon_allowlist(row: &SynonymRow<'_>, allowlist: Option<&HashSet<i32>>) -> bool {
    let Some(allowlist) = allowlist else {
        return true;
    };
    if is_organism_taxon(row) {
        return true;
    }
    let mut has_valid_taxon = false;
    for taxon in parsed_taxa(row) {
        has_valid_taxon = true;
        if allowlist.contains(&taxon) {
            return true;
        }
    }
    // Taxonless rows remain available for categories whose BABEL synonym rows
    // do not carry a positive taxon ID. This includes the conventional
    // `NCBITaxon:0` sentinel and malformed/empty taxon values.
    !has_valid_taxon
}

fn split_curie(curie: &str) -> Option<(&str, &str)> {
    curie
        .split_once(':')
        .filter(|(prefix, local)| !prefix.is_empty() && !local.is_empty())
}

/// The CURIE string carried by one `equivalent_identifiers` element, if any.
fn equivalent_id_value(equiv: &EquivId) -> Option<String> {
    match equiv {
        EquivId::String(s) => Some(s.clone()),
        EquivId::Object(o) => o.identifier.clone(),
    }
}

fn class_id_and_equivalents(row: &ClassRow<'_>) -> Option<(String, Vec<String>)> {
    let mut ids: Vec<String> = Vec::new();
    let id = match row.id.as_deref() {
        Some(id) => id.to_owned(),
        None => row
            .equivalent_identifiers
            .first()
            .and_then(equivalent_id_value)?,
    };
    // Primary id is NOT included in the value Vec — process_synonyms already
    // adds the curie itself as a term, saving ~38 GB of redundant storage.
    for equivalent in &row.equivalent_identifiers {
        if let Some(eid) = equivalent_id_value(equivalent) {
            if !ids.contains(&eid) {
                ids.push(eid);
            }
        }
    }
    Some((id, ids))
}

fn source_name(path: &Path) -> String {
    path.file_stem()
        .and_then(|x| x.to_str())
        .unwrap_or("BABEL")
        .trim_end_matches(".ndjson")
        .to_string()
}

fn hydrate_record(
    curie: &CurieRow,
    source_name: String,
    source_version: String,
    prefix: &str,
    category_name: &str,
) -> FullmapRecord {
    FullmapRecord {
        curie: format!("{}:{}", prefix, curie.local_id),
        preferred_name: curie.preferred_name.clone(),
        category_name: category_name.to_string(),
        taxon_id: i64::from(curie.taxon_id),
        source_name,
        source_version,
    }
}

// ---------------------------------------------------------------------------
// Sharded concurrent map (inspired by datassert's sharded curieCounter)
// ---------------------------------------------------------------------------

#[allow(clippy::cast_possible_truncation)] // xxh64->usize: only the low bits feed the shard mask, so truncation is the intended routing
fn shard_index(key: &str) -> usize {
    (xxh64(key.as_bytes(), 0) as usize) & SHARD_MASK
}

/// A sharded concurrent HashMap that routes keys by xxhash to reduce lock
/// contention.  Each shard is independently locked.
struct ShardedMap<V> {
    shards: Vec<RwLock<FxHashMap<String, V>>>,
}

impl<V> ShardedMap<V> {
    fn new() -> Self {
        let mut shards = Vec::with_capacity(SHARD_COUNT);
        for _ in 0..SHARD_COUNT {
            shards.push(RwLock::new(FxHashMap::default()));
        }
        Self { shards }
    }

    /// Get an existing value or insert a new one computed by `f`.
    /// Returns a clone of the value (requires V: Clone).
    fn get_or_insert_with(&self, key: &str, f: impl FnOnce() -> V) -> V
    where
        V: Clone,
    {
        let idx = shard_index(key);
        // Fast path: read lock.
        {
            let shard = self.shards[idx].read().unwrap();
            if let Some(v) = shard.get(key) {
                return v.clone();
            }
        }
        // Slow path: write lock.
        let mut shard = self.shards[idx].write().unwrap();
        if let Some(v) = shard.get(key) {
            return v.clone();
        }
        let v = f();
        shard.insert(key.to_string(), v.clone());
        v
    }
}

/// A sharded concurrent map keyed by the 128-bit hash of a CURIE, assigning
/// dense u32 ids.  Keying by `xxh3_128(curie)` instead of the CURIE string cuts
/// the dedup map's memory several-fold at full scale (hundreds of millions of
/// CURIEs: ~16-byte key vs a heap-allocated string + HashMap overhead), while a
/// 128-bit key makes collisions astronomically unlikely (~1e-21 at 5e8 keys),
/// preserving one stable `curie_id` per unique CURIE.  Mirrors datassert's
/// hash-keyed `curieCounter`, widened from 64 to 128 bits for safety.
struct CurieIdMap {
    shards: Vec<RwLock<FxHashMap<u128, u32>>>,
}

impl CurieIdMap {
    fn new() -> Self {
        let mut shards = Vec::with_capacity(SHARD_COUNT);
        for _ in 0..SHARD_COUNT {
            shards.push(RwLock::new(FxHashMap::default()));
        }
        Self { shards }
    }

    /// Get the id for `hash`, or insert a new one computed by `f`.
    #[allow(clippy::cast_possible_truncation)] // u128 hash->usize: only the low bits select the shard, so truncation is the intended routing
    fn get_or_insert_with(&self, hash: u128, f: impl FnOnce() -> u32) -> u32 {
        let idx = (hash as usize) & SHARD_MASK;
        // Fast path: read lock.
        {
            let shard = self.shards[idx].read().unwrap();
            if let Some(v) = shard.get(&hash) {
                return *v;
            }
        }
        // Slow path: write lock.
        let mut shard = self.shards[idx].write().unwrap();
        if let Some(v) = shard.get(&hash) {
            return *v;
        }
        let v = f();
        shard.insert(hash, v);
        v
    }
}

// ---------------------------------------------------------------------------
// Progress callback (Rust -> Python).  The GIL is released for the heavy work,
// so we re-acquire it briefly here to report phase/file/batch progress.
// ---------------------------------------------------------------------------

struct Progress {
    cb: Py<PyAny>,
}

impl Progress {
    fn call(&self, phase: i32, completed: u64, total: u64, detail: &str) {
        Python::attach(|py| {
            // Progress is best-effort: a failing Python callback must never kill
            // the build, so the call result is deliberately discarded.
            let _ = self.cb.call1(py, (phase, completed, total, detail));
        });
    }
}

// ---------------------------------------------------------------------------
// Spill runs: sorted on-disk chunks of (term -> pairs).  This is the Rust
// analog of datassert's "stage to Parquet, then GROUP BY externally" — it
// bounds the term-aggregation memory and lets the final grouping stream.
//
// Frame format (little-endian):
//   [u32 term_len][term bytes][u32 pair_count][(u32 curie_id, u8 source_id) x pair_count]
// Within a run each term appears once (per-thread dedup) and frames are sorted
// by term bytes.  The same term recurs across runs and is grouped at merge.
// ---------------------------------------------------------------------------

struct RunWriter {
    w: BufWriter<File>,
}

impl RunWriter {
    fn new(path: &Path) -> std::io::Result<Self> {
        Ok(Self {
            w: BufWriter::with_capacity(8 * 1024 * 1024, File::create(path)?),
        })
    }

    #[allow(clippy::cast_possible_truncation)] // frame format stores term/pair counts as u32 lengths
    fn write_term(&mut self, term: &str, pairs: &[(u32, u8)]) -> std::io::Result<()> {
        let tb = term.as_bytes();
        debug_assert!(u32::try_from(tb.len()).is_ok() && u32::try_from(pairs.len()).is_ok());
        self.w.write_all(&(tb.len() as u32).to_le_bytes())?;
        self.w.write_all(tb)?;
        self.w.write_all(&(pairs.len() as u32).to_le_bytes())?;
        for (curie_id, source_id) in pairs {
            self.w.write_all(&curie_id.to_le_bytes())?;
            self.w.write_all(&[*source_id])?;
        }
        Ok(())
    }

    fn finish(mut self) -> std::io::Result<()> {
        self.w.flush()
    }
}

struct RunReader {
    reader: BufReader<File>,
    cur: Option<TermPairs>,
}

impl RunReader {
    fn new(path: &Path) -> std::io::Result<Self> {
        let mut reader = BufReader::with_capacity(1 << 20, File::open(path)?);
        let cur = Self::read_frame(&mut reader)?;
        Ok(Self { reader, cur })
    }

    fn read_frame(r: &mut BufReader<File>) -> std::io::Result<Option<TermPairs>> {
        // Read the first length byte; a clean 0-byte read means EOF (no more frames).
        let mut first = [0u8; 1];
        if r.read(&mut first)? == 0 {
            return Ok(None);
        }
        let mut rest = [0u8; 3];
        r.read_exact(&mut rest)?;
        let term_len = u32::from_le_bytes([first[0], rest[0], rest[1], rest[2]]) as usize;
        let mut term_buf = vec![0u8; term_len];
        r.read_exact(&mut term_buf)?;
        let term = String::from_utf8(term_buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let mut cnt_buf = [0u8; 4];
        r.read_exact(&mut cnt_buf)?;
        let cnt = u32::from_le_bytes(cnt_buf) as usize;
        let mut pairs = Vec::with_capacity(cnt);
        for _ in 0..cnt {
            let mut pb = [0u8; 5];
            r.read_exact(&mut pb)?;
            let curie_id = u32::from_le_bytes([pb[0], pb[1], pb[2], pb[3]]);
            pairs.push((curie_id, pb[4]));
        }
        Ok(Some((term, pairs)))
    }

    fn advance(&mut self) -> std::io::Result<()> {
        self.cur = Self::read_frame(&mut self.reader)?;
        Ok(())
    }
}

/// Drain a thread-local term buffer into per-shard sorted run files on disk.
/// Terms are partitioned by `term_shard(term, shard_count)` so each shard's runs
/// hold ONLY that shard's terms; Phase 4 then runs one independent k-way merge
/// per shard with no shared producer (datassert-style write-time partitioning).
/// A file `run_s{shard}_{id}.bin` is written only for a shard that has at least
/// one term.  Returns `(shard, path)` pairs so the caller files each run under
/// its shard's list.  The frame format is unchanged (see `RunWriter`); each
/// per-shard file is sorted by `(xxh64(term), term)`, the hash-prefixed ordering
/// `MergeHeap` merges by (hash-first comparisons are a cheap `u64` cmp).
fn spill_run(
    local: &mut FxHashMap<String, Vec<(u32, u8)>>,
    spill_dir: &Path,
    run_id: usize,
    shard_count: usize,
) -> PyResult<Vec<(usize, PathBuf)>> {
    // Partition the buffer's terms by their destination shard.
    let mut by_shard: Vec<Vec<TermPairs>> = vec![Vec::new(); shard_count];
    for (term, pairs) in local.drain() {
        by_shard[term_shard(&term, shard_count)].push((term, pairs));
    }
    let mut out: Vec<(usize, PathBuf)> = Vec::new();
    for (shard, mut entries) in by_shard.into_iter().enumerate() {
        if entries.is_empty() {
            continue; // only non-empty shards write a file
        }
        // Sort by (xxh64(term), term) so the run stream matches MergeHeap's
        // hash-prefixed heap ordering — required for correct grouping, and makes
        // merge comparisons a cheap u64 cmp (term string compared only on hash
        // collision).  The hash is recomputed per comparison (xxh64 is ~3 ns for
        // short terms) rather than stored, keeping the frame format unchanged.
        entries.sort_unstable_by(|a, b| {
            xxh64(a.0.as_bytes(), 0)
                .cmp(&xxh64(b.0.as_bytes(), 0))
                .then_with(|| a.0.cmp(&b.0))
        });
        let path = spill_dir.join(format!("run_s{shard}_{run_id:08}.bin"));
        let mut w = RunWriter::new(&path).map_err(py_err)?;
        for (term, mut pairs) in entries {
            pairs.sort_unstable();
            pairs.dedup();
            w.write_term(&term, &pairs).map_err(py_err)?;
        }
        w.finish().map_err(py_err)?;
        out.push((shard, path));
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Curie-row spill runs: bounded on-disk chunks of (curie_id, encoded CurieRow).
//
// The synonym phase assigns one CurieRow per unique CURIE.  Holding all of them
// in RAM is the dominant full-build memory cost (~50 GB+ at 300-500 M CURIEs),
// so per-thread buffers are drained to these run files once they exceed
// `curie_spill` entries, and Phase 4 streams them straight into the CURIES
// table.  This bounds curie-row memory to O(threads * curie_spill).
//
// Frame format (little-endian): [u32 curie_id][u32 row_len][bincode CurieRow]
// ---------------------------------------------------------------------------

struct CurieRunWriter {
    w: BufWriter<File>,
}

impl CurieRunWriter {
    fn new(path: &Path) -> std::io::Result<Self> {
        Ok(Self {
            w: BufWriter::with_capacity(8 * 1024 * 1024, File::create(path)?),
        })
    }

    #[allow(clippy::cast_possible_truncation)] // frame format stores the encoded row length as a u32
    fn write_row(&mut self, curie_id: u32, encoded: &[u8]) -> std::io::Result<()> {
        debug_assert!(u32::try_from(encoded.len()).is_ok());
        self.w.write_all(&curie_id.to_le_bytes())?;
        self.w.write_all(&(encoded.len() as u32).to_le_bytes())?;
        self.w.write_all(encoded)?;
        Ok(())
    }

    fn finish(mut self) -> std::io::Result<()> {
        self.w.flush()
    }
}

struct CurieRunReader {
    reader: BufReader<File>,
}

impl CurieRunReader {
    fn new(path: &Path) -> std::io::Result<Self> {
        Ok(Self {
            reader: BufReader::with_capacity(1 << 20, File::open(path)?),
        })
    }

    /// Read the next (curie_id, encoded row); None at clean EOF.
    fn next_row(&mut self) -> std::io::Result<Option<(u32, Vec<u8>)>> {
        // Read the first byte to detect EOF; a 1-byte read returns 0 only at a
        // true EOF (multi-byte `read` can legally return a short count mid-file,
        // so we mirror RunReader::read_frame and read_exact the remainder).
        let mut first = [0u8; 1];
        if self.reader.read(&mut first)? == 0 {
            return Ok(None);
        }
        let mut rest = [0u8; 3];
        self.reader.read_exact(&mut rest)?;
        let curie_id = u32::from_le_bytes([first[0], rest[0], rest[1], rest[2]]);
        let mut lb = [0u8; 4];
        self.reader.read_exact(&mut lb)?;
        let len = u32::from_le_bytes(lb) as usize;
        let mut buf = vec![0u8; len];
        self.reader.read_exact(&mut buf)?;
        Ok(Some((curie_id, buf)))
    }
}

/// Drain a thread-local curie-row buffer into a sorted-independent run file on
/// disk.  Each unique curie_id is written exactly once (one row per CURIE hash).
fn spill_curie_run(
    local: &mut Vec<(u32, CurieRow)>,
    spill_dir: &Path,
    run_id: usize,
) -> PyResult<PathBuf> {
    let path = spill_dir.join(format!("curie_run_{run_id:08}.bin"));
    let mut w = CurieRunWriter::new(&path).map_err(py_err)?;
    for (curie_id, row) in local.drain(..) {
        let encoded = bincode::serialize(&row).map_err(py_err)?;
        w.write_row(curie_id, &encoded).map_err(py_err)?;
    }
    w.finish().map_err(py_err)?;
    Ok(path)
}

/// K-way merge of sorted run files, grouping equal terms across runs.
struct MergeHeap {
    readers: Vec<RunReader>,
    heap: BinaryHeap<MergeItem>,
}

impl MergeHeap {
    fn new(paths: &[PathBuf]) -> std::io::Result<Self> {
        let mut readers = Vec::with_capacity(paths.len());
        let mut heap = BinaryHeap::new();
        for (idx, path) in paths.iter().enumerate() {
            let mut rr = RunReader::new(path)?;
            if let Some((term, pairs)) = rr.cur.take() {
                let hash = xxh64(term.as_bytes(), 0);
                heap.push((Reverse(hash), Reverse(term), idx, pairs));
            }
            readers.push(rr);
        }
        Ok(Self { readers, heap })
    }

    /// Return the next term with its merged, sorted, de-duplicated pairs.
    fn next_group(&mut self) -> std::io::Result<Option<TermPairs>> {
        let Some((Reverse(hash), Reverse(term), idx, pairs)) = self.heap.pop() else {
            return Ok(None);
        };
        let mut merged = pairs;
        let mut to_advance = vec![idx];
        // Group every run entry with the same (hash, term).  Hash equality is a
        // necessary precondition for term equality, so comparing the hash first
        // short-circuits the (rare) string comparison on distinct terms.
        while let Some((Reverse(h), Reverse(t), _, _)) = self.heap.peek() {
            if *h != hash || *t != term {
                break;
            }
            let (_, _, i, p) = self.heap.pop().unwrap();
            merged.extend(p);
            to_advance.push(i);
        }
        merged.sort_unstable();
        merged.dedup();
        for i in to_advance {
            self.readers[i].advance()?;
            if let Some((t2, p2)) = self.readers[i].cur.take() {
                let h2 = xxh64(t2.as_bytes(), 0);
                self.heap.push((Reverse(h2), Reverse(t2), i, p2));
            }
        }
        Ok(Some((term, merged)))
    }
}

// ---------------------------------------------------------------------------
// Phase 1: disk-backed equivalents index (sorted file + mmap)
//
// Instead of holding a ~200 GB HashMap<String, Vec<String>> in RAM, we write
// equivalents to a sorted binary file and mmap it.  An in-memory sorted index
// of (hash, offset) pairs (~14 GB for 860 M entries) drives O(log n) binary
// search lookups; the OS page-cache manages the mmap'd string data.
// ---------------------------------------------------------------------------

/// An entry from an equivalents run file: (hash, key, equivs).
type EquivEntry = (u64, String, Vec<String>);
/// Write an equiv entry to a buffered writer (run-file format with hash).
#[allow(clippy::cast_possible_truncation)] // equiv run format stores key/equiv counts and byte lengths as u32
fn write_equiv_entry(
    w: &mut impl Write,
    hash: u64,
    key: &str,
    equivs: &[String],
) -> std::io::Result<()> {
    w.write_all(&hash.to_le_bytes())?;
    let kb = key.as_bytes();
    debug_assert!(u32::try_from(kb.len()).is_ok() && u32::try_from(equivs.len()).is_ok());
    w.write_all(&(kb.len() as u32).to_le_bytes())?;
    w.write_all(kb)?;
    w.write_all(&(equivs.len() as u32).to_le_bytes())?;
    for equiv in equivs {
        let eb = equiv.as_bytes();
        w.write_all(&(eb.len() as u32).to_le_bytes())?;
        w.write_all(eb)?;
    }
    Ok(())
}

/// Read one equiv entry from a buffered reader.  Returns None at clean EOF.
fn read_equiv_entry(r: &mut impl BufRead) -> std::io::Result<Option<EquivEntry>> {
    if r.fill_buf()?.is_empty() {
        return Ok(None);
    }
    let mut hb = [0u8; 8];
    r.read_exact(&mut hb)?;
    let hash = u64::from_le_bytes(hb);
    let mut kl = [0u8; 4];
    r.read_exact(&mut kl)?;
    let key_len = u32::from_le_bytes(kl) as usize;
    let mut key_buf = vec![0u8; key_len];
    r.read_exact(&mut key_buf)?;
    let key = String::from_utf8(key_buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let mut ec = [0u8; 4];
    r.read_exact(&mut ec)?;
    let equiv_count = u32::from_le_bytes(ec) as usize;
    let mut equivs = Vec::with_capacity(equiv_count);
    for _ in 0..equiv_count {
        let mut el = [0u8; 4];
        r.read_exact(&mut el)?;
        let elen = u32::from_le_bytes(el) as usize;
        let mut ebuf = vec![0u8; elen];
        r.read_exact(&mut ebuf)?;
        equivs.push(
            String::from_utf8(ebuf)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?,
        );
    }
    Ok(Some((hash, key, equivs)))
}

/// A buffered reader over one sorted equiv run file (see `write_equiv_entry`).
struct EquivRunReader {
    reader: BufReader<File>,
    cur: Option<EquivEntry>,
}

impl EquivRunReader {
    fn new(path: &Path) -> std::io::Result<Self> {
        let mut reader = BufReader::with_capacity(8 * 1024 * 1024, File::open(path)?);
        let cur = read_equiv_entry(&mut reader)?;
        Ok(Self { reader, cur })
    }

    fn advance(&mut self) -> std::io::Result<()> {
        self.cur = read_equiv_entry(&mut self.reader)?;
        Ok(())
    }
}

/// Heap item for the equiv k-way merge: (Reverse(hash), Reverse(key), reader
/// index, equivs).  `Reverse` makes the `BinaryHeap` (a max-heap) pop the
/// smallest (hash, key) first, matching each run's on-disk sort order.
type EquivMergeItem = (Reverse<u64>, Reverse<String>, usize, Vec<String>);

/// K-way merge of sorted equiv run files, grouping equal (hash, key) entries
/// across runs — the equivalents analog of `MergeHeap` (term runs).  Streams
/// one merged group at a time so Phase 1b never materializes all equivalents
/// in RAM; peak memory is the (hash, offset) index plus one buffered entry per
/// run, preserving the Phase 1a spill bound for the ~200 GB case.
struct EquivMergeHeap {
    readers: Vec<EquivRunReader>,
    heap: BinaryHeap<EquivMergeItem>,
}

impl EquivMergeHeap {
    fn new(paths: &[PathBuf]) -> std::io::Result<Self> {
        let mut readers = Vec::with_capacity(paths.len());
        let mut heap = BinaryHeap::new();
        for (idx, path) in paths.iter().enumerate() {
            let mut rr = EquivRunReader::new(path)?;
            if let Some((hash, key, equivs)) = rr.cur.take() {
                heap.push((Reverse(hash), Reverse(key), idx, equivs));
            }
            readers.push(rr);
        }
        Ok(Self { readers, heap })
    }

    /// Return the next (hash, key) group with its merged (unsorted) equivalents.
    fn next_group(&mut self) -> std::io::Result<Option<EquivEntry>> {
        let Some((Reverse(hash), Reverse(key), idx, equivs)) = self.heap.pop() else {
            return Ok(None);
        };
        let mut merged = equivs;
        let mut to_advance = vec![idx];
        // Group every run entry with the same (hash, key).  Hash equality is a
        // necessary precondition for key equality, so comparing the hash first
        // short-circuits the (rare) string comparison on distinct keys.
        while let Some((Reverse(h), Reverse(k), _, _)) = self.heap.peek() {
            if *h != hash || *k != key {
                break;
            }
            let (_, _, i, e) = self.heap.pop().expect("peeked heap item disappeared");
            merged.extend(e);
            to_advance.push(i);
        }
        for i in to_advance {
            self.readers[i].advance()?;
            if let Some((h2, k2, e2)) = self.readers[i].cur.take() {
                self.heap.push((Reverse(h2), Reverse(k2), i, e2));
            }
        }
        Ok(Some((hash, key, merged)))
    }
}

/// Drain a thread-local equivalents buffer into a sorted run file on disk.
/// Called both mid-file (when the buffer exceeds the spill threshold) and at
/// end-of-file, bounding per-thread anonymous memory during Phase 1a so a
/// single huge class file (e.g. gene_nodes, ~200 M rows) cannot blow up RAM.
fn spill_equiv_local(
    local: &mut HashMap<String, Vec<String>>,
    equiv_dir: &Path,
    run_counter: &AtomicUsize,
    run_paths: &RwLock<Vec<PathBuf>>,
) -> PyResult<()> {
    if local.is_empty() {
        return Ok(());
    }
    let mut entries: Vec<EquivEntry> = local
        .drain()
        .map(|(key, mut ev)| {
            ev.sort();
            ev.dedup();
            (xxh64(key.as_bytes(), 0), key, ev)
        })
        .collect();
    // Sort by (hash, key) so each run's stream matches the k-way merge heap's
    // ordering — required for correct grouping if a hash collision lands two
    // distinct CURIEs in the same run.
    entries.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    let run_id = run_counter.fetch_add(1, Ordering::Relaxed);
    let rp = equiv_dir.join(format!("run_{run_id:08}.bin"));
    let mut w = BufWriter::with_capacity(8 * 1024 * 1024, File::create(&rp).map_err(py_err)?);
    for (h, k, e) in &entries {
        write_equiv_entry(&mut w, *h, k, e).map_err(py_err)?;
    }
    w.flush().map_err(py_err)?;
    run_paths.write().unwrap().push(rp);
    Ok(())
}

/// Disk-backed equivalents lookup: sorted hash index + mmap'd string data.
struct EquivIndex {
    /// Sorted xxh64 hashes (binary-search target).
    hashes: Vec<u64>,
    /// Byte offsets into `data` for each entry.
    offsets: Vec<u64>,
    /// Mmap'd data file: entries packed as
    /// `[u32 key_len][key_bytes][u32 equiv_count][u32 elen][equiv_bytes]…`
    data: memmap2::Mmap,
    /// Kept alive for the mmap.
    _file: File,
    /// Path to the data file (for cleanup on drop).
    data_path: PathBuf,
}

impl Drop for EquivIndex {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.data_path);
    }
}

/// Zero-copy iterator over equivalent strings from the mmap'd data.
struct EquivIter<'a> {
    data: &'a [u8],
    pos: usize,
    remaining: u32,
}

impl<'a> Iterator for EquivIter<'a> {
    type Item = &'a str;
    fn next(&mut self) -> Option<&'a str> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        let d = &self.data[self.pos..];
        let len = u32::from_le_bytes(d.get(..4)?.try_into().ok()?) as usize;
        self.pos += 4 + len;
        std::str::from_utf8(d.get(4..4 + len)?).ok()
    }
}

impl EquivIndex {
    /// Build the disk-backed equivalents index from class files.
    fn build(
        classes: &[PathBuf],
        spill_dir: &Path,
        progress: Option<&Arc<Progress>>,
    ) -> PyResult<Self> {
        let equiv_dir = spill_dir.join("equiv");
        std::fs::create_dir_all(&equiv_dir).map_err(py_err)?;

        // Phase 1a: parallel read class files → sorted spill runs.
        let run_counter = AtomicUsize::new(0);
        let run_paths: RwLock<Vec<PathBuf>> = RwLock::new(Vec::new());
        let total = classes.len() as u64;
        let done = AtomicUsize::new(0);

        if classes.is_empty() {
            // No class files — write an empty data file.
            let data_path = spill_dir.join("equiv_data.bin");
            std::fs::write(&data_path, b"").map_err(py_err)?;
            let file = File::open(&data_path).map_err(py_err)?;
            let data = unsafe { memmap2::Mmap::map(&file).map_err(py_err)? };
            return Ok(Self {
                hashes: Vec::new(),
                offsets: Vec::new(),
                data,
                _file: file,
                data_path,
            });
        }

        let equiv_spill = env_usize(
            "TABLASSERT_FULLMAP_EQUIV_SPILL_ENTRIES",
            DEFAULT_EQUIV_SPILL_ENTRIES,
        );

        let results: Vec<PyResult<()>> = classes
            .par_iter()
            .map(|path| {
                let mut local: HashMap<String, Vec<String>> = HashMap::new();
                for_class_lines(path, |row| {
                    if let Some((id, equivs)) = class_id_and_equivalents(row) {
                        local.entry(id).or_default().extend(equivs);
                        // Bound per-thread memory: spill a sorted run once the
                        // buffer exceeds the threshold instead of holding an
                        // entire class file (gene_nodes ≈ 200 M rows) in RAM.
                        if local.len() >= equiv_spill {
                            spill_equiv_local(&mut local, &equiv_dir, &run_counter, &run_paths)?;
                        }
                    }
                    Ok(())
                })?;
                spill_equiv_local(&mut local, &equiv_dir, &run_counter, &run_paths)?;

                if let Some(p) = progress {
                    let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                    p.call(
                        0,
                        n as u64,
                        total,
                        &path
                            .file_name()
                            .map(|s| s.to_string_lossy().into_owned())
                            .unwrap_or_default(),
                    );
                }
                Ok(())
            })
            .collect();
        for r in results {
            r?;
        }
        let run_paths = run_paths.into_inner().unwrap();

        // Phase 1b: external k-way merge of the sorted runs → write data file + build index.
        let data_path = spill_dir.join("equiv_data.bin");
        let mut dw =
            BufWriter::with_capacity(8 * 1024 * 1024, File::create(&data_path).map_err(py_err)?);
        let mut hashes: Vec<u64> = Vec::new();
        let mut offsets: Vec<u64> = Vec::new();
        let mut offset: u64 = 0;

        // Stream merged (hash, key) groups off disk instead of draining every
        // run into one Vec and par_sort-ing it: that old path made peak RSS
        // O(total equivalents) again, defeating the Phase 1a spill bound for
        // the ~200 GB case in the module header.  This reuses the MergeHeap
        // k-way pattern; only the (hash, offset) index plus one buffered entry
        // per run stay in RAM (not the materialized key+equiv strings).
        let mut merge = EquivMergeHeap::new(&run_paths).map_err(py_err)?;

        // Throttled Phase-0 progress: report every 1M merged groups (not per
        // group) to bound the GIL re-acquire count. total is 0 (indeterminate).
        let mut entries_merged: u64 = 0;
        while let Some((hash, key, mut equivs)) = merge.next_group().map_err(py_err)? {
            equivs.sort();
            equivs.dedup();

            // Write data entry (no hash — it's in the index).
            let kb = key.as_bytes();
            dw.write_all(&(kb.len() as u32).to_le_bytes())
                .map_err(py_err)?;
            dw.write_all(kb).map_err(py_err)?;
            dw.write_all(&(equivs.len() as u32).to_le_bytes())
                .map_err(py_err)?;
            let mut entry_len = 4 + kb.len() + 4;
            for equiv in &equivs {
                let eb = equiv.as_bytes();
                dw.write_all(&(eb.len() as u32).to_le_bytes())
                    .map_err(py_err)?;
                dw.write_all(eb).map_err(py_err)?;
                entry_len += 4 + eb.len();
            }
            hashes.push(hash);
            offsets.push(offset);
            offset += entry_len as u64;

            entries_merged += 1;
            if entries_merged.is_multiple_of(1_000_000) {
                if let Some(p) = progress {
                    p.call(0, entries_merged, 0, "merging equivalents");
                }
            }
        }
        dw.flush().map_err(py_err)?;
        drop(dw);

        // Clean up run files (keep the merged data file).
        let _ = std::fs::remove_dir_all(&equiv_dir);

        // Mmap the data file.
        let file = File::open(&data_path).map_err(py_err)?;
        let data = unsafe { memmap2::Mmap::map(&file).map_err(py_err)? };

        Ok(Self {
            hashes,
            offsets,
            data,
            _file: file,
            data_path,
        })
    }

    /// Look up equivalents for `curie`.  Returns a zero-copy iterator over
    /// equivalent CURIE strings from the mmap'd data, or None if not found.
    fn lookup(&self, curie: &str) -> Option<EquivIter<'_>> {
        let hash = xxh64(curie.as_bytes(), 0);
        let start = self.hashes.partition_point(|&h| h < hash);
        let mut idx = start;
        while idx < self.hashes.len() && self.hashes[idx] == hash {
            let off = self.offsets[idx] as usize;
            let d = &self.data[off..];
            let kl = u32::from_le_bytes(d.get(..4)?.try_into().ok()?) as usize;
            let key = std::str::from_utf8(d.get(4..4 + kl)?).ok()?;
            if key == curie {
                let count = u32::from_le_bytes(d.get(4 + kl..8 + kl)?.try_into().ok()?);
                return Some(EquivIter {
                    data: &self.data,
                    pos: off + 8 + kl,
                    remaining: count,
                });
            }
            idx += 1;
        }
        None
    }
}

/// Process a single term through clean_and_lower → token_qc/is_dead_term →
/// `nlp::normalize_l1` → level_two and insert the resulting normalized forms
/// into `local_terms`.
///
/// The level-one KEY is `nlp::normalize_l1`'s output, which is byte-for-byte the
/// form the Python query side produces (`nlp.level_one` → `rs.normalize_terms` →
/// `normalize_l1`).  fullmap lookups are EXACT string matches, so build and query
/// must stay lock-step: any drift between the two derivations is a SILENT MISS,
/// not an error.  `normalize_l1` is fed the already-cleaned form rather than the
/// raw term: `clean_and_lower` is a fixed point (`clean_slice` re-trims until the
/// slice stops changing, and lowercasing an already-lowercase ASCII string
/// borrows), so the internal re-clean is a one-iteration no-op — the remaining
/// redundancy is one `to_lowercase` per NON-ASCII term, measured at ~2% of the
/// derivation, which the build's throughput budget absorbs (a `pub(crate)`
/// pre-cleaned entry point bought only that ~2%, so the single public
/// `normalize_l1` stays the one derivation both sides share).
///
/// The QUALITY GATES deliberately run on the cleaned+lowercased
/// PRE-normalization form (the exact string they have always seen), not on the
/// normalized key, because normalization would defeat them:
///
/// * `token_qc`'s banned-token check is a substring test for the literal
///   spellings "uncharacterized"/"hypothetical"; Porter2 stems them
///   ("hypothetical" → "hypothet"), so gating post-normalization would silently
///   stop rejecting the junk synonyms the gate exists to keep out of the DB.
///   Its `\t`/`\n`/`\r` guards are likewise only meaningful pre-normalization:
///   `normalize_l1` splits on whitespace and rejoins with single spaces, so a
///   normalized key can never contain them.
/// * `is_dead_term` is an exact-match mirror of the Python `distinct()` drop
///   regex; stemming and word-order canonicalization move terms off those
///   spellings ("not applicable" → "applic not", "variable" → "variabl"), so
///   gating only post-normalization would start indexing sentinel phrases.
///
/// The SECOND `is_dead_term`, on the normalized key, is the other half of the
/// same contract: `distinct()` filters the NORMALIZED query terms, so a key that
/// only becomes dead once normalized ("Nones" → "none", or a duplicate-token
/// sentinel collapsing onto one) can never be queried and storing it is dead
/// weight.  Both gates reject a superset of what the pre-normalization gate
/// alone rejected, so no term that used to be dropped is now indexed.
///
/// Allocations: clean/level_one/level_two borrow in the common case, and
/// `normalize_l1` borrows its input for an unchanged single token; a MULTI-token
/// term additionally pays the normalizer's token `Vec` plus the joined `String`
/// (inherent to sorting/deduping), and each surviving form pays one
/// `into_owned()` when it is inserted as a map key.
fn emit_term<S: BuildHasher>(
    term: &str,
    pair: (u32, u8),
    local_terms: &mut HashMap<String, Vec<(u32, u8)>, S>,
) {
    let cleaned = clean_and_lower(term);
    if !token_qc(&cleaned) || is_dead_term(&cleaned) {
        return;
    }
    let l1 = crate::nlp::normalize_l1(&cleaned);
    if is_dead_term(&l1) {
        return;
    }
    // Derive the (optional) level-two key while `l1` is still borrowable; the
    // block yields an owned String (allocating only when level-two differs and
    // survives QC) so `l1` can be moved into its own entry afterwards.
    let l2_key: Option<String> = {
        let l2 = level_two(&l1);
        if l2 != l1.as_ref() && token_qc(&l2) && !is_dead_term(&l2) {
            Some(l2.into_owned())
        } else {
            None
        }
    };
    local_terms.entry(l1.into_owned()).or_default().push(pair);
    if let Some(l2) = l2_key {
        local_terms.entry(l2).or_default().push(pair);
    }
}

// ---------------------------------------------------------------------------
// Phases 2+3: single-pass synonym processing.
//
// Intra-file parallelism via a producer-consumer pipeline (NOT parallel
// shards — the output is still a single redb file).  A small pool of PRODUCER
// threads decompresses/reads the synonym files and pushes bounded line-chunks
// through a channel; `worker_count` WORKER threads pull chunks and process the
// rows in parallel.  Because every worker draws from one shared queue, the
// large files (protein/smallmolecule/gene/drugchemicalconflated) are processed
// by ALL workers rather than one thread each, while small files still overlap.
//
// Each worker keeps persistent term / curie-row buffers that spill sorted runs
// to disk at `local_spill` / `curie_spill`, exactly as before, so the run count
// stays bounded by total_terms / local_spill (independent of chunking).  All
// shared dimension/CURIE state is concurrent (sharded maps + atomics + RwLock).
// ---------------------------------------------------------------------------

/// Concurrent state shared across all synonym-phase workers (all interior-
/// mutable / atomic, so it is shared by reference).
struct SynonymShared<'a> {
    prefix_map: &'a ShardedMap<u16>,
    prefix_counter: &'a AtomicU32,
    category_map: &'a ShardedMap<u16>,
    category_counter: &'a AtomicU32,
    curie_map: &'a CurieIdMap,
    curie_counter: &'a AtomicU32,
    equivalents: &'a EquivIndex,
    exclude_prefixes: &'a HashSet<String>,
    taxon_allowlist: Option<&'a HashSet<i32>>,
    spill_dir: &'a Path,
    local_spill: usize,
    curie_spill: usize,
    /// On-disk RECORDS shard count; term spill runs are partitioned across this
    /// many per-shard lists at write time (see `spill_run`).
    shard_count: usize,
    run_counter: &'a AtomicUsize,
    curie_run_counter: &'a AtomicUsize,
    /// Per-shard term spill-run files: `run_paths[shard]` holds only the runs
    /// whose terms hash to `shard`, so Phase 4 can merge each shard independently.
    run_paths: &'a RwLock<Vec<Vec<PathBuf>>>,
    curie_run_paths: &'a RwLock<Vec<PathBuf>>,
}

/// A worker's private, non-shared accumulation buffers.
#[derive(Default)]
struct WorkerBuf {
    terms: FxHashMap<String, Vec<(u32, u8)>>,
    curie_rows: Vec<(u32, CurieRow)>,
}

/// Process one parsed synonym row into a worker's buffers, spilling to disk
/// when a buffer exceeds its budget.
fn process_row(
    sh: &SynonymShared<'_>,
    source_id: u8,
    row: &SynonymRow<'_>,
    buf: &mut WorkerBuf,
) -> PyResult<()> {
    let Some(curie) = row.curie.as_deref() else {
        return Ok(());
    };
    let Some((prefix, local_id)) = split_curie(curie) else {
        return Ok(());
    };
    if !retain_for_taxon_allowlist(row, sh.taxon_allowlist) {
        return Ok(());
    }
    // Skip CURIEs whose prefix the caller excludes (opt-in via
    // TABLASSERT_FULLMAP_EXCLUDE_PREFIXES) — filtered out downstream anyway.
    if sh.exclude_prefixes.contains(prefix) {
        return Ok(());
    }

    let prefix_id = sh.prefix_map.get_or_insert_with(prefix, || {
        u16::try_from(sh.prefix_counter.fetch_add(1, Ordering::Relaxed))
            .expect("too many fullmap prefixes")
    });
    let category_name = first_category(row);
    let category_id = sh.category_map.get_or_insert_with(&category_name, || {
        u16::try_from(sh.category_counter.fetch_add(1, Ordering::Relaxed))
            .expect("too many fullmap categories")
    });
    let preferred_name = row.preferred_name.as_deref().unwrap_or(curie);
    let taxon_id = first_taxon(row);

    let mut is_new = false;
    let curie_hash = xxh3_128(curie.as_bytes());
    let curie_id = sh.curie_map.get_or_insert_with(curie_hash, || {
        is_new = true;
        sh.curie_counter.fetch_add(1, Ordering::Relaxed)
    });
    if is_new {
        buf.curie_rows.push((
            curie_id,
            CurieRow {
                prefix_id,
                local_id: local_id.to_string(),
                preferred_name: clean(preferred_name).into_owned(),
                category_id,
                taxon_id,
            },
        ));
    }

    let pair = (curie_id, source_id);
    // Borrow each name as a &str straight from the decoded row; emit_term only
    // needs a &str (the Cow derefs).
    for name in &row.names {
        emit_term(name, pair, &mut buf.terms);
    }
    emit_term(curie, pair, &mut buf.terms);
    if let Some(iter) = sh.equivalents.lookup(curie) {
        for equiv in iter {
            emit_term(equiv, pair, &mut buf.terms);
        }
    }

    if buf.terms.len() >= sh.local_spill {
        let run_id = sh.run_counter.fetch_add(1, Ordering::Relaxed);
        let runs = spill_run(&mut buf.terms, sh.spill_dir, run_id, sh.shard_count)?;
        let mut paths = sh.run_paths.write().unwrap();
        for (shard, p) in runs {
            paths[shard].push(p);
        }
    }
    if buf.curie_rows.len() >= sh.curie_spill {
        let run_id = sh.curie_run_counter.fetch_add(1, Ordering::Relaxed);
        let p = spill_curie_run(&mut buf.curie_rows, sh.spill_dir, run_id)?;
        sh.curie_run_paths.write().unwrap().push(p);
    }
    Ok(())
}

/// Drain a worker's remaining buffers to final spill runs.
fn flush_buf(sh: &SynonymShared<'_>, buf: &mut WorkerBuf) -> PyResult<()> {
    if !buf.terms.is_empty() {
        let run_id = sh.run_counter.fetch_add(1, Ordering::Relaxed);
        let runs = spill_run(&mut buf.terms, sh.spill_dir, run_id, sh.shard_count)?;
        let mut paths = sh.run_paths.write().unwrap();
        for (shard, p) in runs {
            paths[shard].push(p);
        }
    }
    if !buf.curie_rows.is_empty() {
        let run_id = sh.curie_run_counter.fetch_add(1, Ordering::Relaxed);
        let p = spill_curie_run(&mut buf.curie_rows, sh.spill_dir, run_id)?;
        sh.curie_run_paths.write().unwrap().push(p);
    }
    Ok(())
}

/// Producer: read one synonym file (gz or plain), group non-empty lines into
/// byte-bounded chunks (~`chunk_bytes` bytes each), and send each chunk (tagged
/// with its source id) into the channel as raw line bytes.  Decompression happens
/// here; JSON parsing/processing happens in the workers.  The bounded channel
/// provides backpressure so a fast decompressor cannot buffer a whole giant file
/// in RAM.
#[allow(clippy::too_many_arguments)]
fn produce_file(
    path: &Path,
    tx: &SyncSender<LineChunk>,
    source_ids: &FxHashMap<String, u8>,
    chunk_bytes: usize,
    progress: Option<&Arc<Progress>>,
    files_done: &AtomicUsize,
    rows_done: &AtomicU64,
    total_files: usize,
) -> PyResult<()> {
    let src_name = source_name(path);
    let source_id = *source_ids
        .get(&src_name)
        .ok_or_else(|| PyRuntimeError::new_err(format!("uninterned source {src_name}")))?;

    // Read into a reusable byte buffer via `read_until` instead of
    // `BufReader::lines()`: the latter allocates a fresh `String` per line AND
    // runs a UTF-8 validation pass, ~1.4B times over a full build.  Here the
    // buffer is allocated once and reused across lines; each non-empty line is
    // pushed into the chunk as an owned `Vec<u8>` that workers parse straight
    // from the bytes (`serde_json::from_slice`), so no per-line `String` exists.
    let mut reader = BufReader::new(open_reader(path)?);
    let mut line: Vec<u8> = Vec::with_capacity(8192);
    let mut chunk: Vec<Vec<u8>> = Vec::new();
    let mut chunk_len: usize = 0;
    // Rows accumulated in the current (not-yet-flushed) chunk; folded into the
    // shared cumulative counter at each flush and at EOF.
    let mut chunk_rows: u64 = 0;
    loop {
        line.clear();
        let n = reader.read_until(b'\n', &mut line).map_err(py_err)?;
        if n == 0 {
            break; // clean EOF
        }
        while line.last() == Some(&b'\n') || line.last() == Some(&b'\r') {
            line.pop();
        }
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        chunk_rows += 1;
        chunk_len += line.len();
        chunk.push(line.clone());
        // Flush once the chunk reaches the byte budget (bounds per-chunk memory
        // regardless of how long individual lines are).
        if chunk_len >= chunk_bytes {
            tx.send((source_id, std::mem::take(&mut chunk)))
                .map_err(py_err)?;
            chunk_len = 0;
            // Per-chunk Phase-1 progress (the big granularity win): report the
            // CUMULATIVE rows processed across the WHOLE synonym phase — a single
            // monotonic unit shared by every producer via `rows_done` — with
            // total=0 (indeterminate; no cheap global row total exists).  A
            // per-file row count here would clash with a file-index count on the
            // same phase's single high-water mark and overflow the bar.
            if let Some(p) = progress {
                let global = rows_done.fetch_add(chunk_rows, Ordering::Relaxed) + chunk_rows;
                p.call(1, global, 0, &format!("{src_name} · {global} rows"));
            }
            chunk_rows = 0;
        }
    }
    if !chunk.is_empty() {
        tx.send((source_id, chunk)).map_err(py_err)?;
    }

    // EOF: fold the final partial chunk's rows into the cumulative count and emit
    // a file-completion tick in the SAME unit (cumulative rows, total=0) so the
    // phase-1 high-water mark stays monotonic and never mixes in a small file
    // index.  The file count rides in the detail string only (not completed/total).
    // This tick is essential for small files that never hit a chunk flush.
    if let Some(p) = progress {
        let global = rows_done.fetch_add(chunk_rows, Ordering::Relaxed) + chunk_rows;
        let n = files_done.fetch_add(1, Ordering::Relaxed) + 1;
        p.call(
            1,
            global,
            0,
            &format!("{src_name} · {global} rows ({n}/{total_files} files)"),
        );
    }
    Ok(())
}

/// Worker: pull line-chunks from the shared receiver and process their rows
/// into a persistent private buffer, spilling as needed; flush on channel
/// close.  The receiver lock is held only for the `recv` call, never during
/// processing, so workers run in parallel.
fn worker_loop(rx: &Mutex<Receiver<LineChunk>>, sh: &SynonymShared<'_>) -> PyResult<()> {
    let mut buf = WorkerBuf::default();
    loop {
        // Lock is dropped at the end of this statement (before processing).
        let job = rx.lock().unwrap().recv();
        match job {
            Ok((source_id, lines)) => {
                for line in lines {
                    let row: SynonymRow = serde_json::from_slice(&line).map_err(py_err)?;
                    process_row(sh, source_id, &row, &mut buf)?;
                }
            }
            Err(_) => break, // all producers finished and the channel drained
        }
    }
    flush_buf(sh, &mut buf)
}

// ---------------------------------------------------------------------------
// Phases 2+3: single-pass synonym processing (parallel over synonym files)
//
// Collects dimensions (prefixes, categories, sources), assigns CURIE IDs,
// builds CurieRows, and aggregates term -> (curie_id, source_id) pairs into
// BOUNDED per-thread buffers that spill sorted runs to disk when they exceed
// `local_spill` entries.  The runs are merged in Phase 4.
// ---------------------------------------------------------------------------

/// Result of the parallel synonym-processing pass.
struct SynonymBuildResult {
    /// prefix string -> u16 id
    prefix_ids: FxHashMap<String, u16>,
    /// category string -> u16 id
    category_ids: FxHashMap<String, u16>,
    /// source string -> u8 id
    source_ids: FxHashMap<String, u8>,
    /// curie-row spill-run files holding (curie_id, encoded CurieRow)
    curie_run_paths: Vec<PathBuf>,
    /// per-shard sorted spill-run files holding term -> pairs; `run_paths[shard]`
    /// holds only the runs whose terms hash to `shard` (see `spill_run`), so
    /// Phase 4 merges each shard independently.
    run_paths: Vec<Vec<PathBuf>>,
}

#[allow(clippy::too_many_arguments)]
fn process_synonyms(
    synonyms: &[PathBuf],
    equivalents: &EquivIndex,
    spill_dir: &Path,
    local_spill: usize,
    curie_spill: usize,
    exclude_prefixes: &HashSet<String>,
    taxon_allowlist: Option<&HashSet<i32>>,
    worker_count: usize,
    shard_count: usize,
    chunk_bytes: usize,
    producers: usize,
    progress: Option<&Arc<Progress>>,
) -> PyResult<SynonymBuildResult> {
    // Pre-compute source IDs from filenames (small, deterministic).
    let mut source_ids: FxHashMap<String, u8> = FxHashMap::default();
    for path in synonyms {
        let name = source_name(path);
        let next_id = u8::try_from(source_ids.len())
            .map_err(|_| PyRuntimeError::new_err("too many fullmap sources"))?;
        source_ids.entry(name).or_insert(next_id);
    }

    // Concurrent dimension counters.
    let prefix_map: ShardedMap<u16> = ShardedMap::new();
    let prefix_counter = AtomicU32::new(0);
    let category_map: ShardedMap<u16> = ShardedMap::new();
    let category_counter = AtomicU32::new(0);

    // Concurrent CURIE ID assignment + CurieRow storage.  The dedup map is keyed
    // by xxh3_128(curie) (not the string) to bound memory at full scale.
    let curie_map = CurieIdMap::new();
    let curie_counter = AtomicU32::new(0);
    // Curie-row spill runs (bounded on-disk; see spill_curie_run).
    let curie_run_paths: RwLock<Vec<PathBuf>> = RwLock::new(Vec::new());
    let curie_run_counter = AtomicUsize::new(0);

    // Spill-run bookkeeping: one run list per shard so Phase 4 can merge each
    // shard's runs independently (write-time partitioning in `spill_run`).
    let run_paths: RwLock<Vec<Vec<PathBuf>>> = RwLock::new(vec![Vec::new(); shard_count]);
    let run_counter = AtomicUsize::new(0);
    let files_done = AtomicUsize::new(0);
    // Cumulative rows processed across the whole synonym phase, shared by every
    // producer so Phase-1 progress reports one monotonic unit (see produce_file).
    let rows_done = AtomicU64::new(0);
    let total_files = synonyms.len();

    let shared = SynonymShared {
        prefix_map: &prefix_map,
        prefix_counter: &prefix_counter,
        category_map: &category_map,
        category_counter: &category_counter,
        curie_map: &curie_map,
        curie_counter: &curie_counter,
        equivalents,
        exclude_prefixes,
        taxon_allowlist,
        spill_dir,
        local_spill,
        curie_spill,
        shard_count,
        run_counter: &run_counter,
        curie_run_counter: &curie_run_counter,
        run_paths: &run_paths,
        curie_run_paths: &curie_run_paths,
    };

    // Bounded channel of line-chunks: backpressure keeps a fast decompressor
    // from buffering a whole giant file in RAM.  Workers pull chunks in
    // parallel from one shared queue, so the large files are processed by ALL
    // workers rather than one thread each.
    let workers = worker_count.max(1);
    // A modest bound keeps workers fed while bounding the in-flight line-buffer
    // memory (bound x chunk_bytes); decompression outpaces processing, so a
    // shallow queue never starves the workers.
    let (tx, rx) = sync_channel::<LineChunk>(workers);
    let rx = Arc::new(Mutex::new(rx));

    // A handful of producers cover all files (decompression is far faster than
    // parallel processing, so only a few are needed to keep the workers fed).
    let file_idx = AtomicUsize::new(0);
    let producer_count = producers.clamp(1, total_files.max(1));

    let scope_result: PyResult<()> = std::thread::scope(|s| {
        // Copyable reference handles so each `move` closure copies the reference
        // rather than moving the underlying (non-Copy) shared state.
        let shared_ref = &shared;
        let file_idx_ref = &file_idx;
        let source_ids_ref = &source_ids;
        let files_done_ref = &files_done;
        let rows_done_ref = &rows_done;
        let mut handles: Vec<std::thread::ScopedJoinHandle<'_, PyResult<()>>> = Vec::new();

        // Workers: persistent private buffers, spilling at local_spill/curie_spill.
        for _ in 0..workers {
            let rx = Arc::clone(&rx);
            handles.push(s.spawn(move || worker_loop(&rx, shared_ref)));
        }

        // Producers: pull files from a shared counter, push line-chunks.
        for _ in 0..producer_count {
            let tx = tx.clone();
            handles.push(s.spawn(move || loop {
                let i = file_idx_ref.fetch_add(1, Ordering::Relaxed);
                if i >= total_files {
                    return Ok(());
                }
                produce_file(
                    &synonyms[i],
                    &tx,
                    source_ids_ref,
                    chunk_bytes,
                    progress,
                    files_done_ref,
                    rows_done_ref,
                    total_files,
                )?;
            }));
        }

        // Drop the main sender so the channel closes once all producer threads
        // finish; workers then drain remaining chunks and exit.
        drop(tx);

        let mut first_err: Option<PyErr> = None;
        for h in handles {
            match h.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
                Err(_) => {
                    if first_err.is_none() {
                        first_err = Some(PyRuntimeError::new_err("fullmap build worker panicked"));
                    }
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    });
    scope_result?;

    // Build final prefix_ids / category_ids maps from the sharded counters.
    let mut prefix_ids: FxHashMap<String, u16> = FxHashMap::default();
    for shard in &prefix_map.shards {
        prefix_ids.extend(shard.read().unwrap().iter().map(|(k, v)| (k.clone(), *v)));
    }
    let mut category_ids: FxHashMap<String, u16> = FxHashMap::default();
    for shard in &category_map.shards {
        category_ids.extend(shard.read().unwrap().iter().map(|(k, v)| (k.clone(), *v)));
    }

    let curie_run_paths = curie_run_paths.into_inner().unwrap();
    let run_paths = run_paths.into_inner().unwrap();

    Ok(SynonymBuildResult {
        prefix_ids,
        category_ids,
        source_ids,
        curie_run_paths,
        run_paths,
    })
}

// ---------------------------------------------------------------------------
// Phase 4: write final database — k-way merge of runs streamed into redb
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn write_final_database(
    output: &Path,
    prefix_ids: &FxHashMap<String, u16>,
    category_ids: &FxHashMap<String, u16>,
    source_ids: &FxHashMap<String, u8>,
    curie_run_paths: &[PathBuf],
    run_paths: &[Vec<PathBuf>],
    cache_bytes: usize,
    insert_batch: usize,
    shard_count: usize,
    progress: Option<&Arc<Progress>>,
    taxon_allowlist_identity: Option<&str>,
) -> PyResult<()> {
    let build_id = new_build_id();
    let build_id_str = build_id.to_string();
    let database = redb::Builder::new()
        .set_cache_size(cache_bytes)
        .create(output)
        .map_err(py_err)?;

    // Dimension tables + CURIES + META in one small transaction.
    let write = database.begin_write().map_err(py_err)?;
    {
        let mut prefix_table = write.open_table(PREFIXES).map_err(py_err)?;
        for (value, id) in prefix_ids {
            prefix_table.insert(*id, value.as_str()).map_err(py_err)?;
        }
        drop(prefix_table);

        let mut category_table = write.open_table(CATEGORIES).map_err(py_err)?;
        for (value, id) in category_ids {
            category_table.insert(*id, value.as_str()).map_err(py_err)?;
        }
        drop(category_table);

        let mut source_table = write.open_table(SOURCES).map_err(py_err)?;
        for (value, id) in source_ids {
            let encoded = bincode::serialize(&SourceRow {
                source_name: value.clone(),
            })
            .map_err(py_err)?;
            source_table
                .insert(*id, encoded.as_slice())
                .map_err(py_err)?;
        }
        drop(source_table);

        // Stream curie rows from their spill runs straight into the CURIES
        // table (never held in RAM as a whole).  Each unique curie_id appears
        // exactly once across the runs.
        let mut curie_table = write.open_table(CURIES).map_err(py_err)?;
        for path in curie_run_paths {
            let mut reader = CurieRunReader::new(path).map_err(py_err)?;
            while let Some((curie_id, encoded)) = reader.next_row().map_err(py_err)? {
                curie_table
                    .insert(curie_id, encoded.as_slice())
                    .map_err(py_err)?;
            }
        }
        drop(curie_table);

        let mut meta = write.open_table(META).map_err(py_err)?;
        meta.insert("schema", SCHEMA_VERSION).map_err(py_err)?;
        meta.insert("build_id", build_id_str.as_str())
            .map_err(py_err)?;
        let shard_count_str = shard_count.to_string();
        meta.insert("shards", shard_count_str.as_str())
            .map_err(py_err)?;
        if let Some(identity) = taxon_allowlist_identity {
            meta.insert("taxon_allowlist", identity).map_err(py_err)?;
        }
    }
    write.commit().map_err(py_err)?;

    // The primary's dims/CURIES/META transaction is now durable and `database`
    // is never touched again (Phase 4 writes only the separate shard DBs below).
    // Drop it here to release its redb cache and file lock during the long
    // parallel RECORDS write — a free memory win while the shards are built.
    // NOTE: `build_fullmap_db` unlinks all old files before this new primary is
    // committed, and this primary is committed before the new shard files below.
    // During unlink, the primary may be absent while old shards still exist;
    // after the primary appears, new shards may still be absent.
    drop(database);

    // Phase 4: one INDEPENDENT k-way merge per shard, run in parallel — one
    // thread per shard, merge + insert inline (datassert-style).  Because the
    // term spill runs were partitioned by `term_shard` AT WRITE TIME, every term
    // in shard `i`'s runs hashes to shard `i`, so each shard's merge groups its
    // terms completely with NO cross-shard coordination — there is no shared
    // producer and no per-shard channel (the single global producer that capped
    // the old design is gone).  Each thread merges only its own shard's runs,
    // hash-sorts the merged groups into bounded batches for B-tree locality,
    // inserts into its shard's RECORDS table, commits with Durability::None,
    // then does a final durable commit.  Every shard file is created above (even
    // one with zero runs), so the `shard_count`-file layout stays stable.
    let shard_databases: Vec<Database> = (0..shard_count)
        .map(|i| {
            redb::Builder::new()
                .set_cache_size(cache_bytes / shard_count)
                .create(shard_path(output, i))
                .map_err(py_err)
        })
        .collect::<PyResult<_>>()?;

    // Owned per-thread progress handle (cheap Arc clone); `Progress::call`
    // re-acquires the GIL via Python::attach, which is safe from many threads.
    let progress: Option<Arc<Progress>> = progress.map(Arc::clone);
    // Estimate a Phase-2 progress total ONLY when a progress callback is present
    // (tests / no-bar builds skip this read pass entirely): the number of
    // term-frames across every shard's spill runs.  This is an UPPER BOUND on the
    // records written (a term recurs across runs and is merged), so the bar
    // climbs toward ~100% during the merge and the final emission below snaps
    // completed to the exact written count.
    let total_estimate: u64 = if progress.is_some() {
        run_paths
            .iter()
            .flatten()
            .map(|path| count_run_frames(path).unwrap_or(0))
            .sum()
    } else {
        0
    };
    // Global record counter shared across shard writer threads.  Its VALUE is
    // monotonic (each thread adds its flushed batch); the delivery ORDER of the
    // per-thread callbacks may vary, and the consumer clamps to a high-water mark.
    let global_written = AtomicU64::new(0);
    let scope_result: PyResult<u64> = std::thread::scope(|s| {
        let mut handles: Vec<std::thread::ScopedJoinHandle<'_, PyResult<u64>>> = Vec::new();
        for i in 0..shard_count {
            // Each thread owns its shard DB handle, its shard's run list, and a
            // progress handle outright — no shared receiver or borrow — but shares
            // the global record counter so progress reports a single count.
            let db = &shard_databases[i];
            let shard_runs: Vec<PathBuf> = run_paths[i].clone();
            let progress = progress.clone();
            let global = &global_written;
            handles.push(s.spawn(move || {
                write_shard_records(
                    db,
                    &shard_runs,
                    insert_batch,
                    i,
                    global,
                    total_estimate,
                    progress.as_ref(),
                    build_id,
                )
            }));
        }

        // Join all shard threads; propagate the first error (first-err wins) and
        // sum the per-shard record counts on success.
        let mut first_err: Option<PyErr> = None;
        let mut written: u64 = 0;
        for h in handles {
            match h.join() {
                Ok(Ok(n)) => written += n,
                Ok(Err(e)) => {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
                Err(_) => {
                    if first_err.is_none() {
                        first_err = Some(PyRuntimeError::new_err("fullmap shard writer panicked"));
                    }
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(written),
        }
    });
    let written = scope_result?;

    if let Some(p) = progress {
        p.call(2, written, written, &format!("wrote {written} records"));
    }

    Ok(())
}

/// One shard's complete Phase-4 work: k-way merge ONLY this shard's spill runs
/// (every term in them hashes to this shard, so the merge groups each term fully
/// with no cross-shard coordination), insert the merged groups into the shard's
/// RECORDS table in hash-sorted `insert_batch` batches for B-tree locality, then
/// commit (Durability::None) and do a final durable commit to persist the pages.
/// Returns the number of records written to this shard.
///
/// A shard with zero runs still opens and commits an empty RECORDS table, so an
/// empty shard DB is produced and the `shard_count`-file layout stays stable.
#[allow(clippy::too_many_arguments)]
fn write_shard_records(
    database: &Database,
    run_paths: &[PathBuf],
    insert_batch: usize,
    shard_index: usize,
    global_written: &AtomicU64,
    total: u64,
    progress: Option<&Arc<Progress>>,
    build_id: u64,
) -> PyResult<u64> {
    let mut write = database.begin_write().map_err(py_err)?;
    // redb ≥ 3 returns `Result`; it only fails if a persistent savepoint was
    // modified in the transaction.  This crate never uses savepoints (and this
    // is a fresh transaction), so the error is unreachable here.
    write.set_durability(Durability::None).map_err(py_err)?;
    let build_id_str = build_id.to_string();
    let mut meta = write.open_table(META).map_err(py_err)?;
    meta.insert("build_id", build_id_str.as_str())
        .map_err(py_err)?;
    drop(meta);
    let mut table = write.open_table(RECORDS).map_err(py_err)?;
    let mut merge = MergeHeap::new(run_paths).map_err(py_err)?;
    // Last hash inserted into this shard, carried ACROSS batch flushes so a
    // duplicate xxh64 hash straddling a batch boundary is caught (the cursor
    // requires strictly ascending keys; see `flush_shard_batch`).
    let mut last_hash: Option<u64> = None;
    // Pre-size the batch to the flush threshold so it never regrows (each
    // doubling copied up to ~80 MB of accumulated records at the default 2M
    // batch).  insert_batch == 0 (unbounded) yields a zero-capacity Vec.
    let mut batch: Vec<(u64, Vec<u8>)> = Vec::with_capacity(insert_batch);
    // Reusable bincode scratch buffer: serialize_into reuses one right-sized
    // allocation across the ~1-2B merged groups instead of bincode::serialize
    // growing a fresh Vec per record.  The encoded bytes are cloned into the
    // batch (they must outlive the next clear), but the clone is a memcpy from a
    // pre-sized buffer, not a from-scratch allocation + growth.
    let mut enc_buf: Vec<u8> = Vec::with_capacity(256);
    let mut written: u64 = 0;
    loop {
        let Some((term, pairs)) = merge.next_group().map_err(py_err)? else {
            break;
        };
        let hash = xxh64(term.as_bytes(), 0);
        enc_buf.clear();
        bincode::serialize_into(&mut enc_buf, &(term.as_str(), &pairs)).map_err(py_err)?;
        batch.push((hash, enc_buf.clone()));
        if insert_batch > 0 && batch.len() >= insert_batch {
            let flushed = flush_shard_batch(&mut table, &mut batch, &mut last_hash)?;
            written += flushed;
            report_shard_progress(progress, global_written, total, shard_index, flushed);
        }
    }
    let flushed = flush_shard_batch(&mut table, &mut batch, &mut last_hash)?;
    written += flushed;
    report_shard_progress(progress, global_written, total, shard_index, flushed);
    drop(table);
    write.commit().map_err(py_err)?;
    // Final durable commit to persist all pages.
    let write = database.begin_write().map_err(py_err)?;
    write.commit().map_err(py_err)?;
    Ok(written)
}

/// Sort the shard's pending batch by hash and insert it into the shard's RECORDS
/// table through an end-of-table CURSOR, returning the number of records
/// flushed.  Hash-sorted inserts are near-sequential B-tree appends; the redb
/// `experimental_cursor` API (`upper_bound_mut` + `insert_before`) is ~3x
/// faster than per-key `insert()` for exactly this ascending bulk-load pattern
/// (and pairs with redb's ascending-insert page optimization for ~half-size
/// shard files).  Clearing the buffer keeps memory bounded.
///
/// The cursor requires STRICTLY ascending keys and never overwrites
/// (`StorageError::UnorderedKey` on a duplicate), while `Table::insert`
/// replaces an existing key — so a duplicate xxh64 hash (two DISTINCT terms
/// colliding; ~6% odds per full build at ~1.5 B terms) closes the cursor,
/// falls back to one plain `insert()` (preserving the pre-cursor overwrite
/// semantics for that record), and reopens the cursor at the end of the table.
///
/// `last` carries the highest hash inserted so far ACROSS batch flushes
/// (owned by `write_shard_records`), so a collision straddling a batch
/// boundary is caught too — not just duplicates within one batch.
fn flush_shard_batch(
    table: &mut redb::Table<u64, &[u8]>,
    batch: &mut Vec<(u64, Vec<u8>)>,
    last: &mut Option<u64>,
) -> PyResult<u64> {
    if batch.is_empty() {
        return Ok(0);
    }
    batch.sort_unstable_by_key(|(hash, _)| *hash);
    let mut cursor = table
        .upper_bound_mut(std::ops::Bound::<u64>::Unbounded)
        .map_err(py_err)?;
    for (hash, enc) in batch.iter() {
        if last.is_some_and(|h| *hash <= h) {
            // Duplicate hash (an xxh64 collision between distinct terms): the
            // cursor cannot express the historical overwrite, so fall back to a
            // plain insert for this one record.
            cursor.close().map_err(py_err)?;
            table.insert(*hash, enc.as_slice()).map_err(py_err)?;
            cursor = table
                .upper_bound_mut(std::ops::Bound::<u64>::Unbounded)
                .map_err(py_err)?;
        } else {
            cursor
                .insert_before(*hash, enc.as_slice())
                .map_err(py_err)?;
        }
        *last = Some(*hash);
    }
    cursor.close().map_err(py_err)?;
    let flushed = batch.len() as u64;
    batch.clear();
    Ok(flushed)
}

/// Count the term-frames in one spill-run file WITHOUT allocating the pairs:
/// parse the `[u32 term_len][term][u32 pair_count][5*pair_count]` frame headers
/// and skip the payloads.  Used ONLY to estimate a Phase-2 progress total (and
/// only when a progress callback is present).  The count is an UPPER BOUND on the
/// records actually written: a term recurs across runs and is merged/deduped, so
/// the bar climbs toward ~100% during the merge and the final emission snaps
/// completed to the exact written count.  Mirrors `RunReader::read_frame`'s
/// 1-byte EOF detection so a clean trailing read returns the exact count.
fn count_run_frames(path: &Path) -> std::io::Result<u64> {
    let mut r = BufReader::with_capacity(1 << 20, File::open(path)?);
    let mut count: u64 = 0;
    loop {
        let mut first = [0u8; 1];
        if r.read(&mut first)? == 0 {
            return Ok(count);
        }
        let mut rest = [0u8; 3];
        r.read_exact(&mut rest)?;
        let term_len = u32::from_le_bytes([first[0], rest[0], rest[1], rest[2]]) as u64;
        {
            let mut skip = (&mut r).take(term_len);
            std::io::copy(&mut skip, &mut std::io::sink())?;
        }
        let mut cb = [0u8; 4];
        r.read_exact(&mut cb)?;
        let pair_count = u32::from_le_bytes(cb) as u64;
        {
            let mut skip = (&mut r).take(pair_count * 5);
            std::io::copy(&mut skip, &mut std::io::sink())?;
        }
        count += 1;
    }
}

/// Report Phase-2 progress for a flushed batch: add `flushed` to the shared
/// global counter and emit `(2, global, total, "shard {i}: writing {global}")`.
/// The atomic counter VALUE is monotonic (it only grows), but the ORDER in which
/// concurrent shard threads DELIVER these calls to Python is not guaranteed to be
/// sorted (scheduling between `fetch_add` and the call can deliver e.g. 150 then
/// 100).  That is harmless: the consumer (progress.py) clamps to a per-phase
/// high-water mark.  A no-op for an empty flush or when there is no callback.
fn report_shard_progress(
    progress: Option<&Arc<Progress>>,
    global_written: &AtomicU64,
    total: u64,
    shard_index: usize,
    flushed: u64,
) {
    if flushed == 0 {
        return;
    }
    if let Some(p) = progress {
        let global = global_written.fetch_add(flushed, Ordering::Relaxed) + flushed;
        p.call(
            2,
            global,
            total,
            &format!("shard {shard_index}: writing {global} records"),
        );
    }
}

// ---------------------------------------------------------------------------
// Build orchestrator
// ---------------------------------------------------------------------------

/// Route a normalized term to its on-disk RECORDS shard index via xxh64 masked
/// to the shard count.  `shard_count` must be a power of two.  The same hash is
/// the RECORDS key, so a term's shard and its key are derived from one xxh64 call
/// site each (write and read agree).  This is the single routing oracle shared by
/// the writer and the reader.
#[allow(clippy::cast_possible_truncation)] // xxh64->usize: only the low bits feed the shard mask, so truncation is the intended routing
fn term_shard(term: &str, shard_count: usize) -> usize {
    (xxh64(term.as_bytes(), 0) as usize) & (shard_count - 1)
}

/// Sibling shard file path for a primary DB path: `.../fullmap.redb` ->
/// `.../fullmap.s{index}.redb` (same directory, primary file stem preserved).
fn shard_path(primary: &Path, index: usize) -> PathBuf {
    let stem = primary
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "fullmap".to_string());
    let ext = primary
        .extension()
        .map(|e| e.to_string_lossy().into_owned())
        .unwrap_or_else(|| "redb".to_string());
    let name = format!("{stem}.s{index}.{ext}");
    match primary.parent() {
        Some(dir) => dir.join(name),
        None => PathBuf::from(name),
    }
}

fn evict_cached_path(path: &Path) -> PyResult<()> {
    let Some(cache) = DB_CACHE.get() else {
        return Ok(());
    };
    let mut map = cache.write().map_err(py_err)?;
    let primary = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    map.remove(&primary);
    // Evict every shard handle too so a rebuild never reads a stale shard file.
    for index in 0..SHARD_COUNT_SHARDS {
        let shard = shard_path(path, index);
        let canonical = std::fs::canonicalize(&shard).unwrap_or(shard);
        map.remove(&canonical);
    }
    Ok(())
}

fn cache_database(path: &Path, database: Arc<ReadOnlyDatabase>) -> PyResult<()> {
    let canonical = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let generation = generation_of(&canonical).map_err(py_err)?;
    let build_id = read_build_id(&database)?;
    let cache = DB_CACHE.get_or_init(|| RwLock::new(HashMap::new()));
    cache.write().map_err(py_err)?.insert(
        canonical,
        CachedDatabase {
            database,
            generation,
            build_id,
        },
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn build_fullmap_inner(
    output: PathBuf,
    classes: Vec<PathBuf>,
    synonyms: Vec<PathBuf>,
    worker_count: usize,
    shard_count: usize,
    progress: Option<Arc<Progress>>,
    local_spill: usize,
    curie_spill: usize,
    exclude_prefixes: HashSet<String>,
    taxon_allowlist: Option<HashSet<i32>>,
    taxon_allowlist_identity: Option<String>,
    chunk_bytes: usize,
    producers: usize,
    cache_bytes: usize,
    insert_batch: usize,
    spill_dir: PathBuf,
) -> PyResult<()> {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(worker_count)
        .build()
        .map_err(py_err)?;

    pool.install(|| {
        // Fresh spill directory for this build.
        if spill_dir.exists() {
            std::fs::remove_dir_all(&spill_dir).map_err(py_err)?;
        }
        std::fs::create_dir_all(&spill_dir).map_err(py_err)?;

        // Phase 1: disk-backed equivalents index from class files.
        let equivalents = EquivIndex::build(&classes, &spill_dir, progress.as_ref())?;

        // Phases 2+3: single-pass synonym processing with bounded spill runs.
        let result = process_synonyms(
            &synonyms,
            &equivalents,
            &spill_dir,
            local_spill,
            curie_spill,
            &exclude_prefixes,
            taxon_allowlist.as_ref(),
            worker_count,
            shard_count,
            chunk_bytes,
            producers,
            progress.as_ref(),
        )?;
        // Equivalents index (mmap + temp file) is dropped here; the data file
        // is deleted via EquivIndex::Drop.
        drop(equivalents);

        let SynonymBuildResult {
            prefix_ids,
            category_ids,
            source_ids,
            curie_run_paths,
            run_paths,
        } = result;

        // Phase 4: k-way merge of runs streamed into the final database.
        write_final_database(
            &output,
            &prefix_ids,
            &category_ids,
            &source_ids,
            &curie_run_paths,
            &run_paths,
            cache_bytes,
            insert_batch,
            shard_count,
            progress.as_ref(),
            taxon_allowlist_identity.as_deref(),
        )?;

        // Clean up spill runs on success (left in place on error for inspection).
        let _ = std::fs::remove_dir_all(&spill_dir);
        Ok::<(), PyErr>(())
    })
}

#[pyfunction]
#[pyo3(signature = (output, classes, synonyms, progress=None, taxon_allowlist=None))]
pub fn build_fullmap_db(
    py: Python<'_>,
    output: PathBuf,
    classes: Vec<PathBuf>,
    synonyms: Vec<PathBuf>,
    progress: Option<Py<PyAny>>,
    taxon_allowlist: Option<Vec<i32>>,
) -> PyResult<()> {
    if synonyms.is_empty() {
        return Err(PyValueError::new_err(
            "at least one synonym file is required",
        ));
    }

    // Phase 4 opens every spill-run file for a shard at once (MergeHeap), across
    // all shards in parallel — large corpora can need thousands of fds per
    // shard. Best-effort: raise our soft NOFILE limit to the hard limit so
    // this isn't capped by a low default (e.g. ulimit -n 1024). Never fails
    // the build if this doesn't succeed.
    let _ = rlimit::increase_nofile_limit(u64::MAX);

    let worker_count = {
        let cpus = std::thread::available_parallelism()
            .map(std::num::NonZero::get)
            .unwrap_or(1);
        // Cap at available_memory_gb / 2 to prevent swap on memory-constrained
        // machines.  Each thread uses ~400 MB of local buffers; the cap is
        // generous (2 GB/thread) to avoid limiting CPU-bound throughput.
        let avail_kb = std::fs::read_to_string("/proc/meminfo")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.starts_with("MemAvailable:"))
                    .and_then(|l| {
                        l.split_whitespace()
                            .nth(1)
                            .and_then(|v| v.parse::<usize>().ok())
                    })
            })
            .unwrap_or(0);
        if avail_kb > 0 {
            let avail_gb = avail_kb / (1024 * 1024);
            let mem_cap = (avail_gb / 2).max(1);
            cpus.min(mem_cap)
        } else {
            cpus * 9 / 10
        }
    }
    .max(1);

    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent).map_err(py_err)?;
    }
    evict_cached_path(&output)?;
    // All old files are unlinked before the new primary is written, and the new
    // primary is written before the new shards (`write_final_database`).  During
    // unlink, the primary may be absent while old shards still exist.
    if output.exists() {
        std::fs::remove_file(&output).map_err(py_err)?;
    }
    // Remove any stale shard files from a previous build so the new 4-file
    // layout is never mixed with leftover shards.
    for index in 0..SHARD_COUNT_SHARDS {
        let shard = shard_path(&output, index);
        if shard.exists() {
            std::fs::remove_file(&shard).map_err(py_err)?;
        }
    }

    let local_spill = env_usize(
        "TABLASSERT_FULLMAP_LOCAL_SPILL_ENTRIES",
        DEFAULT_LOCAL_SPILL_ENTRIES,
    );
    let curie_spill = env_usize(
        "TABLASSERT_FULLMAP_CURIE_SPILL_ENTRIES",
        DEFAULT_CURIE_SPILL_ENTRIES,
    );
    // Opt-in prefix exclusion: comma-separated CURIE prefixes dropped at build
    // time (e.g. "INCHIKEY,Publication").  Empty/unset = index everything.
    let exclude_prefixes: HashSet<String> = std::env::var("TABLASSERT_FULLMAP_EXCLUDE_PREFIXES")
        .ok()
        .map(|v| {
            v.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();
    // Intra-file parallelism tunables: lines per producer->worker chunk, and the
    // number of producer (decompressor) threads.  Decompression is far faster
    // than parallel processing, so a handful of producers keeps all workers fed.
    let chunk_bytes = env_usize("TABLASSERT_FULLMAP_CHUNK_BYTES", DEFAULT_CHUNK_BYTES);
    let default_producers = (worker_count / 4).max(4).min(synonyms.len().max(1));
    let producers = env_usize("TABLASSERT_FULLMAP_PRODUCERS", default_producers);
    let cache_bytes = env_usize(
        "TABLASSERT_FULLMAP_REDB_CACHE_BYTES",
        DEFAULT_REDB_CACHE_BYTES,
    );
    let insert_batch = env_usize("TABLASSERT_FULLMAP_INSERT_BATCH", DEFAULT_INSERT_BATCH);
    // On-disk RECORDS shard count (one redb file + concurrent writer per shard),
    // fixed at SHARD_COUNT_SHARDS (16).
    let shard_count = SHARD_COUNT_SHARDS;
    let spill_dir = std::env::var("TABLASSERT_FULLMAP_SPILL_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let mut p = output.clone().into_os_string();
            p.push(".spill.d");
            PathBuf::from(p)
        });

    let taxon_allowlist: Option<HashSet<i32>> = taxon_allowlist.map(allowlist_set);
    let taxon_allowlist_identity: Option<String> = taxon_allowlist.as_ref().map(allowlist_identity);
    let progress = progress.map(|cb| Arc::new(Progress { cb }));

    // Release the GIL for the whole build so rich's Live display thread can
    // repaint and Ctrl-C works; progress callbacks re-acquire it briefly.
    py.detach(|| {
        build_fullmap_inner(
            output.clone(),
            classes,
            synonyms,
            worker_count,
            shard_count,
            progress,
            local_spill,
            curie_spill,
            exclude_prefixes,
            taxon_allowlist,
            taxon_allowlist_identity,
            chunk_bytes,
            producers,
            cache_bytes,
            insert_batch,
            spill_dir,
        )
    })?;

    // Cache the freshly-built database for the read path (shared read-only
    // lock, so concurrent lookups — including from other processes — never
    // contend with it).
    let database = Arc::new(open_read_only(&output)?);
    cache_database(&output, database)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Read path
// ---------------------------------------------------------------------------

fn validate_schema(database: &ReadOnlyDatabase) -> PyResult<()> {
    let read = database.begin_read().map_err(py_err)?;
    let meta = read.open_table(META).map_err(py_err)?;
    let schema = meta
        .get("schema")
        .map_err(py_err)?
        .map(|x| x.value().to_string());
    match schema.as_deref() {
        Some(SCHEMA_VERSION) => Ok(()),
        Some(
            SCHEMA_VERSION_V5 | SCHEMA_VERSION_V4 | SCHEMA_VERSION_V3 | SCHEMA_VERSION_V2
            | SCHEMA_VERSION_V1,
        ) => Err(PyRuntimeError::new_err(
            "fullmap DB is outdated; rebuild with 'tablassert build-fullmap'",
        )),
        _ => Err(PyRuntimeError::new_err("unsupported fullmap redb schema")),
    }
}

// ---------------------------------------------------------------------------
// Prebuilt archive extraction
// ---------------------------------------------------------------------------

/// Size of the file buffer feeding the zstd decoder during prebuilt archive
/// extraction (~8 MiB): one large sequential read per chunk keeps the multi-GB
/// decompression IO-bound instead of syscall-bound (the same capacity
/// `RunWriter` uses for its spill writes).
const EXTRACT_READ_BUFFER_BYTES: usize = 8 * 1024 * 1024;

/// Best-effort `(detail: str)` progress report for `extract_prebuilt_fullmap`.
/// The extraction runs with the GIL released, so the GIL is re-acquired briefly
/// per call and the callback result is deliberately discarded — a failing
/// Python callback must never kill the extraction (mirrors `Progress::call`).
fn extract_report(progress: &Option<Py<PyAny>>, detail: &str) {
    if let Some(cb) = progress {
        Python::attach(|py| {
            let _ = cb.call1(py, (detail,));
        });
    }
}

/// Validated RELATIVE member path for one archive entry.
///
/// `tar::Entry::path()` returns the header path AS-IS — tar 0.4's own
/// traversal protection lives in `Entry::unpack_in`, which SILENTLY SKIPS
/// escaping members instead of failing.  The contract here is to ERROR loudly,
/// so the check is explicit: every component must be a plain name (`..`,
/// absolute roots, and drive prefixes all fail the whole extraction).
fn validated_entry_path<R: Read>(entry: &tar::Entry<'_, R>) -> PyResult<PathBuf> {
    use std::path::Component;

    let raw = entry.path().map_err(|e| {
        py_err(format!(
            "prebuilt archive has an entry with an unreadable path: {e}"
        ))
    })?;
    let mut rel = PathBuf::new();
    for component in raw.components() {
        match component {
            Component::CurDir => {} // `./` is harmless; normalize it away
            Component::Normal(part) => rel.push(part),
            other => {
                return Err(py_err(format!(
                    "prebuilt archive entry {} has an unsafe path ({other:?}); \
                     entries must be relative with no '..' components",
                    raw.display()
                )));
            }
        }
    }
    if rel.as_os_str().is_empty() {
        return Err(py_err(format!(
            "prebuilt archive entry {} has an empty path",
            raw.display()
        )));
    }
    Ok(rel)
}

/// The shard index encoded in a `.s<N>.redb` file name (matches
/// `\.s(\d+)\.redb$`), or `None` for a non-shard `.redb` (primary candidate).
fn shard_index_of_name(file_name: &str) -> Option<usize> {
    let stem = file_name.strip_suffix(".redb")?;
    let marker = stem.rfind(".s")?;
    let digits = &stem[marker + 2..];
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// Recursively collect the `.redb` files under `dir` (archive members may be
/// nested in subdirectories), classifying each as a shard (`s<N>` -> index) or
/// a primary candidate.  A duplicate shard index is an error, never a
/// silent overwrite.
fn scan_extracted_redb(
    dir: &Path,
    shards: &mut BTreeMap<usize, PathBuf>,
    primaries: &mut Vec<PathBuf>,
) -> PyResult<()> {
    let read_dir = std::fs::read_dir(dir).map_err(|e| {
        py_err(format!(
            "failed to scan extracted prebuilt archive dir {}: {e}",
            dir.display()
        ))
    })?;
    for item in read_dir {
        let item = item.map_err(py_err)?;
        let path = item.path();
        let file_type = item.file_type().map_err(py_err)?;
        if file_type.is_dir() {
            scan_extracted_redb(&path, shards, primaries)?;
        } else if file_type.is_file() && path.extension().and_then(|x| x.to_str()) == Some("redb") {
            let name = item.file_name().to_string_lossy().into_owned();
            match shard_index_of_name(&name) {
                Some(index) => {
                    if shards.contains_key(&index) {
                        return Err(py_err(format!(
                            "prebuilt archive contains a duplicate shard s{index} at {}",
                            path.display()
                        )));
                    }
                    shards.insert(index, path);
                }
                None => primaries.push(path),
            }
        }
    }
    Ok(())
}

/// Stream-extract, validate, and atomically rename one prebuilt fullmap
/// archive (the shared tail of `extract_prebuilt_fullmap`; see its doc for the
/// full contract).  On success the temp dir is removed last; the caller cleans
/// it up on error.
fn extract_validate_rename(
    archive: &Path,
    temp_dir: &Path,
    output: &Path,
    progress: &Option<Py<PyAny>>,
    taxon_allowlist: Option<&HashSet<i32>>,
) -> PyResult<()> {
    // Stream File -> 8 MiB BufReader -> zstd decoder -> tar entries; the
    // decompressed tar is NEVER materialized on disk.
    let file = File::open(archive).map_err(|e| {
        py_err(format!(
            "failed to open prebuilt archive {}: {e}",
            archive.display()
        ))
    })?;
    let mut reader = BufReader::with_capacity(EXTRACT_READ_BUFFER_BYTES, file);
    // Cheap upfront sanity: a zstd stream starts with the 4-byte frame magic
    // 0xFD2FB528 (little-endian).  `zstd::Decoder::new` is LAZY — it only
    // fails on the first read — so garbage bytes from a torn download would
    // otherwise surface as a cryptic mid-stream tar error instead of the
    // actionable "not valid zstd".
    const ZSTD_FRAME_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];
    let head = reader.fill_buf().map_err(|e| {
        py_err(format!(
            "failed to read prebuilt archive {}: {e}",
            archive.display()
        ))
    })?;
    if head.len() < ZSTD_FRAME_MAGIC.len() || !head.starts_with(&ZSTD_FRAME_MAGIC) {
        return Err(py_err(format!(
            "prebuilt archive {} is not valid zstd (missing the zstd frame magic); \
             re-download the archive or force a local rebuild with \
             'tablassert build-fullmap'",
            archive.display()
        )));
    }
    let decoder = zstd::Decoder::new(reader).map_err(|e| {
        py_err(format!(
            "prebuilt archive {} is not valid zstd: {e}",
            archive.display()
        ))
    })?;
    let mut tar_archive = tar::Archive::new(decoder);
    let entries = tar_archive.entries().map_err(|e| {
        py_err(format!(
            "failed to read tar entries from prebuilt archive {}: {e}",
            archive.display()
        ))
    })?;

    for entry in entries {
        let mut entry = entry.map_err(|e| {
            py_err(format!(
                "failed to read an entry of prebuilt archive {}: {e}",
                archive.display()
            ))
        })?;
        let rel = validated_entry_path(&entry)?;
        // tar-rs silently IGNORES `GNU.sparse.*` PAX records (bsdtar's sparse
        // encoding), so a PAX-sparse archive of redb files — which are
        // genuinely sparse because redb preallocates — would extract WRONG
        // bytes with no error.  Reject such archives loudly instead of landing
        // a silently-corrupt database.  (GNU tar's `GNUSparse` entry type does
        // NOT use these PAX records and is expanded correctly below.)
        if let Some(extensions) = entry.pax_extensions().map_err(|e| {
            py_err(format!(
                "failed to read the PAX extensions of entry {} in prebuilt archive {}: {e}",
                rel.display(),
                archive.display()
            ))
        })? {
            for extension in extensions {
                let extension = extension.map_err(|e| {
                    py_err(format!(
                        "failed to parse a PAX extension of entry {} in prebuilt archive {}: {e}",
                        rel.display(),
                        archive.display()
                    ))
                })?;
                if extension.key_bytes().starts_with(b"GNU.sparse") {
                    return Err(py_err(format!(
                        "prebuilt archive entry {} uses PAX sparse records ({}), which are \
                         unsupported: extracting them would silently land wrong bytes for the \
                         sparse file — repack with GNU tar or re-publish the archive",
                        rel.display(),
                        String::from_utf8_lossy(extension.key_bytes())
                    )));
                }
            }
        }
        extract_report(progress, &format!("extracting {}", rel.display()));
        let dest = temp_dir.join(&rel);
        match entry.header().entry_type() {
            EntryType::Directory => {
                std::fs::create_dir_all(&dest).map_err(|e| {
                    py_err(format!(
                        "failed to create directory {} from prebuilt archive: {e}",
                        dest.display()
                    ))
                })?;
            }
            // `Continuous` is the UStar 'high-performance' type, specified to
            // be treated as a regular file.  `GNUSparse` matters in practice:
            // redb pre-allocates its files, so GNU tar and rust-tar both store
            // the primary/shard DBs as sparse entries, and the tar reader
            // transparently expands them here (holes read as zeros) — the
            // landed file is the fully-materialized database.
            EntryType::Regular | EntryType::Continuous | EntryType::GNUSparse => {
                if let Some(dir) = dest.parent() {
                    std::fs::create_dir_all(dir).map_err(|e| {
                        py_err(format!(
                            "failed to create directory {} from prebuilt archive: {e}",
                            dir.display()
                        ))
                    })?;
                }
                // Copy through an 8 MiB buffer: `Entry::unpack` writes with
                // std's 8 KB default, which costs millions of write syscalls
                // on a multi-GB archive.
                let file = File::create(&dest).map_err(|e| {
                    py_err(format!(
                        "failed to create {} while extracting prebuilt archive: {e}",
                        dest.display()
                    ))
                })?;
                let mut out = BufWriter::with_capacity(EXTRACT_READ_BUFFER_BYTES, file);
                std::io::copy(&mut entry, &mut out).map_err(|e| {
                    py_err(format!(
                        "failed to extract {} from prebuilt archive {}: {e}",
                        rel.display(),
                        archive.display()
                    ))
                })?;
                out.flush().map_err(|e| {
                    py_err(format!(
                        "failed to finish writing {} from prebuilt archive: {e}",
                        dest.display()
                    ))
                })?;
                // Propagate the archived mode (best effort: an unreadable mode
                // never blocks landing the bytes).
                if let Ok(mode) = entry.header().mode() {
                    let perms = std::fs::Permissions::from_mode(mode & 0o777);
                    let _ = std::fs::set_permissions(&dest, perms);
                }
            }
            other => {
                return Err(py_err(format!(
                    "prebuilt archive entry {} has unsupported type {other:?}; \
                     the archive must contain only directories and regular \
                     (including sparse-encoded) redb files",
                    rel.display()
                )));
            }
        }
    }

    // Locate the primary + shards among the extracted members (they may be
    // nested in subdirectories inside the archive).
    let mut shards: BTreeMap<usize, PathBuf> = BTreeMap::new();
    let mut primaries: Vec<PathBuf> = Vec::new();
    scan_extracted_redb(temp_dir, &mut shards, &mut primaries)?;
    primaries.sort();
    // More than one non-shard .redb means a torn or mispackaged archive:
    // proceeding would silently discard every unchosen candidate, so refuse
    // loudly and list them.  The ONE documented exception is the prefer-case
    // itself — exactly one candidate literally named `fullmap.redb` wins over
    // any strays (preserving the historical selection rule).
    if primaries.len() > 1 {
        let named_fullmap = primaries
            .iter()
            .filter(|p| p.file_name() == Some(std::ffi::OsStr::new("fullmap.redb")))
            .count();
        if named_fullmap != 1 {
            let listing = primaries
                .iter()
                .map(|p| p.strip_prefix(temp_dir).unwrap_or(p).display().to_string())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(py_err(format!(
                "prebuilt archive {} contains {} primary .redb candidates ({listing}); exactly \
                 one non-shard .redb is required — a stray primary means a torn or mispackaged \
                 archive, so re-download it or force a local rebuild with \
                 'tablassert build-fullmap'",
                archive.display(),
                primaries.len()
            )));
        }
    }
    let primary = primaries
        .iter()
        .find(|p| p.file_name() == Some(std::ffi::OsStr::new("fullmap.redb")))
        .or_else(|| primaries.first())
        .ok_or_else(|| {
            py_err(format!(
                "prebuilt archive {} contains no primary .redb database \
                 (expected a non-shard member, preferring `fullmap.redb`)",
                archive.display()
            ))
        })?
        .clone();

    extract_report(progress, "validating");

    // Validate the extracted bundle against the force-build contract BEFORE
    // any rename, reusing the exact read-path helpers so the contract cannot
    // drift: exact v6 schema (older tags are rejected loudly), a parseable
    // build_id, the META-advertised shard count, and — when the caller
    // requires a taxon allowlist — the matching `META.taxon_allowlist`
    // identity.
    let validation_ctx = |err: PyErr| {
        py_err(format!(
            "prebuilt archive {} failed validation: {err}",
            archive.display()
        ))
    };
    let database = open_read_only(&primary).map_err(&validation_ctx)?;
    validate_schema(&database).map_err(&validation_ctx)?;
    let build_id = read_build_id(&database).map_err(&validation_ctx)?;
    let shard_count = shard_count_of(&database).map_err(&validation_ctx)?;
    let recorded_allowlist = read_taxon_allowlist_identity(&database).map_err(&validation_ctx)?;
    drop(database);

    // A published archive built WITHOUT the required filter is not the database
    // the caller asked to install: reject it here (nothing has been renamed
    // yet) so the command falls back to a filtered source build instead of
    // silently landing an unfiltered DB.
    if let Some(expected) = taxon_allowlist.map(allowlist_identity) {
        if recorded_allowlist.as_deref() != Some(expected.as_str()) {
            return Err(py_err(format!(
                "prebuilt archive {} is not filtered by the required taxon allowlist \
                 (expected META.taxon_allowlist {expected}, found {}); republish the archive \
                 from a filtered build or force a local rebuild with 'tablassert build-fullmap'",
                archive.display(),
                recorded_allowlist.unwrap_or_else(|| "no allowlist identity".to_string())
            )));
        }
    }

    // The archive must contain EXACTLY s0..s{shard_count-1}: a gap or an
    // extra shard (e.g. s16 alongside a 16-shard primary) means a torn or
    // wrong archive — refuse instead of landing a broken bundle.
    let missing: Vec<usize> = (0..shard_count)
        .filter(|index| !shards.contains_key(index))
        .collect();
    let extra: Vec<usize> = shards
        .keys()
        .copied()
        .filter(|index| *index >= shard_count)
        .collect();
    if !missing.is_empty() || !extra.is_empty() {
        return Err(py_err(format!(
            "prebuilt archive {} has an inconsistent shard set: expected s0..s{}, \
             missing {missing:?}, unexpected {extra:?} — re-download the archive or \
             force a local rebuild with 'tablassert build-fullmap'",
            archive.display(),
            shard_count - 1
        )));
    }

    // Every shard must open read-only and carry the primary's build_id
    // (mirrors the bundle-consistency invariant the read path enforces).
    for index in 0..shard_count {
        let shard = &shards[&index];
        let shard_db = open_read_only(shard).map_err(|e| {
            py_err(format!(
                "shard s{index} ({}) of prebuilt archive {} is not a readable fullmap DB: {e}",
                shard.display(),
                archive.display()
            ))
        })?;
        let shard_build_id = read_build_id(&shard_db).map_err(|e| {
            py_err(format!(
                "shard s{index} ({}) of prebuilt archive {}: {e}",
                shard.display(),
                archive.display()
            ))
        })?;
        if shard_build_id != build_id {
            return Err(py_err(format!(
                "shard s{index} of prebuilt archive {} has build_id {shard_build_id} \
                 but the primary has {build_id}; the archive is inconsistent",
                archive.display()
            )));
        }
    }

    // Every check passed: rename into place atomically (the temp dir lives on
    // the same filesystem as the output).  The primary moves first, mirroring
    // the build's commit order; rename replaces any existing file atomically.
    std::fs::rename(&primary, output).map_err(|e| {
        py_err(format!(
            "failed to move extracted primary into place at {}: {e}",
            output.display()
        ))
    })?;
    for index in 0..shard_count {
        let dest = shard_path(output, index);
        std::fs::rename(&shards[&index], &dest).map_err(|e| {
            py_err(format!(
                "failed to move extracted shard s{index} into place at {}: {e}",
                dest.display()
            ))
        })?;
    }

    // Guaranteed cleanup on success: the temp dir is now empty.
    std::fs::remove_dir_all(temp_dir).map_err(|e| {
        py_err(format!(
            "extraction succeeded but cleaning up {} failed: {e}",
            temp_dir.display()
        ))
    })?;
    Ok(())
}

/// Extract a prebuilt fullmap archive (`fullmap.tar.zst`) into `output` plus
/// its sibling shard files.
///
/// Streams `File -> BufReader -> zstd decoder -> tar entries` (the multi-GB
/// decompressed tar is never materialized on disk) into a fresh dot-prefixed
/// temp dir inside `output`'s directory (same filesystem -> atomic renames),
/// then VALIDATES the extracted bundle against the same contract the read path
/// enforces — exact v6 schema, a parseable `build_id`, the META-advertised
/// shard count with no gaps or extras, a matching `build_id` in every shard,
/// and (when `taxon_allowlist` is given) a `META.taxon_allowlist` identity
/// equal to the one a filtered build would record — BEFORE renaming anything
/// into place: primary -> `output`, shard `i` -> `<output stem>.s<i>.redb`.  A
/// failing validation therefore never lands a broken DB at the output path, and
/// the temp dir is removed on every exit path (guaranteed on success,
/// best-effort on error, so a failed extraction never leaks multi-GB
/// partials).  Runs with the GIL released; the optional `progress` callback
/// receives `(detail: str)` updates (`"opening archive"`,
/// `"extracting <entry>"`, `"validating"`) and is best-effort.
#[pyfunction]
#[pyo3(signature = (archive, output, progress=None, taxon_allowlist=None))]
pub fn extract_prebuilt_fullmap(
    py: Python<'_>,
    archive: PathBuf,
    output: PathBuf,
    progress: Option<Py<PyAny>>,
    taxon_allowlist: Option<Vec<i32>>,
) -> PyResult<()> {
    // Release the GIL for the whole extract->validate->rename so the multi-GB
    // streaming extraction never blocks the interpreter; progress callbacks
    // re-acquire it briefly (see `extract_report`).
    let taxon_allowlist: Option<HashSet<i32>> = taxon_allowlist.map(allowlist_set);
    py.detach(|| {
        extract_prebuilt_fullmap_inner(archive, output, progress, taxon_allowlist.as_ref())
    })
}

fn extract_prebuilt_fullmap_inner(
    archive: PathBuf,
    output: PathBuf,
    progress: Option<Py<PyAny>>,
    taxon_allowlist: Option<&HashSet<i32>>,
) -> PyResult<()> {
    extract_report(&progress, "opening archive");

    // A missing or non-file archive errors BEFORE any extraction work.
    let meta = std::fs::metadata(&archive).map_err(|e| {
        py_err(format!(
            "prebuilt archive {} not found: {e}",
            archive.display()
        ))
    })?;
    if !meta.is_file() {
        return Err(py_err(format!(
            "prebuilt archive {} is not a regular file",
            archive.display()
        )));
    }

    let parent = output
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    std::fs::create_dir_all(&parent).map_err(|e| {
        py_err(format!(
            "failed to create output directory {}: {e}",
            parent.display()
        ))
    })?;

    // Extract into a fresh dot-prefixed temp dir INSIDE the output directory:
    // the same filesystem, so the final primary/shard renames are atomic.  A
    // stale dir from a crashed prior run is removed first, and the dir never
    // leaks — cleanup is guaranteed on success and best-effort on every error
    // path (a failed extraction must not leave multi-GB partials behind).
    // NOTE: the name is deliberately fixed (not randomized): two CONCURRENT
    // extractions to the same output would clobber each other's temp dir, but
    // the failure is loud (validation/rename fails -> BABEL fallback) and can
    // never land a corrupt bundle — and `build-fullmap` is not run concurrently
    // against one output by design.
    let stem = output
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "fullmap".to_string());
    let temp_dir = parent.join(format!(".{stem}.prebuilt-extract.d"));
    if temp_dir.exists() {
        std::fs::remove_dir_all(&temp_dir).map_err(|e| {
            py_err(format!(
                "failed to remove stale extraction dir {}: {e}",
                temp_dir.display()
            ))
        })?;
    }
    std::fs::create_dir_all(&temp_dir).map_err(|e| {
        py_err(format!(
            "failed to create extraction dir {}: {e}",
            temp_dir.display()
        ))
    })?;

    match extract_validate_rename(&archive, &temp_dir, &output, &progress, taxon_allowlist) {
        Ok(()) => Ok(()),
        Err(err) => {
            // Best-effort: never leak multi-GB partials from a failed run.
            let _ = std::fs::remove_dir_all(&temp_dir);
            Err(err)
        }
    }
}

/// How `open_cached_path` served a handle: as the generation currently living
/// at the path, or as the old snapshot cached for a path that is absent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Served {
    /// The path still points at the handle's `(dev, ino)` generation.
    Current,
    /// The path is absent (`NotFound`): the handle is the old snapshot the
    /// cache keeps alive across the mid-rebuild window.
    StaleAbsent,
}

/// Open (and cache) the primary DB, schema-validating it on (re)open.
fn open_cached(db: PathBuf) -> PyResult<Arc<ReadOnlyDatabase>> {
    let canonical = std::fs::canonicalize(&db).unwrap_or(db);
    Ok(open_cached_path(canonical, true)?.0)
}

/// Open (and cache) one RECORDS shard by index, deriving its path from the
/// primary DB path.  Shards carry RECORDS plus META.build_id; they are not
/// schema-validated here — the primary's `validate_schema` gates the layout.
/// Returns the handle plus HOW it was served (`Served`) and its build token so
/// `open_cached_shards` can pin one bundle build.
fn open_cached_shard(
    primary: &Path,
    index: usize,
) -> PyResult<(Arc<ReadOnlyDatabase>, Served, u64)> {
    let path = shard_path(primary, index);
    let canonical = std::fs::canonicalize(&path).unwrap_or(path);
    open_cached_path(canonical, false)
}

/// Shared cache lookup behind `open_cached` / `open_cached_shard`: serve the
/// cached handle only while the canonical path still points at the recorded
/// `(dev, ino)` generation (`Served::Current`); a mismatch evicts the stale
/// entry and reopens the replacement (re-running `validate_schema` when
/// `validate`).  A hit whose path is ABSENT with `io::ErrorKind::NotFound`
/// (the mid-rebuild window) keeps serving the old snapshot as
/// `Served::StaleAbsent`, as readers did before generation pinning; any other
/// metadata error propagates instead of silently pinning readers to stale
/// data.  On a miss the generation is stat'ed BEFORE the open, so a rebuild
/// racing the open costs at most one extra reopen on the next hit — never a
/// stale serve.
fn open_cached_path(
    canonical: PathBuf,
    validate: bool,
) -> PyResult<(Arc<ReadOnlyDatabase>, Served, u64)> {
    let cache = DB_CACHE.get_or_init(|| RwLock::new(HashMap::new()));
    // Copy the hit out of the read lock before any write-lock upgrade attempt.
    let hit = {
        let map = cache.read().map_err(py_err)?;
        map.get(&canonical).map(|entry| {
            (
                Arc::clone(&entry.database),
                entry.generation,
                entry.build_id,
            )
        })
    };
    if let Some((database, generation, build_id)) = hit {
        match generation_of(&canonical) {
            // Same inode generation: the handle is still the file at the path.
            Ok(current) if current == generation => {
                return Ok((database, Served::Current, build_id))
            }
            // Path gone (rebuild in flight or deleted): keep serving the old
            // snapshot; the next lookup after a replacement appears follows it.
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok((database, Served::StaleAbsent, build_id));
            }
            // Any other metadata failure (permissions, I/O): propagate it —
            // serving stale data here would silently pin readers to old state.
            Err(err) => return Err(py_err(err)),
            // A newer generation lives at the path: evict (only the entry we
            // observed; another thread may already have refreshed it) and
            // reopen below.
            Ok(_) => {
                let mut map = cache.write().map_err(py_err)?;
                if map
                    .get(&canonical)
                    .is_some_and(|entry| entry.generation == generation)
                {
                    map.remove(&canonical);
                }
            }
        }
    }
    // Prefer the generation stat'ed BEFORE the open: a rebuild racing the open
    // can then cost at most one extra reopen later, never a stale serve.  If
    // the stat failed (e.g. missing file), `open_read_only` below raises the
    // canonical redb error for it; the re-stat then records the generation of
    // the file actually opened.
    let pre = generation_of(&canonical);
    let database = Arc::new(open_read_only(&canonical)?);
    if validate {
        validate_schema(&database)?;
    }
    let build_id = read_build_id(&database)?;
    let generation = match pre {
        Ok(generation) => generation,
        Err(_) => generation_of(&canonical).map_err(py_err)?,
    };
    let cached = Arc::clone(&database);
    cache.write().map_err(py_err)?.insert(
        canonical,
        CachedDatabase {
            database,
            generation,
            build_id,
        },
    );
    Ok((cached, Served::Current, build_id))
}

/// Read the RECORDS shard count advertised in the primary's META, defaulting to
/// `SHARD_COUNT_SHARDS` when absent.  New builds always record
/// `SHARD_COUNT_SHARDS`; reading the value back keeps the read path compatible
/// with databases built before the count was pinned, opening exactly the shards
/// that exist and routing with the matching mask.
fn shard_count_of(database: &ReadOnlyDatabase) -> PyResult<usize> {
    let read = database.begin_read().map_err(py_err)?;
    let meta = read.open_table(META).map_err(py_err)?;
    let count = meta
        .get("shards")
        .map_err(py_err)?
        .and_then(|v| v.value().parse::<usize>().ok())
        .unwrap_or(SHARD_COUNT_SHARDS);
    // Round down to a power of two before clamping: the routing mask
    // `xxh64(term) & (count - 1)` is only correct for powers of two, so a legacy
    // or hand-edited non-pow2 META.shards (e.g. 3 -> mask &2) would silently
    // misroute/drop lookups onto a subset of shards.
    // `round_down_pow2(SHARD_COUNT_SHARDS) == SHARD_COUNT_SHARDS` (16 is a pow2),
    // so the default fallback above is preserved.
    Ok(round_down_pow2(count).clamp(1, SHARD_COUNT_SHARDS))
}

/// Attempts `open_cached_shards` makes to open one consistent generation
/// before concluding the DB is being rebuilt in a tight loop.
const BUNDLE_OPEN_ATTEMPTS: usize = 5;
const BUNDLE_OPEN_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(50);

/// Open (and cache) all RECORDS shard handles for a primary DB path, pinned to
/// ONE build.  The primary's `(dev, ino)` is captured before opening anything
/// and re-stat'ed after the last shard, and every bundle member reports HOW it
/// was served (`Served`) plus the META `build_id` read when its handle opened.
/// The bundle is accepted only when every member has the same `build_id` and is
/// generationally consistent:
///
/// - ALL `Current` with the primary generation unchanged across the fan-out:
///   every handle belongs to the current build — return it.
/// - ALL `StaleAbsent`: the rebuild window straddled the whole bundle.  All old
///   files were unlinked before the new primary was written, and the new primary
///   is written before the new shards; if every cached handle shares one
///   `build_id`, return the old snapshot (the documented snapshot semantics).
/// - MIXED (any `StaleAbsent` alongside any `Current`), unequal `build_id`s, or
///   primary generation movement during fan-out: retry the WHOLE bundle, and
///   after `BUNDLE_OPEN_ATTEMPTS` raise the exhaustion error below (its
///   "changing generation" text is one of the Python-side
///   `_LOCK_RETRY_TOKENS`).
///
/// The shard count is read from the primary's META so the read path opens
/// exactly the shards the build wrote.
fn open_cached_shards(primary: &Path) -> PyResult<Vec<Arc<ReadOnlyDatabase>>> {
    let canonical = std::fs::canonicalize(primary).unwrap_or_else(|_| primary.to_path_buf());
    for attempt in 0..BUNDLE_OPEN_ATTEMPTS {
        let pinned = generation_of(&canonical).ok();
        let (database, primary_served, primary_build_id) =
            open_cached_path(canonical.clone(), true)?;
        let shard_count = shard_count_of(&database)?;
        let mut all_current = primary_served == Served::Current;
        let mut all_stale_absent = primary_served == Served::StaleAbsent;
        let mut same_build_id = true;
        let mut shards: Vec<Arc<ReadOnlyDatabase>> = Vec::with_capacity(shard_count);
        for index in 0..shard_count {
            let (database, served, build_id) = open_cached_shard(primary, index)?;
            all_current &= served == Served::Current;
            all_stale_absent &= served == Served::StaleAbsent;
            same_build_id &= build_id == primary_build_id;
            shards.push(database);
        }
        // ALL stale: every path is absent; equal build IDs prove the cached
        // handles still form one old snapshot.
        if all_stale_absent && same_build_id {
            return Ok(shards);
        }
        // ALL current: re-stat the primary; an unchanged generation plus equal
        // build IDs means every handle above belongs to one build.
        if same_build_id
            && all_current
            && matches!(
                (pinned, generation_of(&canonical).ok()),
                (Some(before), Some(after)) if before == after
            )
        {
            return Ok(shards);
        }
        // MIXED serve kinds, unequal build IDs, or primary movement during
        // fan-out: the handles may span two builds — retry the whole bundle.
        if attempt + 1 < BUNDLE_OPEN_ATTEMPTS {
            std::thread::sleep(BUNDLE_OPEN_RETRY_DELAY);
        }
    }
    let path = canonical.display();
    Err(PyRuntimeError::new_err(format!(
        "fullmap at {path} kept changing generation; could not pin one primary-plus-shards bundle after {BUNDLE_OPEN_ATTEMPTS} attempts"
    )))
}

fn lookup_pair_chunk(shards: &[Arc<ReadOnlyDatabase>], terms: &[String]) -> PyResult<PairRecords> {
    // One read transaction + RECORDS table per shard, opened once; each query
    // term is routed to its shard via `term_shard`.
    let reads: Vec<_> = shards
        .iter()
        .map(|db| db.begin_read().map_err(py_err))
        .collect::<PyResult<_>>()?;
    let tables: Vec<_> = reads
        .iter()
        .map(|read| read.open_table(RECORDS).map_err(py_err))
        .collect::<PyResult<_>>()?;
    let mut out = Vec::new();
    for term in terms {
        let hash = xxh64(term.as_bytes(), 0);
        let table = &tables[term_shard(term, tables.len())];
        if let Some(bytes) = table.get(hash).map_err(py_err)? {
            let (stored_term, records): (String, Vec<(u32, u8)>) =
                bincode::deserialize(bytes.value()).map_err(py_err)?;
            // Verify the term matches (guards against xxh64 collisions).
            if &stored_term == term {
                out.push((term.clone(), records));
            }
        }
    }
    Ok(out)
}

/// Query a single RECORDS shard for the `(input_index, term)` pairs routed to
/// it.  Opens one read transaction + RECORDS table on `shard`, looks up each
/// term by its xxh64 key, and keeps the `stored_term == term` collision guard.
/// Hits are tagged with their original input index so the caller can re-merge
/// every shard's results back into input order.  Pure Rust (no `Python`), so it
/// is safe to run on a worker thread inside a `py.detach` region.
fn lookup_shard_bucket(
    shard: &ReadOnlyDatabase,
    bucket: &[(usize, String)],
) -> PyResult<Vec<(usize, TermPairs)>> {
    let read = shard.begin_read().map_err(py_err)?;
    let table = read.open_table(RECORDS).map_err(py_err)?;
    let mut out = Vec::new();
    for (index, term) in bucket {
        let hash = xxh64(term.as_bytes(), 0);
        if let Some(bytes) = table.get(hash).map_err(py_err)? {
            let (stored_term, records): (String, Vec<(u32, u8)>) =
                bincode::deserialize(bytes.value()).map_err(py_err)?;
            // Verify the term matches (guards against xxh64 collisions).
            if &stored_term == term {
                out.push((*index, (term.clone(), records)));
            }
        }
    }
    Ok(out)
}

/// Decide how many readers each NON-EMPTY shard bucket gets.
///
/// One reader per bucket by default.  When `workers` exceeds the bucket count,
/// the surplus goes to the bucket with the most terms-per-reader (ties break to
/// the lower bucket index), never splitting a bucket into more readers than it
/// has terms.  Pure and deterministic for a given input; the caller clamps
/// `workers` to the total term count, so the loop always terminates.
fn split_counts(bucket_lens: &[usize], workers: usize) -> Vec<usize> {
    let mut splits: Vec<usize> = vec![1; bucket_lens.len()];
    let mut job_count = bucket_lens.len();
    while job_count < workers {
        let mut best: Option<usize> = None;
        let mut best_load = 0usize;
        for (i, &len) in bucket_lens.iter().enumerate() {
            if len > splits[i] {
                let load = len / splits[i];
                if best.is_none() || load > best_load {
                    best = Some(i);
                    best_load = load;
                }
            }
        }
        let Some(i) = best else { break };
        splits[i] += 1;
        job_count += 1;
    }
    splits
}

/// Split `bucket` into `count` contiguous, near-equal, NON-EMPTY chunks.
///
/// `count` must be ≤ `bucket.len()` (`split_counts` guarantees that); `count <=
/// 1` returns the bucket whole.  Contiguity keeps each chunk's input-index tags
/// ascending, though the caller re-merges by tag regardless.
fn split_bucket(mut bucket: Vec<(usize, String)>, count: usize) -> Vec<Vec<(usize, String)>> {
    debug_assert!(count >= 1 && count <= bucket.len());
    if count <= 1 {
        return vec![bucket];
    }
    let base = bucket.len() / count;
    let rem = bucket.len() % count;
    let mut chunks: Vec<Vec<(usize, String)>> = Vec::with_capacity(count);
    for i in 0..count {
        // Drain from the FRONT: each drain shrinks the vector, so offsets
        // against the original layout would overshoot (the chunks stay
        // contiguous and in input order either way).
        let size = base + usize::from(i < rem);
        chunks.push(bucket.drain(..size).collect());
    }
    chunks
}

/// Plan the read jobs for `lookup_pair_terms_db`: one job per NON-EMPTY shard
/// bucket, and when `workers` exceeds the non-empty bucket count the busiest
/// buckets are SPLIT across additional readers of the SAME shard file.  That is
/// safe because redb readers hold a SHARED lock and each reader opens its own
/// read transaction (`lookup_shard_bucket`), so concurrent reads of one shard
/// never contend — only a writer (`build-fullmap`'s exclusive lock) conflicts.
/// Each job owns its chunk and a clone of its shard handle so worker threads
/// share neither a receiver nor a borrow.
fn plan_shard_jobs(
    shards: &[Arc<ReadOnlyDatabase>],
    buckets: Vec<Vec<(usize, String)>>,
    workers: usize,
) -> Vec<ShardJob> {
    let non_empty: Vec<(usize, Vec<(usize, String)>)> = buckets
        .into_iter()
        .enumerate()
        .filter(|(_, bucket)| !bucket.is_empty())
        .collect();
    let lens: Vec<usize> = non_empty.iter().map(|(_, bucket)| bucket.len()).collect();
    let splits = split_counts(&lens, workers);
    let mut jobs: Vec<ShardJob> = Vec::with_capacity(splits.iter().sum());
    for ((shard, bucket), count) in non_empty.into_iter().zip(splits) {
        for chunk in split_bucket(bucket, count) {
            jobs.push((chunk, Arc::clone(&shards[shard])));
        }
    }
    jobs
}

/// Fan the query terms out across RECORDS shards and read them concurrently.
/// Terms are partitioned by `term_shard` (the shared routing oracle) into
/// per-shard buckets; one worker thread per NON-EMPTY shard — capped at
/// `workers` — reads only its own shard's RECORDS.  When `workers` exceeds the
/// non-empty shard count, the busiest buckets are further SPLIT across extra
/// readers of the SAME shard file: redb readers hold a SHARED lock and each
/// reader opens its own read transaction, so concurrent reads of one shard
/// never contend.  The tagged hits are re-merged into the original input term
/// order at ANY reader count.  Surplus jobs beyond the worker cap are read on
/// the calling thread, which still overlaps with the spawned readers.  Pure
/// Rust end-to-end (no `Python`).
fn lookup_pair_terms_db(
    shards: &[Arc<ReadOnlyDatabase>],
    terms: &[String],
    workers: usize,
) -> PyResult<PairRecords> {
    if terms.is_empty() {
        return Ok(Vec::new());
    }
    let shard_count = shards.len();
    // Single-threaded fast path: one worker, one term, or a single shard.
    if workers <= 1 || terms.len() <= 1 || shard_count <= 1 {
        return lookup_pair_chunk(shards, terms);
    }

    // Partition the query terms into per-shard buckets, tagging each with its
    // input position so hits can be re-merged in the original order afterwards.
    let mut buckets: Vec<Vec<(usize, String)>> = vec![Vec::new(); shard_count];
    for (index, term) in terms.iter().enumerate() {
        buckets[term_shard(term, shard_count)].push((index, term.clone()));
    }

    // One job per NON-EMPTY shard; when `workers` exceeds the non-empty shard
    // count the busiest buckets are further split across extra readers of the
    // same shard file (see `plan_shard_jobs`).
    let mut jobs: Vec<ShardJob> = plan_shard_jobs(shards, buckets, workers);

    // Cap concurrent reads at `workers`; any surplus jobs are read on the
    // calling thread (split_off keeps the first `workers` jobs to spawn).
    let split = jobs.len().min(workers);
    let inline_jobs = jobs.split_off(split);

    let mut handles = Vec::with_capacity(jobs.len());
    for (bucket, shard) in jobs {
        handles.push(thread::spawn(move || lookup_shard_bucket(&shard, &bucket)));
    }

    let mut tagged: Vec<(usize, TermPairs)> = Vec::new();
    for (bucket, shard) in inline_jobs {
        tagged.extend(lookup_shard_bucket(&shard, &bucket)?);
    }
    for handle in handles {
        let mut part = handle
            .join()
            .map_err(|_| PyRuntimeError::new_err("fullmap lookup thread panicked"))??;
        tagged.append(&mut part);
    }

    // Re-merge hits into input term order; misses simply produced no row.
    tagged.sort_by_key(|(index, _)| *index);
    Ok(tagged.into_iter().map(|(_, pairs)| pairs).collect())
}

/// Smallest batch that fans out across the shards in parallel.  Below this,
/// lookups stay single-threaded: a point/small lookup
/// (and any cache-warm path) finishes faster serially than the cost of spawning
/// shard-reader threads.  At/above it, the per-shard fan-out in
/// `lookup_pair_terms_db` wins.  1024 terms ~= a few ms of serial redb point
/// reads, comfortably above the ~tens-of-µs cost of spawning up to 3 extra
/// threads, so the crossover is safely on the parallel side for real batches
/// while never penalizing small lookups.
const LOOKUP_PARALLEL_MIN: usize = 1024;

/// Worker count for lookups, selected automatically from the batch size.
/// The production build-kg resolve sends ONE batch of all distinct node-column
/// terms (often huge); parallelizing that across the RECORDS
/// shards is the win, so large batches default to `available_parallelism`.  Small
/// batches (< `LOOKUP_PARALLEL_MIN`) stay single-threaded to avoid spawn overhead.
/// The fan-out is already capped by the non-empty shard count inside
/// `lookup_pair_terms_db`, so returning `available_parallelism` yields up to
/// SHARD_COUNT_SHARDS actual shard threads regardless of core count.
fn default_lookup_workers(terms_len: usize) -> usize {
    if terms_len < LOOKUP_PARALLEL_MIN {
        return 1;
    }
    std::thread::available_parallelism()
        .map(std::num::NonZero::get)
        .unwrap_or(1)
}

fn lookup_pair_terms(db: PathBuf, terms: Vec<String>) -> PyResult<PairRecords> {
    let workers = default_lookup_workers(terms.len())
        .max(1)
        .min(terms.len().max(1));
    // Open (and schema-validate) the primary, then route pair lookups to shards.
    let _primary = open_cached(db.clone())?;
    let shards = open_cached_shards(&db)?;
    lookup_pair_terms_db(&shards, &terms, workers)
}

fn load_string_table(
    database: &ReadOnlyDatabase,
    table_definition: TableDefinition<u16, &str>,
) -> PyResult<HashMap<u16, String>> {
    let read = database.begin_read().map_err(py_err)?;
    let table = read.open_table(table_definition).map_err(py_err)?;
    let mut out = HashMap::new();
    for item in table.iter().map_err(py_err)? {
        let (id, value) = item.map_err(py_err)?;
        out.insert(id.value(), value.value().to_string());
    }
    Ok(out)
}

fn load_sources(database: &ReadOnlyDatabase) -> PyResult<HashMap<u8, String>> {
    let read = database.begin_read().map_err(py_err)?;
    let table = read.open_table(SOURCES).map_err(py_err)?;
    let mut out = HashMap::new();
    for item in table.iter().map_err(py_err)? {
        let (id, value) = item.map_err(py_err)?;
        let row: SourceRow = bincode::deserialize(value.value()).map_err(py_err)?;
        out.insert(id.value(), row.source_name);
    }
    Ok(out)
}

fn hydrate_curie_rows(database: &ReadOnlyDatabase, curie_ids: &[u32]) -> PyResult<Vec<CurieRow>> {
    let read = database.begin_read().map_err(py_err)?;
    let table = read.open_table(CURIES).map_err(py_err)?;
    let mut out = Vec::with_capacity(curie_ids.len());
    for curie_id in curie_ids {
        let bytes = table.get(*curie_id).map_err(py_err)?.ok_or_else(|| {
            PyRuntimeError::new_err(format!("missing fullmap CURIE id {curie_id}"))
        })?;
        out.push(bincode::deserialize(bytes.value()).map_err(py_err)?);
    }
    Ok(out)
}

fn lookup_terms(db: PathBuf, terms: Vec<String>) -> PyResult<Vec<(String, Vec<FullmapRecord>)>> {
    // Open the primary ONCE (the cached shared-lock handle) for dims/CURIES
    // hydration; pair lookups route to the shard files.  One handle per file is
    // a cache choice, not a lock constraint — read-only opens coexist.
    let database = open_cached(db.clone())?;
    let prefix_map = load_string_table(&database, PREFIXES)?;
    let category_map = load_string_table(&database, CATEGORIES)?;
    let source_map = load_sources(&database)?;
    let shards = open_cached_shards(&db)?;
    let workers = default_lookup_workers(terms.len())
        .max(1)
        .min(terms.len().max(1));
    let pair_rows = lookup_pair_terms_db(&shards, &terms, workers)?;
    let mut curie_ids: Vec<u32> = pair_rows
        .iter()
        .flat_map(|(_term, pairs)| pairs.iter().map(|(curie_id, _source_id)| *curie_id))
        .collect();
    curie_ids.sort_unstable();
    curie_ids.dedup();
    let hydrated = hydrate_curie_rows(&database, &curie_ids)?;
    let curie_map: HashMap<u32, CurieRow> = curie_ids.into_iter().zip(hydrated).collect();
    let source_version = FULLMAP_SOURCE_VERSION.to_string();
    let mut out = Vec::new();
    for (term, pairs) in pair_rows {
        let mut records = Vec::new();
        for (curie_id, source_id) in pairs {
            let curie = curie_map.get(&curie_id).ok_or_else(|| {
                PyRuntimeError::new_err(format!("missing hydrated CURIE id {curie_id}"))
            })?;
            let prefix = prefix_map.get(&curie.prefix_id).ok_or_else(|| {
                PyRuntimeError::new_err(format!("missing prefix id {}", curie.prefix_id))
            })?;
            let category = category_map.get(&curie.category_id).ok_or_else(|| {
                PyRuntimeError::new_err(format!("missing category id {}", curie.category_id))
            })?;
            let source = source_map
                .get(&source_id)
                .ok_or_else(|| PyRuntimeError::new_err(format!("missing source id {source_id}")))?;
            records.push(hydrate_record(
                curie,
                source.clone(),
                source_version.clone(),
                prefix,
                category,
            ));
        }
        out.push((term, records));
    }
    Ok(out)
}

/// Look up fullmap records for `terms`.  The worker count is selected
/// automatically via `default_lookup_workers`: batches >= `LOOKUP_PARALLEL_MIN`
/// fan out across the RECORDS shards in parallel (up to 16 by default), smaller
/// batches stay single-threaded.  The GIL is released for the whole lookup.
#[pyfunction]
#[pyo3(signature = (db, terms, return_format="rows"))]
pub fn lookup_fullmap_terms<'py>(
    py: Python<'py>,
    db: PathBuf,
    terms: Vec<String>,
    return_format: &str,
) -> PyResult<Bound<'py, PyList>> {
    if return_format == "pairs" {
        // Release the GIL for the whole lookup (pure-Rust shard reads); the
        // PyList is built only after re-acquiring it so rich's Live display
        // thread can repaint and Ctrl-C works mid-lookup.
        let pair_rows = py.detach(move || lookup_pair_terms(db, terms))?;
        let list = PyList::empty(py);
        for (term, pairs) in pair_rows {
            let row = PyDict::new(py);
            row.set_item("term", term)?;
            row.set_item("records", pairs)?;
            list.append(row)?;
        }
        return Ok(list);
    }
    if return_format != "rows" {
        return Err(PyValueError::new_err(
            "return_format must be 'rows' or 'pairs'",
        ));
    }
    // Release the GIL for the whole lookup (pure-Rust shard reads + CURIE/dim
    // hydration against the primary); the PyList is built only after
    // re-acquiring the GIL.
    let rows = py.detach(move || lookup_terms(db, terms))?;
    let list = PyList::empty(py);
    for (term, records) in rows {
        for record in records {
            let row = PyDict::new(py);
            row.set_item("term", &term)?;
            row.set_item("CURIE", record.curie)?;
            row.set_item("PREFERRED_NAME", record.preferred_name)?;
            row.set_item("CATEGORY_NAME", record.category_name)?;
            row.set_item("TAXON_ID", record.taxon_id)?;
            row.set_item("SOURCE_NAME", record.source_name)?;
            row.set_item("SOURCE_VERSION", record.source_version)?;
            list.append(row)?;
        }
    }
    Ok(list)
}

#[pyfunction]
pub fn hydrate_curies<'py>(
    py: Python<'py>,
    db: PathBuf,
    curie_ids: Vec<u32>,
) -> PyResult<Bound<'py, PyList>> {
    let database = open_cached(db)?;
    let rows = hydrate_curie_rows(&database, &curie_ids)?;
    let list = PyList::empty(py);
    for row in rows {
        let item = PyDict::new(py);
        item.set_item("prefix_id", row.prefix_id)?;
        item.set_item("local_id", row.local_id)?;
        item.set_item("preferred_name", row.preferred_name)?;
        item.set_item("category_id", row.category_id)?;
        item.set_item("taxon_id", row.taxon_id)?;
        list.append(item)?;
    }
    Ok(list)
}

#[pyfunction]
pub fn hydrate_sources<'py>(py: Python<'py>, db: PathBuf) -> PyResult<Bound<'py, PyList>> {
    let database = open_cached(db)?;
    let sources = load_sources(&database)?;
    let max_id = sources.keys().copied().max().unwrap_or(0);
    let list = PyList::empty(py);
    for id in 0..=max_id {
        list.append(sources.get(&id).cloned().unwrap_or_default())?;
    }
    Ok(list)
}

#[pyfunction]
pub fn hydrate_prefixes<'py>(py: Python<'py>, db: PathBuf) -> PyResult<Bound<'py, PyList>> {
    let database = open_cached(db)?;
    let prefixes = load_string_table(&database, PREFIXES)?;
    let max_id = prefixes.keys().copied().max().unwrap_or(0);
    let list = PyList::empty(py);
    for id in 0..=max_id {
        list.append(prefixes.get(&id).cloned().unwrap_or_default())?;
    }
    Ok(list)
}

#[pyfunction]
pub fn hydrate_categories<'py>(py: Python<'py>, db: PathBuf) -> PyResult<Bound<'py, PyList>> {
    let database = open_cached(db)?;
    let categories = load_string_table(&database, CATEGORIES)?;
    let max_id = categories.keys().copied().max().unwrap_or(0);
    let list = PyList::empty(py);
    for id in 0..=max_id {
        list.append(categories.get(&id).cloned().unwrap_or_default())?;
    }
    Ok(list)
}

#[pyfunction]
pub fn fullmap_source_version() -> &'static str {
    FULLMAP_SOURCE_VERSION
}

/// The deterministic identity a taxon allowlist records in
/// `META.taxon_allowlist` (`count=N;xxh64=...`), exposed so callers can state
/// which filter a database was built with without reimplementing the encoding.
#[pyfunction]
pub fn taxon_allowlist_identity(taxon_allowlist: Vec<i32>) -> String {
    allowlist_identity(&allowlist_set(taxon_allowlist))
}

/// The `META.taxon_allowlist` identity recorded in an existing fullmap primary,
/// or `None` when that database was built unfiltered.
///
/// A best-effort probe for the "is the database already at this path the one I
/// would build?" check: a missing, unreadable, foreign, or outdated-schema file
/// yields `None` (meaning "not reusable") rather than raising, so the caller
/// rebuilds instead of failing on a file it is about to overwrite.
#[pyfunction]
pub fn fullmap_taxon_allowlist_identity(db: PathBuf) -> Option<String> {
    let database = open_read_only(&db).ok()?;
    if validate_schema(&database).is_err() {
        return None;
    }
    read_taxon_allowlist_identity(&database).ok().flatten()
}

#[cfg(test)]
mod tests {
    use super::*;
    use redb::ReadableTableMetadata;
    use std::io::Write;

    /// Test helper: build with explicit tunables (no Python token / env needed).
    fn build_test(
        output: PathBuf,
        classes: Vec<PathBuf>,
        synonyms: Vec<PathBuf>,
        threads: usize,
        local_spill: usize,
    ) -> PyResult<()> {
        let spill_dir = {
            let mut p = output.clone().into_os_string();
            p.push(".spill.d");
            PathBuf::from(p)
        };
        build_fullmap_inner(
            output,
            classes,
            synonyms,
            threads.max(1),
            SHARD_COUNT_SHARDS,
            None,
            local_spill,
            1_000_000,
            HashSet::new(),
            None,
            None,
            DEFAULT_CHUNK_BYTES,
            2,
            64 * 1024 * 1024,
            1000,
            spill_dir,
        )
    }

    /// `build_fullmap_db` raises the soft NOFILE limit to the hard limit so
    /// Phase 4's per-shard MergeHeap (which opens every spill-run file for a
    /// shard at once) isn't capped by a low default (e.g. ulimit -n 1024).
    #[test]
    fn increase_nofile_limit_reaches_hard_limit() {
        let (_, hard) = rlimit::Resource::NOFILE.get().expect("getrlimit");
        let new_soft = rlimit::increase_nofile_limit(u64::MAX).expect("raise nofile limit");
        assert_eq!(new_soft, hard);
        let (soft_after, hard_after) = rlimit::Resource::NOFILE.get().expect("getrlimit");
        assert_eq!(soft_after, hard);
        assert_eq!(hard_after, hard);
    }

    #[test]
    fn clean_strips_matching_and_duplicate_quotes() {
        assert_eq!(clean("  'BRCA1'  "), "BRCA1");
        assert_eq!(clean("\"\"TP53\""), "TP53");
    }

    #[test]
    fn token_qc_rejects_banned_tokens_but_accepts_inchikey() {
        assert!(token_qc("brca1"));
        assert!(token_qc("inchikey=abc"));
        assert!(!token_qc("hypothetical protein"));
        assert!(!token_qc("line\nbreak"));
        assert!(!token_qc("tab\tvalue"));
    }

    #[test]
    fn level_normalization_matches_fullmap_shape() {
        assert_eq!(level_one("BRCA-1"), "brca-1");
        assert_eq!(level_two("brca-1"), "brca1");
    }

    /// `term_shard` is the single routing oracle shared by the writer and the
    /// reader, so it must be deterministic (same term -> same shard every call,
    /// or reads would look in the wrong file), agree with the raw xxh64-mask
    /// definition, and spread a representative sample across all default shards so the
    /// shard files stay roughly even-sized.
    #[test]
    fn term_shard_is_deterministic_and_balanced() {
        // Deterministic, in range, and equal to an independent xxh64-mask
        // re-derivation for known terms.
        for term in ["brca1", "tp53", "water", "alias disease", "gene42"] {
            let expected = (xxh64(term.as_bytes(), 0) as usize) & (SHARD_COUNT_SHARDS - 1);
            assert_eq!(
                term_shard(term, SHARD_COUNT_SHARDS),
                expected,
                "routing mismatch for {term}"
            );
            assert_eq!(
                term_shard(term, SHARD_COUNT_SHARDS),
                term_shard(term, SHARD_COUNT_SHARDS),
                "non-deterministic for {term}"
            );
            assert!(term_shard(term, SHARD_COUNT_SHARDS) < SHARD_COUNT_SHARDS);
        }

        // Balanced: 1000 distinct synthetic terms hit every shard, none
        // dominating (xxh64 is well-mixed; allow a generous +/- 50% band).
        let mut counts = [0usize; SHARD_COUNT_SHARDS];
        for i in 0..1000 {
            counts[term_shard(&format!("term{i}"), SHARD_COUNT_SHARDS)] += 1;
        }
        let expected = 1000 / SHARD_COUNT_SHARDS;
        for (shard, count) in counts.iter().enumerate() {
            assert!(*count > 0, "shard {shard} received no terms: {counts:?}");
            assert!(
                *count > expected / 2 && *count < expected * 2,
                "shard distribution unbalanced: {counts:?}"
            );
        }
    }

    #[test]
    fn builds_and_reads_redb_records() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let classes = dir.path().join("classes.ndjson");
        let synonyms = dir.path().join("HGNC.ndjson");
        let output = dir.path().join("fullmap.redb");

        let mut class_file = File::create(&classes).unwrap();
        writeln!(
            class_file,
            r#"{{"id":"HGNC:1100","equivalent_identifiers":[{{"identifier":"NCBIGene:672"}}]}}"#
        )
        .unwrap();

        let mut synonym_file = File::create(&synonyms).unwrap();
        writeln!(
            synonym_file,
            r#"{{"curie":"HGNC:1100","preferred_name":"BRCA1","names":["BRCA1"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}}"#
        )
        .unwrap();

        build_test(output.clone(), vec![classes], vec![synonyms], 1, 4_000_000).unwrap();
        let rows =
            lookup_terms(output, vec!["brca1".to_string(), "ncbigene672".to_string()]).unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].1[0].curie, "HGNC:1100");
        assert_eq!(rows[0].1[0].source_version, FULLMAP_SOURCE_VERSION);
    }

    /// The sharded (v5) layout keeps dims+CURIES+META in the primary and moves
    /// RECORDS into sibling shard files.  The primary must NOT carry a RECORDS
    /// table, META must advertise schema/shards/build_id, and every shard file
    /// must exist (even empty) holding RECORDS plus the same build_id — this is
    /// the on-disk contract the read path relies on.
    #[test]
    fn build_fullmap_db_writes_sharded_layout() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let synonyms = dir.path().join("HGNC.ndjson");
        let output = dir.path().join("fullmap.redb");

        let mut synonym_file = File::create(&synonyms).unwrap();
        writeln!(
            synonym_file,
            r#"{{"curie":"HGNC:1100","preferred_name":"BRCA1","names":["BRCA1"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}}"#
        )
        .unwrap();

        build_test(output.clone(), Vec::new(), vec![synonyms], 1, 4_000_000).unwrap();

        // Primary: META (schema, shards, build_id) + dims + CURIES, but NO RECORDS.
        let database = open_cached(output.clone()).unwrap();
        let read = database.begin_read().unwrap();
        let meta = read.open_table(META).unwrap();
        assert_eq!(meta.get("schema").unwrap().unwrap().value(), SCHEMA_VERSION);
        assert_eq!(
            meta.get("shards").unwrap().unwrap().value(),
            SHARD_COUNT_SHARDS.to_string()
        );
        let build_id = meta.get("build_id").unwrap().unwrap().value().to_string();
        build_id.parse::<u64>().unwrap();
        drop(meta);
        let _prefixes = read.open_table(PREFIXES).unwrap();
        let _categories = read.open_table(CATEGORIES).unwrap();
        let _sources = read.open_table(SOURCES).unwrap();
        let _curies = read.open_table(CURIES).unwrap();
        assert!(
            read.open_table(RECORDS).is_err(),
            "primary must not hold a RECORDS table in the sharded (v5) layout"
        );
        drop(read);
        drop(database);

        // All default shard files exist and each holds RECORDS + matching build_id.
        for index in 0..SHARD_COUNT_SHARDS {
            let shard = shard_path(&output, index);
            assert!(shard.exists(), "missing shard file {shard:?}");
            let db = ReadOnlyDatabase::open(&shard).unwrap();
            let read = db.begin_read().unwrap();
            let _records = read.open_table(RECORDS).unwrap();
            let meta = read.open_table(META).unwrap();
            assert_eq!(meta.get("build_id").unwrap().unwrap().value(), build_id);
        }

        // The single indexed term still resolves (routed through its shard).
        let rows = lookup_terms(output, vec!["brca1".to_string()]).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1[0].curie, "HGNC:1100");
    }

    /// Per-shard RECORDS row counts for a built primary DB, in shard order.
    /// Used to prove the parallel write routes records identically regardless of
    /// writer count (routing is a pure function of each term's xxh64 hash).
    fn shard_record_counts(primary: &Path) -> Vec<usize> {
        let database = open_cached(primary.to_path_buf()).unwrap();
        let shard_count = shard_count_of(&database).unwrap();
        (0..shard_count)
            .map(|index| {
                let db = open_cached_shard(primary, index).unwrap().0;
                let read = db.begin_read().unwrap();
                let table = read.open_table(RECORDS).unwrap();
                table.iter().unwrap().count()
            })
            .collect()
    }

    /// Mask routing only works for powers of two; `round_down_pow2` must round
    /// 6->4, 3->2, 5->4, leave powers unchanged, and floor tiny/zero inputs at 1
    /// so a legacy META shard count is always a valid non-zero mask.
    #[test]
    fn round_down_pow2_rounds_to_nearest_lower_power_of_two() {
        assert_eq!(round_down_pow2(0), 1);
        assert_eq!(round_down_pow2(1), 1);
        assert_eq!(round_down_pow2(2), 2);
        assert_eq!(round_down_pow2(3), 2);
        assert_eq!(round_down_pow2(4), 4);
        assert_eq!(round_down_pow2(5), 4);
        assert_eq!(round_down_pow2(6), 4);
        assert_eq!(round_down_pow2(7), 4);
        assert_eq!(round_down_pow2(8), 8);
        assert_eq!(round_down_pow2(9), 8);
    }

    /// The read path's `shard_count_of` must round a non-power-of-two META.shards
    /// DOWN to a power of two.  WHY: lookups route with the mask
    /// `xxh64(term) & (count - 1)`, which is only correct for powers of two; a
    /// legacy or hand-edited META.shards of 3 (mask &2) would route terms only onto
    /// shards {0,2}, silently misrouting/dropping lookups (shard 1 opened but never
    /// queried, shard 3 never opened).  Rounding down keeps the mask valid.  A
    /// missing/unparseable value falls back to the default `SHARD_COUNT_SHARDS`
    /// (16) via `unwrap_or`, itself a power of two so the round-down leaves it
    /// unchanged.  `"0"` PARSES (so the fallback does not fire) and rounds down to
    /// 1 (`round_down_pow2(0) == 1`); 1 is still a valid mask (&0), so it is safe.
    #[test]
    fn shard_count_of_rounds_non_pow2_meta_down() {
        pyo3::Python::initialize();

        let check = |shards: Option<&str>| -> usize {
            let dir = tempfile::tempdir().unwrap();
            let output = dir.path().join("fullmap.redb");
            let database = Database::create(&output).unwrap();
            let write = database.begin_write().unwrap();
            {
                let mut meta = write.open_table(META).unwrap();
                meta.insert("schema", SCHEMA_VERSION).unwrap();
                if let Some(value) = shards {
                    meta.insert("shards", value).unwrap();
                }
            }
            write.commit().unwrap();
            drop(database);

            let database = ReadOnlyDatabase::open(&output).unwrap();
            let count = shard_count_of(&database).unwrap();
            drop(database);
            count
        };

        assert_eq!(check(Some("3")), 2);
        assert_eq!(check(Some("5")), 4);
        assert_eq!(check(Some("6")), 4);
        assert_eq!(check(Some("7")), 4);
        assert_eq!(check(Some("4")), 4);
        assert_eq!(check(Some("2")), 2);
        assert_eq!(check(Some("1")), 1);
        // Missing / unparseable -> default SHARD_COUNT_SHARDS (16) via unwrap_or.
        assert_eq!(check(None), SHARD_COUNT_SHARDS);
        assert_eq!(check(Some("not-a-number")), SHARD_COUNT_SHARDS);
        // "0" parses (fallback does NOT fire) and rounds down to 1; 1 is a valid
        // mask, so this is safe, not a misroute.
        assert_eq!(check(Some("0")), 1);
    }

    /// US-201 eliminated the single global merge producer: Phase 4 now runs one
    /// INDEPENDENT k-way merge per shard in parallel (one thread per shard, doing
    /// the merge and insert inline), so the merge parallelism is across SHARDS and
    /// is no longer a function of the synonym-phase `threads` knob.  The build must
    /// therefore stay serial-equivalent: a threads=1 build and a threads>=shard_count
    /// build (which differ in how the synonym phase partitions work and spill runs)
    /// must yield identical term->CURIE results AND identical per-shard record
    /// counts, because a term's shard is a pure function of its xxh64 hash.  This
    /// guards against any routing / spill-partition / batching regression in the
    /// parallel per-shard merges.
    #[test]
    fn parallel_writers_match_single_writer_build() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let synonyms = dir.path().join("many.ndjson");
        let mut synonym_file = File::create(&synonyms).unwrap();
        for i in 0..200 {
            writeln!(
                synonym_file,
                r#"{{"curie":"HGNC:{i}","preferred_name":"GENE{i}","names":["GENE{i}","alias{i}"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}}"#
            )
            .unwrap();
        }
        drop(synonym_file);

        let out_serial = dir.path().join("serial.redb");
        let out_parallel = dir.path().join("parallel.redb");
        // local_spill=100 forces several sorted runs so each shard's k-way merge
        // has real work; threads=1 vs threads=4 vary only the synonym-phase worker
        // count (Phase 4 always runs one independent merge per shard regardless).
        build_test(
            out_serial.clone(),
            Vec::new(),
            vec![synonyms.clone()],
            1,
            100,
        )
        .unwrap();
        build_test(out_parallel.clone(), Vec::new(), vec![synonyms], 4, 100).unwrap();

        // Per-shard record counts are identical (routing is a pure function of the
        // term hash, independent of thread count) and every shard got >=1 record.
        let serial_counts = shard_record_counts(&out_serial);
        let parallel_counts = shard_record_counts(&out_parallel);
        assert_eq!(serial_counts, parallel_counts);
        assert_eq!(parallel_counts.len(), SHARD_COUNT_SHARDS);
        for count in &parallel_counts {
            assert!(*count > 0, "shard received no records: {parallel_counts:?}");
        }

        // term -> sorted CURIE set is identical across thread counts.
        let probes: Vec<String> = (0..200)
            .flat_map(|i| [format!("gene{i}"), format!("alias{i}")])
            .collect();
        let norm = |db: PathBuf| -> Vec<(String, Vec<String>)> {
            let rows = lookup_terms(db, probes.clone()).unwrap();
            let mut out: Vec<(String, Vec<String>)> = rows
                .into_iter()
                .map(|(t, recs)| {
                    let mut curies: Vec<String> = recs.into_iter().map(|r| r.curie).collect();
                    curies.sort();
                    (t, curies)
                })
                .collect();
            out.sort();
            out
        };
        let serial = norm(out_serial);
        let parallel = norm(out_parallel);
        assert_eq!(serial.len(), 400);
        assert_eq!(serial, parallel);
    }

    /// US-201 shards the term spill runs AT WRITE TIME (one `run_s{shard}_{id}.bin`
    /// per non-empty shard) so Phase 4 can run one INDEPENDENT k-way merge per
    /// shard in parallel, eliminating the single global producer that capped
    /// US-105.  This test pins both halves of that contract:
    ///
    /// 1. WRITE-TIME PARTITIONING: `process_synonyms` must return exactly
    ///    `shard_count` per-shard run lists, every file named `run_s{shard}_*.bin`
    ///    and present on disk, and EVERY term read back from a shard's runs must
    ///    hash to that shard (`term_shard(term, shard_count) == shard`).  A term
    ///    filed under the wrong shard would be merged/inserted in the wrong file
    ///    and silently vanish from lookups, so this is the core correctness
    ///    invariant of the partition.  Multiple runs must exist (small local_spill)
    ///    so the per-shard k-way merge has real work.
    /// 2. PARALLEL MERGE EQUIVALENCE: two full builds over the SAME synonyms but
    ///    with different shard counts (4 vs 2) — i.e. a legacy-compatible count
    ///    and a smaller runtime override, producing different per-shard run
    ///    layouts merged by independent threads — must yield identical
    ///    term -> set(CURIE) results.  Because each term's postings all land in one
    ///    shard and the read path routes by the same `term_shard` oracle, the
    ///    reconstructed CURIE set is shard-count-independent; any cross-shard
    ///    duplication or drop in the parallel merges would break this equality.
    #[test]
    fn per_shard_spill_runs_and_parallel_merges_match_reference() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let synonyms = dir.path().join("many.ndjson");
        let mut synonym_file = File::create(&synonyms).unwrap();
        for i in 0..200 {
            writeln!(
                synonym_file,
                r#"{{"curie":"HGNC:{i}","preferred_name":"GENE{i}","names":["GENE{i}","alias{i}"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}}"#
            )
            .unwrap();
        }
        drop(synonym_file);

        // --- 1. write-time partitioning ------------------------------------
        let spill_dir = dir.path().join("spill");
        std::fs::create_dir_all(&spill_dir).unwrap();
        let equivalents = EquivIndex::build(&[], &spill_dir, None).unwrap();
        let shard_count = SHARD_COUNT_SHARDS;
        // local_spill=30 forces many spills so each shard has several runs and the
        // per-shard k-way merge does real grouping work.
        let result = process_synonyms(
            std::slice::from_ref(&synonyms),
            &equivalents,
            &spill_dir,
            30,
            1_000_000,
            &HashSet::new(),
            None,
            4,
            shard_count,
            DEFAULT_CHUNK_BYTES,
            2,
            None,
        )
        .unwrap();
        drop(equivalents);

        // Exactly one run list per shard.
        assert_eq!(result.run_paths.len(), shard_count);

        // Every shard produced runs, each file is named run_s{shard}_*.bin and
        // exists on disk, and every term read back routes to that shard.
        let mut total_runs = 0usize;
        for (shard, paths) in result.run_paths.iter().enumerate() {
            assert!(!paths.is_empty(), "shard {shard} produced no run files");
            let expected_prefix = format!("run_s{shard}_");
            for path in paths {
                let name = path.file_name().unwrap().to_string_lossy().into_owned();
                assert!(
                    name.starts_with(&expected_prefix) && name.ends_with(".bin"),
                    "unexpected run file name {name} in shard {shard}"
                );
                assert!(path.exists(), "run file missing on disk: {path:?}");
                let mut reader = RunReader::new(path).unwrap();
                while let Some((term, _pairs)) = reader.cur.take() {
                    assert_eq!(
                        term_shard(&term, shard_count),
                        shard,
                        "term {term} filed under shard {shard} but hashes elsewhere"
                    );
                    reader.advance().unwrap();
                }
                total_runs += 1;
            }
        }
        // Many runs across the shards prove the per-shard merges each k-way merge
        // more than one run (the whole point of the partition).
        assert!(
            total_runs > shard_count,
            "expected multiple per-shard runs, got {total_runs}"
        );

        // --- 2. parallel merge equivalence (shards=4 vs shards=2) ----------
        let out4 = dir.path().join("s4.redb");
        let out2 = dir.path().join("s2.redb");
        build_fullmap_inner(
            out4.clone(),
            Vec::new(),
            vec![synonyms.clone()],
            4,
            4,
            None,
            100,
            1_000_000,
            HashSet::new(),
            None,
            None,
            DEFAULT_CHUNK_BYTES,
            2,
            64 * 1024 * 1024,
            1000,
            dir.path().join("s4.spill.d"),
        )
        .unwrap();
        build_fullmap_inner(
            out2.clone(),
            Vec::new(),
            vec![synonyms],
            4,
            2,
            None,
            100,
            1_000_000,
            HashSet::new(),
            None,
            None,
            DEFAULT_CHUNK_BYTES,
            2,
            64 * 1024 * 1024,
            1000,
            dir.path().join("s2.spill.d"),
        )
        .unwrap();

        let probes: Vec<String> = (0..200)
            .flat_map(|i| [format!("gene{i}"), format!("alias{i}")])
            .collect();
        let norm = |db: PathBuf| -> Vec<(String, Vec<String>)> {
            let rows = lookup_terms(db, probes.clone()).unwrap();
            let mut out: Vec<(String, Vec<String>)> = rows
                .into_iter()
                .map(|(t, recs)| {
                    let mut curies: Vec<String> = recs.into_iter().map(|r| r.curie).collect();
                    curies.sort();
                    (t, curies)
                })
                .collect();
            out.sort();
            out
        };
        let r4 = norm(out4);
        let r2 = norm(out2);
        assert_eq!(r4.len(), 400);
        assert_eq!(r4, r2, "shard count must not change term -> CURIE results");
    }

    /// US-103 fans the read path out across RECORDS shards: `lookup_pair_terms_db`
    /// partitions query terms by `term_shard`, reads each NON-EMPTY shard on its
    /// own thread (capped at `workers`), and re-merges hits in input order.  This
    /// matters because a wrong routing or merge would silently drop, duplicate, or
    /// reorder rows.  The fixture's probe terms provably hash to >=2 different
    /// shards (asserted), misses are interleaved, and the parallel result
    /// (threads=4) must equal the single-threaded chunk path in BOTH content and
    /// order, with hits appearing in probe order and misses yielding no row.
    #[test]
    fn parallel_shard_fanout_merges_in_input_order() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let synonyms = dir.path().join("many.ndjson");
        let output = dir.path().join("fullmap.redb");
        let mut synonym_file = File::create(&synonyms).unwrap();
        for i in 0..120 {
            writeln!(
                synonym_file,
                r#"{{"curie":"HGNC:{i}","preferred_name":"GENE{i}","names":["GENE{i}"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}}"#
            )
            .unwrap();
        }
        drop(synonym_file);
        build_test(output.clone(), Vec::new(), vec![synonyms], 4, 4_000_000).unwrap();

        // Probe terms in a fixed order, interleaving real terms with misses.
        let mut probes: Vec<String> = Vec::new();
        for i in 0..80 {
            probes.push(format!("gene{i}"));
            if i % 7 == 0 {
                probes.push(format!("missing{i}")); // no such term -> miss
            }
        }

        // The real probe terms must span at least 2 shards, otherwise this test
        // would not actually exercise the multi-shard fan-out.
        let spanned: HashSet<usize> = probes
            .iter()
            .filter(|t| !t.starts_with("missing"))
            .map(|t| term_shard(t, SHARD_COUNT_SHARDS))
            .collect();
        assert!(
            spanned.len() >= 2,
            "probe terms must span >=2 shards, got {spanned:?}"
        );

        let shards = open_cached_shards(&output).unwrap();
        assert_eq!(shards.len(), SHARD_COUNT_SHARDS);

        // Parallel fan-out (workers=4) vs the single-threaded chunk path (workers=1)
        // must agree on both content and order.
        let parallel = lookup_pair_terms_db(&shards, &probes, 4).unwrap();
        let serial = lookup_pair_terms_db(&shards, &probes, 1).unwrap();
        assert_eq!(parallel, serial, "parallel fan-out diverged from serial");

        // Hits appear in probe order; misses produced no row.
        let expected_order: Vec<String> = probes
            .iter()
            .filter(|t| !t.starts_with("missing"))
            .cloned()
            .collect();
        let got_order: Vec<String> = parallel.iter().map(|(t, _)| t.clone()).collect();
        assert_eq!(got_order, expected_order, "merge broke input order");

        // End-to-end hydration (through the primary) agrees with the shard-level
        // pairs and yields one row group per hit term.
        let rows = lookup_terms(output, probes.clone()).unwrap();
        assert_eq!(rows.len(), expected_order.len());
        let pair_counts: Vec<(String, usize)> = parallel
            .iter()
            .map(|(t, pairs)| (t.clone(), pairs.len()))
            .collect();
        let row_counts: Vec<(String, usize)> = rows
            .iter()
            .map(|(t, recs)| (t.clone(), recs.len()))
            .collect();
        assert_eq!(pair_counts, row_counts);
    }

    /// `split_counts` hands surplus workers to the busiest bucket (most
    /// terms-per-reader, ties to the lower index), never splits a bucket into
    /// more readers than it has terms, and leaves buckets alone when `workers`
    /// does not exceed the bucket count (surplus jobs then run inline, as
    /// before).
    #[test]
    fn split_counts_balances_surplus_workers_deterministically() {
        // workers at/below the bucket count: one reader per bucket, unchanged.
        assert_eq!(split_counts(&[10, 5, 3], 3), vec![1, 1, 1]);
        assert_eq!(split_counts(&[10, 5, 3], 2), vec![1, 1, 1]);
        // 3 surplus readers: bucket 0 gains two (load 10 -> 5 -> 3; the 5-vs-5
        // tie keeps the lower index), bucket 1 gains one.
        assert_eq!(split_counts(&[10, 5, 3], 6), vec![3, 2, 1]);
        // workers >= total terms: every bucket splits down to single terms.
        assert_eq!(split_counts(&[10, 5, 3], 18), vec![10, 5, 3]);
        assert_eq!(split_counts(&[10, 5, 3], 100), vec![10, 5, 3]);
        assert_eq!(split_counts(&[2, 1], 100), vec![2, 1]);
        // Deterministic: the same input always yields the same plan.
        assert_eq!(split_counts(&[7, 7, 7], 9), split_counts(&[7, 7, 7], 9));
    }

    /// `split_bucket` partitions without loss, duplication, or empty chunks and
    /// preserves the input order inside every chunk.
    #[test]
    fn split_bucket_partitions_without_loss() {
        let bucket: Vec<(usize, String)> = (0..10).map(|i| (i, format!("t{i}"))).collect();
        for count in 1..=10 {
            let chunks = split_bucket(bucket.clone(), count);
            assert_eq!(chunks.len(), count);
            assert!(chunks.iter().all(|c| !c.is_empty()));
            let flat: Vec<(usize, String)> = chunks.into_iter().flatten().collect();
            assert_eq!(flat, bucket, "count={count} lost or reordered terms");
        }
        // count=1 returns the bucket whole.
        assert_eq!(split_bucket(bucket.clone(), 1), vec![bucket]);
    }

    /// The whole point of `build-kg --threads` values ABOVE the shard count:
    /// redb readers share-lock a shard file and open independent read
    /// transactions, so `lookup_pair_terms_db` splits the busiest buckets
    /// across extra readers of the SAME shard — and the result must stay
    /// content- AND order-identical to the serial path at any reader count.
    /// Same fixture shape as `parallel_shard_fanout_merges_in_input_order`,
    /// probed with `2 * SHARD_COUNT_SHARDS` workers so every non-empty bucket
    /// gains at least one extra reader.
    #[test]
    fn workers_above_shard_count_still_match_serial() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let synonyms = dir.path().join("many.ndjson");
        let output = dir.path().join("fullmap.redb");
        let mut synonym_file = File::create(&synonyms).unwrap();
        for i in 0..120 {
            writeln!(
                synonym_file,
                r#"{{"curie":"HGNC:{i}","preferred_name":"GENE{i}","names":["GENE{i}"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}}"#
            )
            .unwrap();
        }
        drop(synonym_file);
        build_test(output.clone(), Vec::new(), vec![synonyms], 4, 4_000_000).unwrap();

        // Probe terms in a fixed order, interleaving real terms with misses.
        let mut probes: Vec<String> = Vec::new();
        for i in 0..80 {
            probes.push(format!("gene{i}"));
            if i % 7 == 0 {
                probes.push(format!("missing{i}"));
            }
        }
        let spanned: HashSet<usize> = probes
            .iter()
            .filter(|t| !t.starts_with("missing"))
            .map(|t| term_shard(t, SHARD_COUNT_SHARDS))
            .collect();
        assert!(
            spanned.len() >= 2,
            "probe terms must span >=2 shards, got {spanned:?}"
        );

        let shards = open_cached_shards(&output).unwrap();
        let above = 2 * SHARD_COUNT_SHARDS;

        // More readers than shards vs forced-serial: identical content + order.
        let via_above = lookup_pair_terms_db(&shards, &probes, above).unwrap();
        let via_serial = lookup_pair_terms_db(&shards, &probes, 1).unwrap();
        assert_eq!(
            via_above, via_serial,
            "above-shard-count fan-out diverged from serial"
        );
        let expected_order: Vec<String> = probes
            .iter()
            .filter(|t| !t.starts_with("missing"))
            .cloned()
            .collect();
        let got_order: Vec<String> = via_above.iter().map(|(t, _)| t.clone()).collect();
        assert_eq!(
            got_order, expected_order,
            "bucket splitting broke input order"
        );

        // End-to-end (through `lookup_pair_terms` / `lookup_terms`, which clamp
        // the auto-selected workers to the term count) matches the shard-level
        // serial result, through full hydration alike.
        let pairs_auto = lookup_pair_terms(output.clone(), probes.clone()).unwrap();
        assert_eq!(pairs_auto, via_serial);
        let rows_auto = lookup_terms(output, probes.clone()).unwrap();
        let row_counts: Vec<(String, usize)> = rows_auto
            .iter()
            .map(|(t, recs)| (t.clone(), recs.len()))
            .collect();
        let pair_counts: Vec<(String, usize)> = via_serial
            .iter()
            .map(|(t, pairs)| (t.clone(), pairs.len()))
            .collect();
        assert_eq!(row_counts, pair_counts);
    }

    /// The production build-kg resolve calls `lookup_fullmap_terms` with no
    /// thread tuning, so the parallel shard fan-out must kick in from the Rust
    /// DEFAULT alone.  This builds a
    /// large fixture and probes it with a batch that crosses `LOOKUP_PARALLEL_MIN`
    /// and spans every default shard, then asserts: (a) the default worker count
    /// is >1 on any multi-core host, and reaches the 16-shard fan-out cap on a
    /// host with at least 16 CPUs (so `lookup_pair_terms_db` can spawn one reader
    /// per non-empty shard), and (b) the auto-selected parallel lookup returns
    /// results IDENTICAL (content + order) to a forced-serial
    /// `lookup_pair_terms_db(..., 1)`, with misses
    /// dropped.  On smaller hosts the maximum-fanout assertion is skipped but
    /// equivalence still holds.
    #[test]
    fn large_batch_lookup_defaults_to_parallel() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let synonyms = dir.path().join("large.ndjson");
        let output = dir.path().join("fullmap.redb");
        let mut synonym_file = File::create(&synonyms).unwrap();
        for i in 0..1100 {
            writeln!(
                synonym_file,
                r#"{{"curie":"HGNC:{i}","preferred_name":"GENE{i}","names":["GENE{i}"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}}"#
            )
            .unwrap();
        }
        drop(synonym_file);
        build_test(output.clone(), Vec::new(), vec![synonyms], 4, 4_000_000).unwrap();

        // Large probe batch (>= LOOKUP_PARALLEL_MIN) with misses interleaved.
        let mut probes: Vec<String> = Vec::new();
        for i in 0..1100 {
            probes.push(format!("gene{i}"));
            if i % 50 == 0 {
                probes.push(format!("absent{i}")); // miss -> dropped
            }
        }
        assert!(
            probes.len() >= LOOKUP_PARALLEL_MIN,
            "batch must cross the parallel threshold, got {}",
            probes.len()
        );

        // Real probe terms provably span every default shard, so on a sufficiently
        // large machine the default worker policy can drive the full 16-shard
        // lookup fan-out.
        let spanned: HashSet<usize> = probes
            .iter()
            .filter(|t| !t.starts_with("absent"))
            .map(|t| term_shard(t, SHARD_COUNT_SHARDS))
            .collect();
        assert_eq!(
            spanned.len(),
            SHARD_COUNT_SHARDS,
            "probes must span every default shard, got {spanned:?}"
        );

        // The Rust default must select >1 worker for this large batch on any
        // multi-core host; after the `.max(1).min(len)` clamp in `lookup_pair_terms`
        // the effective worker count is still >1. On hosts with at least the
        // default shard count, it reaches that count so all non-empty shards may
        // be queried concurrently.
        let cpus = std::thread::available_parallelism()
            .map(std::num::NonZero::get)
            .unwrap_or(1);
        let default_workers = default_lookup_workers(probes.len());
        if cpus > 1 {
            assert!(default_workers > 1, "large batch must default to parallel");
            let effective = default_workers.max(1).min(probes.len().max(1));
            assert!(
                effective > 1,
                "clamp must not collapse a large batch to serial"
            );
        }
        if cpus >= SHARD_COUNT_SHARDS {
            assert!(
                default_workers >= SHARD_COUNT_SHARDS,
                "large batch should be able to fan out across all default shards"
            );
        }
        // Sub-threshold batches stay serial.
        assert_eq!(default_lookup_workers(0), 1, "empty batch stays serial");
        assert_eq!(
            default_lookup_workers(LOOKUP_PARALLEL_MIN - 1),
            1,
            "sub-threshold batch stays serial"
        );

        // The auto-selected lookup (production path) == forced-serial shard read:
        // identical content AND order, misses dropped.
        let via_default = lookup_pair_terms(output.clone(), probes.clone()).unwrap();
        let shards = open_cached_shards(&output).unwrap();
        let via_serial = lookup_pair_terms_db(&shards, &probes, 1).unwrap();
        assert_eq!(
            via_default, via_serial,
            "auto-selected workers diverged from forced-serial"
        );
        let expected_order: Vec<String> = probes
            .iter()
            .filter(|t| !t.starts_with("absent"))
            .cloned()
            .collect();
        let got_order: Vec<String> = via_default.iter().map(|(t, _)| t.clone()).collect();
        assert_eq!(got_order, expected_order, "merge broke input order");
    }

    /// US-103's contract is that `lookup_fullmap_terms` releases the GIL for the
    /// whole lookup: every pure-Rust data-fetch call (`lookup_terms` /
    /// `lookup_pair_terms`) must be wrapped in `py.detach`, with the `PyList`
    /// built only afterwards.  A behavioral GIL test is timing-dependent and
    /// flaky from a Rust `#[test]` (and Python test files are out of scope for
    /// this story), so this is a precise code-level guard: it isolates the
    /// function source and asserts each fetch call appears exactly once and only
    /// inside a `py.detach(move || ...)` wrapper.  If someone removes a detach or
    /// adds a bare fetch call, the read path would silently hold the GIL again
    /// (blocking rich's Live repaint thread) and this test fails loudly.
    #[test]
    fn lookup_fullmap_terms_wraps_every_fetch_in_detach() {
        let src = include_str!("fullmap.rs");
        let start = src
            .find("pub fn lookup_fullmap_terms")
            .expect("lookup_fullmap_terms present");
        let tail = &src[start..];
        let end = tail
            .find("\n#[pyfunction]")
            .expect("next pyfunction bounds fn");
        let body = &tail[..end];

        // Rows fetch: exactly one call, and it is the detached one.
        let rows_calls = body.matches("lookup_terms(db, terms)").count();
        let rows_detached = body
            .matches("py.detach(move || lookup_terms(db, terms)")
            .count();
        assert_eq!(rows_calls, 1, "rows fetch must be called exactly once");
        assert_eq!(
            rows_detached, 1,
            "rows fetch must be wrapped in py.detach (GIL released)"
        );

        // Pairs fetch: exactly one call, and it is the detached one.
        let pair_calls = body.matches("lookup_pair_terms(db, terms)").count();
        let pair_detached = body
            .matches("py.detach(move || lookup_pair_terms(db, terms)")
            .count();
        assert_eq!(pair_calls, 1, "pairs fetch must be called exactly once");
        assert_eq!(
            pair_detached, 1,
            "pairs fetch must be wrapped in py.detach (GIL released)"
        );
    }

    /// The build and read paths agree on a parameterized shard count.  A 2-shard
    /// build must write META.shards="2", create exactly s0+s1 (each with RECORDS
    /// plus META.build_id, even the one that receives no terms), create no higher shard files up
    /// to the compile-time cap, and the read path must open exactly 2 shards (from
    /// META) and still resolve every term.  The public entry point pins the count
    /// to `SHARD_COUNT_SHARDS`, but this exercises the same parameterized path the
    /// read path relies on for databases built with fewer shards.
    #[test]
    fn runtime_shard_count_builds_and_reads_fewer_shards() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let synonyms = dir.path().join("HGNC.ndjson");
        let output = dir.path().join("fullmap.redb");
        let mut synonym_file = File::create(&synonyms).unwrap();
        for i in 0..50 {
            writeln!(
                synonym_file,
                r#"{{"curie":"HGNC:{i}","preferred_name":"GENE{i}","names":["GENE{i}"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}}"#
            )
            .unwrap();
        }
        drop(synonym_file);

        let spill_dir = {
            let mut p = output.clone().into_os_string();
            p.push(".spill.d");
            PathBuf::from(p)
        };
        // worker_count=4, shard_count=2.
        build_fullmap_inner(
            output.clone(),
            Vec::new(),
            vec![synonyms],
            4,
            2,
            None,
            4_000_000,
            1_000_000,
            HashSet::new(),
            None,
            None,
            DEFAULT_CHUNK_BYTES,
            2,
            64 * 1024 * 1024,
            1000,
            spill_dir,
        )
        .unwrap();

        // META.shards reflects the parameterized shard count.
        let database = open_cached(output.clone()).unwrap();
        let read = database.begin_read().unwrap();
        let meta = read.open_table(META).unwrap();
        assert_eq!(meta.get("shards").unwrap().unwrap().value(), "2");
        drop(meta);
        drop(read);

        // Exactly s0+s1 exist, each holding a RECORDS table; higher shard files
        // up to the compile-time cap must NOT exist. (Read-only opens take a
        // SHARED lock, so they coexist even with the cached read-only opens
        // that follow.)
        for index in 0..2 {
            let shard = shard_path(&output, index);
            assert!(shard.exists(), "missing shard file {shard:?}");
            let db = ReadOnlyDatabase::open(&shard).unwrap();
            let read = db.begin_read().unwrap();
            let _records = read.open_table(RECORDS).unwrap();
        }
        for index in 2..SHARD_COUNT_SHARDS {
            assert!(
                !shard_path(&output, index).exists(),
                "shard {index} must not exist for a 2-shard build"
            );
        }

        // Read path opens exactly 2 shards and resolves every term.
        let shards = open_cached_shards(&output).unwrap();
        assert_eq!(shards.len(), 2);
        let terms: Vec<String> = (0..50).map(|i| format!("gene{i}")).collect();
        let rows = lookup_terms(output, terms).unwrap();
        assert_eq!(rows.len(), 50);
    }

    /// The public entry point ignores `TABLASSERT_FULLMAP_SHARDS`: even when it is
    /// set to a smaller value, `build_fullmap_db` writes exactly
    /// `SHARD_COUNT_SHARDS` (16) shard files and records META.shards="16".  This is
    /// the regression guard for removing the env-var tunable — the layout tests
    /// above call `build_fullmap_inner` / `build_test` directly and so cannot catch
    /// a public path that re-reads the variable.  Setting the variable here is safe
    /// across concurrent tests: the build no longer reads it (its only reader,
    /// `resolve_shard_count`, was removed), and it is removed again before any
    /// assertion can panic.
    #[test]
    fn public_build_ignores_shards_env_var() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let synonyms = dir.path().join("HGNC.ndjson");
        let output = dir.path().join("fullmap.redb");
        let mut synonym_file = File::create(&synonyms).unwrap();
        for i in 0..50 {
            writeln!(
                synonym_file,
                r#"{{"curie":"HGNC:{i}","preferred_name":"GENE{i}","names":["GENE{i}"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}}"#
            )
            .unwrap();
        }
        drop(synonym_file);

        std::env::set_var("TABLASSERT_FULLMAP_SHARDS", "2");
        let built = Python::attach(|py| {
            build_fullmap_db(py, output.clone(), Vec::new(), vec![synonyms], None, None)
        });
        std::env::remove_var("TABLASSERT_FULLMAP_SHARDS");
        built.unwrap();

        // The env var is ignored: META advertises 16 shards, all 16 sibling shard
        // files exist (s0..s15), and there is no s16.
        let database = open_cached(output.clone()).unwrap();
        let read = database.begin_read().unwrap();
        let meta = read.open_table(META).unwrap();
        assert_eq!(meta.get("shards").unwrap().unwrap().value(), "16");
        drop(meta);
        drop(read);
        for index in 0..SHARD_COUNT_SHARDS {
            assert!(
                shard_path(&output, index).exists(),
                "missing shard file {index}"
            );
        }
        assert!(!shard_path(&output, SHARD_COUNT_SHARDS).exists());

        // Lookups still resolve across all 16 shards.
        let terms: Vec<String> = (0..50).map(|i| format!("gene{i}")).collect();
        let rows = lookup_terms(output, terms).unwrap();
        assert_eq!(rows.len(), 50);
    }

    /// Backward compatibility: databases built/advertising the former 4-shard
    /// layout must continue to open exactly those four shard files and route terms
    /// with a 4-shard mask, even though new default builds now write 16 shards.
    #[test]
    fn legacy_four_shard_db_reads_only_four_shards() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let synonyms = dir.path().join("HGNC.ndjson");
        let output = dir.path().join("fullmap.redb");
        let mut synonym_file = File::create(&synonyms).unwrap();
        for i in 0..80 {
            writeln!(
                synonym_file,
                r#"{{"curie":"HGNC:{i}","preferred_name":"GENE{i}","names":["GENE{i}"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}}"#
            )
            .unwrap();
        }
        drop(synonym_file);

        let spill_dir = {
            let mut p = output.clone().into_os_string();
            p.push(".spill.d");
            PathBuf::from(p)
        };
        build_fullmap_inner(
            output.clone(),
            Vec::new(),
            vec![synonyms],
            4,
            4,
            None,
            4_000_000,
            1_000_000,
            HashSet::new(),
            None,
            None,
            DEFAULT_CHUNK_BYTES,
            2,
            64 * 1024 * 1024,
            1000,
            spill_dir,
        )
        .unwrap();

        let database = open_cached(output.clone()).unwrap();
        let read = database.begin_read().unwrap();
        let meta = read.open_table(META).unwrap();
        assert_eq!(meta.get("shards").unwrap().unwrap().value(), "4");
        drop(meta);
        drop(read);

        let shards = open_cached_shards(&output).unwrap();
        assert_eq!(shards.len(), 4);
        for index in 0..4 {
            assert!(
                shard_path(&output, index).exists(),
                "missing legacy shard {index}"
            );
        }
        for index in 4..SHARD_COUNT_SHARDS {
            assert!(
                !shard_path(&output, index).exists(),
                "new default shard {index} must not be required for legacy 4-shard DBs"
            );
        }

        let terms: Vec<String> = (0..80).map(|i| format!("gene{i}")).collect();
        let rows = lookup_terms(output, terms).unwrap();
        assert_eq!(rows.len(), 80);
    }

    #[test]
    fn lookup_rejects_v1_schema() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("fullmap.redb");
        let database = Database::create(&output).unwrap();
        let write = database.begin_write().unwrap();
        {
            let mut meta = write.open_table(META).unwrap();
            meta.insert("schema", SCHEMA_VERSION_V1).unwrap();
        }
        write.commit().unwrap();
        drop(database);

        let err = lookup_terms(output, vec!["brca1".to_string()]).unwrap_err();
        assert!(err
            .to_string()
            .contains("fullmap DB is outdated; rebuild with 'tablassert build-fullmap'"));
    }

    /// v3 was the last single-file schema; the sharded read path must reject a
    /// v3 primary as outdated (rebuild hint), not as a generic unsupported
    /// schema, so migrating users get actionable guidance.
    #[test]
    fn lookup_rejects_v3_schema_as_outdated() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("fullmap.redb");
        let database = Database::create(&output).unwrap();
        let write = database.begin_write().unwrap();
        {
            let mut meta = write.open_table(META).unwrap();
            meta.insert("schema", SCHEMA_VERSION_V3).unwrap();
        }
        write.commit().unwrap();
        drop(database);

        let err = lookup_terms(output, vec!["brca1".to_string()]).unwrap_err();
        assert!(err
            .to_string()
            .contains("fullmap DB is outdated; rebuild with 'tablassert build-fullmap'"));
    }

    /// v4 and v5 are the outdated tags the per-version tests above did not
    /// cover, and v5 is the schema this bump migrates FROM: a v5 DB's RECORDS
    /// keys are cleaned+lowercased terms, NOT `nlp::normalize_l1` output, so a
    /// normalized query can never hit them (a v5 DB opened silently would look
    /// like "nothing resolves" rather than "rebuild me").  Both must fail at
    /// LOOKUP with the actionable rebuild instruction — never as the generic
    /// unsupported-schema error, and never by returning zero matches.
    #[test]
    fn lookup_rejects_v4_and_v5_schemas_as_outdated() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        for (index, tag) in [SCHEMA_VERSION_V4, SCHEMA_VERSION_V5]
            .into_iter()
            .enumerate()
        {
            let path = dir.path().join(format!("outdated{index}.redb"));
            let database = Database::create(&path).unwrap();
            let write = database.begin_write().unwrap();
            {
                let mut meta = write.open_table(META).unwrap();
                meta.insert("schema", tag).unwrap();
            }
            write.commit().unwrap();
            drop(database);

            let err = lookup_terms(path, vec!["brca1".to_string()]).unwrap_err();
            assert!(
                err.to_string()
                    .contains("fullmap DB is outdated; rebuild with 'tablassert build-fullmap'"),
                "{tag} must be rejected as outdated with the rebuild instruction, got: {err}"
            );
        }
    }

    /// Two `ReadOnlyDatabase` handles on one file coexist (shared locks) and
    /// both read — the in-process analogue of the cross-process reader
    /// guarantee that motivated the redb 4 read-only switch.
    #[test]
    fn two_read_only_handles_coexist_on_one_file() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("fullmap.redb");
        let database = Database::create(&output).unwrap();
        let write = database.begin_write().unwrap();
        {
            let mut meta = write.open_table(META).unwrap();
            meta.insert("schema", SCHEMA_VERSION).unwrap();
        }
        write.commit().unwrap();
        drop(database);

        let first = ReadOnlyDatabase::open(&output).unwrap();
        let second = ReadOnlyDatabase::open(&output).unwrap();
        for db in [&first, &second] {
            let read = db.begin_read().unwrap();
            let meta = read.open_table(META).unwrap();
            assert_eq!(meta.get("schema").unwrap().unwrap().value(), SCHEMA_VERSION);
        }
    }

    fn test_build_id(marker: &str) -> u64 {
        xxh64(marker.as_bytes(), 0)
    }

    /// Write a minimal primary-shaped DB at `path` whose META carries `marker`
    /// alongside the current schema tag and build token.
    /// The writer is dropped before returning, so the file is unlocked.
    fn write_marker_primary(path: &Path, marker: &str, shard_count: usize) {
        let database = Database::create(path).unwrap();
        let write = database.begin_write().unwrap();
        {
            let mut meta = write.open_table(META).unwrap();
            meta.insert("schema", SCHEMA_VERSION).unwrap();
            let build_id = test_build_id(marker).to_string();
            meta.insert("build_id", build_id.as_str()).unwrap();
            meta.insert("marker", marker).unwrap();
            let shards = shard_count.to_string();
            meta.insert("shards", shards.as_str()).unwrap();
        }
        write.commit().unwrap();
        drop(database);
    }

    fn write_marker_db(path: &Path, marker: &str) {
        write_marker_primary(path, marker, SHARD_COUNT_SHARDS);
    }

    fn marker_of(database: &ReadOnlyDatabase) -> String {
        let read = database.begin_read().unwrap();
        let meta = read.open_table(META).unwrap();
        // redb 4.2-to-be (experimental-api-5, implied by experimental_cursor)
        // removed the 'static-guard inherent `ReadOnlyTable::get()`; `get_owned()`
        // is its reference-counted replacement (keeps the transaction alive).
        meta.get_owned("marker")
            .unwrap()
            .unwrap()
            .value()
            .to_string()
    }

    /// Write a shard-shaped DB holding one record `1 -> marker` plus META.build_id.
    fn write_shard_db(path: &Path, marker: &str) {
        let database = Database::create(path).unwrap();
        let write = database.begin_write().unwrap();
        {
            let build_id = test_build_id(marker).to_string();
            let mut meta = write.open_table(META).unwrap();
            meta.insert("build_id", build_id.as_str()).unwrap();
            drop(meta);
            let mut records = write.open_table(RECORDS).unwrap();
            records.insert(1u64, marker.as_bytes()).unwrap();
        }
        write.commit().unwrap();
        drop(database);
    }

    fn shard_payload(database: &ReadOnlyDatabase) -> Vec<u8> {
        let read = database.begin_read().unwrap();
        let table = read.open_table(RECORDS).unwrap();
        // Same `get_owned()` note as `marker_of` above.
        table.get_owned(1u64).unwrap().unwrap().value().to_vec()
    }

    /// Rename the full sharded build at `src` (primary + every shard) over the
    /// same-named files at `dst` — the multi-file generation swap a rebuild
    /// performs, atomic per file and deterministic (no sleeps/races).
    fn swap_build_over(src: &Path, dst: &Path) {
        std::fs::rename(src, dst).unwrap();
        for index in 0..SHARD_COUNT_SHARDS {
            std::fs::rename(shard_path(src, index), shard_path(dst, index)).unwrap();
        }
    }

    /// Write a minimal consistent bundle at `primary`: a marker primary
    /// advertising `shard_count` shards in META plus that many shard DBs, all
    /// carrying `marker`, so tests can warm `open_cached_shards` without a
    /// full build.
    fn write_marker_bundle(primary: &Path, marker: &str, shard_count: usize) {
        write_marker_primary(primary, marker, shard_count);
        for index in 0..shard_count {
            write_shard_db(&shard_path(primary, index), marker);
        }
    }

    /// Mid-rebuild window: the build commits the new primary BEFORE the shard
    /// files exist, so a warm-cache reader sees a NEW primary with ABSENT
    /// shards.  The bundle must never pair them (new primary + old shards):
    /// the MIXED serve kinds retry until exhaustion — raising with the stable
    /// "changing generation" token the Python-side `_LOCK_RETRY_TOKENS`
    /// retries on — and once the gen2 shards appear the same lookup pins the
    /// consistent gen2 bundle.
    #[test]
    fn bundle_open_mixed_window_exhausts_then_follows_new_generation() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let primary = dir.path().join("fullmap.redb");

        // Warm the cache with a consistent gen1 bundle.
        write_marker_bundle(&primary, "gen1", 2);
        let shards = open_cached_shards(&primary).unwrap();
        assert!(shards.iter().all(|db| shard_payload(db) == b"gen1"));

        // Mid-window: shard files absent, gen2 primary already at the path.
        std::fs::remove_file(shard_path(&primary, 0)).unwrap();
        std::fs::remove_file(shard_path(&primary, 1)).unwrap();
        let staging = dir.path().join("fullmap.next.redb");
        write_marker_bundle(&staging, "gen2", 2);
        std::fs::rename(&staging, &primary).unwrap();

        // No bundle may mix the new primary with the old shards: retries
        // exhaust and raise (the exhaustion message carries the Python retry
        // token "changing generation").
        let err = match open_cached_shards(&primary) {
            Ok(_) => panic!("mixed new-primary + old-shard bundle must not be served"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("changing generation"),
            "exhaustion message must carry the Python retry token: {err}"
        );

        // Once the gen2 shards appear, the same lookup pins the consistent
        // gen2 bundle.
        write_shard_db(&shard_path(&primary, 0), "gen2");
        write_shard_db(&shard_path(&primary, 1), "gen2");
        let shards = open_cached_shards(&primary).unwrap();
        assert!(
            shards.iter().all(|db| shard_payload(db) == b"gen2"),
            "the recovered bundle must be the NEW generation"
        );
        let database = open_cached(primary).unwrap();
        assert_eq!(marker_of(&database), "gen2");
    }

    /// Unlink-phase shape: the primary can be absent while old shard paths are
    /// still present.  Mixed stale/current serve kinds must exhaust, then a
    /// reappearing primary with the shards' build_id forms a consistent bundle.
    #[test]
    fn bundle_open_primary_absent_shards_current_exhausts_then_follows_primary() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let primary = dir.path().join("fullmap.redb");

        write_marker_bundle(&primary, "gen1", 2);
        let shards = open_cached_shards(&primary).unwrap();
        assert!(shards.iter().all(|db| shard_payload(db) == b"gen1"));

        std::fs::remove_file(&primary).unwrap();
        let err = match open_cached_shards(&primary) {
            Ok(_) => panic!("primary-absent + shard-current bundle must not be served"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("changing generation"),
            "exhaustion message must carry the Python retry token: {err}"
        );

        write_marker_primary(&primary, "gen1", 2);
        let shards = open_cached_shards(&primary).unwrap();
        assert!(shards.iter().all(|db| shard_payload(db) == b"gen1"));
        let database = open_cached(primary).unwrap();
        assert_eq!(marker_of(&database), "gen1");
    }

    /// Primary-only readers can advance just the primary cache entry.  If all
    /// paths then go absent, the all-stale arm must reject primary=gen2 plus
    /// shards=gen1 rather than serving a torn cached bundle.
    #[test]
    fn bundle_open_all_absent_rejects_diverged_cached_build_ids() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let primary = dir.path().join("fullmap.redb");

        write_marker_bundle(&primary, "gen1", 2);
        let shards = open_cached_shards(&primary).unwrap();
        assert!(shards.iter().all(|db| shard_payload(db) == b"gen1"));

        let staging = dir.path().join("fullmap.next.redb");
        write_marker_primary(&staging, "gen2", 2);
        std::fs::rename(&staging, &primary).unwrap();
        let database = open_cached(primary.clone()).unwrap();
        assert_eq!(marker_of(&database), "gen2");

        std::fs::remove_file(&primary).unwrap();
        std::fs::remove_file(shard_path(&primary, 0)).unwrap();
        std::fs::remove_file(shard_path(&primary, 1)).unwrap();

        let err = match open_cached_shards(&primary) {
            Ok(_) => panic!("all-stale cache with diverged build IDs must not be served"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("changing generation"),
            "exhaustion message must carry the Python retry token: {err}"
        );
    }

    /// ALL paths absent with a warm cache: the all-stale arm returns the old
    /// bundle as one consistent snapshot — the documented snapshot semantics
    /// (generalizing the old (None, None) generation check).
    #[test]
    fn bundle_open_all_absent_serves_old_snapshot() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let primary = dir.path().join("fullmap.redb");
        write_marker_bundle(&primary, "gen1", 2);
        let shards = open_cached_shards(&primary).unwrap();
        assert!(shards.iter().all(|db| shard_payload(db) == b"gen1"));

        std::fs::remove_file(&primary).unwrap();
        std::fs::remove_file(shard_path(&primary, 0)).unwrap();
        std::fs::remove_file(shard_path(&primary, 1)).unwrap();

        let shards = open_cached_shards(&primary).unwrap();
        assert!(
            shards.iter().all(|db| shard_payload(db) == b"gen1"),
            "an all-absent bundle must serve the old snapshot"
        );
        let database = open_cached(primary).unwrap();
        assert_eq!(marker_of(&database), "gen1");
    }

    /// While the path is absent (mid-rebuild window), a cached hit keeps
    /// serving the old snapshot instead of erroring — the cross-process reader
    /// guarantee from before generation pinning.  The lookup after the
    /// replacement appears follows the new generation (covered above).
    #[test]
    fn cached_handle_serves_old_snapshot_while_path_absent() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("fullmap.redb");
        write_marker_db(&output, "gen1");
        let handle = open_cached(output.clone()).unwrap();
        assert_eq!(marker_of(&handle), "gen1");

        std::fs::remove_file(&output).unwrap();
        let again = open_cached(output).unwrap();
        assert_eq!(
            marker_of(&again),
            "gen1",
            "an absent path must serve the cached old snapshot, not error"
        );
    }

    /// A rebuild replaces the file at a path on a FRESH inode (unlink +
    /// recreate); the cache must follow the path: after renaming a new primary
    /// over a cached one, the next `open_cached` returns the replacement's
    /// content even though the stale handle is still alive.
    #[test]
    fn cached_primary_follows_replacement_generation() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("fullmap.redb");
        write_marker_db(&output, "gen1");
        let stale = open_cached(output.clone()).unwrap();
        assert_eq!(marker_of(&stale), "gen1");

        // Stage the replacement at a sibling path, then rename over — one
        // atomic, deterministic generation swap (fresh inode).
        let staging = dir.path().join("fullmap.next.redb");
        write_marker_db(&staging, "gen2");
        std::fs::rename(&staging, &output).unwrap();

        let fresh = open_cached(output).unwrap();
        assert_eq!(
            marker_of(&fresh),
            "gen2",
            "the next open must see the replacement generation"
        );
        assert_eq!(
            marker_of(&stale),
            "gen1",
            "the evicted handle still reads its own old snapshot"
        );
    }

    /// Same generation-follow guarantee for shard handles: replacing a shard
    /// file under a cached handle makes the next `open_cached_shard` read the
    /// replacement's records.
    #[test]
    fn cached_shard_follows_replacement_generation() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let primary = dir.path().join("fullmap.redb");
        let shard = shard_path(&primary, 3);
        write_shard_db(&shard, "gen1");
        let stale = open_cached_shard(&primary, 3).unwrap().0;
        assert_eq!(shard_payload(&stale), b"gen1");

        let staging = dir.path().join("fullmap.s3.next.redb");
        write_shard_db(&staging, "gen2");
        std::fs::rename(&staging, &shard).unwrap();

        let fresh = open_cached_shard(&primary, 3).unwrap().0;
        assert_eq!(
            shard_payload(&fresh),
            b"gen2",
            "the next shard open must see the replacement generation"
        );
    }

    /// Rebuild-at-same-path: after replacing the whole cached bundle, the next
    /// lookup reads ONLY the new generation — the old term is gone, the new
    /// term resolves, and its hydrated CURIE row comes from the NEW primary.
    /// A mixed-generation lookup would resurrect the old term or hydrate the
    /// new records against the old primary's CURIES and surface the wrong
    /// curie/preferred_name.
    #[test]
    fn lookup_after_rebuild_reads_one_consistent_generation() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("fullmap.redb");

        // Generation 1: indexes BRCA1 under HGNC:1100; warm the cache with a lookup.
        let synonyms1 = dir.path().join("gen1.ndjson");
        let mut file = File::create(&synonyms1).unwrap();
        writeln!(
            file,
            r#"{{"curie":"HGNC:1100","preferred_name":"BRCA1","names":["BRCA1"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}}"#
        )
        .unwrap();
        build_test(output.clone(), Vec::new(), vec![synonyms1], 1, 4_000_000).unwrap();
        let rows = lookup_terms(output.clone(), vec!["brca1".to_string()]).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1[0].curie, "HGNC:1100");

        // Generation 2 (built elsewhere, then renamed over): indexes TP53.
        let dir2 = tempfile::tempdir().unwrap();
        let built = dir2.path().join("fullmap.redb");
        let synonyms2 = dir2.path().join("gen2.ndjson");
        let mut file = File::create(&synonyms2).unwrap();
        writeln!(
            file,
            r#"{{"curie":"NCBIGene:7157","preferred_name":"TP53","names":["TP53"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}}"#
        )
        .unwrap();
        build_test(built.clone(), Vec::new(), vec![synonyms2], 1, 4_000_000).unwrap();
        swap_build_over(&built, &output);

        // Old-generation term must be gone (stale shards would resurrect it).
        let rows = lookup_terms(output.clone(), vec!["brca1".to_string()]).unwrap();
        assert!(
            rows.is_empty(),
            "stale shard generation resurrected an old term: {rows:?}"
        );
        // New term resolves against the NEW primary's dims/CURIES (a stale
        // primary would hydrate the wrong curie/preferred_name).
        let rows = lookup_terms(output, vec!["tp53".to_string()]).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1[0].curie, "NCBIGene:7157");
        assert_eq!(rows[0].1[0].preferred_name, "TP53");
    }

    /// A live writer holds the exclusive lock, so a read-only open fails with
    /// `DatabaseAlreadyOpen` — and succeeds once the writer drops.  This is the
    /// reader-vs-rebuild window that `_call_with_lock_retry` (Python side)
    /// still covers after the shared-lock switch.
    #[test]
    fn writer_blocks_read_only_open_until_dropped() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("fullmap.redb");
        let writer = Database::create(&output).unwrap();

        let err = match ReadOnlyDatabase::open(&output) {
            Ok(_) => panic!("expected read-only open to be blocked by the writer"),
            Err(err) => err,
        };
        assert!(matches!(err, redb::DatabaseError::DatabaseAlreadyOpen));

        drop(writer);
        ReadOnlyDatabase::open(&output).unwrap();
    }

    /// A file in the old (v2) redb file format must map to the actionable
    /// rebuild hint, not a cryptic redb error: redb ≥ 3 dropped the v2 format
    /// that pre-upgrade builds wrote, and a rebuild is the only path forward.
    /// Simulate the old format by patching the primary commit slot's
    /// format-version byte (file offset 64) to 2 — exactly the condition redb
    /// 4 detects as `UpgradeRequired`.
    #[test]
    fn read_only_open_maps_outdated_file_format_to_rebuild_hint() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("fullmap.redb");
        let database = Database::create(&output).unwrap();
        let write = database.begin_write().unwrap();
        {
            let mut meta = write.open_table(META).unwrap();
            meta.insert("schema", SCHEMA_VERSION).unwrap();
        }
        write.commit().unwrap();
        drop(database);

        patch_db_byte(&output, 64, |version| {
            assert_eq!(version, 3, "expected the v3 file format version");
            2 // v2 format version -> UpgradeRequired
        });

        // Pin the RAW variant first (both UpgradeRequired and RepairAborted map
        // to the same rebuild hint in open_read_only, so only this asserts the
        // patch hit the format-version check and not some unrelated failure).
        let raw = match ReadOnlyDatabase::open(&output) {
            Ok(_) => panic!("expected ReadOnlyDatabase::open to fail"),
            Err(err) => err,
        };
        assert!(matches!(raw, redb::DatabaseError::UpgradeRequired(2)));

        let err = match open_read_only(&output) {
            Ok(_) => panic!("expected open_read_only to fail"),
            Err(err) => err,
        };
        assert!(err.to_string().contains(
            "fullmap DB is outdated or needs repair; rebuild with 'tablassert build-fullmap'"
        ));
    }

    /// A crash-damaged file (recovery required) cannot be repaired through a
    /// read-only open — redb aborts the repair — so it must surface the same
    /// rebuild hint.  Simulate the crash state by setting the RECOVERY_REQUIRED
    /// flag (bit 2 of the god byte at file offset 9), as redb's own repair
    /// tests do.
    #[test]
    fn read_only_open_maps_recovery_required_to_rebuild_hint() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("fullmap.redb");
        let database = Database::create(&output).unwrap();
        drop(database);

        patch_db_byte(&output, 9, |god| god | 2);

        // Pin the RAW variant (see the outdated-format test: both variants map
        // to the same rebuild hint, so only this asserts the god-byte patch
        // actually put the file into the recovery-required state).
        let raw = match ReadOnlyDatabase::open(&output) {
            Ok(_) => panic!("expected ReadOnlyDatabase::open to fail"),
            Err(err) => err,
        };
        assert!(matches!(raw, redb::DatabaseError::RepairAborted));

        let err = match open_read_only(&output) {
            Ok(_) => panic!("expected open_read_only to fail"),
            Err(err) => err,
        };
        assert!(err.to_string().contains(
            "fullmap DB is outdated or needs repair; rebuild with 'tablassert build-fullmap'"
        ));
    }

    /// Test helper: read one byte at `offset` in `path`, transform it through
    /// `f`, and write the result back (no other bytes touched).
    fn patch_db_byte(path: &Path, offset: u64, f: impl FnOnce(u8) -> u8) {
        use std::io::{Read, Seek, SeekFrom, Write};
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        file.seek(SeekFrom::Start(offset)).unwrap();
        let mut byte = [0u8; 1];
        file.read_exact(&mut byte).unwrap();
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&[f(byte[0])]).unwrap();
    }

    /// `evict_cached_path` must drop the primary AND all default shard handles so
    /// a rebuild never serves a stale file.  Verified by caching the primary plus
    /// all shard handles, evicting, and confirming none of the canonical paths
    /// remain in the cache map.
    #[test]
    fn evict_cached_path_removes_primary_and_all_shards() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let synonyms = dir.path().join("HGNC.ndjson");
        let output = dir.path().join("fullmap.redb");
        let mut synonym_file = File::create(&synonyms).unwrap();
        writeln!(
            synonym_file,
            r#"{{"curie":"HGNC:1100","preferred_name":"BRCA1","names":["BRCA1"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}}"#
        )
        .unwrap();
        drop(synonym_file);

        build_test(output.clone(), Vec::new(), vec![synonyms], 1, 4_000_000).unwrap();

        // Populate the cache with the primary + all default shards.
        let _primary = open_cached(output.clone()).unwrap();
        let shards = open_cached_shards(&output).unwrap();
        assert_eq!(shards.len(), SHARD_COUNT_SHARDS);

        let cache = DB_CACHE.get().unwrap();
        {
            let map = cache.read().unwrap();
            assert!(map.contains_key(&std::fs::canonicalize(&output).unwrap()));
            for index in 0..SHARD_COUNT_SHARDS {
                let key = std::fs::canonicalize(shard_path(&output, index)).unwrap();
                assert!(map.contains_key(&key), "shard {index} not cached");
            }
        }

        evict_cached_path(&output).unwrap();

        // After eviction none of the primary/shard paths remain cached.
        {
            let map = cache.read().unwrap();
            assert!(!map.contains_key(&std::fs::canonicalize(&output).unwrap()));
            for index in 0..SHARD_COUNT_SHARDS {
                let key = std::fs::canonicalize(shard_path(&output, index)).unwrap();
                assert!(!map.contains_key(&key), "shard {index} survived eviction");
            }
        }
    }

    #[test]
    fn build_fullmap_db_deduplicates_curie_rows() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let synonyms = dir.path().join("HGNC.ndjson");
        let output = dir.path().join("fullmap.redb");

        let mut synonym_file = File::create(&synonyms).unwrap();
        writeln!(
            synonym_file,
            r#"{{"curie":"HGNC:1100","preferred_name":"BRCA1","names":["BRCA1","breast cancer 1"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}}"#
        )
        .unwrap();

        build_test(output.clone(), Vec::new(), vec![synonyms], 1, 4_000_000).unwrap();

        let database = open_cached(output).unwrap();
        let read = database.begin_read().unwrap();
        let curies = read.open_table(CURIES).unwrap();
        assert_eq!(curies.iter().unwrap().count(), 1);
    }

    #[test]
    fn curie_run_roundtrip_spans_buffer_boundary() {
        // 20k rows -> a run file larger than the 1 MB BufReader capacity, so
        // next_row must correctly handle records spanning a buffer refill.
        let dir = tempfile::tempdir().unwrap();
        let mut rows: Vec<(u32, CurieRow)> = Vec::new();
        for i in 0..20000u32 {
            rows.push((
                i,
                CurieRow {
                    prefix_id: (i % 100) as u16,
                    local_id: format!("ID{i:06}"),
                    preferred_name: format!(
                        "Preferred Name Number {i} with padding to grow the record size"
                    ),
                    category_id: (i % 50) as u16,
                    taxon_id: i as i32,
                },
            ));
        }
        let path = spill_curie_run(&mut rows, dir.path(), 0).unwrap();
        assert!(std::fs::metadata(&path).unwrap().len() > (1 << 20));
        let mut reader = CurieRunReader::new(&path).unwrap();
        let mut count = 0u32;
        while let Some((id, encoded)) = reader.next_row().unwrap() {
            let row: CurieRow = bincode::deserialize(&encoded).unwrap();
            assert_eq!(id, count);
            assert_eq!(row.local_id, format!("ID{count:06}"));
            count += 1;
        }
        assert_eq!(count, 20000);
    }

    #[test]
    fn curie_rows_spill_to_disk_and_stream_back() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let synonyms = dir.path().join("multi.ndjson");
        let output = dir.path().join("fullmap.redb");

        let mut synonym_file = File::create(&synonyms).unwrap();
        // Five distinct CURIEs; with curie_spill=2 this forces multiple curie runs.
        for i in 0..5 {
            writeln!(
                synonym_file,
                r#"{{"curie":"HGNC:{i}","preferred_name":"GENE{i}","names":["GENE{i}"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}}"#
            )
            .unwrap();
        }

        let spill_dir = {
            let mut p = output.clone().into_os_string();
            p.push(".spill.d");
            PathBuf::from(p)
        };
        // curie_spill = 2 forces the curie-row buffer to spill to disk run files,
        // exercising spill_curie_run + the Phase-4 streaming reader.
        build_fullmap_inner(
            output.clone(),
            Vec::new(),
            vec![synonyms],
            1,
            SHARD_COUNT_SHARDS,
            None,
            4_000_000,
            2,
            HashSet::new(),
            None,
            None,
            DEFAULT_CHUNK_BYTES,
            2,
            64 * 1024 * 1024,
            1000,
            spill_dir,
        )
        .unwrap();

        let database = open_cached(output.clone()).unwrap();
        let read = database.begin_read().unwrap();
        let curies = read.open_table(CURIES).unwrap();
        assert_eq!(curies.iter().unwrap().count(), 5);
        drop(curies);
        drop(read);
        drop(database);

        // Every gene resolves through the streamed CURIES table.
        let rows = lookup_terms(output, (0..5).map(|i| format!("gene{i}")).collect()).unwrap();
        let mut got: Vec<String> = rows
            .iter()
            .flat_map(|(_, recs)| recs.iter().map(|r| r.curie.clone()))
            .collect();
        got.sort();
        got.dedup();
        let want: Vec<String> = (0..5).map(|i| format!("HGNC:{i}")).collect();
        assert_eq!(got, want);
    }

    #[test]
    fn dead_term_filter_drops_numeric_synonyms() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let synonyms = dir.path().join("HGNC.ndjson");
        let output = dir.path().join("fullmap.redb");

        let mut synonym_file = File::create(&synonyms).unwrap();
        // "12345" matches the distinct() bad-regex (^\d+$) and must be dropped;
        // "realname" survives.
        writeln!(
            synonym_file,
            r#"{{"curie":"HGNC:1100","preferred_name":"BRCA1","names":["12345","realname"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}}"#
        )
        .unwrap();

        build_test(output.clone(), Vec::new(), vec![synonyms], 1, 4_000_000).unwrap();

        // Lookups are EXACT key matches and the build stores `nlp::normalize_l1`
        // keys, so the probe is the NORMALIZED form of the synonym ("realname"
        // -> "realnam"); the raw spelling is no longer a key at all.
        let alive = lookup_terms(output.clone(), vec!["realnam".to_string()]).unwrap();
        assert_eq!(alive.len(), 1);
        assert_eq!(alive[0].1[0].curie, "HGNC:1100");

        let dead = lookup_terms(output, vec!["12345".to_string()]).unwrap();
        assert!(dead.is_empty() || dead[0].1.is_empty());
    }

    #[test]
    fn prefix_exclusion_drops_matching_rows() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let synonyms = dir.path().join("mixed.ndjson");
        let output = dir.path().join("fullmap.redb");

        let mut synonym_file = File::create(&synonyms).unwrap();
        writeln!(
            synonym_file,
            r#"{{"curie":"HGNC:1","preferred_name":"GENEA","names":["GENEA"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}}"#
        )
        .unwrap();
        writeln!(
            synonym_file,
            r#"{{"curie":"CHEBI:2","preferred_name":"water","names":["water"],"types":["ChemicalEntity"],"taxa":[]}}"#
        )
        .unwrap();

        let spill_dir = {
            let mut p = output.clone().into_os_string();
            p.push(".spill.d");
            PathBuf::from(p)
        };
        let mut exclude = HashSet::new();
        exclude.insert("HGNC".to_string());
        build_fullmap_inner(
            output.clone(),
            Vec::new(),
            vec![synonyms],
            1,
            SHARD_COUNT_SHARDS,
            None,
            4_000_000,
            1_000_000,
            exclude,
            None,
            None,
            DEFAULT_CHUNK_BYTES,
            2,
            64 * 1024 * 1024,
            1000,
            spill_dir,
        )
        .unwrap();

        // Only CHEBI:2 survives; HGNC:1 is excluded entirely.
        let database = open_cached(output.clone()).unwrap();
        let read = database.begin_read().unwrap();
        let curies = read.open_table(CURIES).unwrap();
        assert_eq!(curies.iter().unwrap().count(), 1);
        drop(curies);
        drop(read);
        drop(database);

        let kept = lookup_terms(output.clone(), vec!["water".to_string()]).unwrap();
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].1[0].curie, "CHEBI:2");

        let dropped = lookup_terms(output, vec!["genea".to_string()]).unwrap();
        assert!(dropped.is_empty() || dropped[0].1.is_empty());
    }

    #[test]
    fn intra_file_parallelism_single_file_many_workers() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let synonyms = dir.path().join("big.ndjson");
        let output = dir.path().join("fullmap.redb");

        // One file, 5000 distinct CURIEs.
        let mut synonym_file = File::create(&synonyms).unwrap();
        for i in 0..5000 {
            writeln!(
                synonym_file,
                r#"{{"curie":"HGNC:{i}","preferred_name":"GENE{i}","names":["GENE{i}","alias{i}"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}}"#
            )
            .unwrap();
        }

        let spill_dir = {
            let mut p = output.clone().into_os_string();
            p.push(".spill.d");
            PathBuf::from(p)
        };
        // 4 workers, chunk_bytes=8192 (forces many chunks), 2 producers: one
        // file is split across workers and chunks (intra-file parallelism).
        build_fullmap_inner(
            output.clone(),
            Vec::new(),
            vec![synonyms],
            4,
            SHARD_COUNT_SHARDS,
            None,
            4_000_000,
            1_000_000,
            HashSet::new(),
            None,
            None,
            8192,
            2,
            64 * 1024 * 1024,
            1000,
            spill_dir,
        )
        .unwrap();

        let database = open_cached(output.clone()).unwrap();
        let read = database.begin_read().unwrap();
        let curies = read.open_table(CURIES).unwrap();
        assert_eq!(curies.iter().unwrap().count(), 5000);
        drop(curies);
        drop(read);
        drop(database);

        // Sample lookups across the file (start / middle / end) all resolve.
        let terms: Vec<String> = [0, 1, 2499, 2500, 4998, 4999]
            .into_iter()
            .map(|i| format!("gene{i}"))
            .collect();
        let rows = lookup_terms(output, terms).unwrap();
        let mut got: Vec<String> = rows
            .iter()
            .flat_map(|(_, recs)| recs.iter().map(|r| r.curie.clone()))
            .collect();
        got.sort();
        got.dedup();
        assert_eq!(got.len(), 6);
    }

    #[test]
    fn build_fullmap_db_rejects_empty_synonym_list() {
        pyo3::Python::initialize();
        Python::attach(|py| {
            let err = build_fullmap_db(
                py,
                PathBuf::from("/tmp/should-not-exist.redb"),
                Vec::new(),
                Vec::new(),
                None,
                None,
            )
            .expect_err("empty synonyms should fail");
            assert!(err
                .to_string()
                .contains("at least one synonym file is required"));
        });
    }

    #[test]
    fn builds_records_from_alias_fields() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let synonyms = dir.path().join("BABEL.ndjson");
        let output = dir.path().join("fullmap.redb");

        let mut synonym_file = File::create(&synonyms).unwrap();
        writeln!(
            synonym_file,
            r#"{{"id":"MONDO:1","name":"Alias disease","names":["Alias disease"],"categories":["biolink:Disease"],"taxon":["NCBITaxon:0"]}}"#
        )
        .unwrap();

        build_test(output.clone(), Vec::new(), vec![synonyms], 1, 4_000_000).unwrap();
        // The key is the alias field's NORMALIZED level-one form ("Alias
        // disease" -> tokens stemmed and byte-sorted -> "alia diseas"); the
        // alias plumbing (id/name/categories/taxon) is what this test pins, and
        // it must survive the normalization the build now applies.
        let rows = lookup_terms(output, vec!["alia diseas".to_string()]).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1[0].curie, "MONDO:1");
        assert_eq!(rows[0].1[0].preferred_name, "Alias disease");
        assert_eq!(rows[0].1[0].category_name, "Disease");
    }

    #[test]
    fn build_fullmap_db_does_not_index_banned_name_tokens() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let synonyms = dir.path().join("HGNC.ndjson");
        let output = dir.path().join("fullmap.redb");

        let mut synonym_file = File::create(&synonyms).unwrap();
        writeln!(
            synonym_file,
            r#"{{"curie":"HGNC:1","preferred_name":"GENE1","names":["hypothetical protein","GENE1"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}}"#
        )
        .unwrap();

        build_test(output.clone(), Vec::new(), vec![synonyms], 1, 4_000_000).unwrap();
        let rows = lookup_terms(
            output,
            vec!["hypothetical protein".to_string(), "gene1".to_string()],
        )
        .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "gene1");
    }

    #[test]
    fn build_fullmap_db_cleans_quoted_names_before_indexing() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let synonyms = dir.path().join("HGNC.ndjson");
        let output = dir.path().join("fullmap.redb");

        let mut synonym_file = File::create(&synonyms).unwrap();
        writeln!(
            synonym_file,
            r#"{{"curie":"HGNC:2","preferred_name":"'Quoted Gene'","names":["\"Quoted Gene\""],"types":["Gene"],"taxa":["NCBITaxon:9606"]}}"#
        )
        .unwrap();

        build_test(output.clone(), Vec::new(), vec![synonyms], 1, 4_000_000).unwrap();
        // Cleaning still happens (the quotes are gone from the key) and the
        // cleaned value then goes through `normalize_l1`, which stems "quoted"
        // and byte-sorts the tokens: `"Quoted Gene"` -> `gene quot`.
        let rows = lookup_terms(output, vec!["gene quot".to_string()]).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1[0].preferred_name, "Quoted Gene");
    }

    /// Forcing a tiny spill threshold produces many sorted runs that the k-way
    /// merge must regroup; the result must match a single-run build exactly.
    #[test]
    fn spill_merge_matches_single_run_build() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let synonyms = dir.path().join("multi.ndjson");
        let mut synonym_file = File::create(&synonyms).unwrap();
        // Several curies sharing overlapping names so terms map to multiple pairs.
        writeln!(synonym_file, r#"{{"curie":"HGNC:1","preferred_name":"Alpha","names":["Alpha","shared"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}}"#).unwrap();
        writeln!(synonym_file, r#"{{"curie":"HGNC:2","preferred_name":"Beta","names":["Beta","shared"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}}"#).unwrap();
        writeln!(synonym_file, r#"{{"curie":"MONDO:1","preferred_name":"Gamma","names":["Gamma","shared"],"types":["Disease"],"taxa":["NCBITaxon:0"]}}"#).unwrap();
        writeln!(synonym_file, r#"{{"curie":"HGNC:3","preferred_name":"Delta","names":["Delta","alpha"],"types":["Gene"],"taxa":["NCBITaxon:10090"]}}"#).unwrap();
        drop(synonym_file);

        let out_big = dir.path().join("big.redb");
        let out_tiny = dir.path().join("tiny.redb");
        // local_spill=1 forces a spill after essentially every term => many runs.
        build_test(
            out_big.clone(),
            Vec::new(),
            vec![synonyms.clone()],
            2,
            4_000_000,
        )
        .unwrap();
        build_test(out_tiny.clone(), Vec::new(), vec![synonyms], 2, 1).unwrap();

        let probes = vec![
            "alpha".to_string(),
            // "shared" normalizes to "share" (Porter2), so the probe must use the
            // stored key form; the spill/merge equivalence this test pins is
            // about run regrouping, not about the key spelling.
            "share".to_string(),
            "gamma".to_string(),
            "beta".to_string(),
            "delta".to_string(),
        ];
        let big = lookup_terms(out_big, probes.clone()).unwrap();
        let tiny = lookup_terms(out_tiny, probes).unwrap();

        // Same terms resolved, same hydrated records (order-independent compare).
        let norm = |v: Vec<(String, Vec<FullmapRecord>)>| -> Vec<(String, Vec<String>)> {
            let mut out: Vec<(String, Vec<String>)> = v
                .into_iter()
                .map(|(t, recs)| {
                    let mut curies: Vec<String> = recs.into_iter().map(|r| r.curie).collect();
                    curies.sort();
                    (t, curies)
                })
                .collect();
            out.sort();
            out
        };
        // "shared" must resolve to all three curies that list it.
        let shared = tiny
            .iter()
            .find(|(t, _)| t == "share")
            .map(|(_, recs)| {
                let mut c: Vec<String> = recs.iter().map(|r| r.curie.clone()).collect();
                c.sort();
                c
            })
            .unwrap_or_default();
        assert_eq!(shared, vec!["HGNC:1", "HGNC:2", "MONDO:1"]);
        assert_eq!(norm(big), norm(tiny));
    }

    /// Regression: repeated lookups in one process must not trip redb's flock
    /// (the old mtime-keyed cache re-opened the DB and failed).
    #[test]
    fn repeated_lookups_reuse_cached_database() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        let synonyms = dir.path().join("HGNC.ndjson");
        let output = dir.path().join("fullmap.redb");
        let mut synonym_file = File::create(&synonyms).unwrap();
        writeln!(
            synonym_file,
            r#"{{"curie":"HGNC:1100","preferred_name":"BRCA1","names":["BRCA1"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}}"#
        )
        .unwrap();
        drop(synonym_file);

        build_test(output.clone(), Vec::new(), vec![synonyms], 1, 4_000_000).unwrap();

        // Several consecutive lookups (rows path) must all succeed.
        for _ in 0..3 {
            let rows = lookup_terms(output.clone(), vec!["brca1".to_string()]).unwrap();
            assert_eq!(rows.len(), 1);
        }
        // open_cached returns the same handle across calls.
        let a = open_cached(output.clone()).unwrap();
        let b = open_cached(output).unwrap();
        assert!(Arc::ptr_eq(&a, &b));
    }

    /// `MergeHeap::next_group` must k-way merge several runs sorted by
    /// `(xxh64(term), term)` (the Tier-3a hash-prefixed ordering): for each term
    /// the pair lists from every run that carries it are concatenated then
    /// sorted+deduped (the pairs within a single run are deliberately
    /// unsorted/duplicated here to prove the merge, not the writer, establishes
    /// the final ordering).  Groups emerge in HASH order (not lexicographic), so
    /// the merged (term -> pairs) map is verified order-independently.  Returns
    /// None at a clean EOF.
    #[test]
    fn merge_heap_merges_overlapping_runs_sorted_and_deduped() {
        let dir = tempfile::tempdir().unwrap();
        let run0 = dir.path().join("r0.bin");
        let run1 = dir.path().join("r1.bin");
        let run2 = dir.path().join("r2.bin");

        // Write each run sorted by (xxh64(term), term) — the MergeHeap invariant.
        fn write_run(path: &Path, mut terms: Vec<(&str, Vec<(u32, u8)>)>) {
            terms.sort_by(|a, b| {
                xxh64(a.0.as_bytes(), 0)
                    .cmp(&xxh64(b.0.as_bytes(), 0))
                    .then_with(|| a.0.cmp(b.0))
            });
            let mut w = RunWriter::new(path).unwrap();
            for (term, pairs) in terms {
                w.write_term(term, &pairs).unwrap();
            }
            w.finish().unwrap();
        }

        write_run(
            &run0,
            vec![
                ("apple", vec![(3, 1), (1, 0), (2, 2)]),
                ("cherry", vec![(5, 0)]),
            ],
        );
        write_run(
            &run1,
            vec![
                ("apple", vec![(1, 0), (9, 3)]),
                ("banana", vec![(7, 2), (7, 2)]),
            ],
        );
        write_run(
            &run2,
            vec![("banana", vec![(4, 1)]), ("cherry", vec![(5, 0), (0, 0)])],
        );

        let paths = vec![run0, run1, run2];
        let mut merge = MergeHeap::new(&paths).unwrap();

        // Drain every group into a term -> pairs map (emergence order is hash
        // order, so compare order-independently).  Draining until None also proves
        // the clean-EOF behavior.
        let mut got: std::collections::BTreeMap<String, Vec<(u32, u8)>> =
            std::collections::BTreeMap::new();
        while let Some((term, pairs)) = merge.next_group().unwrap() {
            got.insert(term, pairs);
        }

        let mut expected: std::collections::BTreeMap<String, Vec<(u32, u8)>> =
            std::collections::BTreeMap::new();
        // "apple" appears in run0 + run1: merged, sorted, deduped.
        expected.insert("apple".to_string(), vec![(1, 0), (2, 2), (3, 1), (9, 3)]);
        // "banana" appears in run1 (dup pair) + run2.
        expected.insert("banana".to_string(), vec![(4, 1), (7, 2)]);
        // "cherry" appears in run0 + run2 (overlapping (5,0) pair).
        expected.insert("cherry".to_string(), vec![(0, 0), (5, 0)]);
        assert_eq!(got, expected);
    }

    /// `RunWriter`/`RunReader` must roundtrip term frames exactly: an empty pair
    /// list, a 100k-pair term, a unicode term, and enough padding terms to grow
    /// the file past the 1 MB BufReader capacity so frames span a buffer refill.
    /// The reader returns frames in file order and reports a clean EOF (cur=None).
    #[test]
    fn term_run_writer_reader_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("terms.bin");

        let big_pairs: Vec<(u32, u8)> = (0..100_000).map(|i| (i, (i % 256) as u8)).collect();
        let unicode = "héllo–wörld·αβγ";

        let mut w = RunWriter::new(&path).unwrap();
        w.write_term("empty", &[]).unwrap();
        w.write_term("big", &big_pairs).unwrap();
        w.write_term(unicode, &[(1, 2)]).unwrap();
        // Padding terms push the file well past the 1 MB BufReader capacity.
        for i in 0..20_000u32 {
            w.write_term(&format!("pad{i:08}"), &[(i, 0), (i, 1)])
                .unwrap();
        }
        w.finish().unwrap();
        assert!(std::fs::metadata(&path).unwrap().len() > (1 << 20));

        let mut reader = RunReader::new(&path).unwrap();

        let (term, pairs) = reader.cur.take().unwrap();
        assert_eq!(term, "empty");
        assert!(pairs.is_empty());
        reader.advance().unwrap();

        let (term, pairs) = reader.cur.take().unwrap();
        assert_eq!(term, "big");
        assert_eq!(pairs, big_pairs);
        reader.advance().unwrap();

        let (term, pairs) = reader.cur.take().unwrap();
        assert_eq!(term, unicode);
        assert_eq!(pairs, vec![(1, 2)]);
        reader.advance().unwrap();

        for i in 0..20_000u32 {
            let (term, pairs) = reader.cur.take().unwrap();
            assert_eq!(term, format!("pad{i:08}"));
            assert_eq!(pairs, vec![(i, 0), (i, 1)]);
            reader.advance().unwrap();
        }
        assert!(reader.cur.is_none(), "clean EOF expected");
    }

    /// `EquivIndex::build` must k-way merge MULTIPLE equiv spill runs (forced here
    /// via a tiny `TABLASSERT_FULLMAP_EQUIV_SPILL_ENTRIES`) so a curie that recurs
    /// across several class files/runs resolves to the union of its equivalents,
    /// sorted and deduped.  Two distinct curies must both resolve, and a miss must
    /// return None.
    #[test]
    fn equiv_index_multi_run_merge_and_lookup() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();
        // A tiny spill threshold forces one run per row, so X:1 (present in all
        // three files) is merged across several runs in Phase 1b.  A small
        // threshold only changes HOW MANY runs are written, never the merged
        // result, so concurrent builds in other tests stay correct.
        std::env::set_var("TABLASSERT_FULLMAP_EQUIV_SPILL_ENTRIES", "1");

        let write_classes = |name: &str, rows: &[&str]| -> PathBuf {
            let path = dir.path().join(name);
            let mut file = File::create(&path).unwrap();
            for row in rows {
                writeln!(file, "{row}").unwrap();
            }
            path
        };
        let a = write_classes(
            "a.ndjson",
            &[
                r#"{"id":"X:1","equivalent_identifiers":[{"identifier":"E:b"},{"identifier":"E:a"}]}"#,
                r#"{"id":"Y:2","equivalent_identifiers":[{"identifier":"E:z"}]}"#,
            ],
        );
        let b = write_classes(
            "b.ndjson",
            &[
                r#"{"id":"X:1","equivalent_identifiers":[{"identifier":"E:c"},{"identifier":"E:a"}]}"#,
            ],
        );
        let c = write_classes(
            "c.ndjson",
            &[
                r#"{"id":"X:1","equivalent_identifiers":[{"identifier":"E:b"}]}"#,
                r#"{"id":"Z:3","equivalent_identifiers":[{"identifier":"E:q"}]}"#,
            ],
        );

        let spill_dir = dir.path().join("spill");
        std::fs::create_dir_all(&spill_dir).unwrap();
        let index = EquivIndex::build(&[a, b, c], &spill_dir, None).unwrap();
        std::env::remove_var("TABLASSERT_FULLMAP_EQUIV_SPILL_ENTRIES");

        // X:1 appears in all three files: equivs merged across runs, sorted+deduped
        // (E:a duplicated across a+b, E:b duplicated across a+c).
        let x: Vec<String> = index
            .lookup("X:1")
            .expect("X:1 present")
            .map(String::from)
            .collect();
        assert_eq!(
            x,
            vec!["E:a".to_string(), "E:b".to_string(), "E:c".to_string()]
        );

        // Two distinct curies both resolve to their own equiv sets.
        let y: Vec<String> = index
            .lookup("Y:2")
            .expect("Y:2 present")
            .map(String::from)
            .collect();
        assert_eq!(y, vec!["E:z".to_string()]);
        let z: Vec<String> = index
            .lookup("Z:3")
            .expect("Z:3 present")
            .map(String::from)
            .collect();
        assert_eq!(z, vec!["E:q".to_string()]);

        // A curie that was never indexed is a miss.
        assert!(index.lookup("ABSENT:9").is_none());
    }

    /// `emit_term` inserts the level-one form and, when distinct, the level-two
    /// form; skips level-two when it equals level-one; drops dead terms and
    /// token_qc failures; and accumulates one pair per call under each key.
    #[test]
    fn emit_term_inserts_normalized_forms_and_accumulates() {
        // l1 and l2 both inserted when they differ ("BRCA-1" -> "brca-1" + "brca1").
        let mut terms: HashMap<String, Vec<(u32, u8)>> = HashMap::new();
        emit_term("BRCA-1", (1, 0), &mut terms);
        assert_eq!(
            terms.get("brca-1").map(Vec::as_slice),
            Some([(1, 0)].as_slice())
        );
        assert_eq!(
            terms.get("brca1").map(Vec::as_slice),
            Some([(1, 0)].as_slice())
        );
        assert_eq!(terms.len(), 2);

        // l2 skipped when it equals l1 ("TP53" -> "tp53" is already alnum).
        let mut terms: HashMap<String, Vec<(u32, u8)>> = HashMap::new();
        emit_term("TP53", (2, 1), &mut terms);
        assert_eq!(
            terms.get("tp53").map(Vec::as_slice),
            Some([(2, 1)].as_slice())
        );
        assert_eq!(terms.len(), 1, "l2 == l1 must not insert a second key");

        // Dead terms (banned token + all-digit) are dropped at level one.
        let mut terms: HashMap<String, Vec<(u32, u8)>> = HashMap::new();
        emit_term("none", (3, 0), &mut terms);
        emit_term("12345", (3, 0), &mut terms);
        assert!(terms.is_empty(), "dead terms must not be indexed");

        // A token_qc failure skips the term entirely.
        let mut terms: HashMap<String, Vec<(u32, u8)>> = HashMap::new();
        emit_term("hypothetical protein", (4, 0), &mut terms);
        assert!(terms.is_empty(), "qc-failing term must not be indexed");

        // Pair accumulation: the same term called twice keeps both pairs in order.
        let mut terms: HashMap<String, Vec<(u32, u8)>> = HashMap::new();
        emit_term("gene", (5, 0), &mut terms);
        emit_term("gene", (6, 1), &mut terms);
        assert_eq!(
            terms.get("gene").map(Vec::as_slice),
            Some([(5, 0), (6, 1)].as_slice())
        );
    }

    /// The level-one key `emit_term` stores is EXACTLY `nlp::normalize_l1`'s
    /// output — the same form the Python query side produces (`nlp.level_one` →
    /// `rs.normalize_terms`) — while the `token_qc` / `is_dead_term` gates keep
    /// seeing the PRE-normalization cleaned spelling, so stemming cannot smuggle
    /// a banned or sentinel term into the index.  fullmap lookups are exact
    /// string matches, so either half drifting is a silent miss, not an error.
    #[test]
    fn emit_term_keys_match_normalize_l1_and_gates_stay_pre_normalization() {
        // Multi-word names canonicalize: whitespace runs collapse and tokens are
        // stemmed and byte-sorted, so word order stops mattering and both
        // spellings accumulate under ONE key.
        let mut terms: HashMap<String, Vec<(u32, u8)>> = HashMap::new();
        emit_term("Oral  Aspirin", (1, 0), &mut terms);
        emit_term("aspirin oral", (2, 0), &mut terms);
        assert_eq!(
            terms.get("aspirin oral").map(Vec::as_slice),
            Some([(1, 0), (2, 0)].as_slice()),
            "both spellings must land on one normalized level-one key"
        );
        // Level-two still composes AFTER level-one: it is the `[a-z0-9_]` strip
        // of the NORMALIZED key ("aspirin oral" -> "aspirinoral"), not of the
        // raw term (which would have been "oralaspirin").
        assert_eq!(
            terms.get("aspirinoral").map(Vec::as_slice),
            Some([(1, 0), (2, 0)].as_slice())
        );
        assert_eq!(terms.len(), 2);

        // Stemming folds morphological variants onto one key and duplicates
        // collapse after stemming, so no distinct level-two key is emitted.
        let mut terms: HashMap<String, Vec<(u32, u8)>> = HashMap::new();
        emit_term("Genes gene", (3, 0), &mut terms);
        assert_eq!(terms.len(), 1, "expected only the key `gene`: {terms:?}");
        assert!(terms.contains_key("gene"));

        // Digit- and punctuation-bearing tokens survive verbatim (never stemmed)
        // and non-ASCII folds case but skips stemming, so the level-two strip is
        // the only place those characters disappear.
        let mut terms: HashMap<String, Vec<(u32, u8)>> = HashMap::new();
        emit_term("TNF-alpha", (4, 0), &mut terms);
        emit_term("café", (5, 0), &mut terms);
        for key in ["tnf-alpha", "tnfalpha", "café", "caf"] {
            assert!(terms.contains_key(key), "missing key {key:?}: {terms:?}");
        }
        assert_eq!(terms.len(), 4);

        // The gates run BEFORE stemming: "Hypothetical Protein" is rejected on
        // its cleaned spelling (post-normalization it reads "hypothet protein"
        // and would slip past token_qc's literal-needle substring check), and
        // "Not Applicable" is rejected by is_dead_term's exact-match sentinel
        // list (post-normalization it reads "applic not" and would be indexed).
        let mut terms: HashMap<String, Vec<(u32, u8)>> = HashMap::new();
        emit_term("Hypothetical Protein", (6, 0), &mut terms);
        emit_term("Not Applicable", (7, 0), &mut terms);
        emit_term("uncharacterized", (8, 0), &mut terms);
        assert!(
            terms.is_empty(),
            "banned/sentinel terms must be rejected pre-normalization: {terms:?}"
        );

        // A key that only BECOMES dead once normalized is dropped as well: the
        // Python `distinct()` drop regex filters the NORMALIZED query terms, so
        // "Nones" -> "none" could never be queried and must not be stored.
        let mut terms: HashMap<String, Vec<(u32, u8)>> = HashMap::new();
        emit_term("Nones", (9, 0), &mut terms);
        emit_term("unknowns", (10, 0), &mut terms);
        assert!(
            terms.is_empty(),
            "keys normalizing onto a sentinel must not be stored: {terms:?}"
        );
    }

    /// `is_dead_term` is an exact-match filter: every named banned token, the
    /// empty string, and all-digit strings are dead; ordinary terms (and terms
    /// that merely CONTAIN a banned token as a substring) are alive.
    #[test]
    fn is_dead_term_classifies_banned_empty_and_numeric() {
        for banned in [
            "none",
            "nan",
            "na",
            "null",
            "unknown",
            "not applicable",
            "p_value",
            "variable",
            "result",
            "exposure",
            "expression",
            "symbol",
        ] {
            assert!(is_dead_term(banned), "{banned} must be dead");
        }
        assert!(is_dead_term(""), "empty must be dead");
        assert!(is_dead_term("0"), "all-digit must be dead");
        assert!(is_dead_term("1234567890"), "all-digit must be dead");
        assert!(!is_dead_term("brca1"), "normal term must be alive");
        assert!(!is_dead_term("gene42"), "normal term must be alive");
        // Exact match, not a substring test.
        assert!(!is_dead_term("none-like"), "substring is not an exact ban");
    }

    /// `validate_schema` must reject a v2 primary as OUTDATED (rebuild hint,
    /// mirroring the v1/v3 tests) and a missing or garbage schema as a generic
    /// UNSUPPORTED schema, so migrating users get actionable guidance while a
    /// corrupt/foreign DB fails loudly.
    #[test]
    fn lookup_rejects_v2_and_unsupported_schemas() {
        pyo3::Python::initialize();
        let dir = tempfile::tempdir().unwrap();

        // v2 => outdated (rebuild hint).
        let v2 = dir.path().join("v2.redb");
        let database = Database::create(&v2).unwrap();
        let write = database.begin_write().unwrap();
        {
            let mut meta = write.open_table(META).unwrap();
            meta.insert("schema", SCHEMA_VERSION_V2).unwrap();
        }
        write.commit().unwrap();
        drop(database);
        let err = lookup_terms(v2, vec!["brca1".to_string()]).unwrap_err();
        assert!(err
            .to_string()
            .contains("fullmap DB is outdated; rebuild with 'tablassert build-fullmap'"));

        // Missing schema key => unsupported.
        let missing = dir.path().join("missing.redb");
        let database = Database::create(&missing).unwrap();
        let write = database.begin_write().unwrap();
        {
            // Create the META table but write no "schema" entry.
            let meta = write.open_table(META).unwrap();
            drop(meta);
        }
        write.commit().unwrap();
        drop(database);
        let err = lookup_terms(missing, vec!["brca1".to_string()]).unwrap_err();
        assert!(err.to_string().contains("unsupported fullmap redb schema"));

        // Garbage schema value => unsupported.
        let garbage = dir.path().join("garbage.redb");
        let database = Database::create(&garbage).unwrap();
        let write = database.begin_write().unwrap();
        {
            let mut meta = write.open_table(META).unwrap();
            meta.insert("schema", "tablassert.fullmap.v999").unwrap();
        }
        write.commit().unwrap();
        drop(database);
        let err = lookup_terms(garbage, vec!["brca1".to_string()]).unwrap_err();
        assert!(err.to_string().contains("unsupported fullmap redb schema"));
    }

    /// `flush_shard_batch` inserts a hash-sorted batch through the redb
    /// end-of-table cursor (the faster ascending bulk-load path).  The cursor
    /// requires STRICTLY ascending keys and never overwrites, so a duplicate
    /// hash (an xxh64 collision between distinct terms) must fall back to a
    /// plain `insert()` — preserving the historical overwrite semantics (the
    /// later value wins) — and the cursor is reopened so the following
    /// ascending entries keep using the fast path.  `last` is threaded across
    /// batch flushes, so a collision straddling a BATCH BOUNDARY is caught too.
    #[test]
    fn flush_shard_batch_cursor_falls_back_on_duplicate_hash() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shard.redb");
        {
            let database = Database::create(&path).unwrap();
            let write = database.begin_write().unwrap();
            {
                let mut table = write.open_table(RECORDS).unwrap();
                let mut last: Option<u64> = None;

                // Batch 1: ascending except for the repeated 5 (intra-batch
                // collision).  The second 5 hits the fallback.
                let mut b1: Vec<(u64, Vec<u8>)> = vec![
                    (5, b"first".to_vec()),
                    (5, b"second".to_vec()),
                    (7, b"seven".to_vec()),
                ];
                assert_eq!(
                    flush_shard_batch(&mut table, &mut b1, &mut last).unwrap(),
                    3
                );
                assert!(b1.is_empty());
                assert_eq!(last, Some(7));

                // Batch 2: starts with 7 AGAIN — a collision straddling the
                // batch boundary.  Without cross-batch `last` tracking this
                // would raise `UnorderedKey` from the cursor.
                let mut b2: Vec<(u64, Vec<u8>)> =
                    vec![(7, b"seven-b".to_vec()), (9, b"nine".to_vec())];
                assert_eq!(
                    flush_shard_batch(&mut table, &mut b2, &mut last).unwrap(),
                    2
                );
                assert_eq!(last, Some(9));
            }
            write.commit().unwrap();
        }
        let database = open_read_only(&path).unwrap();
        let read = database.begin_read().unwrap();
        let table = read.open_table(RECORDS).unwrap();
        // Intra-batch overwrite preserved: the later duplicate value wins.
        assert_eq!(
            table.get_owned(5u64).unwrap().unwrap().value().to_vec(),
            b"second"
        );
        // Cross-batch overwrite preserved: batch-2's 7 overwrote batch-1's 7.
        assert_eq!(
            table.get_owned(7u64).unwrap().unwrap().value().to_vec(),
            b"seven-b"
        );
        // The entry after each fallback still landed via the reopened cursor.
        assert_eq!(
            table.get_owned(9u64).unwrap().unwrap().value().to_vec(),
            b"nine"
        );
        assert!(table.get_owned(6u64).unwrap().is_none());
        assert_eq!(table.len().unwrap(), 3);
    }
}
