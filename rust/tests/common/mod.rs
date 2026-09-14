//! Shared fixture + canonical-form helpers for the fullmap integration tests.
//!
//! Both `build_golden.rs` and `extract_prebuilt.rs` include this module via
//! `mod common;` — the Cargo convention of a `tests/common/mod.rs` keeps it
//! from being compiled as its own test target.  The helper BODIES must stay
//! stable: the golden tests pin byte-exact output through `canonical_dump`,
//! and `extract_prebuilt` compares against the exact same canonical form.

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

// redb table definitions — MUST match the names/types in `src/fullmap.rs`.
pub const RECORDS: TableDefinition<u64, &[u8]> = TableDefinition::new("records");
pub const PREFIXES: TableDefinition<u16, &str> = TableDefinition::new("prefixes");
// CATEGORIES/SOURCES (and `SourceRow` below) are only exercised by
// `build_golden`'s dimension round-trip test today; the targeted allows keep
// this shared module compiling under the crate's `deny(dead_code)` for test
// targets (like `extract_prebuilt`) that do not read those tables.
#[allow(dead_code)]
pub const CATEGORIES: TableDefinition<u16, &str> = TableDefinition::new("categories");
#[allow(dead_code)]
pub const SOURCES: TableDefinition<u8, &[u8]> = TableDefinition::new("sources");
pub const CURIES: TableDefinition<u32, &[u8]> = TableDefinition::new("curies");
pub const META: TableDefinition<&str, &str> = TableDefinition::new("meta");

// Only asserted by `build_golden`'s schema pin; the allow keeps this shared
// module compiling under `deny(dead_code)` for targets that never read it.
#[allow(dead_code)]
pub const SCHEMA_VERSION: &str = "tablassert.fullmap.v5";
pub const SHARD_COUNT: usize = 16;

/// bincode layout MUST match `CurieRow` in `src/fullmap.rs` (field order + types).
#[derive(Deserialize)]
pub struct CurieRow {
    pub prefix_id: u16,
    pub local_id: String,
    // Required for the bincode layout; only read by some test targets, so the
    // allows keep this shared module compiling under `deny(dead_code)` for the
    // targets that never read them.
    #[allow(dead_code)]
    pub preferred_name: String,
    #[allow(dead_code)]
    pub category_id: u16,
    #[allow(dead_code)]
    pub taxon_id: i32,
}

/// bincode layout MUST match `SourceRow` in `src/fullmap.rs`.
#[allow(dead_code)]
#[derive(Deserialize)]
pub struct SourceRow {
    pub source_name: String,
}

// ---------------------------------------------------------------------------
// Fixed embedded fixture.  Deliberately covers: plain ASCII names, unicode names
// (café / naïve), escaped JSON (quotes, backslashes, \uXXXX), alias fields
// (id/name/categories/taxon), dead terms (12345/none/nan), equivalent
// identifiers, multiple names per row, an empty names array, a null
// preferred_name, a row with no names array, and a class row with no
// equivalent_identifiers.  The synonym source file is named "SRC.ndjson" so the
// single source interns as "SRC" (source_id 0).
// ---------------------------------------------------------------------------

pub const SYNONYM_LINES: &[&str] = &[
    r#"{"curie":"HGNC:1","preferred_name":"Alpha Gene","names":["Alpha Gene","alpha"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}"#,
    r#"{"curie":"HGNC:2","preferred_name":"café","names":["café","naïve"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}"#,
    r#"{"curie":"HGNC:3","preferred_name":"Esc","names":["\"Quoted Name\"","back\\slash","\u00e9t\u00e9"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}"#,
    r#"{"id":"MONDO:1","name":"Alias Disease","names":["alias disease"],"categories":["biolink:Disease"],"taxon":["NCBITaxon:0"]}"#,
    r#"{"curie":"HGNC:4","preferred_name":"Dead","names":["12345","none","nan","realname"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}"#,
    r#"{"curie":"HGNC:5","preferred_name":"Empty Names","names":[],"types":["Gene"],"taxa":["NCBITaxon:9606"]}"#,
    r#"{"curie":"HGNC:6","preferred_name":"Shared Hit","names":["shared"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}"#,
    r#"{"curie":"MONDO:2","preferred_name":"Shared Disease","names":["shared"],"types":["Disease"],"taxa":["NCBITaxon:0"]}"#,
    r#"{"curie":"HGNC:7","preferred_name":null,"names":["nullname"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}"#,
    r#"{"curie":"HGNC:8","preferred_name":"No Names","types":["Gene"],"taxa":["NCBITaxon:9606"]}"#,
    r#"{"curie":"HGNC:9","preferred_name":"Equiv Free","names":["equivfree"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}"#,
    r#"{"curie":"HGNC:10","preferred_name":"Multi A","names":["multi","alpha"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}"#,
    r#"{"curie":"MONDO:3","preferred_name":"Multi B","names":["multi"],"types":["Disease"],"taxa":["NCBITaxon:0"]}"#,
];

pub const CLASS_LINES: &[&str] = &[
    r#"{"id":"HGNC:1","equivalent_identifiers":[{"identifier":"NCBIGene:100"},{"identifier":"NCBIGene:101"}]}"#,
    r#"{"id":"MONDO:1","equivalent_identifiers":[{"identifier":"DOID:999"}]}"#,
    r#"{"id":"HGNC:9"}"#,
];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub fn write_jsonl(path: &Path, lines: &[&str]) {
    let mut file = File::create(path).unwrap();
    for line in lines {
        writeln!(file, "{line}").unwrap();
    }
}

/// Sibling shard path for a primary, mirroring `shard_path` in `src/fullmap.rs`.
pub fn shard_path(primary: &Path, index: usize) -> PathBuf {
    let stem = primary.file_stem().unwrap().to_string_lossy().into_owned();
    let ext = primary.extension().unwrap().to_string_lossy().into_owned();
    primary.with_file_name(format!("{stem}.s{index}.{ext}"))
}

/// Build the fixed fixture at `<dir>/fullmap.redb` (parallelism is selected
/// automatically by the build) and return the primary path.  Uses the public
/// `build_fullmap_db` (the production entry point), exactly as Python callers do.
pub fn build_fixture(dir: &Path) -> PathBuf {
    pyo3::Python::initialize();
    let classes = dir.join("classes.ndjson");
    let synonyms = dir.join("SRC.ndjson");
    write_jsonl(&classes, CLASS_LINES);
    write_jsonl(&synonyms, SYNONYM_LINES);
    let output = dir.join("fullmap.redb");
    pyo3::Python::attach(|py| {
        tablassert_rs::build_fullmap_db(
            py,
            output.clone(),
            vec![classes],
            vec![synonyms],
            None,
            None,
        )
        .unwrap();
    });
    output
}

/// Build the fixed fixture with a taxon allowlist applied (the shape a filtered
/// published archive carries) at `<dir>/filtered.redb`; returns the primary path.
/// Only `extract_prebuilt` needs a filtered bundle today; the allow keeps this
/// shared module compiling under `deny(dead_code)` for `build_golden`.
#[allow(dead_code)]
pub fn build_filtered_fixture(dir: &Path, taxon_allowlist: Vec<i32>) -> PathBuf {
    pyo3::Python::initialize();
    let classes = dir.join("classes.ndjson");
    let synonyms = dir.join("SRC.ndjson");
    write_jsonl(&classes, CLASS_LINES);
    write_jsonl(&synonyms, SYNONYM_LINES);
    let output = dir.join("filtered.redb");
    pyo3::Python::attach(|py| {
        tablassert_rs::build_fullmap_db(
            py,
            output.clone(),
            vec![classes],
            vec![synonyms],
            None,
            Some(taxon_allowlist),
        )
        .unwrap();
    });
    output
}

/// Open a COPY of the (flock-locked) primary so its dims/CURIES/META tables can
/// be read directly.  The build commits everything before caching the original,
/// so the copied bytes are a complete, consistent database on a fresh inode.
pub fn open_primary_copy(primary: &Path) -> Database {
    let copy = primary.with_file_name("primary_copy.redb");
    std::fs::copy(primary, &copy).unwrap();
    Database::open(&copy).unwrap()
}

/// Build the canonical `term -> sorted(CURIE strings)` map by iterating EVERY
/// record across ALL shard files and hydrating curie_ids through the primary's
/// CURIES + PREFIXES tables.  Independent of curie_id assignment and thread count.
pub fn term_curie_map(primary: &Path) -> BTreeMap<String, Vec<String>> {
    // prefix_id -> prefix string, and curie_id -> "prefix:local_id".
    let db = open_primary_copy(primary);
    let read = db.begin_read().unwrap();
    let prefixes = read.open_table(PREFIXES).unwrap();
    let mut prefix_by_id: HashMap<u16, String> = HashMap::new();
    for item in prefixes.iter().unwrap() {
        let (id, value) = item.unwrap();
        prefix_by_id.insert(id.value(), value.value().to_string());
    }
    drop(prefixes);
    let curies = read.open_table(CURIES).unwrap();
    let mut curie_by_id: HashMap<u32, String> = HashMap::new();
    for item in curies.iter().unwrap() {
        let (id, bytes) = item.unwrap();
        let row: CurieRow = bincode::deserialize(bytes.value()).unwrap();
        let prefix = prefix_by_id[&row.prefix_id].clone();
        curie_by_id.insert(id.value(), format!("{}:{}", prefix, row.local_id));
    }
    drop(curies);
    let meta = read.open_table(META).unwrap();
    let shard_count = meta
        .get("shards")
        .unwrap()
        .unwrap()
        .value()
        .parse::<usize>()
        .unwrap();
    drop(meta);
    drop(read);
    drop(db);

    // Iterate every shard's RECORDS, mapping pairs to CURIE strings.
    let mut map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for index in 0..shard_count {
        let shard_db = Database::open(shard_path(primary, index)).unwrap();
        let shard_read = shard_db.begin_read().unwrap();
        let records = shard_read.open_table(RECORDS).unwrap();
        for item in records.iter().unwrap() {
            let (_hash, bytes) = item.unwrap();
            let (term, pairs): (String, Vec<(u32, u8)>) =
                bincode::deserialize(bytes.value()).unwrap();
            let entry = map.entry(term).or_default();
            for (curie_id, _source_id) in pairs {
                entry.push(curie_by_id[&curie_id].clone());
            }
        }
    }
    for curie_list in map.values_mut() {
        curie_list.sort();
        curie_list.dedup();
    }
    map
}

/// Serialize a `term -> sorted(CURIEs)` map to the canonical multi-line string.
pub fn canonical_dump(map: &BTreeMap<String, Vec<String>>) -> String {
    let mut out = String::new();
    for (term, curie_list) in map {
        out.push_str(term);
        out.push('|');
        out.push_str(&curie_list.join(","));
        out.push('\n');
    }
    out
}
