//! STRICT golden integration tests for the fullmap database build.
//!
//! These pin the EXACT on-disk result of the build pipeline so any future
//! optimization that changes the output (terms indexed, CURIE strings, dimension
//! tables, schema, or shard layout) fails loudly.  They are integration tests
//! (separate crate from the unit tests in `src/fullmap.rs`) and therefore drive
//! the build through the PUBLIC `build_fullmap_db` re-exported at the crate root,
//! then inspect the resulting redb files DIRECTLY:
//!
//! * RECORDS plus META.build_id live in the 16 sibling shard files
//!   (`fullmap.s{0..15}.redb`), which the build does NOT cache, so they open
//!   cleanly with `Database::open`.
//! * dims/CURIES/META live in the primary (`fullmap.redb`), which `build_fullmap_db`
//!   caches and holds under redb's exclusive flock.  To read it directly we COPY
//!   the committed primary file to a fresh inode (no lock) and open the copy.
//!
//! The canonical comparison form is `term -> sorted(CURIE strings)`, serialized
//! one `term|curie,curie` line per term, sorted by term.  Using CURIE STRINGS
//! (not `curie_id`s) makes the golden invariant under thread count and internal
//! id-assignment order, so it pins the RESULT while tolerating scheduling changes.
//!
//! All tests are OFFLINE and use `tempfile` scratch dirs.

mod common;

use flate2::write::GzEncoder;
use flate2::Compression;
use redb::{Database, ReadableDatabase, ReadableTable};
use std::collections::HashMap;
use std::io::Write;

// Shared fixture + canonical-form helpers (see `tests/common/mod.rs`).
use common::{
    build_fixture, canonical_dump, open_primary_copy, shard_path, term_curie_map, write_jsonl,
    CurieRow, SourceRow, CATEGORIES, CLASS_LINES, CURIES, META, PREFIXES, SCHEMA_VERSION,
    SHARD_COUNT, SOURCES, SYNONYM_LINES,
};

/// The pinned canonical output: `term|curie,curie` lines, sorted by term, CURIEs
/// sorted within each term.  Every key here is a `nlp::normalize_l1` (level-one)
/// or `level_two` form of a raw fixture input, so the pin is ALSO the build-side
/// half of the query/build lock-step contract: if `emit_term`'s key derivation
/// drifts from the Python query side (`nlp.level_one` -> `rs.normalize_terms`),
/// this file diverges and fullmap lookups would silently miss.
///
/// REGENERATE ONLY DELIBERATELY, after an intentional and reviewed output change
/// (a schema bump, a normalization change), never to make a red build green:
///   cargo test --manifest-path rust/Cargo.toml --test build_golden regenerate_golden -- --ignored --nocapture
/// then diff the printed block against this constant and justify EVERY changed
/// line.  US-003 (`tablassert.fullmap.v6`, level-one keys via `normalize_l1`)
/// renamed exactly 8 of these keys — the fixture's multi-word and stemmable
/// names ("alias disease" -> "alia diseas", "Quoted Name" -> "name quot",
/// "shared" -> "share", "realname" -> "realnam", "nullname" -> "nullnam",
/// "equivfree" -> "equivfre", plus the two `\W+`-stripped level-two twins) —
/// with CURIE attribution byte-identical; each was re-derived independently
/// through `rs.normalize_terms` rather than copied from the regenerator.
const GOLDEN: &str = r#"alia diseas|MONDO:1
aliadiseas|MONDO:1
alpha|HGNC:1,HGNC:10
alpha gene|HGNC:1
alphagene|HGNC:1
back\slash|HGNC:3
backslash|HGNC:3
caf|HGNC:2
café|HGNC:2
doid999|MONDO:1
doid:999|MONDO:1
equivfre|HGNC:9
hgnc1|HGNC:1
hgnc10|HGNC:10
hgnc2|HGNC:2
hgnc3|HGNC:3
hgnc4|HGNC:4
hgnc5|HGNC:5
hgnc6|HGNC:6
hgnc7|HGNC:7
hgnc8|HGNC:8
hgnc9|HGNC:9
hgnc:1|HGNC:1
hgnc:10|HGNC:10
hgnc:2|HGNC:2
hgnc:3|HGNC:3
hgnc:4|HGNC:4
hgnc:5|HGNC:5
hgnc:6|HGNC:6
hgnc:7|HGNC:7
hgnc:8|HGNC:8
hgnc:9|HGNC:9
mondo1|MONDO:1
mondo2|MONDO:2
mondo3|MONDO:3
mondo:1|MONDO:1
mondo:2|MONDO:2
mondo:3|MONDO:3
multi|HGNC:10,MONDO:3
name quot|HGNC:3
namequot|HGNC:3
nave|HGNC:2
naïve|HGNC:2
ncbigene100|HGNC:1
ncbigene101|HGNC:1
ncbigene:100|HGNC:1
ncbigene:101|HGNC:1
nullnam|HGNC:7
realnam|HGNC:4
share|HGNC:6,MONDO:2
t|HGNC:3
été|HGNC:3
"#;

// ---------------------------------------------------------------------------
// (a) GOLDEN FILE TEST — the critical pin.
// ---------------------------------------------------------------------------

#[test]
fn golden_output_is_pinned() {
    let dir = tempfile::tempdir().unwrap();
    let output = build_fixture(dir.path());
    let map = term_curie_map(&output);
    let actual = canonical_dump(&map);
    assert!(
        !actual.is_empty(),
        "fixture must produce at least one indexed term"
    );
    assert_eq!(
        GOLDEN, actual,
        "fullmap build output diverged from the pinned golden file"
    );
}

/// Ignored helper: print the current canonical dump so GOLDEN can be regenerated
/// after an intentional, reviewed output change.
#[test]
#[ignore]
fn regenerate_golden() {
    let dir = tempfile::tempdir().unwrap();
    let output = build_fixture(dir.path());
    let map = term_curie_map(&output);
    println!("===GOLDEN-START===");
    print!("{}", canonical_dump(&map));
    println!("===GOLDEN-END===");
}

// ---------------------------------------------------------------------------
// (b) DETERMINISM — same input built twice yields identical canonical output.
// ---------------------------------------------------------------------------

#[test]
fn deterministic_across_rebuilds() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let out_a = build_fixture(dir_a.path());
    let out_b = build_fixture(dir_b.path());
    let dump_a = canonical_dump(&term_curie_map(&out_a));
    let dump_b = canonical_dump(&term_curie_map(&out_b));
    assert!(!dump_a.is_empty());
    assert_eq!(
        dump_a, dump_b,
        "two builds of the same input must be identical"
    );
}

// ---------------------------------------------------------------------------
// (c) THREAD INVARIANCE — the public build API now selects parallelism
// automatically, so an explicit threads=1 vs threads=4 comparison can no longer
// be driven from an integration test.  The explicit worker-count determinism
// coverage (worker_count 1 vs 4 through `build_test` / `build_fullmap_inner`,
// comparing term -> CURIE results and per-shard record counts) lives in the
// in-crate unit test `parallel_writers_match_single_writer_build` in
// src/fullmap.rs.  Here we pin the integration-level half: a build whose
// worker count was auto-selected (parallel on any multi-core host) reproduces
// the pinned GOLDEN — which was generated single-threaded — exactly.
// ---------------------------------------------------------------------------

#[test]
fn automatic_parallelism_matches_golden() {
    let dir = tempfile::tempdir().unwrap();
    let actual = canonical_dump(&term_curie_map(&build_fixture(dir.path())));
    assert_eq!(
        GOLDEN, actual,
        "auto-parallel build diverged from the pinned (serial-origin) golden"
    );
}

// ---------------------------------------------------------------------------
// (d) DIMENSION ROUND-TRIP — every dimension table is complete and consistent.
// ---------------------------------------------------------------------------

#[test]
fn dimension_tables_are_complete_and_consistent() {
    let dir = tempfile::tempdir().unwrap();
    let output = build_fixture(dir.path());
    let db = open_primary_copy(&output);
    let read = db.begin_read().unwrap();

    let prefixes = read.open_table(PREFIXES).unwrap();
    let mut prefix_ids: HashMap<u16, String> = HashMap::new();
    for item in prefixes.iter().unwrap() {
        let (id, value) = item.unwrap();
        prefix_ids.insert(id.value(), value.value().to_string());
    }
    drop(prefixes);

    let categories = read.open_table(CATEGORIES).unwrap();
    let mut category_ids: HashMap<u16, String> = HashMap::new();
    for item in categories.iter().unwrap() {
        let (id, value) = item.unwrap();
        category_ids.insert(id.value(), value.value().to_string());
    }
    drop(categories);

    let sources = read.open_table(SOURCES).unwrap();
    let mut source_names: Vec<String> = Vec::new();
    for item in sources.iter().unwrap() {
        let (_id, bytes) = item.unwrap();
        let row: SourceRow = bincode::deserialize(bytes.value()).unwrap();
        source_names.push(row.source_name);
    }
    drop(sources);

    // The fixture's synonym CURIEs use exactly the HGNC + MONDO prefixes and the
    // Gene + Disease categories; the single synonym file interns as source "SRC".
    let mut prefix_values: Vec<String> = prefix_ids.values().cloned().collect();
    prefix_values.sort();
    assert_eq!(prefix_values, vec!["HGNC".to_string(), "MONDO".to_string()]);
    let mut category_values: Vec<String> = category_ids.values().cloned().collect();
    category_values.sort();
    assert_eq!(
        category_values,
        vec!["Disease".to_string(), "Gene".to_string()]
    );
    assert_eq!(source_names, vec!["SRC".to_string()]);

    // Every CURIE row's prefix_id and category_id resolve to a valid dimension
    // entry, and the reconstructed CURIE strings are the expected 13 entities.
    let curies = read.open_table(CURIES).unwrap();
    let mut curie_strings: Vec<String> = Vec::new();
    let mut count = 0usize;
    for item in curies.iter().unwrap() {
        let (_id, bytes) = item.unwrap();
        let row: CurieRow = bincode::deserialize(bytes.value()).unwrap();
        let prefix = prefix_ids
            .get(&row.prefix_id)
            .unwrap_or_else(|| panic!("CURIE references missing prefix_id {}", row.prefix_id));
        assert!(
            category_ids.contains_key(&row.category_id),
            "CURIE references missing category_id {}",
            row.category_id
        );
        curie_strings.push(format!("{}:{}", prefix, row.local_id));
        count += 1;
    }
    assert_eq!(count, 13, "fixture must produce exactly 13 CURIE rows");
    curie_strings.sort();
    let expected: Vec<String> = [
        "HGNC:1", "HGNC:10", "HGNC:2", "HGNC:3", "HGNC:4", "HGNC:5", "HGNC:6", "HGNC:7", "HGNC:8",
        "HGNC:9", "MONDO:1", "MONDO:2", "MONDO:3",
    ]
    .into_iter()
    .map(String::from)
    .collect();
    assert_eq!(curie_strings, expected);
}

// ---------------------------------------------------------------------------
// (e) SCHEMA PIN — META advertises the v6 schema, build_id, and 16 shards.
// ---------------------------------------------------------------------------

#[test]
fn schema_and_shard_count_are_pinned() {
    let dir = tempfile::tempdir().unwrap();
    let output = build_fixture(dir.path());
    let db = open_primary_copy(&output);
    let read = db.begin_read().unwrap();
    let meta = read.open_table(META).unwrap();
    assert_eq!(meta.get("schema").unwrap().unwrap().value(), SCHEMA_VERSION);
    assert_eq!(meta.get("shards").unwrap().unwrap().value(), "16");
    let build_id = meta.get("build_id").unwrap().unwrap().value().to_string();
    build_id.parse::<u64>().unwrap();
    drop(meta);
    drop(read);
    drop(db);

    // Exactly 16 shard files exist on disk (s0..s15), each carries the same
    // build_id, and no s16 exists.
    for index in 0..SHARD_COUNT {
        let shard = shard_path(&output, index);
        assert!(shard.exists(), "missing shard file {index}");
        let shard_db = Database::open(shard).unwrap();
        let shard_read = shard_db.begin_read().unwrap();
        let shard_meta = shard_read.open_table(META).unwrap();
        assert_eq!(
            shard_meta.get("build_id").unwrap().unwrap().value(),
            build_id
        );
    }
    assert!(
        !shard_path(&output, SHARD_COUNT).exists(),
        "s16 must not exist"
    );
}

#[test]
fn taxon_allowlist_metadata_is_stable_and_omitted_for_default_builds() {
    let dir = tempfile::tempdir().unwrap();
    let synonyms = dir.path().join("SRC.ndjson");
    write_jsonl(
        &synonyms,
        &[
            r#"{"curie":"HGNC:1","preferred_name":"Gene","names":["gene"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}"#,
        ],
    );
    let filtered_a = dir.path().join("filtered_a.redb");
    let filtered_b = dir.path().join("filtered_b.redb");
    let unfiltered = dir.path().join("unfiltered.redb");
    pyo3::Python::initialize();
    pyo3::Python::attach(|py| {
        tablassert_rs::build_fullmap_db(
            py,
            filtered_a.clone(),
            vec![],
            vec![synonyms.clone()],
            None,
            Some(vec![10090, 9606]),
        )
        .unwrap();
        tablassert_rs::build_fullmap_db(
            py,
            filtered_b.clone(),
            vec![],
            vec![synonyms.clone()],
            None,
            Some(vec![9606, 10090]),
        )
        .unwrap();
        tablassert_rs::build_fullmap_db(py, unfiltered.clone(), vec![], vec![synonyms], None, None)
            .unwrap();
    });

    let read_allowlist = |path: &std::path::Path| -> Option<String> {
        let db = open_primary_copy(path);
        let read = db.begin_read().unwrap();
        let meta = read.open_table(META).unwrap();
        meta.get("taxon_allowlist")
            .unwrap()
            .map(|value| value.value().to_string())
    };
    let identity = read_allowlist(&filtered_a).unwrap();
    assert_eq!(Some(identity.clone()), read_allowlist(&filtered_b));
    assert_eq!(None, read_allowlist(&unfiltered));
    assert!(identity.starts_with("count=2;xxh64="));

    // The exposed probes must agree with the recorded META exactly: the CLI
    // decides whether a database already at `--output` is the one this build
    // would produce by comparing `fullmap_taxon_allowlist_identity(existing)`
    // with `taxon_allowlist_identity(built-in ids)`, so any drift between the
    // encoding and the recording silently reuses (or endlessly rebuilds) the
    // wrong database.
    assert_eq!(
        tablassert_rs::taxon_allowlist_identity(vec![9606, 10090]),
        identity
    );
    assert_eq!(
        tablassert_rs::taxon_allowlist_identity(vec![10090, 0, 9606, -5]),
        identity,
        "the identity is order-insensitive and ignores non-positive ids"
    );
    assert_eq!(
        tablassert_rs::fullmap_taxon_allowlist_identity(filtered_a),
        Some(identity.clone())
    );
    assert_eq!(
        tablassert_rs::fullmap_taxon_allowlist_identity(unfiltered),
        None,
        "an unfiltered build records no identity"
    );
    assert_eq!(
        tablassert_rs::fullmap_taxon_allowlist_identity(dir.path().join("absent.redb")),
        None,
        "an absent database reports not-reusable instead of raising"
    );
}

// ---------------------------------------------------------------------------
// (f) GZ INPUT — a gzipped synonym file yields identical results to plain.
// ---------------------------------------------------------------------------

#[test]
fn gz_input_matches_plain_input() {
    // Plain build.
    let dir_plain = tempfile::tempdir().unwrap();
    let plain = build_fixture(dir_plain.path());
    let plain_map = term_curie_map(&plain);

    // Gz build: same class + synonym content, synonym file gzipped.  The source
    // name derives from the stem ("SRC.ndjson.gz" -> "SRC.ndjson" -> "SRC"), so
    // it interns identically to the plain build.
    pyo3::Python::initialize();
    let dir_gz = tempfile::tempdir().unwrap();
    let classes = dir_gz.path().join("classes.ndjson");
    write_jsonl(&classes, CLASS_LINES);
    let synonym_text = {
        let mut s = String::new();
        for line in SYNONYM_LINES {
            s.push_str(line);
            s.push('\n');
        }
        s
    };
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(synonym_text.as_bytes()).unwrap();
    let gz_bytes = encoder.finish().unwrap();
    let synonyms_gz = dir_gz.path().join("SRC.ndjson.gz");
    std::fs::write(&synonyms_gz, gz_bytes).unwrap();
    let output_gz = dir_gz.path().join("fullmap.redb");
    pyo3::Python::attach(|py| {
        tablassert_rs::build_fullmap_db(
            py,
            output_gz.clone(),
            vec![classes],
            vec![synonyms_gz],
            None,
            None,
        )
        .unwrap();
    });

    let gz_map = term_curie_map(&output_gz);
    assert!(!gz_map.is_empty());
    assert_eq!(
        plain_map, gz_map,
        "gz input must match the uncompressed equivalent"
    );
}

// ---------------------------------------------------------------------------
// (g) EDGE CASES
// ---------------------------------------------------------------------------

/// An empty synonym file (0 rows) builds an empty DB: no records, no CURIE rows.
#[test]
fn empty_synonym_file_builds_empty_db() {
    pyo3::Python::initialize();
    let dir = tempfile::tempdir().unwrap();
    let classes = dir.path().join("classes.ndjson");
    write_jsonl(&classes, CLASS_LINES);
    let synonyms = dir.path().join("SRC.ndjson");
    write_jsonl(&synonyms, &[]); // 0 rows
    let output = dir.path().join("fullmap.redb");
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

    assert!(
        term_curie_map(&output).is_empty(),
        "no rows -> no indexed terms"
    );
    let db = open_primary_copy(&output);
    let read = db.begin_read().unwrap();
    let curies = read.open_table(CURIES).unwrap();
    assert_eq!(
        curies.iter().unwrap().count(),
        0,
        "no rows -> no CURIE rows"
    );
}

/// A synonym row with no `names` array indexes only its CURIE (l1 + l2 forms).
#[test]
fn synonym_row_with_no_names_indexes_only_curie() {
    pyo3::Python::initialize();
    let dir = tempfile::tempdir().unwrap();
    let synonyms = dir.path().join("SRC.ndjson");
    write_jsonl(
        &synonyms,
        &[
            r#"{"curie":"HGNC:8","preferred_name":"No Names","types":["Gene"],"taxa":["NCBITaxon:9606"]}"#,
        ],
    );
    let output = dir.path().join("fullmap.redb");
    pyo3::Python::attach(|py| {
        tablassert_rs::build_fullmap_db(py, output.clone(), vec![], vec![synonyms], None, None)
            .unwrap();
    });

    let map = term_curie_map(&output);
    let terms: Vec<String> = map.keys().cloned().collect();
    // Only the CURIE's level-one ("hgnc:8") and level-two ("hgnc8") forms appear
    // (BTreeMap keys are already sorted; "hgnc8" < "hgnc:8" since '8' < ':').
    assert_eq!(terms, vec!["hgnc8".to_string(), "hgnc:8".to_string()]);
    assert_eq!(map["hgnc:8"], vec!["HGNC:8".to_string()]);
    assert_eq!(map["hgnc8"], vec!["HGNC:8".to_string()]);
}

/// A null `preferred_name` falls back to the CURIE itself on the CURIE row.
#[test]
fn null_preferred_name_falls_back_to_curie() {
    pyo3::Python::initialize();
    let dir = tempfile::tempdir().unwrap();
    let synonyms = dir.path().join("SRC.ndjson");
    write_jsonl(
        &synonyms,
        &[
            r#"{"curie":"HGNC:7","preferred_name":null,"names":["nullname"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}"#,
        ],
    );
    let output = dir.path().join("fullmap.redb");
    pyo3::Python::attach(|py| {
        tablassert_rs::build_fullmap_db(py, output.clone(), vec![], vec![synonyms], None, None)
            .unwrap();
    });

    let db = open_primary_copy(&output);
    let read = db.begin_read().unwrap();
    let curies = read.open_table(CURIES).unwrap();
    let rows: Vec<CurieRow> = curies
        .iter()
        .unwrap()
        .map(|item| bincode::deserialize(item.unwrap().1.value()).unwrap())
        .collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].preferred_name, "HGNC:7",
        "null preferred_name -> CURIE"
    );
}

/// A class row with no `equivalent_identifiers` builds cleanly and adds no
/// equivalent-derived terms (only the synonym's own names + CURIE resolve).
#[test]
fn class_row_without_equivalents_builds() {
    pyo3::Python::initialize();
    let dir = tempfile::tempdir().unwrap();
    let classes = dir.path().join("classes.ndjson");
    write_jsonl(&classes, &[r#"{"id":"HGNC:9"}"#]);
    let synonyms = dir.path().join("SRC.ndjson");
    write_jsonl(
        &synonyms,
        &[
            r#"{"curie":"HGNC:9","preferred_name":"Equiv Free","names":["equivfree"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}"#,
        ],
    );
    let output = dir.path().join("fullmap.redb");
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

    let map = term_curie_map(&output);
    // "equivfree" and the CURIE forms resolve; no equivalent-derived terms exist.
    // The name key is its `nlp::normalize_l1` form (Porter2 strips the final
    // "e": "equivfree" -> "equivfre"), and level-two of that is identical, so
    // there is still exactly one name key.  The CURIE keys are unchanged: a
    // token carrying a digit or punctuation is never stemmed.
    assert_eq!(map["equivfre"], vec!["HGNC:9".to_string()]);
    assert_eq!(map["hgnc:9"], vec!["HGNC:9".to_string()]);
    assert!(
        map.keys()
            .all(|t| ["equivfre", "hgnc:9", "hgnc9"].contains(&t.as_str())),
        "unexpected extra terms: {:?}",
        map.keys().collect::<Vec<_>>()
    );
}
