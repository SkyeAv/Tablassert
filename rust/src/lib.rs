use pyo3::exceptions::PyOSError;
use pyo3::prelude::*;

// mimalloc returns memory to the OS far better than glibc malloc under heavy
// multi-threaded allocation (the synonym phase runs many worker threads each
// making millions of small String/Vec allocations).  Without it, per-thread
// malloc arenas retain freed memory and inflate peak RSS several-fold.
use mimalloc::MiMalloc;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use xxhash_rust::xxh64::{xxh64 as xxh64_digest, Xxh64};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

mod fullmap;
mod json;
mod ndjson;
mod nlp;
mod uuid;

/// XXH64 hex digest (seed 0) of a string's UTF-8 bytes.
///
/// Mirrors Python ``xxhash.xxh64(s.encode()).hexdigest()`` (16 lowercase hex
/// chars, zero-padded) so the pure-Python ``xxhash`` dependency can be dropped
/// in favour of the already-bundled ``xxhash-rust`` crate.  This is the
/// content-hash primitive behind ``tablassert.utils.mkhash``.
#[pyfunction]
fn xxh64(data: &str) -> String {
    format!("{:016x}", xxh64_digest(data.as_bytes(), 0))
}

/// XXH64 hex digest (seed 0) of a file's raw bytes, streamed in fixed-size chunks.
#[pyfunction]
fn xxh64_file(path: PathBuf) -> PyResult<String> {
    const CHUNK_SIZE: usize = 8 * 1024 * 1024;

    let mut file = File::open(&path)
        .map_err(|error| PyOSError::new_err(format!("{}: {}", path.display(), error)))?;
    let mut hasher = Xxh64::new(0);
    let mut chunk = vec![0; CHUNK_SIZE];
    loop {
        let bytes_read = file
            .read(&mut chunk)
            .map_err(|error| PyOSError::new_err(format!("{}: {}", path.display(), error)))?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&chunk[..bytes_read]);
    }
    Ok(format!("{:016x}", hasher.digest()))
}

// Public re-exports of the fullmap `#[pyfunction]`s so Rust integration tests
// (`rust/tests/`) and other non-Python embedders can drive the exact production
// build/read path.  The `fullmap` module itself stays private; only these
// intended entry points are surfaced at the crate root.
pub use fullmap::{
    build_fullmap_db, extract_prebuilt_fullmap, fullmap_source_version,
    fullmap_taxon_allowlist_identity, hydrate_categories, hydrate_curies, hydrate_prefixes,
    hydrate_sources, lookup_fullmap_terms, taxon_allowlist_identity,
};
// Level-one normalization.  `normalize_l1` is a plain Rust function consumed
// by `fullmap::emit_term` (US-003) and by integration tests; `normalize_terms`
// is the batch pyo3 surface the Python side calls (US-002).  Both are
// intentional public exports, not dead code.
pub use nlp::{normalize_l1, normalize_terms};

#[pymodule]
fn rs(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(fullmap::build_fullmap_db, module)?)?;
    module.add_function(wrap_pyfunction!(fullmap::extract_prebuilt_fullmap, module)?)?;
    module.add_function(wrap_pyfunction!(fullmap::fullmap_source_version, module)?)?;
    module.add_function(wrap_pyfunction!(
        fullmap::fullmap_taxon_allowlist_identity,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(fullmap::hydrate_categories, module)?)?;
    module.add_function(wrap_pyfunction!(fullmap::hydrate_curies, module)?)?;
    module.add_function(wrap_pyfunction!(fullmap::hydrate_prefixes, module)?)?;
    module.add_function(wrap_pyfunction!(fullmap::hydrate_sources, module)?)?;
    module.add_function(wrap_pyfunction!(fullmap::lookup_fullmap_terms, module)?)?;
    module.add_function(wrap_pyfunction!(fullmap::taxon_allowlist_identity, module)?)?;
    module.add_function(wrap_pyfunction!(ndjson::dedup_ndjson, module)?)?;
    module.add_function(wrap_pyfunction!(nlp::normalize_terms, module)?)?;
    module.add_function(wrap_pyfunction!(uuid::namespace_uuid, module)?)?;
    module.add_function(wrap_pyfunction!(xxh64, module)?)?;
    module.add_function(wrap_pyfunction!(xxh64_file, module)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{xxh64, xxh64_file};

    #[test]
    fn xxh64_matches_known_digests() {
        // WHY: pin the XXH64 (seed 0) output to values cross-checked against
        // Python's `xxhash.xxh64(...).hexdigest()` so the Rust primitive stays
        // drop-in compatible; 16 lowercase hex chars, zero-padded.
        assert_eq!(xxh64("hello"), "26c7827d889f6da3");
        assert_eq!(xxh64(""), "ef46db3751d8e999");
    }

    #[test]
    fn xxh64_file_matches_known_digests() {
        // WHY: pin file hashing to the same seed-0 digest as the string primitive,
        // including the empty-file identity value and UTF-8 bytes.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hello");
        std::fs::write(&path, b"hello").unwrap();
        assert_eq!(xxh64_file(path).unwrap(), "26c7827d889f6da3");

        let empty = dir.path().join("empty");
        std::fs::write(&empty, b"").unwrap();
        assert_eq!(xxh64_file(empty).unwrap(), "ef46db3751d8e999");
    }

    #[test]
    fn xxh64_file_reports_io_failures_as_os_error() {
        // WHY: callers must distinguish I/O failures from valid digests and see
        // the offending path, including for missing files and directories.
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing");
        let error = xxh64_file(missing.clone()).unwrap_err();
        assert!(error.to_string().contains(&missing.display().to_string()));

        let error = xxh64_file(dir.path().to_path_buf()).unwrap_err();
        assert!(error
            .to_string()
            .contains(&dir.path().display().to_string()));
    }
}
