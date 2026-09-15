//! Level-one term normalization (`normalize_l1`) plus its batch pyo3 surface
//! (`normalize_terms`).
//!
//! The pipeline per term: clean (trim + strip matching/duplicate quotes, the
//! exact `fullmap::clean_and_lower` semantics) -> Unicode lowercase ->
//! `split_whitespace` -> stem every all-ASCII-alphabetic token with the English
//! Porter2 (Snowball) stemmer -> dedupe -> byte-wise sort -> single-space join.
//! Lowercasing runs BEFORE stemming because rust-stemmers expects already
//! lowercased input; tokens containing digits or punctuation are passed through
//! unstemmed so identifiers like `tnf-alpha` or `bcl2` survive verbatim.

use std::borrow::Cow;
use std::sync::OnceLock;

use pyo3::prelude::*;
use rayon::prelude::*;
use rust_stemmers::{Algorithm, Stemmer};

/// The shared English (Porter2) stemmer. `Stemmer` is a plain `fn` pointer, so
/// it is `Send + Sync` and one instance serves every rayon worker via a
/// process-wide `OnceLock` (no per-call construction cost).
fn english_stemmer() -> &'static Stemmer {
    static ENGLISH: OnceLock<Stemmer> = OnceLock::new();
    ENGLISH.get_or_init(|| Stemmer::create(Algorithm::English))
}

/// Stem ONE whitespace token when it is purely ASCII-alphabetic; pass any token
/// containing a digit, punctuation, or non-ASCII letter through unchanged.
/// Returns a `Cow`: rust-stemmers itself borrows the input when the stem is an
/// identity (e.g. "aspirin"), so the no-op case allocates nothing.
fn stem_ascii_token(token: &str) -> Cow<'_, str> {
    if token.bytes().all(|b| b.is_ascii_alphabetic()) {
        english_stemmer().stem(token)
    } else {
        Cow::Borrowed(token)
    }
}

/// Sort tokens byte-wise, drop duplicates, and join with single spaces.
/// `str` ordering IS byte-wise lexicographic ordering, so `sort_unstable` gives
/// the required byte sort, and sorting puts equal tokens adjacent so `dedup`
/// removes every duplicate in one linear pass.
fn sorted_space_joined(mut tokens: Vec<Cow<'_, str>>) -> String {
    tokens.sort_unstable();
    tokens.dedup();
    tokens.join(" ")
}

/// Normalize one term to its level-one form.
///
/// Pipeline: `fullmap::clean_and_lower` (trim + strip quotes, then Unicode
/// lowercase — the shared single source of truth, so normalized terms compose
/// exactly with the fullmap emit path) -> split on whitespace -> stem
/// all-ASCII-alphabetic tokens (Porter2, English) -> dedupe -> byte-wise sort
/// -> join with single spaces.
///
/// Allocation discipline (matching the fullmap `Cow` chain): tokens borrow the
/// cleaned+lowercased term, and the whole result borrows the INPUT when the
/// term needs no change at all (a single token whose stem is an identity).
/// Otherwise exactly one `String` is built, for the joined output.
///
/// This function is intentionally `pub` (re-exported at the crate root) so
/// US-003 can route `fullmap`'s `emit_term` through it, and so Rust
/// integration tests under `rust/tests/` can drive the exact production
/// normalization path.
pub fn normalize_l1(value: &str) -> Cow<'_, str> {
    match crate::fullmap::clean_and_lower(value) {
        // Borrowed case: token slices keep the INPUT lifetime, so an unchanged
        // single-token term can be returned as a zero-copy borrow.
        Cow::Borrowed(lowered) => {
            let tokens: Vec<Cow<'_, str>> =
                lowered.split_whitespace().map(stem_ascii_token).collect();
            if tokens.is_empty() {
                return Cow::Borrowed("");
            }
            if let [Cow::Borrowed(single)] = tokens.as_slice() {
                // `clean_and_lower` never leaves surrounding whitespace (the
                // clean loop always re-trims), so one token spans the whole
                // string: nothing to reorder, dedupe, or join — borrow it.
                return Cow::Borrowed(single);
            }
            Cow::Owned(sorted_space_joined(tokens))
        }
        // Owned case (term was upper-case or non-ASCII): the lowered form lives
        // in a local `String`, so only the joined output can be returned.
        Cow::Owned(lowered) => {
            let tokens: Vec<Cow<'_, str>> =
                lowered.split_whitespace().map(stem_ascii_token).collect();
            if tokens.is_empty() {
                return Cow::Borrowed("");
            }
            Cow::Owned(sorted_space_joined(tokens))
        }
    }
}

/// Batch pyo3 surface for `normalize_l1`: normalize a list of terms, returning
/// an INDEX-ALIGNED list of normalized strings (rayon's `into_par_iter` with
/// `collect` preserves input order).
///
/// The GIL is released for the whole batch via `py.detach` (the crate's
/// established GIL-release pattern, e.g. `lookup_fullmap_terms`): argument
/// extraction to `Vec<String>` happens while holding the GIL, then the pure
/// Rust normalization fan-out runs GIL-free across the rayon pool.
///
/// This is the pyo3 surface US-002 wires into the Python side; it is
/// re-exported at the crate root like the fullmap pyfunctions so Rust
/// integration tests drive the exact production path.
#[pyfunction]
pub fn normalize_terms(py: Python<'_>, terms: Vec<String>) -> Vec<String> {
    py.detach(move || {
        terms
            .into_par_iter()
            .map(|term| normalize_l1(&term).into_owned())
            .collect()
    })
}

#[cfg(test)]
mod tests {
    use super::{normalize_l1, normalize_terms};
    use pyo3::prelude::*;
    use std::borrow::Cow;

    #[test]
    fn single_token_normalizes_to_itself() {
        // WHY: the dominant real-world case is a single clean lowercase word;
        // it must round-trip unchanged (Porter2 leaves "aspirin" alone) so the
        // normalizer is a no-op for already-canonical single-token terms.
        assert_eq!(normalize_l1("aspirin"), "aspirin");
    }

    #[test]
    fn unchanged_single_token_borrows_the_input() {
        // WHY: pin the Cow allocation discipline — a single token whose stem is
        // an identity (and a non-stemmed token like "tnf-alpha") must come back
        // as Cow::Borrowed, never force a fresh String on the hot path.
        assert!(matches!(normalize_l1("aspirin"), Cow::Borrowed(s) if s == "aspirin"));
        assert!(matches!(normalize_l1("tnf-alpha"), Cow::Borrowed(s) if s == "tnf-alpha"));
        // Multi-token results always own their joined String.
        assert!(matches!(normalize_l1("oral aspirin"), Cow::Owned(_)));
    }

    #[test]
    fn token_order_is_canonicalized_by_sorting() {
        // WHY: term equality must not depend on word order — "aspirin oral" and
        // "oral aspirin" are the same drug term, and sorting to one canonical
        // order makes downstream dedupe/joins order-insensitive.
        assert_eq!(normalize_l1("aspirin oral"), "aspirin oral");
        assert_eq!(normalize_l1("oral aspirin"), "aspirin oral");
    }

    #[test]
    fn duplicate_tokens_collapse() {
        // WHY: repeated words (a common artifact of concatenating name
        // synonyms) must collapse to one token so terms compare equal after
        // normalization; "gene genes" also proves dedupe happens AFTER stemming
        // — both tokens stem to "gene" and then dedupe to a single token.
        assert_eq!(normalize_l1("aspirin aspirin"), "aspirin");
        assert_eq!(normalize_l1("genes gene"), "gene");
    }

    #[test]
    fn whitespace_runs_collapse_to_single_spaces() {
        // WHY: source synonyms arrive with stray runs of spaces/tabs/newlines;
        // the output must have exactly one ASCII space between tokens and none
        // at the edges, regardless of input spacing. Tokens are chosen to be
        // Porter2 identities so this pins SPACING, not stemming (which
        // `porter2_stemming_applies_to_alpha_tokens` covers separately); the
        // non-breaking-space case proves trim/split use Unicode whitespace.
        assert_eq!(normalize_l1("  aspirin   oral\t"), "aspirin oral");
        assert_eq!(
            normalize_l1("\u{a0}aspirin\u{a0}oral\u{a0}"),
            "aspirin oral"
        );
    }

    #[test]
    fn porter2_stemming_applies_to_alpha_tokens() {
        // WHY: stemming folds morphological variants (plural/gerund) onto one
        // canonical key so "gene(s)" and "inhibit(ing)" match their base forms;
        // these three pins cover suffix 's', 'ing', and 'ts' respectively.
        assert_eq!(normalize_l1("genes"), "gene");
        assert_eq!(normalize_l1("inhibiting"), "inhibit");
        assert_eq!(normalize_l1("tablets"), "tablet");
    }

    #[test]
    fn digit_tokens_pass_through_unstemmed() {
        // WHY: pure numbers ("51") and digit-bearing identifiers ("bcl2") are
        // not English words — stemming them would corrupt the identifier, and
        // is_dead_term-style guards downstream rely on digits surviving.
        assert_eq!(normalize_l1("51"), "51");
        assert_eq!(normalize_l1("bcl2"), "bcl2");
    }

    #[test]
    fn punctuated_tokens_pass_through_unstemmed() {
        // WHY: hyphenated biomedical identifiers like "tnf-alpha" are single
        // tokens by construction (split_whitespace never splits on hyphens) and
        // must not be mangled by the stemmer, which would treat "alpha" and
        // "tnf" pieces independently and destroy the identifier.
        assert_eq!(normalize_l1("tnf-alpha"), "tnf-alpha");
    }

    #[test]
    fn unicode_uppercase_folds_before_stemming() {
        // WHY: non-ASCII source casing ("ÄTHÉROGENIC") must fold via full
        // Unicode lowercase (not just ASCII): Ä->ä and É->é. Because the folded
        // token is not all-ASCII-alphabetic it must skip stemming and survive
        // verbatim — pinning both the lowercase step and the ASCII-stem gate.
        assert_eq!(normalize_l1("ÄTHÉROGENIC"), "äthérogenic");
    }

    #[test]
    fn empty_string_normalizes_to_empty_string() {
        // WHY: empty terms occur for blank cells / missing values; the result
        // must be exactly "" (no spaces, no panic) so downstream emptiness
        // guards keep working.
        assert_eq!(normalize_l1(""), "");
        assert_eq!(normalize_l1("   "), "");
    }

    #[test]
    fn clean_semantics_compose_with_fullmap() {
        // WHY: `normalize_l1` reuses `fullmap::clean_and_lower` as the single
        // source of cleaning truth, so quoted/whitespace-padded synonyms must
        // normalize to the same form as their bare equivalents — if either side
        // drifts (trim, quote stripping, case), this pin fails loudly.
        assert_eq!(normalize_l1("\"  Aspirin Oral  \""), "aspirin oral");
        assert_eq!(normalize_l1("'GENE'"), "gene");
    }

    #[test]
    fn normalize_terms_batch_is_index_aligned_and_handles_empty() {
        // WHY: US-002 consumes the batch from Python with positional pairing;
        // rayon's collect preserves input order even under full parallelism, so
        // output[i] must be normalize_l1(input[i]) — including "" and an empty
        // batch — or term alignment would silently corrupt downstream joins.
        pyo3::Python::initialize();
        pyo3::Python::attach(|py| {
            let func = pyo3::wrap_pyfunction!(normalize_terms, py).unwrap();
            let input = vec![
                "Oral  Aspirin".to_string(),
                String::new(),
                "Genes".to_string(),
                "51".to_string(),
            ];
            let output: Vec<String> = func.call1((input,)).unwrap().extract().unwrap();
            assert_eq!(output, vec!["aspirin oral", "", "gene", "51"]);

            let empty: Vec<String> = func
                .call1((Vec::<String>::new(),))
                .unwrap()
                .extract()
                .unwrap();
            assert!(empty.is_empty());
        });
    }
}
