//! Shared level-one normalization golden vectors.

const GOLDEN: &str = include_str!("fixtures/nlp_golden.tsv");

#[test]
fn normalize_l1_matches_shared_golden_fixture() {
    // WHY: fullmap ingestion uses normalize_l1 directly while Python queries use
    // rs.normalize_terms; one checked-in fixture keeps those public paths tied
    // to the same expected outputs without duplicating vectors in each test.
    let mut rows = GOLDEN.lines();
    assert_eq!(rows.next(), Some("raw\texpected"));

    let mut count = 0usize;
    for (line_number, line) in rows.enumerate() {
        let (raw, expected) = line
            .split_once('\t')
            .unwrap_or_else(|| panic!("fixture line {} is not TSV", line_number + 2));
        let raw = raw.strip_prefix("@EMPTY").map_or(raw, |_| "");
        let expected = expected.strip_prefix("@EMPTY").map_or(expected, |_| "");
        assert_eq!(
            tablassert_rs::normalize_l1(raw),
            expected,
            "fixture line {}",
            line_number + 2
        );
        count += 1;
    }
    assert!(
        count >= 8,
        "fixture must cover representative normalization cases"
    );
}
