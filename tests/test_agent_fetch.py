"""Offline unit tests for US-002 ``fetch_pmc_article`` (PMC ``s3://pmc-oa-opendata``).

Everything here runs with NO network and NO ``[agent]`` extra: the PURE parsers are
exercised directly, and ``fetch_pmc_article`` runs against the two mocked I/O seams
(``_http_get_text`` / ``_http_get_bytes``) routed by URL. The fetch flow is a fail-fast
ladder: list version prefixes -> pick the LATEST -> check OA metadata -> enumerate the
version's objects -> confirm a table exists -> download ONLY the useful files (main text
``.xml/.nxml``, ``.json`` metadata, and data tables; never images/``.docx``/``.txt``/``.pdf``).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from tablassert.agent import (
    MIN_TABLE_ROWS,
    _effective_rows,
    candidate_tables,
    excel_sheet_heights,
    fetch_pmc_article,
    fetch_pmc_tables,
    is_open_access,
    is_table_file,
    is_useful_file,
    latest_version_prefix,
    normalize_pmc_id,
    object_keys_from_listing,
    public_url,
    version_prefixes_from_listing,
)

# A realistic list-objects-v2 XML body for VERSION prefixes. The echoed request
# ``<Prefix>PMC11708054.`` (no trailing slash) must be filtered out; only the
# ``CommonPrefixes`` entry survives.
LISTING_XML: str = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
    "<Name>pmc-oa-opendata</Name>"
    "<Prefix>PMC11708054.</Prefix>"
    "<KeyCount>1</KeyCount>"
    "<CommonPrefixes><Prefix>PMC11708054.1/</Prefix></CommonPrefixes>"
    "</ListBucketResult>"
)
LISTING_JSON: str = '{"CommonPrefixes": [{"Prefix": "PMC11708054.1/"}]}'

METADATA_OA: str = '{"is_pmc_openaccess": true, "license_code": "CC-BY"}'
METADATA_NON_OA: str = '{"is_pmc_openaccess": false}'

# The full object inventory of PMC11708054.1/ (mirrors the live bucket): main text
# (.json/.pdf/.txt/.xml), two data tables (.xlsx), one binary .docx and one .jpg figure.
# The .txt and .pdf stay in the listing (the bucket ships them) to prove both redundant
# main-text copies are now SKIPPED.
OBJECT_KEYS: list[str] = [
    "PMC11708054.1/PMC11708054.1.json",
    "PMC11708054.1/PMC11708054.1.pdf",
    "PMC11708054.1/PMC11708054.1.txt",
    "PMC11708054.1/PMC11708054.1.xml",
    "PMC11708054.1/mbio.01679-24-s0001.docx",
    "PMC11708054.1/mbio.01679-24-s0002.xlsx",
    "PMC11708054.1/mbio.01679-24-s0003.xlsx",
    "PMC11708054.1/mbio.01679-24.f001.jpg",
]
# Only the useful files are downloaded (main text + metadata + tables; NOT .txt/.pdf/.docx/.jpg).
USEFUL_NAMES: list[str] = ["PMC11708054.1.json", "PMC11708054.1.xml", "mbio.01679-24-s0002.xlsx", "mbio.01679-24-s0003.xlsx"]


def _object_listing(keys: list[str]) -> str:
    """Build a list-objects-v2 XML body whose ``<Contents><Key>`` entries are ``keys``."""
    contents: str = "".join(f"<Contents><Key>{key}</Key><Size>1</Size></Contents>" for key in keys)
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        "<Name>pmc-oa-opendata</Name>"
        f"{contents}"
        "</ListBucketResult>"
    )


def _patch_http(
    monkeypatch: pytest.MonkeyPatch, *, version_listing: str = LISTING_XML, object_listing: str | None = None, metadata: str = METADATA_OA
) -> tuple[list[str], list[str]]:
    """Route the two mocked HTTP seams by URL; return (requested_text_urls, downloaded_urls).

    WHY: ``_http_get_text``/``_http_get_bytes`` are the SINGLE I/O seam. The version listing
    is the ``list-type=2`` call WITH ``delimiter=``; the object listing is ``list-type=2``
    WITHOUT a delimiter; ``.json`` is the metadata. Recording the URLs lets tests assert the
    fail-fast ORDER (e.g. a non-OA id never reaches the object listing nor downloads bytes).
    """
    requested: list[str] = []
    downloaded: list[str] = []
    objects: str = object_listing if object_listing is not None else _object_listing(OBJECT_KEYS)

    def get_text(url: str, *, timeout: int = 120) -> str:
        requested.append(url)
        if "list-type=2" in url and "delimiter=" in url:
            return version_listing
        if "list-type=2" in url:
            return objects
        if url.endswith(".json"):
            return metadata
        raise AssertionError(f"unexpected text url: {url}")

    def get_bytes(url: str, *, timeout: int = 120) -> bytes:
        downloaded.append(url)
        return b"FAKEBYTES"

    monkeypatch.setattr("tablassert.agent._http_get_text", get_text)
    monkeypatch.setattr("tablassert.agent._http_get_bytes", get_bytes)
    return requested, downloaded


# --------------------------------------------------------------------------- #
# normalize_pmc_id / version_prefixes_from_listing / is_table_file (unchanged)
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("raw", ["PMC11708054", "11708054", "pmc11708054", " PMC11708054 "])
def test_normalize_pmc_id_accepts_common_forms(raw: str) -> None:
    """A bare number, either case prefix, and surrounding whitespace all normalize."""
    assert normalize_pmc_id(raw) == "PMC11708054"


@pytest.mark.parametrize("raw", ["abc", "", "PMC"])
def test_normalize_pmc_id_rejects_garbage(raw: str) -> None:
    """No remaining digits -> a loud ``ValueError`` instead of a bogus S3 prefix."""
    with pytest.raises(ValueError, match="Invalid PMC id"):
        normalize_pmc_id(raw)


def test_version_prefixes_from_listing_xml() -> None:
    """XML listing -> the single version prefix; the echoed request prefix is dropped."""
    assert version_prefixes_from_listing(LISTING_XML) == ["PMC11708054.1/"]


def test_version_prefixes_from_listing_json() -> None:
    """The JSON variant some clients return parses to the same prefix list."""
    assert version_prefixes_from_listing(LISTING_JSON) == ["PMC11708054.1/"]


@pytest.mark.parametrize("listing", ["<<garbage>>", "", "{not valid json"])
def test_version_prefixes_from_listing_bad(listing: str) -> None:
    """Garbage / empty / malformed bodies yield ``[]`` (never a raise)."""
    assert version_prefixes_from_listing(listing) == []


@pytest.mark.parametrize(
    ("filename", "label", "expected"),
    [
        ("a.xlsx", None, True),
        ("a.csv", None, True),
        ("a.tsv", None, True),
        ("a.parquet", None, True),
        ("fig.jpg", "Table 1", False),  # DROP extension wins over a "Table" label
        ("data.bin", "Table S2", True),  # label-only match for an unknown extension
        ("data.bin", None, False),
        ("A.XLSX", None, True),  # case-insensitive extension
    ],
)
def test_is_table_file(filename: str, label: str | None, expected: bool) -> None:
    """Extension drop > table extension > ``Table`` label, case-insensitively."""
    assert is_table_file(filename, label) is expected


def test_is_open_access_cc_by_json() -> None:
    """CC-BY metadata string is open access."""
    assert is_open_access(METADATA_OA) is True


def test_is_open_access_non_oa() -> None:
    """``is_pmc_openaccess: false`` with no CC license is NOT open access."""
    assert is_open_access(METADATA_NON_OA) is False


def test_is_open_access_dict_input() -> None:
    """A pre-parsed dict is accepted on the same code path."""
    assert is_open_access({"is_pmc_openaccess": True, "license_code": "CC-BY"}) is True


def test_is_open_access_bad_json() -> None:
    """An unparseable metadata string is treated as non-OA (False), never a raise."""
    assert is_open_access("{not json") is False


def test_public_url() -> None:
    """Trailing/leading slashes are normalized into a single public HTTPS URL."""
    assert public_url("PMC11708054.1/", "f.xlsx") == "https://pmc-oa-opendata.s3.amazonaws.com/PMC11708054.1/f.xlsx"


# --------------------------------------------------------------------------- #
# New pure helpers: latest_version_prefix / object_keys_from_listing /
# is_useful_file / candidate_tables
# --------------------------------------------------------------------------- #


def test_latest_version_prefix_numeric() -> None:
    """Numeric compare: ``.10`` beats ``.2`` (a string sort would get this wrong)."""
    assert latest_version_prefix(["PMC1.1/", "PMC1.10/", "PMC1.2/"]) == "PMC1.10/"


def test_latest_version_prefix_single() -> None:
    """A single prefix is returned unchanged."""
    assert latest_version_prefix(["PMC11708054.1/"]) == "PMC11708054.1/"


def test_object_keys_from_listing_xml() -> None:
    """XML ``<Contents><Key>`` -> sorted unique keys."""
    keys: list[str] = object_keys_from_listing(_object_listing(["p/b.xlsx", "p/a.xml"]))
    assert keys == ["p/a.xml", "p/b.xlsx"]


def test_object_keys_from_listing_json() -> None:
    """The JSON variant parses to the same key list."""
    assert object_keys_from_listing('{"Contents": [{"Key": "p/a.xml"}, {"Key": "p/b.xlsx"}]}') == ["p/a.xml", "p/b.xlsx"]


def test_object_keys_from_listing_drops_folder_markers() -> None:
    """Keys ending in ``/`` (folder markers) and empty keys are dropped."""
    body: str = '{"Contents": [{"Key": "p/"}, {"Key": ""}, {"Key": "p/a.xml"}]}'
    assert object_keys_from_listing(body) == ["p/a.xml"]


@pytest.mark.parametrize("listing", ["<<garbage>>", "", "{not valid json"])
def test_object_keys_from_listing_bad(listing: str) -> None:
    """Garbage / empty / malformed bodies yield ``[]`` (never a raise)."""
    assert object_keys_from_listing(listing) == []


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        ("PMC1.1.xml", True),
        ("PMC1.1.nxml", True),
        ("PMC1.1.txt", False),  # redundant main-text copy; never downloaded
        ("PMC1.1.pdf", False),  # binary main-text copy; every version ships JATS .xml
        ("PMC1.1.json", True),
        ("s.xlsx", True),
        ("s.csv", True),
        ("s.tsv", True),
        ("fig.jpg", False),
        ("fig.png", False),
        ("suppl.docx", False),
        ("blob.bin", False),
    ],
)
def test_is_useful_file(filename: str, expected: bool) -> None:
    """Main text + metadata + data tables are useful; binary media is not."""
    assert is_useful_file(filename) is expected


def test_effective_rows_counts_parsed_rows_not_physical_lines(tmp_path: Path) -> None:
    """Quoted newlines are one parsed row, and an all-empty row does not satisfy the guard."""
    table: Path = tmp_path / "quoted.csv"
    table.write_text('gene,value\nBRCA1,1\n"multi\nline",2\n,\n')

    assert _effective_rows(table) == 2

    tsv: Path = tmp_path / "quoted.tsv"
    tsv.write_text("gene\tvalue\nBRCA1\t1\nMAPK1\t2\n")
    assert _effective_rows(tsv) == 2


def test_effective_rows_header_only_is_empty(tmp_path: Path) -> None:
    """A header-only delimited file has zero effective data rows."""
    table: Path = tmp_path / "header.tsv"
    table.write_text("gene\tvalue\n")

    assert _effective_rows(table) == 0


def test_effective_rows_cache_invalidates_when_file_changes(tmp_path: Path) -> None:
    """The row-count cache keys file metadata, so rewriting a file never returns its old count."""
    table: Path = tmp_path / "changing.csv"
    table.write_text("value\n1\n")
    assert _effective_rows(table) == 1

    table.write_text("value\n1\n2\n")
    assert _effective_rows(table) == 2


def test_effective_rows_counts_parquet_excluding_all_null_rows(tmp_path: Path) -> None:
    """A ``.parquet`` table counts data rows minus rows null in every column.

    The agent shows candidates with a row threshold, so a parquet payload must count the same
    way the Excel branch does (formatted blank rows are not data). A row that is null in every
    column is skipped; partial-null rows still count.
    """
    pp: Path = tmp_path / "counts.parquet"
    pl.DataFrame({"a": ["x", None, "y", "z"], "b": ["1", None, "2", None]}).write_parquet(pp)
    assert _effective_rows(pp) == 3


def test_excel_sheet_heights_ignores_blank_rows(tmp_path: Path) -> None:
    """Worksheet heights use effective rows, not formatted blank trailing rows."""
    openpyxl = pytest.importorskip("openpyxl")
    path: Path = tmp_path / "workbook.xlsx"
    workbook = openpyxl.Workbook()
    data = workbook.active
    assert data is not None
    data.title = "data"
    data.append(["gene", "value"])
    for index in range(3):
        data.append([f"GENE{index}", index])
    for _ in range(20):
        data.append([None, None])
    workbook.create_sheet("header_only").append(["note"])
    workbook.save(path)

    assert _effective_rows(path, sheet="data") == 3
    assert excel_sheet_heights(path) == {"data": 3, "header_only": 0}


def test_candidate_tables_returns_all_tables(tmp_path: Path) -> None:
    """Every table-extension path is returned (not just the first), in order.

    Missing files are retained by the guard's fail-open policy so the existing extension-filter
    contract remains useful before the files have been materialized.
    """
    files: list[Path] = [tmp_path / "a.xlsx", tmp_path / "b.jpg", tmp_path / "c.csv"]
    assert candidate_tables(files) == [tmp_path / "a.xlsx", tmp_path / "c.csv"]


def test_candidate_tables_filters_small_delimited_files(tmp_path: Path) -> None:
    """Readable delimited files below the threshold are excluded before the agent is built."""
    small: Path = tmp_path / "small.csv"
    small.write_text("value\n1\n2\n")
    enough: Path = tmp_path / "enough.csv"
    enough.write_text("value\n" + "\n".join(str(i) for i in range(3)) + "\n")

    assert candidate_tables([small, enough], min_rows=3) == [enough]


def test_candidate_tables_all_small_has_diagnostics(tmp_path: Path) -> None:
    """When every readable table is too small, the fail-fast error names the threshold and file count."""
    small: Path = tmp_path / "small.csv"
    small.write_text("value\n1\n2\n")

    with pytest.raises(FileNotFoundError, match=r"at least 3 data rows") as exc_info:
        candidate_tables([small], min_rows=3)
    assert "small.csv" in str(exc_info.value)
    assert "2 rows" in str(exc_info.value)


def test_candidate_tables_unreadable_file_is_fail_open(tmp_path: Path) -> None:
    """An unreadable table stays visible so the normal coded read error can reach the agent."""
    broken: Path = tmp_path / "broken.csv"
    broken.write_bytes(b"value\n\xff\n")

    assert candidate_tables([broken], min_rows=MIN_TABLE_ROWS) == [broken]


def test_candidate_tables_keeps_mixed_workbook_and_drops_all_small_workbook(tmp_path: Path) -> None:
    """A workbook survives if one sheet qualifies, but an all-small workbook is excluded."""
    openpyxl = pytest.importorskip("openpyxl")

    def make_workbook(path: Path, sheet_rows: dict[str, int]) -> None:
        workbook = openpyxl.Workbook()
        first = workbook.active
        assert first is not None
        for sheet_index, (name, rows) in enumerate(sheet_rows.items()):
            worksheet = first if sheet_index == 0 else workbook.create_sheet()
            worksheet.title = name
            worksheet.append(["value"])
            for row in range(rows):
                worksheet.append([row])
        workbook.save(path)

    mixed: Path = tmp_path / "mixed.xlsx"
    make_workbook(mixed, {"small": 1, "large": 3})
    assert candidate_tables([mixed], min_rows=3) == [mixed]

    all_small: Path = tmp_path / "all-small.xlsx"
    make_workbook(all_small, {"one": 1, "two": 2})
    with pytest.raises(FileNotFoundError, match=r"at least 3 data rows"):
        candidate_tables([all_small], min_rows=3)


def test_candidate_tables_none_raises(tmp_path: Path) -> None:
    """No table among the files -> ``FileNotFoundError`` (fail-fast when a fetch yields no tables)."""
    with pytest.raises(FileNotFoundError, match="No supplementary table"):
        candidate_tables([tmp_path / "a.xml", tmp_path / "b.jpg"])


# --------------------------------------------------------------------------- #
# fetch_pmc_article (mocked HTTP) — happy path + fail-fast ladder
# --------------------------------------------------------------------------- #


def test_fetch_pmc_article_happy_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """OA article -> only the useful files land on disk under ``outdir/<key>`` (no .txt/.pdf/.docx/.jpg)."""
    _, downloaded = _patch_http(monkeypatch)
    outdir: Path = tmp_path / "out"
    result: list[Path] = fetch_pmc_article("PMC11708054", outdir)

    assert sorted(p.name for p in result) == sorted(USEFUL_NAMES)
    # every returned path really exists under the version subdir
    for path in result:
        assert path.is_file()
        assert path.parent.name == "PMC11708054.1"
    # binary media and the redundant .txt/.pdf main-text copies were never downloaded
    assert not any(url.endswith((".jpg", ".docx", ".txt", ".pdf")) for url in downloaded)
    # the useful files were
    assert sum(1 for url in downloaded if url.endswith((".xml", ".json", ".xlsx"))) == len(USEFUL_NAMES)


def test_fetch_pmc_tables_wrapper_returns_only_tables(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The backward-compat wrapper filters the full payload down to data tables."""
    _patch_http(monkeypatch)
    result: list[Path] = fetch_pmc_tables("PMC11708054", tmp_path / "out")
    assert sorted(p.name for p in result) == ["mbio.01679-24-s0002.xlsx", "mbio.01679-24-s0003.xlsx"]


def test_fetch_pmc_article_latest_version_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """With two versions, ONLY the latest (``PMC1.2``) is listed/downloaded; ``PMC1.1`` is ignored."""
    version_listing: str = (
        '<?xml version="1.0"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        "<CommonPrefixes><Prefix>PMC1.1/</Prefix></CommonPrefixes>"
        "<CommonPrefixes><Prefix>PMC1.2/</Prefix></CommonPrefixes>"
        "</ListBucketResult>"
    )
    objects: str = _object_listing(["PMC1.2/PMC1.2.xml", "PMC1.2/t.xlsx"])
    requested, downloaded = _patch_http(monkeypatch, version_listing=version_listing, object_listing=objects)

    result: list[Path] = fetch_pmc_article("PMC1", tmp_path / "out")
    assert sorted(p.name for p in result) == ["PMC1.2.xml", "t.xlsx"]
    # the object listing + metadata targeted PMC1.2, and nothing ever touched PMC1.1
    assert any("prefix=PMC1.2/" in url for url in requested)
    assert not any("PMC1.1" in url for url in requested)
    assert all("PMC1.2/" in url for url in downloaded)


def test_fetch_pmc_article_non_oa_fails_fast(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-OA metadata -> ``PermissionError`` BEFORE the object listing and BEFORE any byte download."""
    requested, downloaded = _patch_http(monkeypatch, metadata=METADATA_NON_OA)
    with pytest.raises(PermissionError, match="not open access"):
        fetch_pmc_article("PMC11708054", tmp_path / "out")
    # fail-fast order: no object listing (list-type=2 without delimiter) and no bytes
    assert not any("list-type=2" in url and "delimiter" not in url for url in requested)
    assert downloaded == []


def test_fetch_pmc_article_empty_object_listing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A version with no objects -> ``FileNotFoundError`` (no files)."""
    _patch_http(monkeypatch, object_listing=_object_listing([]))
    with pytest.raises(FileNotFoundError, match="No files found"):
        fetch_pmc_article("PMC11708054", tmp_path / "out")


def test_fetch_pmc_article_no_tables_fails_fast(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """OA article with main text + a figure but NO table -> ``FileNotFoundError`` before any download."""
    keys: list[str] = [
        "PMC11708054.1/PMC11708054.1.xml",
        "PMC11708054.1/PMC11708054.1.txt",
        "PMC11708054.1/PMC11708054.1.json",
        "PMC11708054.1/fig.jpg",
    ]
    _, downloaded = _patch_http(monkeypatch, object_listing=_object_listing(keys))
    with pytest.raises(FileNotFoundError, match="No supplementary tables"):
        fetch_pmc_article("PMC11708054", tmp_path / "out")
    assert downloaded == []  # the table gate precedes every byte download


def test_fetch_pmc_article_empty_version_listing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """No version prefixes -> ``FileNotFoundError`` (not OA or wrong id)."""
    empty: str = '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Prefix>PMC999.</Prefix></ListBucketResult>'
    _patch_http(monkeypatch, version_listing=empty)
    with pytest.raises(FileNotFoundError, match="No PMC open-access versions"):
        fetch_pmc_article("PMC999", tmp_path / "out")


def test_fetch_pmc_article_bad_id(tmp_path: Path) -> None:
    """A bad id raises ``ValueError`` before any HTTP is attempted (no mock needed)."""
    with pytest.raises(ValueError, match="Invalid PMC id"):
        fetch_pmc_article("not-an-id", tmp_path)
