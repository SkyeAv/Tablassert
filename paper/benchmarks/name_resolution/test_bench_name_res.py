"""Offline tests for the name-resolution benchmark harness (mocked network)."""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import cast

import pytest

sys.path.insert(0, str(Path(__file__).parent))

import bench_name_res as bench


class FakeResponse:
    def __init__(self, status: int, body: object) -> None:
        self.status = status
        self._body = json.dumps(body).encode("utf-8")

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def read(self) -> bytes:
        return self._body


def test_unique_terms_deterministic() -> None:
    assert bench.main is not None  # module imports offline
    from local_runner import unique_terms_deterministic

    assert unique_terms_deterministic(["b", "a", "a", "c"]) == ["a", "b", "c"]
    assert unique_terms_deterministic(["a"]) * 2 == unique_terms_deterministic(["a", "a"]) * 2


def test_read_terms_skips_comments_and_blanks(tmp_path: Path) -> None:
    terms_file = tmp_path / "terms.txt"
    terms_file.write_text("# comment\n\nTP53\n  lung cancer  \n", encoding="utf-8")
    from local_runner import read_terms

    assert read_terms(terms_file) == ["TP53", "lung cancer"]


def test_read_terms_stdin_dash(monkeypatch: pytest.MonkeyPatch) -> None:
    import io

    from local_runner import read_terms

    monkeypatch.setattr(sys, "stdin", io.StringIO("TP53\n\nBRCA1\n"))
    assert read_terms(Path("-")) == ["TP53", "BRCA1"]


def test_canonical_record_complete_vs_incomplete() -> None:
    full = {"id": {"identifier": "HGNC:11998", "label": "TP53"}, "type": ["biolink:Gene"], "taxa": ["NCBITaxon:9606"]}
    record = bench.canonical_record(full)
    assert record == {"id": "HGNC:11998", "preferred_name": "TP53", "categories": ["biolink:Gene"], "taxa": ["NCBITaxon:9606"]}
    # no label -> not a complete record
    assert bench.canonical_record({"id": {"identifier": "X:1"}, "type": ["biolink:Gene"]}) is None
    # no categories -> not a complete record
    assert bench.canonical_record({"id": {"identifier": "X:1", "label": "x"}, "type": []}) is None
    assert bench.canonical_record("junk") is None


def test_prefers_human_policy() -> None:
    human = {"id": {"identifier": "H:1"}, "taxa": ["NCBITaxon:9606"]}
    pig = {"id": {"identifier": "P:1"}, "taxa": ["NCBITaxon:9823"]}
    no_taxa = {"id": {"identifier": "N:1"}}
    assert bench.prefers_human(human) is True
    assert bench.prefers_human(pig) is False
    # no taxa information stays eligible
    assert bench.prefers_human(no_taxa) is True
    assert bench.prefers_human(None) is False


def test_lookup_one_nonempty_candidates_and_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    budget = bench.RequestBudget(10)
    body = [{"curie": "HGNC:11998"}, {"curie": "NCBIGene:7157"}, {"curie": None}]
    monkeypatch.setattr(urllib.request, "urlopen", lambda req, timeout: FakeResponse(200, body))
    record = bench.lookup_one("TP53", 5, budget)
    assert record["http_status"] == 200
    assert record["curies"] == ["HGNC:11998", "NCBIGene:7157"]
    assert record["error"] is None
    assert budget.remaining == 9

    def boom(req: urllib.request.Request, timeout: float) -> FakeResponse:
        raise urllib.error.URLError("connection refused")

    monkeypatch.setattr(urllib.request, "urlopen", boom)
    failed = bench.lookup_one("net down", 5, bench.RequestBudget(10))
    assert failed["http_status"] is None
    assert failed["curies"] == []
    assert "URLError" in str(failed["error"])


def test_lookup_one_retries_on_429(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[int] = []

    def rate_limited(req: urllib.request.Request, timeout: float) -> FakeResponse:
        calls.append(1)
        if len(calls) < 2:
            raise urllib.error.HTTPError(req.full_url, 429, "slow down", {}, None)  # type: ignore[arg-type]
        return FakeResponse(200, [])

    monkeypatch.setattr(urllib.request, "urlopen", rate_limited)
    monkeypatch.setattr(bench.time, "sleep", lambda _s: None)
    budget = bench.RequestBudget(10)
    record = bench.lookup_one("aspirin", 5, budget)
    assert record["http_status"] == 200
    assert len(calls) == 2
    assert budget.remaining == 8


def test_normalize_batch_chunks_and_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, list[str]] = {}
    seen["chunks"] = []

    def capture(req: urllib.request.Request, timeout: float) -> FakeResponse:
        data = req.data
        assert isinstance(data, bytes)
        payload = json.loads(data.decode("utf-8"))
        seen["chunks"].append(payload["curies"])
        return FakeResponse(200, {curie: {"id": {"identifier": curie, "label": "x"}} for curie in payload["curies"]})

    monkeypatch.setattr(urllib.request, "urlopen", capture)
    budget = bench.RequestBudget(5)
    result = bench.normalize_batch([f"X:{i}" for i in range(250)], budget, batch_size=100)
    assert result["http_calls"] == 3
    assert [len(chunk) for chunk in seen["chunks"]] == [100, 100, 50]
    assert result["error"] is None
    assert budget.remaining == 2


def test_normalize_batch_budget_exhaustion() -> None:
    budget = bench.RequestBudget(1)
    with pytest.raises(RuntimeError, match="budget"):
        bench.normalize_batch([f"X:{i}" for i in range(250)], budget, batch_size=100)


def test_lookup_budget_exhaustion_propagates() -> None:
    with pytest.raises(bench.BudgetExhausted, match="budget"):
        bench.lookup_one("TP53", 5, bench.RequestBudget(0))


def test_hosted_budget_preflight() -> None:
    with pytest.raises(bench.BudgetExhausted, match="at least"):
        bench.bench_hosted(["TP53", "BRCA1"], 1, 5, bench.RequestBudget(2))


def test_group_normalized_by_requested_alias_keying() -> None:
    # NodeNorm keys its response by the requested (input) CURIE, not the canonical id
    requested = ["hgnc:11998", "MISSING:1"]
    normalized: dict[str, object] = {"hgnc:11998": {"id": {"identifier": "NCBIGene:7157", "label": "TP53"}, "type": ["biolink:Gene"]}}
    grouped = bench.group_normalized_by_requested(requested, normalized)
    entry = cast("dict[str, object]", grouped["hgnc:11998"])
    assert cast("dict[str, object]", entry["id"])["identifier"] == "NCBIGene:7157"  # canonical id inside the entry
    assert grouped["MISSING:1"] is None


def test_group_normalized_by_requested_canonical_keyed_fallback() -> None:
    # Robustness: if the service ever keys entries by canonical id, the requested
    # alias candidate still resolves via id + equivalent_identifiers indexing.
    normalized: dict[str, object] = {
        "NCBIGene:7157": {
            "id": {"identifier": "NCBIGene:7157", "label": "TP53"},
            "type": ["biolink:Gene"],
            "equivalent_identifiers": [{"identifier": "HGNC:11998"}, {"identifier": "NCBIGene:7157"}],
        }
    }
    grouped = bench.group_normalized_by_requested(["HGNC:11998"], normalized)
    entry = cast("dict[str, object]", grouped["HGNC:11998"])
    assert cast("dict[str, object]", entry["id"])["identifier"] == "NCBIGene:7157"


def test_bench_hosted_ranked_top_candidate_not_first_complete(monkeypatch: pytest.MonkeyPatch) -> None:
    # Top-ranked candidate has no NodeNorm record; a lower-ranked one does.
    # canonical must follow the /lookup ranking (top), not 'first complete'.
    lower_complete: dict[str, object] = {"id": {"identifier": "HGNC:1", "label": "T"}, "type": ["biolink:Gene"], "taxa": ["NCBITaxon:9606"]}

    def fake_lookup(term: str, limit: int, budget: bench.RequestBudget) -> bench.LookupRecord:
        return {"term": term, "http_status": 200, "error": None, "curies": ["Z:9", "HGNC:1"], "latency_s": 0.3}

    def fake_normalize(curies: list[str], budget: bench.RequestBudget, batch_size: int = 100) -> bench.NormalizeResult:
        return {"normalized": {"HGNC:1": lower_complete}, "http_calls": 1, "error": None}

    monkeypatch.setattr(bench, "lookup_one", fake_lookup)
    monkeypatch.setattr(bench, "normalize_batch", fake_normalize)
    result = bench.bench_hosted(["t"], concurrency=1, limit=5, budget=bench.RequestBudget(50))
    coverage = cast("dict[str, object]", result["coverage"])
    record = cast("list[dict[str, object]]", result["records"])[0]
    assert record["canonical"] is None  # top-ranked Z:9 has no record
    assert [c["curie"] for c in cast("list[dict[str, object]]", record["candidate_records"])] == ["Z:9", "HGNC:1"]
    assert cast("dict[str, object]", record["human_pref"])["id"] == "HGNC:1"  # human preference is separate
    assert coverage["complete_record"] == 0
    assert coverage["complete_record_human_pref"] == 1


def test_bench_local_cold_one_fresh_process_per_repeat(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import subprocess

    calls: list[list[str]] = []
    inputs: list[str] = []

    def fake_run(argv: list[str], input: str, capture_output: bool, text: bool, check: bool) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        inputs.append(input)
        out = Path(argv[argv.index("--out") + 1])
        mode = argv[argv.index("--mode") + 1]
        reps = int(argv[argv.index("--repeats") + 1])
        wall = 0.5 if mode == "cold" else 0.1
        out.write_text(
            json.dumps(
                {
                    "mode": mode,
                    "repeats": reps,
                    "wall_s": [wall] * reps,
                    "wall_s_median": wall,
                    "resolved_rows_per_run": [7] * reps,
                    "resolved_terms_per_run": 7,
                    "records": [],
                }
            )
        )
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setattr(bench.subprocess, "run", fake_run)
    result = cast("dict[str, object]", bench.bench_local("python", tmp_path / "f", ["t1"], repeats=3, out_dir=tmp_path))
    cold_calls = [argv for argv in calls if argv[argv.index("--mode") + 1] == "cold"]
    warm_calls = [argv for argv in calls if argv[argv.index("--mode") + 1] == "warm"]
    # cold: one fresh process per repeat, each with --repeats 1; warm: one process, --repeats N
    assert len(cold_calls) == 3
    assert all(argv[argv.index("--repeats") + 1] == "1" for argv in cold_calls)
    assert len(warm_calls) == 1
    assert warm_calls[0][warm_calls[0].index("--repeats") + 1] == "3"
    assert inputs == ["t1\n"] * 4
    cold = cast("dict[str, object]", result["cold"])
    assert cold["fresh_process_per_repeat"] is True
    assert cold["os_cache_cleared"] is False
    assert cast("list[float]", cold["wall_s"]) == [0.5, 0.5, 0.5]
    assert cast("float", cold["wall_s_median"]) == 0.5
    assert cast("list[int]", cold["resolved_rows_per_run"]) == [7, 7, 7]
    warm = cast("dict[str, object]", result["warm"])
    assert warm["mode"] == "warm"


def test_bench_hosted_coverage_levels(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_lookup(term: str, limit: int, budget: bench.RequestBudget) -> bench.LookupRecord:
        if term == "httpfail":
            return {"term": term, "http_status": None, "error": "URLError: down", "curies": [], "latency_s": 0.1}
        if term == "empty":
            return {"term": term, "http_status": 200, "error": None, "curies": [], "latency_s": 0.2}
        return {"term": term, "http_status": 200, "error": None, "curies": ["HGNC:1"], "latency_s": 0.3}

    def fake_normalize(curies: list[str], budget: bench.RequestBudget, batch_size: int = 100) -> bench.NormalizeResult:
        return {"normalized": {"HGNC:1": {"id": {"identifier": "HGNC:1", "label": "T"}, "type": ["biolink:Gene"]}}, "http_calls": 1, "error": None}

    monkeypatch.setattr(bench, "lookup_one", fake_lookup)
    monkeypatch.setattr(bench, "normalize_batch", fake_normalize)
    result = bench.bench_hosted(["ok", "empty", "httpfail"], concurrency=2, limit=5, budget=bench.RequestBudget(50))
    coverage = cast("dict[str, object]", result["coverage"])
    records_list = cast("list[dict[str, object]]", result["records"])
    assert coverage["n_terms"] == 3
    assert coverage["http_ok"] == 2
    assert coverage["nonempty_candidates"] == 1
    assert coverage["complete_record"] == 1
    assert coverage["complete_record_human_pref"] == 1
    records_list_by_term = {cast("str", r["term"]): r for r in records_list}
    empty = cast("dict[str, object]", records_list_by_term["empty"])
    httpfail = cast("dict[str, object]", records_list_by_term["httpfail"])
    ok = cast("dict[str, object]", records_list_by_term["ok"])
    assert empty["canonical"] is None
    assert str(httpfail["error"]) == "URLError: down"
    assert cast("dict[str, object]", ok["canonical"])["id"] == "HGNC:1"
    assert cast("dict[str, object]", ok["canonical"])["preferred_name"] == "T"


def test_api_spec_documents_supported_params_and_no_synonyms() -> None:
    spec = cast("dict[str, object]", bench.api_spec())
    lookup = cast("dict[str, object]", spec["lookup"])
    assert cast("dict[str, object]", spec["node_normalization"])["path"] == "/get_normalized_nodes"
    assert str(cast("dict[str, object]", spec["node_normalization"])["synonyms_endpoint"]).startswith("not used")
    assert str(cast("dict[str, object]", spec["node_normalization"])["response_keying"]).startswith("response is keyed by the requested")
    # lookup params as actually supported by the live service
    params = cast("dict[str, str]", lookup["params"])
    assert "only_taxa" in params
    assert "biolink_type" in params
    taxon_filter = str(lookup["taxon_filter"])
    assert bench.HUMAN_TAXON in taxon_filter
    assert not taxon_filter.startswith("none")  # no hardcoded unsupported 'no taxon' claim
    assert "not established" in str(lookup["unknown_taxa_handling"])
    assert cast("dict[str, object]", spec["local_fullmap"])["column_context"] is False
