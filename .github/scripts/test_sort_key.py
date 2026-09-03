"""Offline tests for the recorded sort key. No Fabric, no network, no credentials.

WHAT THIS PINS IS A SILENT GAP. The dashboard captions a sorted bar with the columns the run
ordered by, and the key is a property of the RUN — the model declared `['date','time','DUID']` for a
while and `['date','time']` since. A constant in the render layer was right for today's model only,
and captioned run 30955591822, a DUID sort, `by date, time`. Nothing errored; the page just said
something untrue.

So the run has to write its own key down. `stats.py` no longer does — the `sort_by` dispatch input
it read is deleted, one field naming one key being unable to serve five marts — so the ONLY witness
is `fabric_run.py`'s scrape of duckrun's own picker, pinned in `test_record.py`. What is left here
is the other half of that write config: that no revived `declared_sort_key` sneaks back in, that
`stats.py` writes no `dbt` branch, and that the GEOMETRY is recorded on a run whose sort is off —
which the old `DUCKDB_SORTED` gate would have silently dropped.

    python -m pytest .github/scripts/test_sort_key.py -q
"""
import json
import pathlib
import os
import subprocess
import sys
import types

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, HERE)


@pytest.fixture(autouse=True)
def _no_ambient_duckdb_env(monkeypatch):
    """Clear every write-config var before each test.

    THE GATE MUST NOT BE A FUNCTION OF THE DISPATCH IT IS GATING. `benchmark.yml` puts
    `DUCKDB_SORTED` and the geometry into the job env, so a test that reads one without setting it
    asserts against whatever the human happened to type into the dispatch form. That is exactly what
    killed run 31073309328: a `sort_by=date,time,DUID` dispatch failed the free checks on a test that
    hardcoded `date,time`, and the run never reached a paid leg. (That input no longer exists; the
    rule it taught applies to every var in this tuple.)

    `DWH_VORDER` is here for the same reason and not because it is a DuckDB knob — it is the dwh
    leg's V-Order input, and it reaches `stats.py` through that same workflow-level env.
    """
    for k in ("DUCKDB_SORTED", "DUCKDB_ROW_GROUP_SIZE", "DUCKDB_FILE_SIZE_MB",
              "DWH_VORDER"):
        monkeypatch.delenv(k, raising=False)


@pytest.fixture(scope="module")
def stats():
    """`stats.py` with its Fabric-facing imports stubbed.

    It mints a token at MODULE level (`H = {...fabric_token()}`), so importing it for real either
    waits minutes on Azure or shells out to an `az` that is not installed. This gate runs before any
    leg spends capacity and has to stay in seconds, so `duckrun.auth` hands back a dummy.
    """
    duckrun = sys.modules.setdefault("duckrun", types.ModuleType("duckrun"))
    auth = sys.modules.setdefault("duckrun.auth", types.ModuleType("duckrun.auth"))
    auth.get_fabric_token = lambda *a, **k: "stub-token"
    duckrun.auth = auth
    sys.modules.setdefault("requests", types.ModuleType("requests"))
    os.environ.setdefault("WS_ID", "00000000-0000-0000-0000-000000000000")
    import stats as mod
    return mod


def test_every_name_stats_py_calls_is_defined(stats):
    """The one that would have saved run 31067443454.

    A rewrite of the since-deleted `declared_sort_key` replaced a text RANGE that `encoding_table`
    happened to sit inside, deleting it. Nothing here caught it: the function only prints to the step summary, so no
    test calls it, and `main()` is the only caller — which no offline test reaches either. The layout
    job then died on `NameError: name 'encoding_table' is not defined`, ten minutes into a paid run,
    after the build had already been paid for.

    This walks the AST and checks every bare-name call resolves to something: a def in this module,
    an import, a module-level binding, or a builtin. It costs milliseconds and catches the entire
    class — a deleted function, a typo'd call, an import dropped as "unused" that was not.
    """
    import ast
    import builtins
    src = open(os.path.join(HERE, "stats.py"), encoding="utf-8").read()
    tree = ast.parse(src)
    bound = set(dir(builtins))
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            bound.add(node.name)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            bound.update((a.asname or a.name).split(".")[0] for a in node.names)
        elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
            bound.add(node.id)
        elif isinstance(node, ast.arg):
            bound.add(node.arg)
    called = {n.func.id for n in ast.walk(tree)
              if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
    assert not (called - bound), f"stats.py calls undefined name(s): {sorted(called - bound)}"


def test_nothing_declares_a_sort_key_any_more(stats, monkeypatch):
    """`declared_sort_key()` is GONE and must not come back, on any spelling.

    It read a dispatched column list out of `DUCKDB_SORT_BY` and wrote it as `dbt.duckrun.sort_by`.
    That input is deleted — one form field naming one key could not serve five marts — so the model
    declares `auto` or nothing and a revived version could only ever return `{}`: dead code that
    looks live. The chosen columns now reach the record ONLY through `fabric_run.py`'s log scrape,
    as `dbt.<engine>.sort_by_auto`, which `test_record.py` pins.
    """
    assert not hasattr(stats, "declared_sort_key")
    src = pathlib.Path(stats.__file__).read_text(encoding="utf-8")
    for line in src.splitlines():
        if line.lstrip().startswith("#"):
            continue
        assert "DUCKDB_SORT_BY" not in line, f"stats.py reads the deleted input again: {line!r}"


def test_geometry_is_recorded_even_when_the_run_is_unsorted(stats, monkeypatch):
    """A run that wrote the parquet the history wrote must key to the SAME dashboard column.
    `variant()` skips null, so absence keeps the column; a value splits it.

    THE UNSORTED HALF IS THE REGRESSION THIS EXISTS FOR, and it is a reversal. `_nonbaseline` was
    gated on `DUCKDB_SORTED`, which was correct while blanking `sort_by` was how an unsorted run was
    dispatched — blanking it declared no geometry either. `duckrun_auto` carries the sort now, so
    OFF means "unsorted AT the pinned row group / file size", i.e. the case where the geometry is
    most deliberately chosen. Under the old gate both keys would vanish from the record on exactly
    those runs, the run would join the baseline column, and nothing would look broken.
    """
    monkeypatch.setenv("DUCKDB_SORTED", "true")
    monkeypatch.setenv("DUCKDB_ROW_GROUP_SIZE", "16000000")
    monkeypatch.setenv("DUCKDB_FILE_SIZE_MB", "1024")
    assert stats._nonbaseline("DUCKDB_ROW_GROUP_SIZE", "16000000") is None
    assert stats._nonbaseline("DUCKDB_FILE_SIZE_MB", "1024") is None
    monkeypatch.setenv("DUCKDB_ROW_GROUP_SIZE", "4000000")
    monkeypatch.setenv("DUCKDB_FILE_SIZE_MB", "128")
    assert stats._nonbaseline("DUCKDB_ROW_GROUP_SIZE", "16000000") == "4000000"
    assert stats._nonbaseline("DUCKDB_FILE_SIZE_MB", "1024") == "128"
    # ...and an UNSORTED run at a pinned geometry still records it, on every spelling of unsorted.
    monkeypatch.setenv("DUCKDB_SORTED", "false")
    assert stats._nonbaseline("DUCKDB_ROW_GROUP_SIZE", "16000000") == "4000000"
    assert stats._nonbaseline("DUCKDB_FILE_SIZE_MB", "1024") == "128"
    monkeypatch.delenv("DUCKDB_SORTED", raising=False)
    assert stats._nonbaseline("DUCKDB_ROW_GROUP_SIZE", "16000000") == "4000000"


def test_the_baseline_is_history_not_the_current_dispatch_default(stats, monkeypatch):
    """THE TRAP THIS PINS: the baseline is the geometry `history/` was written under, and it must NOT
    follow the dispatch default when that moves. That default has now moved THREE TIMES — 16000000
    for the 13+ oldest recorded runs, 6000000 once the knee was measured, 16000000 again when
    `date,time,price` became the default key, and 2000000 since — which is exactly why it cannot be
    read live.

    Were the baseline the live default, a 6M run would record `None`, share an `(engine, config)`
    column with the 16M history, and `columnsFor` — latest run per column — would hide six runs of
    9-RG history behind one 24-RG run. The BARS would still separate (`layoutKey` bands the measured
    counts), so nothing looks broken; the CU and sources tables just report the wrong geometry.
    """
    monkeypatch.setenv("DUCKDB_SORTED", "true")
    monkeypatch.setenv("DUCKDB_ROW_GROUP_SIZE", "6000000")   # a default this has genuinely held
    assert stats._nonbaseline("DUCKDB_ROW_GROUP_SIZE", "16000000") == "6000000",         "a run at today's default must still record its geometry — history wrote 16M"

    src = pathlib.Path(stats.__file__).read_text(encoding="utf-8")
    assert '_nonbaseline("DUCKDB_ROW_GROUP_SIZE", "16000000")' in src,         "the call site's baseline was moved off what history holds"
    assert '_nonbaseline("DUCKDB_FILE_SIZE_MB", "1024")' in src


def test_write_json_records_no_sort_key_of_its_own(stats, tmp_path, monkeypatch):
    """`write_json` used to merge `dbt.duckrun.sort_by` beside the layout doc. It writes no `dbt`
    branch at all now — the only sort witness is `fabric_run.py`'s scrape — and the layout doc must
    still land under `layout`, never `layout.dbt`, which the page does not read."""
    import record
    rec = tmp_path / "run.json"
    monkeypatch.setenv("RUN_RECORD", str(rec))
    monkeypatch.setenv("DUCKDB_SORTED", "true")
    monkeypatch.setattr(stats, "build_doc", lambda *a, **k: {"stats": {}})
    stats.write_json({"stats": {"duckrun": {}}}, ["duckrun"])
    doc = json.loads(rec.read_text(encoding="utf-8"))
    assert "dbt" not in doc, "stats.py must not write a sort key; fabric_run.py owns that now"
    assert "dbt" not in doc.get("layout", {}), "layout.dbt is invisible to the page"
    assert "sort_by" not in doc.get("layout", {}).get("config", {}).get("duckrun", {}), \
        "layout.config is walked by variant() — a commit-varying key would split the column"
    assert record  # imported for the merge under test


def _sample_parquet(tmp_path):
    """A file with one dictionary-encoded column and one that falls back to PLAIN, so the
    aggregation is exercised on both branches rather than on a uniform file."""
    import duckdb
    p = tmp_path / "p.parquet"
    duckdb.connect().execute(
        f"COPY (SELECT i::INT AS mw, (i%7)::VARCHAR AS duid FROM range(200000) t(i)) "
        f"TO '{p.as_posix()}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
    return p


def _chunks_over(stats, path):
    """`mart_chunks`' own output for a local parquet file — the (name->index, rows) every
    aggregation in stats.py now consumes."""
    import duckdb

    class C:
        def get_stats(self, table=None, detailed=False):
            return duckdb.connect().sql(f"SELECT * FROM parquet_metadata('{path.as_posix()}')")
    return stats.mart_chunks(C(), "mart.fct_summary")


def test_encodings_are_aggregated_per_column_not_per_chunk(stats, tmp_path):
    """The record has to stay small: `parquet_metadata` is one row per column per row group, and
    iceberg's 1,172 row groups would be six figures of rows. One row per COLUMN is the contract."""
    at, rows = _chunks_over(stats, _sample_parquet(tmp_path))
    got = stats.encodings_from(at, rows)
    assert set(got) == {"mw", "duid"}, "one entry per column"
    assert got["duid"]["encodings"] == ["PLAIN_DICTIONARY"]
    assert got["duid"]["dict_pages"] == got["duid"]["chunks"], "every chunk wrote a dictionary"
    # The discriminating case: a high-cardinality column the writer gave up dictionary-encoding on.
    # If this ever reads PLAIN_DICTIONARY the measurement has stopped telling engines apart.
    assert got["mw"]["encodings"] == ["PLAIN"]
    assert got["mw"]["dict_pages"] == 0
    assert got["mw"]["mb"] > got["duid"]["mb"], "and it is the one that costs bytes"
    assert isinstance(got["mw"]["encodings"], list), "sets do not survive json.dump"


def test_the_profiled_table_is_schema_qualified(stats, monkeypatch):
    """A BARE name does not resolve. `get_stats()` with no argument sweeps every catalog and keys by
    table name, but `get_stats('fct_summary')` raises — a one-part name is looked up in the CURRENT
    schema, and dbt writes the mart to `mart`. Run 31008858454 hit exactly this: the layout job went
    green and the record simply had no `encodings`. The schema comes from `stats_for`, so the
    profiled table cannot drift from the one the rest of the document describes."""
    seen = []
    monkeypatch.setattr(stats, "find_guid", lambda kind, item: "guid-1")
    monkeypatch.setattr(stats, "reader", lambda guid: object())
    monkeypatch.setattr(stats, "stats_for",
                        lambda con: {stats.MART: {"schema": "mart", "total_rows": 1}})
    monkeypatch.setattr(stats, "mart_chunks",
                        lambda con, table: seen.append(table) or ({"path_in_schema": 0}, [("mw",)]))
    monkeypatch.setattr(stats, "encodings_from", lambda at, rows: {"mw": {}})
    monkeypatch.setattr(stats, "ordering_for", lambda *a: {})
    guid, st, enc, order = stats.one_engine("dbt_spark", "lakehouses")
    assert seen == ["mart.fct_summary"], seen
    assert enc == {"mw": {}}


def test_a_mart_with_no_schema_is_skipped_rather_than_guessed(stats, monkeypatch):
    """No schema recorded means the aggregate read did not see the table at all. Guessing `mart`
    would send a name we have no evidence for and log a confusing resolution error."""
    monkeypatch.setattr(stats, "find_guid", lambda kind, item: "guid-1")
    monkeypatch.setattr(stats, "reader", lambda guid: object())
    monkeypatch.setattr(stats, "stats_for", lambda con: {})
    monkeypatch.setattr(stats, "mart_chunks",
                        lambda con, table: pytest.fail("must not be called"))
    assert stats.one_engine("dbt_spark", "lakehouses")[2] == {}


def test_a_failed_or_empty_profile_is_absent_never_empty(stats):
    """`{}` per column would read as "no encodings", which parquet cannot be. Absent means the
    layout job could not profile it — the same rule `landing` follows."""
    class Boom:
        def get_stats(self, table=None, detailed=False):
            raise RuntimeError("OneLake said no")
    assert stats.mart_chunks(Boom(), "mart.fct_summary") == (None, [])
    assert stats.encodings_from(None, []) == {}
    doc = stats.build_doc({}, ["duckrun"], {}, None, {"duckrun": {}})
    assert "encodings" not in doc, "nothing profiled -> no key at all"


def test_the_encodings_reach_the_document_under_their_engine(stats, tmp_path):
    at, rows = _chunks_over(stats, _sample_parquet(tmp_path))
    enc = {"duckrun": stats.encodings_from(at, rows), "spark": {}}
    doc = stats.build_doc({}, ["duckrun", "spark"], {}, None, enc)
    assert doc["encodings"]["duckrun"]["mw"]["encodings"] == ["PLAIN"]
    assert "spark" not in doc["encodings"], "an engine that profiled nothing adds no column"
    json.dumps(doc, default=str)      # the record is written with json.dump


@pytest.mark.parametrize("env,want", [(None, None), ("true", "true"), ("false", "false")])
def test_the_dwh_vorder_input_is_recorded_on_BOTH_values(stats, monkeypatch, env, want):
    """UNLIKE `sorted`, which is recorded only when it is ON. The asymmetry is not an oversight.

    `sorted` can rely on absence because duckrun always records `vcores`, so its signature is never
    empty. dwh carries NO other config key — so if only `false` were recorded, a default run's
    signature would be `[]` and `variantTag` renders that as the literal `unrecorded`. The page would
    read `dwh·unrecorded` beside `dwh·noVOrder`: it would claim not to know the thing it had just
    measured. The six records predating the input were backfilled to `"true"` to match.
    """
    if env is None:
        monkeypatch.delenv("DWH_VORDER", raising=False)
    else:
        monkeypatch.setenv("DWH_VORDER", env)
    doc = stats.build_doc({}, ["dwh"], {}, None, {})
    assert doc["config"]["dwh"] == {"vorder": want}


def test_iceberg_records_the_geometry_now_that_the_geometry_reaches_its_writer(stats, monkeypatch):
    """THIS IS A REVERSAL, and the comment it reverses is still in `stats.py` guarding the SORT.

    iceberg used to record `vcores` and nothing else, because the mart's `sort_by` /
    `max_row_group_size` / `target_file_size_mb` were duckrun keys that dbt-duckdb reads nowhere —
    so two iceberg runs at different dispatched geometries wrote byte-identical parquet, and a key
    would have split one dashboard column into two for no difference.

    `iceberg_geometry()` ends that for the GEOMETRY: it turns the same two dispatch inputs into
    Iceberg table properties (`write.parquet.row-group-size`, `write.target-file-size-bytes`) and
    `duckdb__create_table_as` puts them in the CTAS, so the parquet genuinely differs. Unrecorded,
    `layoutKey` would key both runs to one row and `groupMid` would print a median across two
    layouts.

    THE SORT IS NOT REVERSED and must stay off this entry: dbt-duckdb can express no sort, and
    duckdb-iceberg's `ALTER TABLE … SET SORTED BY` is not reachable from a model config.
    """
    monkeypatch.setenv("FABRIC_CORES", "8")
    monkeypatch.setenv("DUCKDB_SORTED", "true")
    monkeypatch.setenv("DUCKDB_ROW_GROUP_SIZE", "5000000")
    monkeypatch.setenv("DUCKDB_FILE_SIZE_MB", "128")
    cfg = stats.build_doc({}, ["iceberg"], {}, None, {})["config"]["iceberg"]
    assert cfg["row_group_size"] == "5000000"
    assert cfg["file_size_mb"] == "128"
    assert "sorted" not in cfg, "no sort reaches this writer — see the module comment"


def test_only_auto_keys_an_iceberg_run_to_the_history_column(stats, monkeypatch):
    """NO BASELINE ON THIS LEG, and `16000000` is the case that proves why. A bare dispatch sends
    `auto`, `iceberg_geometry()` returns an empty property dict, the CTAS is the adapter's own SQL
    and the parquet is what every earlier iceberg run wrote — so `auto` records as absence and joins
    that column. But this leg has NO pinned-geometry history to join, so duckrun's 16M/1024 MB
    baselines mean nothing here: a dispatch at either value emits a property no earlier run emitted,
    and folding it into a baseline records a layout the run did not write.

    Live example, run 33739194650: dispatched `file_size_mb=1024`, recorded `None`, and so sat in the
    same cell as an `auto` run while emitting `write.target-file-size-bytes` = 1024 MB where `auto`
    emits nothing and the writer defaults to 512 MB."""
    monkeypatch.setenv("FABRIC_CORES", "8")

    monkeypatch.setenv("DUCKDB_ROW_GROUP_SIZE", "auto")
    monkeypatch.setenv("DUCKDB_FILE_SIZE_MB", "auto")
    cfg = stats.build_doc({}, ["iceberg"], {}, None, {})["config"]["iceberg"]
    assert cfg["row_group_size"] is None
    assert cfg["file_size_mb"] is None

    # duckrun's baselines, dispatched explicitly. duckrun folds these to None; iceberg must not.
    monkeypatch.setenv("DUCKDB_ROW_GROUP_SIZE", "16000000")
    monkeypatch.setenv("DUCKDB_FILE_SIZE_MB", "1024")
    cfg = stats.build_doc({}, ["iceberg"], {}, None, {})["config"]["iceberg"]
    assert cfg["row_group_size"] == "16000000", "a pinned row count is a real iceberg layout"
    assert cfg["file_size_mb"] == "1024", "1024 emits a file target; auto emits none. Not the same."

    # AND THE CALL SITE, because the behaviour above is one word away from wrong: iceberg must use
    # `_iceberg_geometry` with NO baseline (only `auto` is absence) while duckrun keeps
    # `_nonbaseline` WITH its baselines (auto is the estimator, a layout of its own, and 16M/1024 is
    # the geometry `history/` was written at). Swapping either way passes every other test here.
    src = pathlib.Path(stats.__file__).read_text(encoding="utf-8")
    assert '_iceberg_geometry("DUCKDB_ROW_GROUP_SIZE")' in src
    assert '_iceberg_geometry("DUCKDB_FILE_SIZE_MB")' in src
    assert '_nonbaseline("DUCKDB_ROW_GROUP_SIZE", "16000000")' in src, "duckrun keeps its baseline"
    assert '_nonbaseline("DUCKDB_FILE_SIZE_MB", "1024")' in src, "duckrun keeps its baseline"


def test_the_declared_vorder_and_the_measured_one_are_separate_keys(stats, monkeypatch):
    """`layout.config` is DECLARED and splits the COLUMN; `layout.ordering.dwh.vorder_enabled` is
    MEASURED (`dwh_vorder.py` reads `sys.databases`) and splits the BAR. Two witnesses for one fact,
    on purpose: an `ALTER` that was accepted and did nothing shows up as the contradiction it is
    rather than being taken on trust. So `stats.py` must not write the measured key and must not
    infer the declared one from it."""
    monkeypatch.setenv("DWH_VORDER", "false")
    doc = stats.build_doc({}, ["dwh"], {}, None, {})
    assert "vorder_enabled" not in doc["config"]["dwh"]
    assert "vorder" not in doc.get("ordering", {}).get("dwh", {})


def test_an_unsorted_run_records_no_key_at_all(stats, tmp_path, monkeypatch):
    """Absence is what tells the page a run wrote unsorted parquet. A key here would caption an
    unsorted bar `by date, time`. Its sorted counterpart is
    `test_write_json_records_no_sort_key_of_its_own` — neither writes a `dbt` branch now, and the
    pair is kept because they reach it from opposite sides of the switch."""
    rec = tmp_path / "run.json"
    monkeypatch.setenv("RUN_RECORD", str(rec))
    monkeypatch.delenv("DUCKDB_SORTED", raising=False)
    stats.write_json({"stats": {"duckrun": {}}}, ["duckrun"])
    assert "dbt" not in json.loads(rec.read_text(encoding="utf-8"))
