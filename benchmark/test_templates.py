"""Guards on the semantic-model template, checked against duckrun's OWN regexes.

Every assertion here fails at *deploy* time otherwise — after the job has already installed
ADOMD.NET and resolved the workspace — or worse, deploys something that quietly points at the wrong
item or the wrong query mode. All of it is a JSON read; no Fabric, no network.

There is ONE template, deployed to every engine, and that is the experiment: identical DAX over
identical semantic models, with the dbt adapter that wrote the parquet as the only variable. A second
template, or a per-engine storage mode, would put a second variable in the comparison.

There were two, briefly — this one plus a hand-authored `fct_summary_dq.SemanticModel` — because
before duckrun 0.4.36 a warehouse could only be read by DirectQuery. They had to be kept in lockstep
or the one DAX suite silently stopped being comparable, and the dwh timings measured SQL-endpoint
pushdown rather than a layout. `deploy(mode=)` made the mode independent of the item kind, so the copy
is gone and every engine is Direct Lake.

The deleted file's sharpest trap, worth remembering if anyone ever hand-authors a DirectQuery bim
instead of passing `mode=`: `_is_directlake_bim()` greps the model.bim's RAW BYTES for the camelCase
Direct-Lake token, so a *description string* mentioning the mode was enough to flip it and make deploy
attempt a reframe the model could not serve. Prose counts. It caught that for real, once.
"""
import json
import os
import pathlib
import re
import sys

import pytest

from duckrun.workspace import _ONELAKE_REF, _is_directlake_bim, _normalize_mode

HERE = os.path.dirname(os.path.abspath(__file__))
DL = os.path.join(HERE, "fct_summary.SemanticModel", "model.bim")


def _raw(path):
    with open(path, "rb") as f:
        return f.read()


def _parts(path):
    """{table: (partition mode, schemaName, entityName)}"""
    m = json.loads(_raw(path))
    return {t["name"]: (t["partitions"][0]["mode"],
                        t["partitions"][0]["source"].get("schemaName"),
                        t["partitions"][0]["source"].get("entityName"))
            for t in m["model"]["tables"]}


# Every shared table each engine emits, and the schema it lands in. Same set `.github/scripts/stats.py`
# reports on — the two must not disagree about what "all the tables" means.
EXPECTED = {"stg_csv_archive_log": "landing",
            "dim_calendar": "mart",
            "dim_duid": "mart",
            "fct_price": "landing",
            "fct_scada": "landing",
            "fct_price_today": "landing",
            "fct_scada_today": "landing",
            "fct_summary": "mart"}


def test_template_carries_every_shared_table():
    assert set(_parts(DL)) == set(EXPECTED)


def test_template_table_set_matches_the_parity_dashboard():
    """The dataset registry's `tables` is the definition of "every shared table each engine emits".
    If a model is added or renamed there and not here, the benchmark quietly stops covering it.

    It reads `.github/scripts/datasets.py` by REGEX rather than importing it, deliberately: this
    directory is built to be deletable by removing one folder and one workflow file, and an import
    would end that. The regex is why the assertion skips rather than fails when the file is out of
    reach — this test guards a mismatch, not the layout of someone's checkout.

    (It used to read `TABLES = [...]` straight out of stats.py. That literal moved into the registry
    when the second dataset arrived, and this test was the thing that noticed.)"""
    reg = pathlib.Path(".github/scripts/datasets.py")
    if not reg.exists():             # running from outside the repo root
        pytest.skip("datasets.py not reachable from cwd")
    src = reg.read_text(encoding="utf-8")
    block = re.search(r'"aemo":\s*\{.*?"tables":\s*\[(.*?)\]', src, re.S)
    assert block, "could not find the aemo dataset's tables in datasets.py"
    assert set(re.findall(r'"([^"]+)"', block.group(1))) == set(_parts(DL))


# ------------------------------------------------------------------ Direct Lake template

def test_direct_lake_template_is_recognised_as_direct_lake():
    """It must be, or deploy() skips the post-deploy reframe and the model serves stale/no data."""
    assert _is_directlake_bim(_raw(DL))


def test_direct_lake_template_carries_a_repointable_onelake_reference():
    """`deploy(lakehouse=...)` RAISES when the bim has no OneLake reference to rewrite, so without
    this every lakehouse engine fails at deploy rather than pointing somewhere wrong."""
    assert _ONELAKE_REF.search(_raw(DL).decode("utf-8"))


def test_direct_lake_template_reads_the_real_tables_in_the_real_schemas():
    """The entity/schema pair is what Direct Lake resolves against OneLake, and it is the only place
    the split between `landing` and `mart` is written down on this side. Upstream's copy had
    dim_calendar under a 'sources' schema and the fact under 'tests' — neither exists here."""
    assert _parts(DL) == {t: ("direct" + "Lake", schema, t) for t, schema in EXPECTED.items()}


# ------------------------------------------------------------------ relationships

def test_relationships_point_at_tables_and_columns_that_exist():
    """A relationship naming a column that was dropped from the curated set deploys fine and then
    breaks every query that crosses it."""
    m = json.loads(_raw(DL))
    cols = {t["name"]: {c["name"] for c in t["columns"]} for t in m["model"]["tables"]}
    for r in m["model"]["relationships"]:
        assert r["fromColumn"] in cols[r["fromTable"]], f"{r['name']}: bad fromColumn"
        assert r["toColumn"] in cols[r["toTable"]], f"{r['name']}: bad toColumn"


def test_only_fct_summary_relies_on_referential_integrity():
    """`relyOnReferentialIntegrity` lets the engine use an INNER join, which SILENTLY DROPS rows whose
    key is missing from the dimension. fct_summary is built with an INNER JOIN to dim_duid so its RI
    holds by construction — the RAW facts carry retired units absent from the current AEMO
    registration list, which is exactly what stats.py's `duid_probe` exists to diagnose. Asserting RI
    there would make the benchmark quietly measure fewer rows on the very tables it is comparing."""
    m = json.loads(_raw(DL))
    ri = {r["name"] for r in m["model"]["relationships"]
          if r.get("relyOnReferentialIntegrity")}
    assert ri == {"fct_summary_to_dim_duid", "fct_summary_to_dim_calendar"}


# ------------------------------------------------------------------ the DAX suite resolves

def test_every_dax_reference_exists_in_the_model():
    """The suite is text until it reaches XMLA, so a mistyped `Table[Column]` or `[Measure]` is not
    caught by anything else until the benchmark is already running on paid capacity — and then it
    fails one query mid-flight, after the model has been deployed and warmed.

    Parses every `Table[Column]` and bare `[Measure]` out of xmla_compare.QUERIES and checks it
    against this template."""
    import xmla_compare as xc

    m = json.loads(_raw(DL))
    cols = {t["name"]: {c["name"] for c in t["columns"]} for t in m["model"]["tables"]}
    measures = {x["name"] for t in m["model"]["tables"] for x in t.get("measures", [])}

    for _tier, name, dax in xc.QUERIES:
        # Table[Column] — the table name is the run of identifier chars before the bracket.
        for tbl, col in re.findall(r"\b(\w+)\[([^\]]+)\]", dax):
            assert tbl in cols, f"{name}: unknown table {tbl!r}"
            assert col in cols[tbl], f"{name}: {tbl} has no column {col!r}"
        # A bare [Name] is either a model measure or an EXTENSION COLUMN the query defined itself:
        # SUMMARIZECOLUMNS(..., "MWh", [Total MWh]) introduces `[MWh]`, which TOPN then orders by.
        # Every such name arrives as a double-quoted literal in the same query, so collect those.
        local = set(re.findall(r'"([^"]+)"', dax))
        # ...and a QUOTED table is not a bare measure, hence the `'` added to the lookbehind.
        # A query-local DEFINE COLUMN/MEASURE introduces its own name, so that counts as local too —
        # the capture's rank visual defines `'__SQDS0VisualCalcs'[Rank]` and then reads `[Rank]`.
        local |= set(re.findall(r"DEFINE\s+(?:COLUMN|MEASURE)\s+'?\w+'?\[([^\]]+)\]", dax))
        for meas in re.findall(r"(?<![\w\]'])\[([^\]]+)\]", dax):
            assert meas in measures or meas in local, f"{name}: unknown measure [{meas}]"


# ------------------------------------------------------------------ storage mode is a premise

def test_the_deploy_mode_is_one_constant_per_phase_duckrun_accepts():
    """`engines.DEPLOY_MODE` / `DEPLOY_MODE_DQ` are single strings, never a per-engine dict:
    comparing engines requires that within a phase all four models be read the same way, so each
    phase's mode is its premise rather than a knob. Checked against duckrun's OWN normaliser — a
    typo raises inside duckrun partway through a paid run, after ADOMD.NET is installed and the
    first models are deployed."""
    import engines as E

    assert isinstance(E.DEPLOY_MODE, str), "DEPLOY_MODE must be one mode for every engine"
    assert _normalize_mode(E.DEPLOY_MODE) == "direct" + "Lake"
    assert isinstance(E.DEPLOY_MODE_DQ, str), "DEPLOY_MODE_DQ must be one mode for every engine"
    assert _normalize_mode(E.DEPLOY_MODE_DQ) == "direct" + "Query"
    assert not hasattr(E, "MODE"), "per-engine MODE is gone: the mode is not a variable under test"


def test_deploy_binds_every_engine_to_its_phase_lakehouse(monkeypatch):
    """The only per-engine deploy argument is WHICH shortcut lakehouse — always `lakehouse=`, dwh
    included, because both phases read through the phase's shortcut lakehouse rather than the output
    item (`provision.py bench_prepare` creates it; OneLake bills reads to the item hosting the
    shortcut, which is the attribution this exists for)."""
    import deploy_models as D

    monkeypatch.delenv("DATASET", raising=False)
    monkeypatch.delenv("BENCH_PHASE", raising=False)
    assert D.deploy_kwargs("duckrun", "dl") == {
        "lakehouse": "dbt_delta_dl", "mode": "direct_lake"}
    assert D.deploy_kwargs("dwh", "dl") == {
        "lakehouse": "dbt_dwh_dl", "mode": "direct_lake"}
    assert D.deploy_kwargs("duckrun", "dq") == {
        "lakehouse": "dbt_delta_dq", "mode": "direct_query"}
    assert D.deploy_kwargs("dwh", "dq") == {
        "lakehouse": "dbt_dwh_dq", "mode": "direct_query"}
    # And the phase defaults to the env, so the workflow's BENCH_PHASE reaches the deploy unchanged.
    monkeypatch.setenv("BENCH_PHASE", "dq")
    assert D.deploy_kwargs("spark") == {"lakehouse": "dbt_spark_dq", "mode": "direct_query"}


def test_model_names_and_phases_round_trip(monkeypatch):
    """`model_name` appends `_dq` only for the DQ phase, `is_dq` reads it back, and `engine_of`
    keeps the suffix — `aemo_spark_dq` labels its own column as `spark_dq` everywhere the render
    layer derives labels from model names."""
    import engines as E

    monkeypatch.delenv("DATASET", raising=False)
    monkeypatch.delenv("BENCH_PHASE", raising=False)
    assert E.model_name("spark", "dl") == "aemo_spark"
    assert E.model_name("spark", "dq") == "aemo_spark_dq"
    assert E.model_name("spark") == "aemo_spark", "no BENCH_PHASE means the Direct Lake phase"
    monkeypatch.setenv("BENCH_PHASE", "dq")
    assert E.model_name("spark") == "aemo_spark_dq"
    assert E.is_dq("aemo_spark_dq") and not E.is_dq("aemo_spark")
    assert E.engine_of("aemo_spark_dq") == "spark_dq"
    monkeypatch.setenv("BENCH_PHASE", "warm")
    with pytest.raises(SystemExit):
        E.phase()


def test_there_is_exactly_one_template_per_dataset():
    """One `.bim` per DATASET, and within a run one `.bim` for four engines.

    The invariant this guards has not moved: four adapters is the variable, the semantic model is
    not, and a second template for the SAME data reintroduces the lockstep problem that once made
    the one DAX suite silently non-comparable across engines (`fct_summary_dq`, the DirectQuery
    copy, is the case in point). Two templates over two different datasets are not that: they are
    never deployed in the same run, they share no query, and `deploy_models.TEMPLATES` maps exactly
    one to each dataset.

    So the assertion is that the set of templates is exactly the set of datasets — no orphan
    `.SemanticModel` folder, and no dataset whose template is missing. The second half matters most:
    a missing template is what would silently deploy the other dataset's model over this dataset's
    lakehouse, and `deploy_models.template()` refuses precisely because of it."""
    import deploy_models as D
    bims = sorted(pathlib.Path(HERE).glob("*.SemanticModel/model.bim"))
    assert sorted(b.parent.name for b in bims) == sorted(D.TEMPLATES.values())


# ------------------------------------------------------------------ the NYC taxi template
#
# The same guards, against the second dataset's `.bim`. They are spelled out rather than
# parameterised over both files on purpose: the two templates describe different stars, so the
# EXPECTED table/schema map and the relationship rules are genuinely different data, and a
# parameterised version would have to carry both maps anyway — with the loop hiding which one
# failed.

NYC = os.path.join(HERE, "fct_trips.SemanticModel", "model.bim")

NYC_EXPECTED = {"stg_parquet_archive_log": "landing",
                "dim_date": "mart",
                "dim_zone": "mart",
                "fct_trips": "mart"}


def test_nyc_template_carries_every_shared_table():
    assert set(_parts(NYC)) == set(NYC_EXPECTED)


def test_nyc_template_table_set_matches_the_dataset_registry():
    """Same guard as the AEMO one: if a model is added or renamed in the registry and not here, the
    benchmark quietly stops covering it."""
    reg = pathlib.Path(".github/scripts/datasets.py")
    if not reg.exists():
        pytest.skip("datasets.py not reachable from cwd")
    src = reg.read_text(encoding="utf-8")
    block = re.search(r'"nyc":\s*\{.*?"tables":\s*\[(.*?)\]', src, re.S)
    assert block, "could not find the nyc dataset's tables in datasets.py"
    assert set(re.findall(r'"([^"]+)"', block.group(1))) == set(_parts(NYC))


def test_nyc_template_is_direct_lake_and_repointable():
    """Direct Lake, or deploy() skips the reframe and the model serves nothing; and a OneLake
    reference to rewrite, or `deploy(lakehouse=...)` raises for every lakehouse engine."""
    assert _is_directlake_bim(_raw(NYC))
    assert _ONELAKE_REF.search(_raw(NYC).decode("utf-8"))


def test_nyc_template_reads_the_real_tables_in_the_real_schemas():
    assert _parts(NYC) == {t: ("direct" + "Lake", schema, t) for t, schema in NYC_EXPECTED.items()}


def test_nyc_relationships_point_at_columns_that_exist():
    m = json.loads(_raw(NYC))
    cols = {t["name"]: {c["name"] for c in t["columns"]} for t in m["model"]["tables"]}
    for r in m["model"]["relationships"]:
        assert r["fromColumn"] in cols[r["fromTable"]], f"{r['name']}: bad fromColumn"
        assert r["toColumn"] in cols[r["toTable"]], f"{r['name']}: bad toColumn"


def test_only_the_nyc_mart_relies_on_referential_integrity():
    """`relyOnReferentialIntegrity` permits an INNER join, which silently drops rows whose key is
    missing from the dimension. Same rule as the AEMO template: only the MART's relationships may
    set it. Here every relationship happens to be the mart's, so the assertion is that nothing
    else has crept in — a relationship from a dimension or the archive log would be a modelling
    mistake before it was an RI one."""
    m = json.loads(_raw(NYC))
    for r in m["model"]["relationships"]:
        if r.get("relyOnReferentialIntegrity"):
            assert r["fromTable"] == "fct_trips", f"{r['name']} is not the mart's"


def test_the_nyc_dax_suite_only_names_things_the_template_has():
    """Every table, column and measure the NYC suite references must exist in the NYC template.

    This is the check that would otherwise fail at QUERY time, one query at a time, in the most
    expensive job in the workflow — and a suite where every query errors still produces a report
    shaped like a result. `probe_rowcount` last among the probes is pinned in test_verdicts.py.

    IT READS `NYC_QUERIES` DIRECTLY AND SETS NOTHING. `xmla_compare.QUERIES` and `stats.MART` bind
    the dataset at IMPORT time, so a test that sets DATASET and re-imports leaks into whatever the
    collector loads next — which is exactly what an earlier version of this test did: it turned the
    AEMO copy of this assertion and two of test_sort_key's green locally and red on CI, purely on
    collection order. The per-dataset query lists are plain module constants, so neither the env nor
    sys.modules has to be touched to read one."""
    import xmla_compare as xc

    m = json.loads(_raw(NYC))
    cols = {t["name"]: {c["name"] for c in t["columns"]} for t in m["model"]["tables"]}
    measures = {x["name"] for t in m["model"]["tables"] for x in t.get("measures", [])}

    for _tier, name, dax in xc.NYC_QUERIES:
        for tbl, col in re.findall(r"(\w+)\[([^\]]+)\]", dax):
            assert tbl in cols, f"{name}: unknown table {tbl!r}"
            assert col in cols[tbl], f"{name}: {tbl} has no column {col!r}"
        # A bare [Name] is either a model measure or an EXTENSION COLUMN the query defined itself —
        # SUMMARIZECOLUMNS(..., "Fare", [Total Fare]) introduces `[Fare]`, which TOPN then orders by.
        local = set(re.findall(r'"([^"]+)"', dax))
        # ...and a QUOTED table is not a bare measure, hence the `'` added to the lookbehind.
        # A query-local DEFINE COLUMN/MEASURE introduces its own name, so that counts as local too —
        # the capture's rank visual defines `'__SQDS0VisualCalcs'[Rank]` and then reads `[Rank]`.
        local |= set(re.findall(r"DEFINE\s+(?:COLUMN|MEASURE)\s+'?\w+'?\[([^\]]+)\]", dax))
        for meas in re.findall(r"(?<![\w\]'])\[([^\]]+)\]", dax):
            assert meas in measures or meas in local, f"{name}: unknown measure [{meas}]"


# ------------------------------------------------------------------ the BTS flights template
#
# The same guards, against the third dataset's `.bim`. Spelled out rather than parameterised, for
# the same reason the NYC block is — different stars are genuinely different data, and a loop would
# hide which one failed.

BTS = os.path.join(HERE, "fct_flights.SemanticModel", "model.bim")

BTS_EXPECTED = {"stg_flights_archive_log": "landing",
                "dim_flight_date": "mart",
                "dim_carrier": "mart",
                "fct_flights": "mart"}


def test_bts_template_carries_every_shared_table():
    assert set(_parts(BTS)) == set(BTS_EXPECTED)


def test_bts_template_table_set_matches_the_dataset_registry():
    """Same guard as the other two: if a model is added or renamed in the registry and not here,
    the benchmark quietly stops covering it."""
    reg = pathlib.Path(".github/scripts/datasets.py")
    if not reg.exists():
        pytest.skip("datasets.py not reachable from cwd")
    src = reg.read_text(encoding="utf-8")
    block = re.search(r'"bts":\s*\{.*?"tables":\s*\[(.*?)\]', src, re.S)
    assert block, "could not find the bts dataset's tables in datasets.py"
    assert set(re.findall(r'"([^"]+)"', block.group(1))) == set(_parts(BTS))


def test_bts_template_is_direct_lake_and_repointable():
    assert _is_directlake_bim(_raw(BTS))
    assert _ONELAKE_REF.search(_raw(BTS).decode("utf-8"))


def test_bts_template_reads_the_real_tables_in_the_real_schemas():
    assert _parts(BTS) == {t: ("direct" + "Lake", schema, t) for t, schema in BTS_EXPECTED.items()}


def test_bts_relationships_point_at_columns_that_exist():
    m = json.loads(_raw(BTS))
    cols = {t["name"]: {c["name"] for c in t["columns"]} for t in m["model"]["tables"]}
    for r in m["model"]["relationships"]:
        assert r["fromColumn"] in cols[r["fromTable"]], f"{r['name']}: bad fromColumn"
        assert r["toColumn"] in cols[r["toTable"]], f"{r['name']}: bad toColumn"


def test_only_the_bts_mart_relies_on_referential_integrity():
    """Same rule as the other two templates: only the MART's relationships may set RI. Here the
    fact's Reporting_Airline joins BTS's own unique-carrier list and FlightDate a dimension
    spanning the whole archive, so RI holds by construction — but a relationship from a dimension
    or the archive log would be a modelling mistake before it was an RI one."""
    m = json.loads(_raw(BTS))
    for r in m["model"]["relationships"]:
        if r.get("relyOnReferentialIntegrity"):
            assert r["fromTable"] == "fct_flights", f"{r['name']} is not the mart's"


def test_every_bts_dax_string_resolves_against_the_bts_template():
    """Queries, readiness probe and ladder resolver — reads SUITES directly and sets nothing, for
    the import-time-binding reason documented on the aemo/nyc copies."""
    import xmla_compare as xc
    _assert_dax_resolves(BTS, xc.SUITES["bts"])


def test_the_aemo_suite_is_what_a_default_import_binds():
    """`xmla_compare.QUERIES` is the suite the process will actually run, bound once at import from
    DATASET. Nothing in this suite sets that variable, so a default import must bind AEMO — and if
    some future test starts leaking it, this is the assertion that says so instead of three
    unrelated files going red on collection order."""
    import xmla_compare as xc

    assert xc.QUERIES is xc.AEMO_QUERIES
    assert os.environ.get("DATASET") in (None, "", "aemo")


def _dax_strings(suite):
    """EVERY DAX string a run will issue for one dataset — the query suite PLUS the readiness probe
    and the ladder resolver.

    The last two are the reason this helper exists. They live outside `queries`, so a test that
    walked only the query list passed while `warm_up` probed `dim_calendar` against a model whose
    date dimension is `dim_date` — the whole benchmark job spent its sixteen readiness retries and
    ran no query at all. Nothing about that is visible in the query list."""
    return ([(name, dax) for _tier, name, dax in suite["queries"]]
            + [(k, suite[k]) for k in ("resolve", "ready")])


def _assert_dax_resolves(bim_path, suite):
    m = json.loads(_raw(bim_path))
    cols = {t["name"]: {c["name"] for c in t["columns"]} for t in m["model"]["tables"]}
    measures = {x["name"] for t in m["model"]["tables"] for x in t.get("measures", [])}
    # A COLUMN REFERENCE COMES IN TWO SPELLINGS AND BOTH ARE CHECKED. The hand-written suites use
    # the bare `fct_summary[mw]`; a Performance Analyzer capture always QUOTES the table --
    # `'dim_duid'[DUID]`. The bare pattern alone did not merely miss those, it MISFILED them: the
    # measure check below reads any `[Name]` not preceded by a word character, and a quote is not
    # one, so every quoted column arrived there and was asserted to be a measure.
    # `__SQDS0VisualCalcs` is skipped: a visual calculation's table is DEFINEd by the query
    # itself, so it is correct for the model not to carry it.
    LOCAL_TABLES = {"__SQDS0VisualCalcs"}
    for name, dax in _dax_strings(suite):
        refs = (re.findall(r"'([^']+)'\[([^\]]+)\]", dax)          # 'Quoted Table'[col]
                + re.findall(r"(?<!')\b(\w+)\[([^\]]+)\]", dax))   # bare_table[col]
        for tbl, col in refs:
            if tbl in LOCAL_TABLES:
                continue
            assert tbl in cols, f"{name}: unknown table {tbl!r}"
            assert col in cols[tbl], f"{name}: {tbl} has no column {col!r}"
        # A BARE table argument — `COUNTROWS(dim_calendar)`, `SUMX(fct_trips, ...)` — carries no
        # brackets, so the pattern above cannot see it. That is not a gap in theory: the readiness
        # probe is exactly this shape, and it named dim_calendar against a model whose date
        # dimension is dim_date, which cost a whole benchmark job. The functions listed are the ones
        # these suites actually use; a new one taking a TABLE argument belongs here.
        for tbl in re.findall(
                r"\b(?:COUNTROWS|SUMX|AVERAGEX|COUNTX|MAXX|MINX|ALL|ALLSELECTED"
                r"|FILTER|DISTINCT)\s*\(\s*([A-Za-z_]\w*)\s*[,)]", dax):
            assert tbl in cols, f"{name}: unknown table {tbl!r}"
        # A bare [Name] is either a model measure or an EXTENSION COLUMN the query defined itself:
        # SUMMARIZECOLUMNS(..., "MWh", [Total MWh]) introduces `[MWh]`, which TOPN then orders by.
        local = set(re.findall(r'"([^"]+)"', dax))
        # ...and a QUOTED table is not a bare measure, hence the `'` added to the lookbehind.
        # A query-local DEFINE COLUMN/MEASURE introduces its own name, so that counts as local too —
        # the capture's rank visual defines `'__SQDS0VisualCalcs'[Rank]` and then reads `[Rank]`.
        local |= set(re.findall(r"DEFINE\s+(?:COLUMN|MEASURE)\s+'?\w+'?\[([^\]]+)\]", dax))
        for meas in re.findall(r"(?<![\w\]'])\[([^\]]+)\]", dax):
            assert meas in measures or meas in local, f"{name}: unknown measure [{meas}]"


def test_every_aemo_dax_string_resolves_against_the_aemo_template():
    """Queries, readiness probe and ladder resolver — all of them, against the AEMO model."""
    import xmla_compare as xc
    _assert_dax_resolves(DL, xc.SUITES["aemo"])


def test_every_nyc_dax_string_resolves_against_the_nyc_template():
    """The same, against the NYC model. Reads SUITES directly and sets nothing — see the note on
    import-time binding in test_the_aemo_suite_is_what_a_default_import_binds."""
    import xmla_compare as xc
    _assert_dax_resolves(NYC, xc.SUITES["nyc"])


def test_no_dax_hides_outside_the_suite_dict():
    """A DAX literal in a function body is invisible to the two tests above.

    That is not hypothetical — it is exactly what happened: `warm_up` carried
    `COUNTROWS(dim_calendar)` inline, the NYC model has `dim_date`, and the benchmark job failed
    after sixteen readiness retries having issued no measured query. So the rule is that every DAX
    string lives in SUITES, and this is what enforces it."""
    import xmla_compare as xc
    src = pathlib.Path(xc.__file__).read_text(encoding="utf-8")
    # Everything after the SUITES dict is function bodies. `EVALUATE` there means a DAX literal
    # that no template test can reach.
    body = src[src.index("DAX_KEYS = "):]
    stray = [ln.strip() for ln in body.splitlines()
             if "EVALUATE" in ln and not ln.strip().startswith("#")]
    assert not stray, f"DAX outside SUITES, unreachable by the template tests: {stray}"


# ------------------------------------------------------------------ the green taxi template
#
# The same guards, against the fourth dataset's `.bim`. Spelled out rather than parameterised, for
# the same reason the NYC and BTS blocks are — different stars are genuinely different data, and a
# loop would hide which one failed.

GREEN = os.path.join(HERE, "fct_green_trips.SemanticModel", "model.bim")

GREEN_EXPECTED = {"stg_green_archive_log": "landing",
                  "dim_green_date": "mart",
                  "dim_green_zone": "mart",
                  "fct_green_trips": "mart"}


def test_green_template_carries_every_shared_table():
    assert set(_parts(GREEN)) == set(GREEN_EXPECTED)


def test_green_template_table_set_matches_the_dataset_registry():
    """Same guard as the other three: if a model is added or renamed in the registry and not here,
    the benchmark quietly stops covering it."""
    reg = pathlib.Path(".github/scripts/datasets.py")
    if not reg.exists():
        pytest.skip("datasets.py not reachable from cwd")
    src = reg.read_text(encoding="utf-8")
    block = re.search(r'"green":\s*\{.*?"tables":\s*\[(.*?)\]', src, re.S)
    assert block, "could not find the green dataset's tables in datasets.py"
    assert set(re.findall(r'"([^"]+)"', block.group(1))) == set(_parts(GREEN))


def test_green_template_is_direct_lake_and_repointable():
    assert _is_directlake_bim(_raw(GREEN))
    assert _ONELAKE_REF.search(_raw(GREEN).decode("utf-8"))


def test_green_template_reads_the_real_tables_in_the_real_schemas():
    assert _parts(GREEN) == {t: ("direct" + "Lake", schema, t)
                             for t, schema in GREEN_EXPECTED.items()}


def test_green_relationships_point_at_columns_that_exist():
    m = json.loads(_raw(GREEN))
    cols = {t["name"]: {c["name"] for c in t["columns"]} for t in m["model"]["tables"]}
    for r in m["model"]["relationships"]:
        assert r["fromColumn"] in cols[r["fromTable"]], f"{r['name']}: bad fromColumn"
        assert r["toColumn"] in cols[r["toTable"]], f"{r['name']}: bad toColumn"


def test_only_the_green_mart_relies_on_referential_integrity():
    """Same rule as the other three templates: only the MART's relationships may set RI. A
    relationship from a dimension or the archive log would be a modelling mistake before it was
    an RI one."""
    m = json.loads(_raw(GREEN))
    for r in m["model"]["relationships"]:
        if r.get("relyOnReferentialIntegrity"):
            assert r["fromTable"] == "fct_green_trips", f"{r['name']} is not the mart's"


def test_every_green_dax_string_resolves_against_the_green_template():
    """Queries, readiness probe and ladder resolver — reads SUITES directly and sets nothing, for
    the import-time-binding reason documented on the aemo/nyc copies."""
    import xmla_compare as xc
    _assert_dax_resolves(GREEN, xc.SUITES["green"])


# ------------------------------------------------------------------ the CMS Open Payments template
#
# The same guards, against the fifth dataset's `.bim`. Spelled out rather than parameterised, for
# the same reason the other blocks are — different stars are genuinely different data, and a loop
# would hide which one failed.

CMS = os.path.join(HERE, "fct_cms_payments.SemanticModel", "model.bim")

CMS_EXPECTED = {"stg_cms_archive_log": "landing",
                "dim_cms_date": "mart",
                "dim_cms_payer": "mart",
                "fct_cms_payments": "mart"}


def test_cms_template_carries_every_shared_table():
    assert set(_parts(CMS)) == set(CMS_EXPECTED)


def test_cms_template_table_set_matches_the_dataset_registry():
    """Same guard as the other four: if a model is added or renamed in the registry and not here,
    the benchmark quietly stops covering it."""
    reg = pathlib.Path(".github/scripts/datasets.py")
    if not reg.exists():
        pytest.skip("datasets.py not reachable from cwd")
    src = reg.read_text(encoding="utf-8")
    block = re.search(r'"cms":\s*\{.*?"tables":\s*\[(.*?)\]', src, re.S)
    assert block, "could not find the cms dataset's tables in datasets.py"
    assert set(re.findall(r'"([^"]+)"', block.group(1))) == set(_parts(CMS))


def test_cms_template_is_direct_lake_and_repointable():
    assert _is_directlake_bim(_raw(CMS))
    assert _ONELAKE_REF.search(_raw(CMS).decode("utf-8"))


def test_cms_template_reads_the_real_tables_in_the_real_schemas():
    assert _parts(CMS) == {t: ("direct" + "Lake", schema, t)
                           for t, schema in CMS_EXPECTED.items()}


def test_cms_relationships_point_at_columns_that_exist():
    m = json.loads(_raw(CMS))
    cols = {t["name"]: {c["name"] for c in t["columns"]} for t in m["model"]["tables"]}
    for r in m["model"]["relationships"]:
        assert r["fromColumn"] in cols[r["fromTable"]], f"{r['name']}: bad fromColumn"
        assert r["toColumn"] in cols[r["toTable"]], f"{r['name']}: bad toColumn"


def test_only_the_cms_mart_relies_on_referential_integrity():
    """Same rule as the other four templates: only the MART's relationships may set RI."""
    m = json.loads(_raw(CMS))
    for r in m["model"]["relationships"]:
        if r.get("relyOnReferentialIntegrity"):
            assert r["fromTable"] == "fct_cms_payments", f"{r['name']} is not the mart's"


def test_every_cms_dax_string_resolves_against_the_cms_template():
    """Queries, readiness probe and ladder resolver — reads SUITES directly and sets nothing, for
    the import-time-binding reason documented on the aemo/nyc copies."""
    import xmla_compare as xc
    _assert_dax_resolves(CMS, xc.SUITES["cms"])


def test_the_cms_fact_carries_every_source_column():
    """The .bim must expose all 91 source columns plus `file`, not a readable subset.

    THIS IS THE ONE GUARD THAT IS SPECIFIC TO THIS DATASET, and it exists because the semantic
    model is where a wide table is easiest to quietly narrow: dropping the sparse tail members from
    the .bim would leave dbt building a 91-column table that Power BI never transcodes past the
    first product slot, so the layout numbers would still look wide while the QUERY numbers
    measured something else entirely. Direct Lake transcodes per column on demand, so an undeclared
    column costs nothing and shows nothing — which is exactly what makes the omission invisible."""
    m = json.loads(_raw(CMS))
    fact = next(t for t in m["model"]["tables"] if t["name"] == "fct_cms_payments")
    declared = [c["sourceColumn"] for c in fact["columns"]]
    reg = pathlib.Path(".github/scripts/datasets.py")
    if not reg.exists():
        pytest.skip("datasets.py not reachable from cwd")
    src = reg.read_text(encoding="utf-8")
    block = re.search(r'"cms":\s*\{.*?"mart_columns":\s*\[(.*?)\],\s*"landing_tables"', src, re.S)
    assert block, "could not find the cms dataset's mart_columns in datasets.py"
    assert declared == re.findall(r'"([^"]+)"', block.group(1))
    assert len(declared) == 92, f"91 source columns plus `file`, got {len(declared)}"


def test_the_cms_sparse_pair_is_both_declared_and_both_probed():
    """probe_product_head and probe_product_tail are the only matched sparse pair in the project.

    They are the same column family, the same type and the same DAX over a ~7%-NULL column and a
    ~99%-NULL one. The pair does NOT isolate sparsity — NULL rate and cardinality move together by
    construction, since the non-NULL rows are all there is to be distinct over — so the gap it
    measures is the two combined. What makes it worth pinning is that everything else IS held
    constant, which no other pair in this project manages at all.

    Dropping either one leaves a probe whose number means nothing on its own, so both the columns
    and both the queries are asserted together."""
    m = json.loads(_raw(CMS))
    fact = next(t for t in m["model"]["tables"] if t["name"] == "fct_cms_payments")
    cols = {c["sourceColumn"] for c in fact["columns"]}
    for i in (1, 5):
        assert f"Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_{i}" in cols
    import xmla_compare as xc
    named = {name for _tier, name, _dax in xc.SUITES["cms"]["queries"]}
    assert {"probe_product_head", "probe_product_tail"} <= named
