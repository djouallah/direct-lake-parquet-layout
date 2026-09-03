"""The iceberg write geometry: the macro that builds the properties, and the one that emits them.

WHY THIS IS WORTH A TEST AT ALL. Every failure here is SILENT. `iceberg_geometry()` returning `{}`
when it should not, or `duckdb__create_table_as` dropping the `WITH` clause, produces a leg that
runs green, writes parquet at the writer's default, and records a dispatched geometry in
`layout.config.iceberg` that the files do not have — the record and the page would then both state
a layout nobody wrote. Nothing downstream can tell that apart from a run that worked.

It renders the macro FILES with plain Jinja rather than going through dbt: `dbt parse` needs a
profile and thirty seconds, this needs neither and runs in the free `checks` job. What it cannot
cover is dbt's own dispatch — that a `duckdb__`-prefixed macro reaches the iceberg target and not
duckrun — which is a property of the adapter, not of this text, and is asserted by
`check_gating.py` parsing both targets in the same job.
"""
import json
import pathlib

import jinja2
import pytest

MACROS = pathlib.Path(__file__).resolve().parents[2] / "macros"


class _Return(Exception):
    """dbt's `return()` unwinds the macro; plain Jinja has no such thing, so it is stubbed."""

    def __init__(self, value):
        self.value = value


class _Relation:
    def include(self, **_kw):
        return "onelake.mart.fct_summary"

    def __str__(self):
        return "onelake.mart.fct_summary"


class _Config:
    def __init__(self, props):
        self.props = props

    def get(self, key, default=None):
        if key == "iceberg_properties":
            return self.props
        if key == "contract":
            return type("C", (), {"enforced": False})()
        return default


def _env():
    # `do` is a dbt-enabled extension; `iceberg_geometry` builds its dict with `{% do %}`.
    return jinja2.Environment(extensions=["jinja2.ext.do"])


def _source():
    return ((MACROS / "iceberg_geometry.sql").read_text(encoding="utf-8") + "\n"
            + (MACROS / "iceberg_adapter_overrides.sql").read_text(encoding="utf-8"))


def geometry(row_group_size, file_size_mb):
    """`iceberg_geometry()`'s return value for one pair of env values."""
    seen = {"DUCKDB_ROW_GROUP_SIZE": row_group_size, "DUCKDB_FILE_SIZE_MB": file_size_mb}
    tpl = _env().from_string(_source() + "\n{{ iceberg_geometry() }}")
    try:
        tpl.render(env_var=lambda name, default=None: seen.get(name, default),
                   **{"return": lambda v: (_ for _ in ()).throw(_Return(v))})
    except _Return as ret:
        return ret.value
    raise AssertionError("iceberg_geometry() returned nothing")


def create_sql(props, temporary=False):
    """`duckdb__create_table_as`'s output, whitespace collapsed."""
    tpl = _env().from_string(
        _source() + "\n{{ duckdb__create_table_as(temporary, relation, compiled_code) }}")
    return " ".join(tpl.render(
        config=_Config(props), relation=_Relation(), compiled_code="select 1",
        temporary=temporary, env_var=lambda n, d=None: d,
        get_assert_columns_equivalent=lambda c: "",
        get_table_columns_and_constraints=lambda: "", get_column_names=lambda: "",
        get_select_subquery=lambda c: c, py_write_table=lambda **k: "").split())


# ------------------------------------------------------------------ iceberg_geometry()

@pytest.mark.parametrize("rg,mb", [("auto", "auto"), ("AUTO", "Auto"), ("auto", "AUTO")])
def test_auto_emits_no_property_at_all(rg, mb):
    """A DEFAULT DISPATCH MUST BE BYTE-IDENTICAL TO BEFORE THIS EXISTED, which is what lets every
    iceberg run in `history/` stay in one dashboard column with the ones after it. `auto` is what
    `duckrun_auto` (on by default, and on for every scheduled run) forces, so this is the common
    case, not an edge. Case-insensitive because the models' own `.lower()` gate is."""
    assert geometry(rg, mb) == {}


def test_the_row_group_size_is_passed_as_rows_with_no_conversion():
    """THE WHOLE REASON ONE DISPATCH INPUT CAN DRIVE BOTH WRITERS. duckrun's `max_row_group_size`
    counts rows; DuckDB's `write.parquet.row-group-size` counts rows too — it is duckdb-iceberg's
    own extension, NOT the Iceberg spec's `write.parquet.row-group-size-bytes`, which is a byte
    budget. Multiplying or renaming this to the `-bytes` spelling would hand the writer a 5-million
    BYTE row group and quietly produce ~1,000x too many of them."""
    assert geometry("5000000", "auto")["write.parquet.row-group-size"] == 5000000


def test_a_pinned_row_count_also_raises_the_byte_budget():
    """WITHOUT THIS THE ROW COUNT IS INERT, and run 33733500776 is the proof: nyc dispatched at
    `row_group_size=5000000` wrote 729 row groups at 811,700 rows, against the baseline's 728 at
    812,815 — no change whatever. DuckDB flushes on WHICHEVER threshold is reached first and
    duckdb-iceberg defaults the byte one to 128 MB, which over nyc's 20 columns is ~812K rows and
    over aemo's 5 narrow ones ~2.7M. Both datasets had always been byte-bound at the default.

    Measured locally on 3M rows of four columns, same data both times:
        ROW_GROUP_SIZE 5000000                             -> 1 row group,  3,000,000 rows
        ROW_GROUP_SIZE 5000000, ROW_GROUP_SIZE_BYTES 128MB -> 2 row groups, max 2,004,992 rows
    """
    assert geometry("5000000", "auto") == {"write.parquet.row-group-size": 5000000,
                                           "write.parquet.row-group-size-bytes": 512 * 1048576}


def test_the_byte_budget_follows_the_dispatched_file_size():
    """The budget is the FILE target, because duckdb-iceberg caps a row group's bytes at the file
    size anyway — a file cannot hold less than one row group — so that is the largest value with
    any meaning, and it leaves the ROW COUNT as the binding threshold, which is what the dispatch
    input promises. 512 MiB when the file size is `auto`, that being the writer's own default."""
    pinned = geometry("5000000", "128")
    assert pinned["write.parquet.row-group-size-bytes"] == 128 * 1048576
    assert pinned["write.target-file-size-bytes"] == 128 * 1048576
    assert geometry("5000000", "auto")["write.parquet.row-group-size-bytes"] == 512 * 1048576


def test_a_file_size_alone_does_not_touch_the_row_group_budget():
    """`file_size_mb` on its own is a file-rotation setting and nothing else. Raising the row-group
    byte budget there would silently change row-group sizing on a dispatch that only asked about
    files — and it is the ROW COUNT this budget exists to make binding."""
    assert geometry("auto", "1024") == {"write.target-file-size-bytes": 1024 * 1048576}


def test_the_file_size_is_converted_from_MiB_to_bytes():
    """`file_size_mb` is megabytes and `write.target-file-size-bytes` is bytes. Passing the number
    through unconverted asks for 128-BYTE files."""
    assert geometry("auto", "128") == {"write.target-file-size-bytes": 128 * 1048576}
    assert geometry("auto", "1024") == {"write.target-file-size-bytes": 1073741824}


def test_the_two_knobs_are_independent():
    """`auto` on one and a value on the other is a legal dispatch — `row_group_size` is free text
    while `file_size_mb` is a choice — so neither may gate the other. Note the row count brings its
    own byte budget along (see above); the file size does not."""
    assert set(geometry("2000000", "auto")) == {"write.parquet.row-group-size",
                                               "write.parquet.row-group-size-bytes"}
    assert set(geometry("auto", "64")) == {"write.target-file-size-bytes"}
    assert set(geometry("2000000", "64")) == {"write.parquet.row-group-size",
                                              "write.parquet.row-group-size-bytes",
                                              "write.target-file-size-bytes"}


def test_the_values_are_integers_not_human_units():
    """duckdb-iceberg's `ParseByteSizeOptionallyFormatted` accepts `'128MB'` and then writes THAT
    STRING into the table metadata (duckdb-iceberg#1150), where another engine reading the table may
    not parse it. The property is metadata other readers see, so it is spelled in plain bytes."""
    for value in geometry("5000000", "128").values():
        assert isinstance(value, int), value
    assert "MB" not in json.dumps(geometry("5000000", "128"))


# ------------------------------------------------------------------ duckdb__create_table_as

def test_no_properties_means_the_adapters_own_sql_byte_for_byte():
    """The override is a COPY of dbt-duckdb's macro with one insertion. With nothing to insert the
    emitted SQL must be what the adapter would have emitted — otherwise every default iceberg run
    is running modified DDL for no reason, and a difference would surface as a mystery much later."""
    assert create_sql({}) == "create table onelake.mart.fct_summary as ( select 1 );"


def test_the_clause_lands_between_the_relation_and_the_select():
    """`CREATE TABLE <rel> WITH (…) AS SELECT …` is the only position DuckDB parses. After the
    `AS` it is a syntax error; the parser is unambiguous about this and a misplaced clause fails the
    whole leg rather than being ignored."""
    sql = create_sql({"write.parquet.row-group-size": 5000000})
    assert sql == ("create table onelake.mart.fct_summary "
                   "with ('write.parquet.row-group-size' = '5000000') as ( select 1 );")


def test_several_properties_are_emitted_in_a_stable_order():
    """Sorted, so two runs of one dispatch emit identical DDL. A dict's iteration order is not a
    layout, and DDL that changes between runs of the same config is a diff nobody can explain."""
    props = {"write.target-file-size-bytes": 134217728, "write.parquet.row-group-size": 5000000}
    assert create_sql(props) == (
        "create table onelake.mart.fct_summary "
        "with ('write.parquet.row-group-size' = '5000000', "
        "'write.target-file-size-bytes' = '134217728') as ( select 1 );")


def test_a_temporary_relation_never_carries_the_clause():
    """THE ONE THAT BREAKS EVERY INCREMENTAL RUN IF IT REGRESSES. dbt-duckdb's incremental
    materialization stages its source in a real `CREATE TEMPORARY TABLE` (`temporary` is
    `not is_motherduck()`, so always true here), and a temp table lives in the DUCKDB catalog, which
    refuses the clause outright: `Catalog Error: WITH clause is not supported for tables in a duckdb
    catalog`. The properties belong to the iceberg table, not to the staging copy."""
    sql = create_sql({"write.parquet.row-group-size": 5000000}, temporary=True)
    assert "with (" not in sql, sql
    assert sql.startswith("create temporary table")


# The lines the override copies verbatim. Each is a branch the adapter's own macro has and this one
# must keep: the SIGNATURE (1.11.0 added `partitioned_by`/`sorted_by` and `incremental.sql` passes
# them BY KEYWORD, so an older signature fails the leg outright), the contract check, the sql_header,
# the constraints path, both DuckLake alter calls, and the python path with its new arguments.
COPIED = ("{% macro duckdb__create_table_as(temporary, relation, compiled_code, language='sql', "
          "partitioned_by=none, sorted_by=none) -%}",
          "{% set contract_config = config.get('contract') %}",
          "{%- set sql_header = config.get('sql_header', none) -%}",
          "{{ get_table_columns_and_constraints() }} ;",
          "{{ duckdb__alter_table_set_partitioned_by(relation, partitioned_by) }}",
          "{{ duckdb__alter_table_set_sorted_by(relation, sorted_by) }}",
          "{{ py_write_table(temporary=temporary, relation=relation, compiled_code=compiled_code, "
          "partitioned_by=partitioned_by, sorted_by=sorted_by) }}")


def _upstream_macro_body():
    """dbt-duckdb's own `duckdb__create_table_as`, found by SEARCHING its macro tree.

    ⚠️ NOT BY FILENAME. It lives in `macros/adapters.sql` on the version installed here, and the
    first draft of this test hardcoded that path — which passed locally and failed in CI on a
    different resolved version, where that file holds only the schema macros. The macro's home is
    not part of the contract; its body is.
    """
    pkg = pytest.importorskip("dbt.include.duckdb")
    root = pathlib.Path(pkg.__file__).parent
    for path in sorted(root.rglob("*.sql")):
        text = path.read_text(encoding="utf-8")
        if "macro duckdb__create_table_as(" in text:
            return text
    pytest.skip("dbt-duckdb installed but duckdb__create_table_as not found in its macros")


def test_the_override_still_matches_the_adapter_it_was_copied_from():
    """A BUMP OF dbt-duckdb CAN SILENTLY DIVERGE THIS. The override is a copy of the adapter's
    `duckdb__create_table_as` plus one line, so a change upstream — a new config key, a different
    contract branch — is inherited by every other model and NOT by this one. Skipped when the
    adapter is not installed, so the suite still runs offline."""
    # Whitespace-collapsed on both sides, so a REFLOW upstream — the same call wrapped across two
    # lines — does not read as a semantic change and block every build over a line break.
    import importlib.metadata as md
    try:
        installed = md.version("dbt-duckdb")
    except md.PackageNotFoundError:                        # pragma: no cover
        installed = "?"
    flat = lambda t: " ".join(t.split())
    body, ours = flat(_upstream_macro_body()), flat(
        (MACROS / "iceberg_adapter_overrides.sql").read_text(encoding="utf-8"))
    for marker in COPIED:
        # NAMING THE INSTALLED VERSION IS THE WHOLE VALUE OF THIS MESSAGE. The first time this fired
        # it said "upstream changed shape" while the truth was the opposite — the LAPTOP had 1.10.1
        # and CI had 1.11.0 — and the direction is not guessable from a missing substring.
        assert flat(marker) in body, (
            f"dbt-duckdb {installed} does not carry this line, so the override was copied from a "
            f"different version. Re-copy `duckdb__create_table_as` from the INSTALLED adapter and "
            f"update COPIED: {marker!r}")
        assert flat(marker) in ours, (
            f"the override drifted from dbt-duckdb {installed}: {marker!r}")
