"""Execute download_cms_payments.py's OWN SQL against a synthetic CSV.

WHY THIS EXISTS, and it is not hypothetical. Run 31810902120 downloaded 5.95 GB of PY2019 and then
died on the partition statement:

    Could not choose a best candidate function for the function call
    strftime(VARCHAR, STRING_LITERAL)

`read_csv(all_varchar = true)` makes every column VARCHAR, and DuckDB resolves a select-list column
reference against the FROM rather than against a sibling alias — so the `CAST(... AS DATE)` in the
projection never reached the `_ym` expression beside it. Nothing offline could have caught it:
`test_cms_columns.py` compares two lists, `check_gating.py` parses models, and the render harness
covers models/. This SQL lives in a downloader, and the only way to test SQL is to run it.

So: build a 91-column CSV of a handful of rows, extract the REAL statements out of the script by
regex, and execute them. The extraction is deliberate — re-typing the SQL here would test the copy,
not the script. Same idiom `test_cms_columns.py` already uses to read CORE_COLUMNS, and
`test_templates.py` to read datasets.py.

Runs in about a second and needs no credentials, no network and no Fabric.
"""
import os
import re

import duckdb
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
SCRIPT = os.path.join(ROOT, "download_cms_payments.py")


def _src():
    return open(SCRIPT, encoding="utf-8").read()


def _core_columns():
    body = re.search(r"^CORE_COLUMNS = \[(.*?)^\]", _src(), re.S | re.M)
    return re.findall(r'"([^"]+)"', body.group(1))


def _canonical():
    body = re.search(r"^CANONICAL = \{(.*?)^\}", _src(), re.S | re.M)
    return dict(re.findall(r'"([^"]+)":\s*"([^"]+)"', body.group(1)))


def _partition_sql(select, q, part_dir):
    """The real COPY ... PARTITION_BY statement, lifted out of land_year() and f-string-evaluated.

    Bound with the same local names the script uses, so the text under test is byte-identical to
    the text that runs in CI."""
    m = re.search(r'con\.sql\(f"""(\s*\n\s*COPY \(.*?)"""\)', _src(), re.S)
    assert m, "the PARTITION_BY COPY statement was not found in download_cms_payments.py"
    return eval('f"""' + m.group(1) + '"""',
                {"select": select, "q": q, "part_dir": part_dir, "chr": chr,
                 "DATE_FORMAT": _date_format()})


def _consolidate_sql(src_glob, dst):
    """The real per-month consolidation COPY, same treatment. It is written as two adjacent
    f-string literals in the script, so both halves are captured and joined."""
    m = re.search(r'con\.sql\(f"(COPY \(SELECT \* EXCLUDE[^"]*)"\s*\n\s*f"([^"]*)"\)', _src())
    assert m, "the consolidation COPY statement was not found in download_cms_payments.py"
    return eval('f"""' + m.group(1) + m.group(2) + '"""',
                {"src_glob": src_glob, "dst": dst, "chr": chr})


def _canonical_type(col):
    return _canonical().get(col, "VARCHAR")


def _date_format():
    m = re.search(r'^DATE_FORMAT = "([^"]+)"', _src(), re.M)
    assert m, "DATE_FORMAT not found in download_cms_payments.py"
    return m.group(1)


def _canonical_expr(col):
    """canonical_expr() from the script, lifted rather than re-typed — it is what decides whether
    a date is PARSED or merely cast, which is the difference between a landed year and a
    ConversionException 6 GB into a download."""
    body = re.search(r"def canonical_expr\(col\):.*?\n\n\n", _src(), re.S)
    assert body, "canonical_expr() not found in download_cms_payments.py"
    ns = {"CANONICAL": _canonical(), "DATE_FORMAT": _date_format(),
          "canonical_type": _canonical_type}
    exec(compile(body.group(0), "x", "exec"), ns)
    return ns["canonical_expr"](col)


# Three rows in two real months, plus one whose date DuckDB cannot parse — the row that decides
# whether an unparseable date refuses a whole 6 GB year or lands as cms_<year>-00.
ROWS = [
    {"Date_of_Payment": "01/15/2019", "Program_Year": "2019", "Record_ID": "1"},
    {"Date_of_Payment": "01/20/2019", "Program_Year": "2019", "Record_ID": "2"},
    {"Date_of_Payment": "02/03/2019", "Program_Year": "2019", "Record_ID": "3"},
    {"Date_of_Payment": "not a date", "Program_Year": "2019", "Record_ID": "4"},
]


def _write_csv(path, cols):
    # CMS serves dates as MM/DD/YYYY. Numeric columns get plausible values; everything else is
    # left empty, which is what most of this source's columns actually are.
    numeric = {"Total_Amount_of_Payment_USDollars": "12.34",
               "Number_of_Payments_Included_in_Total_Amount": "1",
               "Covered_Recipient_NPI": "1234567890",
               "Covered_Recipient_Profile_ID": "99887766",
               "Teaching_Hospital_ID": ""}
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(",".join(cols) + "\n")
        for r in ROWS:
            f.write(",".join(r.get(c, numeric.get(c, "")) for c in cols) + "\n")


@pytest.fixture
def landed(tmp_path):
    cols = _core_columns()
    csv_path = str(tmp_path / "src.csv").replace("\\", "/")
    part_dir = str(tmp_path / "parts")
    _write_csv(csv_path, cols)

    con = duckdb.connect()
    select = ", ".join(_canonical_expr(c) for c in cols)
    con.sql(_partition_sql(select, csv_path, part_dir))
    return con, part_dir, cols, tmp_path


def test_the_partition_statement_binds_and_runs():
    """The regression. It only has to EXECUTE — the bug was a bind error, not a wrong answer."""
    # Exercised by the fixture; a bind failure raises there and fails every test in this module,
    # which is the point. This one states it explicitly so the failure has a name.
    assert _partition_sql("1 AS x", "/tmp/x.csv", "/tmp/parts").strip().startswith("COPY (")


def test_rows_land_in_the_month_of_their_payment_date(landed):
    _con, part_dir, _cols, _tmp = landed
    parts = sorted(p for p in os.listdir(part_dir) if p.startswith("_ym="))
    assert parts == ["_ym=2019-00", "_ym=2019-01", "_ym=2019-02"], parts


def test_an_unparseable_date_lands_as_month_00_rather_than_refusing_the_year(landed):
    """TRY_CAST + COALESCE, not CAST. With CAST the whole COPY raises; with TRY_CAST alone the row
    goes to __HIVE_DEFAULT_PARTITION__, is skipped, and the land-time reconciliation refuses a
    program year over one bad row — after the GBs have been downloaded."""
    con, part_dir, _cols, _tmp = landed
    glob = os.path.join(part_dir, "_ym=2019-00", "*.parquet").replace("\\", "/")
    n = con.sql(f"SELECT count(*) FROM read_parquet('{glob}')").fetchone()[0]
    assert n == 1
    assert "__HIVE_DEFAULT_PARTITION__" not in os.listdir(part_dir)


def test_nothing_is_lost_in_the_split(landed):
    """The land-time reconciliation is the only check on the month/year split — the downloader
    writes both sides, so no dbt test can do it. Assert the property it depends on."""
    con, part_dir, _cols, _tmp = landed
    glob = os.path.join(part_dir, "**", "*.parquet").replace("\\", "/")
    total = con.sql(f"SELECT count(*) FROM read_parquet('{glob}')").fetchone()[0]
    assert total == len(ROWS)


def test_the_landed_schema_is_the_canonical_one(landed):
    """Every engine's stored types come from this projection, and `layout` compares encodings BY
    COLUMN — so a column that landed VARCHAR where the macro casts DATE is a cross-engine
    difference produced by the downloader rather than by a writer."""
    con, part_dir, cols, _tmp = landed
    glob = os.path.join(part_dir, "_ym=2019-01", "*.parquet").replace("\\", "/")
    got = {r[0]: r[1] for r in
           con.sql(f"DESCRIBE SELECT * FROM read_parquet('{glob}')").fetchall()}
    for c in cols:
        expected = _canonical_type(c)
        assert got[c] == expected, f"{c} landed {got[c]}, canonical says {expected}"
    assert got["Date_of_Payment"] == "DATE"
    assert got["Record_ID"] == "VARCHAR", "Record_ID is a documented STRING; see CANONICAL"


def test_dates_are_PARSED_as_month_day_year_and_not_merely_cast(landed):
    """CMS serves MM/DD/YYYY. Two distinct failures live here and only one is loud.

    LOUD: a plain `CAST(x AS DATE)` raises `invalid date field format` on every row — and a probe
    written as `count(*)` over a subquery PASSES anyway, because DuckDB prunes the projection.

    SILENT, and the reason this test asserts a VALUE rather than a type: with `%d/%m/%Y` the string
    `02/03/2019` parses perfectly well, as 2 March instead of 3 February. Every row would land in a
    real month, the schema would be right, the reconciliation would balance, and the archive would
    be wrong. `02/03` is the cheapest input that can tell the two apart."""
    con, part_dir, _cols, _tmp = landed
    glob = os.path.join(part_dir, "_ym=2019-02", "*.parquet").replace("\\", "/")
    got = con.sql(f'SELECT "Date_of_Payment", "Record_ID" FROM read_parquet(\'{glob}\')').fetchall()
    assert [(str(d), r) for d, r in got] == [("2019-02-03", "3")], (
        "02/03/2019 must land as 3 February; 2 March means the format string is %d/%m/%Y")
    # The publication date is the other DATE column and takes the same parse.
    jan = os.path.join(part_dir, "_ym=2019-01", "*.parquet").replace("\\", "/")
    d = con.sql(f'SELECT min("Date_of_Payment"), max("Date_of_Payment") '
                f"FROM read_parquet('{jan}')").fetchone()
    assert [str(x) for x in d] == ["2019-01-15", "2019-01-20"]


def test_the_consolidation_drops_the_partition_column(landed):
    """`SELECT * EXCLUDE (_ym)` — the landed file must carry the 91 source columns and nothing
    else, or every dialect's read gains a column the macro does not list."""
    con, part_dir, cols, tmp_path = landed
    src_glob = os.path.join(part_dir, "_ym=2019-01", "*.parquet").replace("\\", "/")
    dst = str(tmp_path / "cms_2019-01.parquet")
    con.sql(_consolidate_sql(src_glob, dst))
    got = [r[0] for r in con.sql(
        f"DESCRIBE SELECT * FROM read_parquet('{dst.replace(chr(92), '/')}')").fetchall()]
    assert got == cols, "the consolidated month must be exactly CORE_COLUMNS, in order"
    assert "_ym" not in got
