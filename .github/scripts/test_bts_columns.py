"""The BTS core column list is written THREE times — assert the copies never drift.

`download_bts_flights.py:CORE_COLUMNS` is the LAND-time guard: a month whose CSV header lacks any
of these is refused rather than archived. `macros/bts_flight_columns.sql` is what the three model
trees generate their SELECT lists from. `datasets.py`'s `mart_columns` is what the `plan` job
validates a `sort_by` against. None can import the others — Python on a runner, Jinja inside dbt,
and a registry the workflow reads — so this is the only thing holding them together.

Drift is silent in every direction and all are expensive:
  a column in the macro but not the guard      -> a month missing it is archived, and every
                                                  engine's read fails mid-write, on paid capacity
  a column in the guard but not the macro      -> months are refused for a column nothing reads
  a column in the macro but not mart_columns   -> `plan` refuses a legitimate sort_by, or worse,
                                                  accepts one naming a column the mart lacks

Same pattern as test_nyc_columns.py, extended to the registry because bts's list was born in three
places where nyc's was born in two.
"""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, HERE)

import datasets  # noqa: E402


def _python_list():
    src = open(os.path.join(ROOT, "download_bts_flights.py"), encoding="utf-8").read()
    body = re.search(r"^CORE_COLUMNS = \[(.*?)^\]", src, re.S | re.M)
    assert body, "CORE_COLUMNS not found in download_bts_flights.py"
    return re.findall(r'"([^"]+)"', body.group(1))


def _macro_list():
    src = open(os.path.join(ROOT, "macros", "bts_flight_columns.sql"), encoding="utf-8").read()
    body = re.search(r"macro bts_flight_columns\(\).*?return\(\[(.*?)\]\)", src, re.S)
    assert body, "bts_flight_columns() return list not found"
    return re.findall(r"'([^']+)'", body.group(1))


def test_the_macro_and_the_downloader_agree_in_order():
    # Order matters as well as membership: both lists are documented as "file order", and the
    # dialect SELECTs are generated positionally from the macro's.
    assert _macro_list() == _python_list()


def test_the_registry_is_the_core_list_plus_file():
    # `mart_columns` is the STORED table — the 22 core columns plus the derived `file` — and it is
    # what `plan` validates a sort_by against, so a drift here either refuses a real column or
    # admits a missing one.
    assert datasets.DATASETS["bts"]["mart_columns"] == _macro_list() + ["file"]


def _python_types():
    """{column: INTEGER|VARCHAR|DATE|DOUBLE} as the downloader's CANONICAL dict declares it,
    with DOUBLE for anything unlisted — the same fallthrough canonical_expr() applies."""
    src = open(os.path.join(ROOT, "download_bts_flights.py"), encoding="utf-8").read()
    body = re.search(r"^CANONICAL = \{(.*?)^\}", src, re.S | re.M)
    assert body, "CANONICAL not found in download_bts_flights.py"
    declared = dict(re.findall(r'"(\w+)":\s*"(\w+)"', body.group(1)))
    return {c: declared.get(c, "DOUBLE") for c in _python_list()}


def _macro_types():
    """The same map from the macro's dialect-independent branches: its `ints`/`dates`/`strings`
    sets, DOUBLE for the rest."""
    src = open(os.path.join(ROOT, "macros", "bts_flight_columns.sql"), encoding="utf-8").read()
    ints = set(re.findall(r"'(\w+)'", re.search(r"set ints = \[(.*?)\]", src, re.S).group(1)))
    dates = set(re.findall(r"'(\w+)'", re.search(r"set dates = \[(.*?)\]", src, re.S).group(1)))
    strings = set(re.findall(r"'(\w+)':", re.search(r"set strings = \{(.*?)\}", src, re.S).group(1)))
    out = {}
    for c in _macro_list():
        out[c] = ("INTEGER" if c in ints else "DATE" if c in dates
                  else "VARCHAR" if c in strings else "DOUBLE")
    return out


def test_the_type_maps_agree():
    # The bug this pins was real and SILENT on the expensive side: Origin/Dest missing from both
    # string maps fell through to DOUBLE, which the models fail on loudly — but the DOWNLOADER
    # would have TRY_CAST('ATL' AS DOUBLE)'d every airport code to NULL and archived the result,
    # and nothing after that could tell a NULL from BTS's own missing value. Caught by running the
    # duckdb leg locally; kept caught by this.
    assert _macro_types() == _python_types()


def test_the_list_is_the_documented_twenty_two():
    cols = _macro_list()
    assert len(cols) == 22, f"expected the 22-column core, got {len(cols)}: {cols}"


def test_the_competing_categoricals_are_all_present():
    # These are the reason this dataset was added at all: independent, moderately skewed columns
    # at every rung of the cardinality ladder, so V-Order's greedy ordering has to choose between
    # them. Dropping one to tidy the list would quietly weaken the experiment.
    cols = set(_macro_list())
    for c in ("DayOfWeek", "Reporting_Airline", "Origin", "Dest", "Tail_Number",
              "CRSDepTime", "CancellationCode", "DistanceGroup"):
        assert c in cols, f"{c} is part of the competing skew this dataset exists to measure"
