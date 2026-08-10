"""The NYC core column list is written twice — assert the two copies never drift.

`download_nyc_taxi.py:CORE_COLUMNS` is the LAND-time guard: a month whose parquet footer lacks any
of these is refused rather than archived. `macros/nyc_trip_columns.sql` is what the three model
trees generate their SELECT lists from. Neither can import the other — one is Python on a runner,
the other Jinja inside dbt — so this is the only thing holding them together.

Drift is silent in both directions and both are expensive:
  a column in the macro but not the guard  -> a month missing it is archived, and every engine's
                                              read of it fails mid-write, on paid capacity
  a column in the guard but not the macro  -> months are refused for a column nothing reads

Same reason `.github/scripts` already tests that stats.py and benchmark/engines.py agree about the
Fabric item map: two hand-maintained copies of one list, no shared import, no error if they differ.
"""
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))


def _python_list():
    src = open(os.path.join(ROOT, "download_nyc_taxi.py"), encoding="utf-8").read()
    body = re.search(r"^CORE_COLUMNS = \[(.*?)^\]", src, re.S | re.M)
    assert body, "CORE_COLUMNS not found in download_nyc_taxi.py"
    return re.findall(r'"([^"]+)"', body.group(1))


def _macro_list():
    src = open(os.path.join(ROOT, "macros", "nyc_trip_columns.sql"), encoding="utf-8").read()
    body = re.search(r"macro nyc_trip_columns\(\).*?return\(\[(.*?)\]\)", src, re.S)
    assert body, "nyc_trip_columns() return list not found"
    return re.findall(r"'([^']+)'", body.group(1))


def test_the_two_column_lists_are_identical_and_in_the_same_order():
    # Order matters as well as membership: both lists are documented as "file order", and the
    # dialect SELECTs are generated positionally from the macro's.
    assert _macro_list() == _python_list()


def test_the_list_is_the_documented_seventeen():
    cols = _macro_list()
    assert len(cols) == 17, f"expected the 17-column core, got {len(cols)}: {cols}"
    # The two TLC columns deliberately excluded — see macros/nyc_trip_columns.sql. Re-adding either
    # without handling its era (and `Airport_fee`'s casing) breaks every file older than it.
    for late in ("congestion_surcharge", "airport_fee", "Airport_fee"):
        assert late not in cols, f"{late} does not exist in every archived month"


def test_the_skewed_categoricals_are_all_present():
    # These are the reason this dataset was added at all: 97-99% single-value columns plus the two
    # Zipfian zone ids. Dropping one to tidy the list would quietly weaken the experiment.
    cols = set(_macro_list())
    for c in ("VendorID", "RatecodeID", "store_and_fwd_flag", "payment_type",
              "PULocationID", "DOLocationID"):
        assert c in cols, f"{c} is part of the skew this dataset exists to measure"
