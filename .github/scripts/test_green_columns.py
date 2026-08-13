"""The green core column list is written twice — assert the two copies never drift.

`download_green_taxi.py:CORE_COLUMNS` is the LAND-time guard: a month whose parquet footer lacks
any of these is refused rather than archived. `macros/green_trip_columns.sql` is what the three
model trees generate their SELECT lists from. Neither can import the other — one is Python on a
runner, the other Jinja inside dbt — so this is the only thing holding them together.

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
    src = open(os.path.join(ROOT, "download_green_taxi.py"), encoding="utf-8").read()
    body = re.search(r"^CORE_COLUMNS = \[(.*?)^\]", src, re.S | re.M)
    assert body, "CORE_COLUMNS not found in download_green_taxi.py"
    return re.findall(r'"([^"]+)"', body.group(1))


def _macro_list():
    src = open(os.path.join(ROOT, "macros", "green_trip_columns.sql"), encoding="utf-8").read()
    body = re.search(r"macro green_trip_columns\(\).*?return\(\[(.*?)\]\)", src, re.S)
    assert body, "green_trip_columns() return list not found"
    return re.findall(r"'([^']+)'", body.group(1))


def test_the_two_column_lists_are_identical_and_in_the_same_order():
    # Order matters as well as membership: both lists are documented as "file order", and the
    # dialect SELECTs are generated positionally from the macro's.
    assert _macro_list() == _python_list()


def test_the_list_is_the_documented_twenty():
    cols = _macro_list()
    assert len(cols) == 20, f"expected the 20-column core, got {len(cols)}: {cols}"
    # The one TLC column deliberately excluded — 2025-onward only, green's `airport_fee` analogue.
    # Re-adding it without handling its era breaks every file older than 2025.
    assert "cbd_congestion_fee" not in cols, \
        "cbd_congestion_fee does not exist in every archived month"
    # The INVERSE of yellow's exclusions: green's parquet carries these in every month from
    # 2014-01 (congestion_surcharge as NULL before 2019), and dropping one to mirror the yellow
    # list would quietly shrink the surface this dataset exists to measure.
    for kept in ("congestion_surcharge", "ehail_fee", "trip_type"):
        assert kept in cols, f"{kept} exists in every archived green month and must stay"


def test_the_skewed_categoricals_are_all_present():
    # These are the reason this dataset was added at all: 97-99% single-value columns plus the two
    # Zipfian zone ids, and green's own trip_type on top. Dropping one to tidy the list would
    # quietly weaken the experiment.
    cols = set(_macro_list())
    for c in ("VendorID", "RatecodeID", "store_and_fwd_flag", "payment_type",
              "PULocationID", "DOLocationID", "trip_type"):
        assert c in cols, f"{c} is part of the skew this dataset exists to measure"
