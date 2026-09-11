"""Pin the tpcds column lists across the three places that carry them.

There are three copies of every column list and they cannot be reduced to one: the GENERATOR
(download_tpcds.py's COLUMNS, which decides what is written to parquet), the MACRO
(macros/tpcds_columns.sql, which decides what the models select, and which dbt reads as Jinja rather
than Python), and the REGISTRY (datasets.py's `mart_columns`, which the dashboard renders the
encodings table in). Sibling of test_{cms,bts,nyc,green}_columns.py, and the same reasoning: a
divergence here is silent. A column the generator writes and the macro does not select is simply
absent from every engine's table, and nothing goes red.

It also pins the two things the white paper's section 4.5 adds, because they are the whole
difference between TPC-DS and the paper's TPC-DS: `cache_buster` last on both facts, `d_date_sk_1`
last on date_dim.

Offline, needs no credentials, runs in the free `checks` job.
"""
import io
import os
import re
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, HERE)

import datasets  # noqa: E402

TABLES = ("store_sales", "catalog_sales", "date_dim", "item", "store", "promotion", "ship_mode",
          "catalog_page", "customer_address", "customer_demographics")

# What each table must hold AFTER the paper's customisation: dsdgen's own columns plus the two
# additions. Spelled as counts rather than lists so this file states the invariant and the three
# sources supply the names -- a fourth copy of the names would be one more thing to drift.
COUNTS = {"store_sales": 24, "catalog_sales": 35, "date_dim": 29, "item": 22, "store": 29,
          "promotion": 19, "ship_mode": 6, "catalog_page": 9, "customer_address": 13,
          "customer_demographics": 9}


def generator():
    """download_tpcds.py's COLUMNS, imported rather than parsed.

    The module is importable with no side effects -- everything that touches OneLake is inside a
    function -- which is what lets this and test_tpcds_landing_sql.py read the real definitions."""
    sys.path.insert(0, ROOT)
    import download_tpcds
    return download_tpcds.COLUMNS


def macro():
    """The lists out of macros/tpcds_columns.sql, read as text.

    Parsed rather than rendered because rendering needs a dbt context; the file is a flat dict of
    quoted literals precisely so this stays a regex."""
    src = io.open(os.path.join(ROOT, "macros", "tpcds_columns.sql"), encoding="utf-8").read()
    body = re.search(r"\{%-\s*set cols = \{(.*?)\n  \}\s*-%\}", src, re.S)
    assert body, "could not find the cols dict in macros/tpcds_columns.sql"
    out = {}
    for m in re.finditer(r"'(\w+)':\s*\[(.*?)\]", body.group(1), re.S):
        out[m.group(1)] = re.findall(r"'([^']+)'", m.group(2))
    return out


@pytest.mark.parametrize("table", TABLES)
def test_the_generator_and_the_macro_agree_column_for_column_and_in_order(table):
    """ORDER matters, not just membership: the models select by position in this list, so a
    reordering would still build and would still pass a set comparison, while writing every engine's
    columns in an order the registry's mart_columns no longer describes."""
    assert generator()[table] == macro()[table]


@pytest.mark.parametrize("table", TABLES)
def test_every_table_has_the_documented_column_count(table):
    assert len(generator()[table]) == COUNTS[table]


def test_the_macro_covers_exactly_the_ten_tables():
    assert set(macro()) == set(TABLES)
    assert set(generator()) == set(TABLES)


def test_the_registry_mart_columns_are_the_marts_columns():
    """`mart_columns` is what the dashboard renders the encodings table in, in the model's own select
    order -- so it has to BE the mart's list, not a copy that once was."""
    assert datasets.spec("tpcds")["mart_columns"] == generator()["store_sales"]


def test_both_facts_end_with_cache_buster():
    """The paper's section 4.5 adds it, initialised to 1, so a load test can randomise a filter
    against it. Nothing here reads it; it is carried so the tables match the paper's, and it is LAST
    because it is an addition to dsdgen's schema rather than part of it."""
    for fact in ("store_sales", "catalog_sales"):
        assert generator()[fact][-1] == "cache_buster"
        assert generator()[fact].count("cache_buster") == 1


def test_date_dim_ends_with_the_shifted_key_and_keeps_the_original():
    """d_date_sk_1 REPLACES d_date_sk as the key the facts join, but the paper's table still carries
    d_date_sk -- so the landed column list has both and the semantic model exposes only the shift."""
    cols = generator()["date_dim"]
    assert cols[-1] == "d_date_sk_1"
    assert "d_date_sk" in cols


def test_no_fact_carries_a_file_column():
    """Every other dataset's fact carries `file` and increments along it. dsdgen emits a whole scale
    factor at once, so this one rebuilds instead -- and a `file` column here would be a stored column
    nothing reads, in a benchmark whose subject is write cost."""
    for fact in ("store_sales", "catalog_sales"):
        assert "file" not in generator()[fact]


def test_the_scale_factors_the_generator_accepts_are_the_ones_plan_enforces():
    """`plan` refuses a scale factor outside the registry's tuple before capacity is spent, and the
    generator refuses it again on the runner. They have to be the same set or one of the two is
    theatre."""
    sys.path.insert(0, ROOT)
    import download_tpcds
    assert download_tpcds.SCALE_FACTORS == datasets.spec("tpcds")["download_limits"]
