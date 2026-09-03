"""Execute download_tpcds.py's REAL customisation SQL against a tiny dsdgen, in local DuckDB.

WHY THIS EXISTS, AND IT IS THE cms LESSON. `test_cms_landing_sql.py` was written after a
`strftime(VARCHAR, STRING_LITERAL)` bind failure killed run 31810902120 -- after 5.95 GB had been
downloaded and paid for -- because the SQL that does the landing lives in a downloader and nothing
offline covered it. This dataset has more of that SQL than any other: landing is where the white
paper's whole section 4.5 customisation happens, so it is the only place in the project where a
value is edited rather than passed through, and every claim the models and the semantic model make
rests on it being right.

`customise_sql()` is a pure function of the table name precisely so this file can run the real text.
`dsdgen(sf=0.01)` finishes in about a second and produces every table with real values, which is
enough to check each rule.

Offline, no credentials, runs in the free `checks` job before any leg spends capacity.
"""
import datetime
import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, ROOT)

duckdb = pytest.importorskip("duckdb")

import download_tpcds as G  # noqa: E402

SF = 0.01


@pytest.fixture(scope="module")
def con():
    """One tiny dsdgen, customised exactly as a real landing would customise it."""
    c = duckdb.connect()
    try:
        c.sql("INSTALL tpcds; LOAD tpcds;")
    except Exception as ex:                                        # noqa: BLE001
        pytest.skip("the duckdb tpcds extension is unavailable here: " + str(ex))
    c.sql("CALL dsdgen(sf=" + str(SF) + ")")
    for t in G.TABLES:
        c.sql("CREATE OR REPLACE TABLE " + t + "_landed AS " + G.customise_sql(t))
    return c


def cols(con, table):
    return [r[0] for r in con.sql("DESCRIBE " + table + "_landed").fetchall()]


@pytest.mark.parametrize("table", sorted(G.TABLES))
def test_the_landed_schema_is_the_declared_schema(con, table):
    """The SQL and the COLUMNS dict have to agree, or the models select columns the parquet lacks."""
    assert cols(con, table) == G.COLUMNS[table]


@pytest.mark.parametrize("fact", sorted(G.FACTS))
def test_no_fact_row_survives_with_a_null_in_any_column(con, fact):
    """The paper's rule, as reconstructed: "All nulls in both Fact Tables were removed". What makes
    the reconstruction credible is the row counts it reproduces (see the generator's docstring); what
    this asserts is that the SQL actually implements the rule it claims."""
    checks = " OR ".join('"' + c + '" IS NULL' for c in G.COLUMNS[fact])
    n = con.sql("SELECT count(*) FROM " + fact + "_landed WHERE " + checks).fetchone()[0]
    assert n == 0


@pytest.mark.parametrize("fact", sorted(G.FACTS))
def test_the_fact_primary_key_is_unique_after_the_drop(con, fact):
    """This is what lets the duckdb models merge rather than append, and what makes a doubled write
    impossible rather than merely detectable. nyc and green have no such key, which is why their
    facts append; do not carry that decision over here without re-running this."""
    key = ", ".join(G.FACT_KEYS[fact])
    dupes = con.sql("SELECT count(*) FROM (SELECT " + key + " FROM " + fact
                    + "_landed GROUP BY " + key + " HAVING count(*) > 1)").fetchone()[0]
    assert dupes == 0


@pytest.mark.parametrize("fact", sorted(G.FACTS))
def test_cache_buster_is_the_constant_one(con, fact):
    n = con.sql("SELECT count(*) FROM " + fact + "_landed WHERE cache_buster <> 1").fetchone()[0]
    assert n == 0


def test_date_dim_is_the_papers_2191_rows(con):
    """Table 4.3.1 reports 2,191 rows at every scale factor, and 2021-01-01..2026-12-31 is 2,191
    days. A different number means the trim moved."""
    assert con.sql("SELECT count(*) FROM date_dim_landed").fetchone()[0] == 2191
    lo, hi = con.sql("SELECT min(d_date), max(d_date) FROM date_dim_landed").fetchone()
    assert (str(lo), str(hi)) == (G.DATE_LO, G.DATE_HI)


def test_the_shifted_key_is_exactly_the_documented_offset(con):
    """d_date_sk_1 = d_date_sk - 8401 on every row. The offset is the days between 1998-01-01, where
    TPC-DS sales start, and 2021-01-01, where the paper puts them -- so both facts' *_sold_date_sk
    land inside the trimmed window."""
    off = con.sql("SELECT DISTINCT d_date_sk - d_date_sk_1 FROM date_dim_landed").fetchall()
    assert off == [(G.DATE_SHIFT_DAYS,)]
    assert (datetime.date(2021, 1, 1) - datetime.date(1998, 1, 1)).days == G.DATE_SHIFT_DAYS


@pytest.mark.parametrize("rel", G.RELATIONSHIPS, ids=[r[0] + "." + r[1] for r in G.RELATIONSHIPS])
def test_every_modelled_relationship_has_no_orphans(con, rel):
    """The semantic model sets relyOnReferentialIntegrity on all thirteen, which permits an INNER
    join -- so an orphan would silently remove fact rows from every query rather than failing. The
    paper drops null fact rows for exactly this reason; this is the check that it worked."""
    fact, col, dim, key = rel
    n = con.sql("SELECT count(*) FROM " + fact + "_landed f WHERE f." + col
                + " IS NOT NULL AND NOT EXISTS (SELECT 1 FROM " + dim + "_landed d WHERE d."
                + key + " = f." + col + ")").fetchone()[0]
    assert n == 0


def test_a_dimension_passes_through_untouched(con):
    """Only the two facts and date_dim are edited. If a dimension's SQL ever grew a filter, its row
    count would stop matching the TPC-DS spec and the paper comparison would quietly shift."""
    for t in ("item", "store", "promotion", "ship_mode", "catalog_page", "customer_address",
              "customer_demographics"):
        raw = con.sql("SELECT count(*) FROM " + t).fetchone()[0]
        landed = con.sql("SELECT count(*) FROM " + t + "_landed").fetchone()[0]
        assert raw == landed, t
