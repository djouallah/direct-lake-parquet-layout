"""Generate the white paper's TPC-DS subset with DuckDB `dsdgen` and land it in OneLake.

THE ONLY GENERATOR HERE, AND THE ONLY ONE THAT SPENDS COMPUTE. The other five scripts in this
directory download a public archive; this one MAKES its input. Everything else about it follows the
same idiom -- an archive-log parquet at the Files root, one row per landed file, so a re-run only
does what is new -- because the models, the macros and stats.py all read that log and know nothing
about where the bytes came from.

WHAT IT BUILDS. The 2 facts and 8 dimensions of the Data Leaps / Microsoft white paper *Modern Power
BI Architecture Choices for Reporting on Azure Databricks*, with that paper's section 4.5
customisation applied AT LAND TIME:

  * every row with a NULL in any column is dropped from both fact tables -- the paper does this so
    "Assume Referential Integrity" is true and Power BI can emit inner joins;
  * a `cache_buster` INTEGER of 1 is added to both facts -- the paper randomises a filter against it
    during load testing to defeat Databricks result caching; nothing here reads it, and it is
    carried so the tables match;
  * `d_date_sk_1` = `d_date_sk` - 8401 days is added to date_dim and REPLACES d_date_sk as the key,
    mapping the 1998-2003 sales keys onto 2021-2026 dates so time intelligence has a recent year;
  * date_dim keeps only 2021-01-01..2026-12-31, which is 2,191 rows.

Customising here rather than in the models is the same division of labour every dataset here uses:
landing is the irreversible half, so the downloader normalises and the models are pass-throughs. It
also means all four engines read byte-identical input, which is the whole basis of the comparison.

THE PAPER'S SCRIPT IS NOT PUBLISHED, so the null rule is a RECONSTRUCTION. What makes it credible is
that it reproduces the paper's own Table 4.3.1 row counts: at SF1 the drop keeps 0.910 of store_sales
and 0.990 of catalog_sales, and the paper's SF10 counts are 26,206,837 / 28,800,991 = 0.910 and
14,257,451 / 14,401,261 = 0.990. The run REPORTS the comparison at every scale factor and does not
fail on it -- see `verdict` -- because a mismatch is a finding about the reconstruction, not a
corrupt landing. It DOES fail on a raw dsdgen count that disagrees with the spec, which is a real
fault.

`download_limit` IS THE SCALE FACTOR, not a file count. cms already reinterprets it (program years);
this reinterprets it again, and the `plan` job refuses anything outside SCALE_FACTORS for free,
before a leg spends capacity -- the dispatch form's default is 200, which would otherwise ask dsdgen
for SF200.

WHERE IT RUNS, AND WHY THAT IS NEW. dsdgen materialises a whole scale factor at once; SF10 is ~10 GB
and SF100 ~100 GB, which no free GitHub runner can hold. So this script has two modes. On the runner
it validates, decides whether anything needs doing, and RELAYS ITSELF into a throwaway Fabric Python
notebook with duckrun's `run_python` -- the same mechanism `.github/scripts/fabric_run.py` uses for
the DuckDB legs. In the notebook (`--in-notebook`) it generates, customises, checks and uploads,
data-local to OneLake.

⚠️ THIS MAKES `land` A JOB THAT SPENDS FABRIC COMPUTE, which it never was before. The notebook's item
GUID is therefore written into the run record under role `compute` with `engine: landing`, so the CU
ledger attributes it to the generator and not to whichever engine the dispatch happened to build.

IT IS A ONE-OFF. A scale factor is generated once and read by every engine, every dispatch,
afterwards -- so this is not part of the normal run and is not meant to be. Three things keep it
that way, and none of them is a convention someone has to remember:

  * the `land` job only calls it when `skip_download=false`, which is off by default and FORCED off
    on every scheduled run;
  * it skips itself when the archive log already carries `etag = sf<N>` for all ten tables, so even
    an explicit re-dispatch is a no-op (`TPCDS_FORCE=1` overrides);
  * the notebook it creates is deleted and the deletion is CONFIRMED, so nothing is left billing
    between the one-off and the runs that use its output.

Run it once per scale factor, by hand, and never think about it again:

    WS_ID=<workspace guid> FILES_PATH=<abfss .../dbt_tpcds_landing/Files>       download_limit=10 DATASET=tpcds FABRIC_CORES=8 python download_tpcds.py

    python download_tpcds.py --status      # what is landed, without generating anything

Env in:
  FILES_PATH        the landing lakehouse's Files root (set by `provision.py land`)
  download_limit    the scale factor: 1, 10 or 100
  TPCDS_FORCE       "1" to re-land a scale factor that is already logged
  WS_ID             Fabric workspace GUID (runner side only, for the relay)
  FABRIC_CORES      notebook size (runner side only); SF100 needs at least 32
"""
import json
import os
import shutil
import sys
import tempfile
import time
import uuid

IN_NOTEBOOK = "--in-notebook" in sys.argv
STATUS = "--status" in sys.argv

FILES_PATH = os.environ.get("FILES_PATH", "/tmp/landing").rstrip("/")
SF_RAW = (os.environ.get("download_limit") or "10").strip()
FORCE = os.environ.get("TPCDS_FORCE", "").strip() == "1"

# The scale factors this project will run. SF1 is the local smoke; SF10 is the paper's smallest
# volume and the default here; SF100 is the volume its Direct Lake findings turn on. SF1000 is
# deliberately absent: dsdgen holds every table in memory, so it needs a node this workspace cannot
# allocate, and the paper's own SF1000 mirrored run exceeded the memory of an F128 anyway.
SCALE_FACTORS = ("1", "10", "100")

# The ten tables of the paper's subset (section 4.3). dsdgen produces 24; the rest are dropped from
# memory before anything is written, because at SF100 they are the difference between fitting and
# not.
TABLES = ("store_sales", "catalog_sales", "catalog_page", "customer_address",
          "customer_demographics", "date_dim", "item", "promotion", "ship_mode", "store")
FACTS = ("store_sales", "catalog_sales")

# The landed schema of every table: dsdgen's own column order, plus the paper's two additions.
# MIRRORS macros/tpcds_columns.sql, and `.github/scripts/test_tpcds_columns.py` asserts the two
# agree column for column and in order. Regenerate rather than retype:
#   INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf=0); DESCRIBE <table>
COLUMNS = {
    "store_sales": [
        "ss_sold_date_sk", "ss_sold_time_sk", "ss_item_sk", "ss_customer_sk", "ss_cdemo_sk",
        "ss_hdemo_sk", "ss_addr_sk", "ss_store_sk", "ss_promo_sk", "ss_ticket_number",
        "ss_quantity", "ss_wholesale_cost", "ss_list_price", "ss_sales_price",
        "ss_ext_discount_amt", "ss_ext_sales_price", "ss_ext_wholesale_cost", "ss_ext_list_price",
        "ss_ext_tax", "ss_coupon_amt", "ss_net_paid", "ss_net_paid_inc_tax", "ss_net_profit",
        "cache_buster",
    ],
    "catalog_sales": [
        "cs_sold_date_sk", "cs_sold_time_sk", "cs_ship_date_sk", "cs_bill_customer_sk",
        "cs_bill_cdemo_sk", "cs_bill_hdemo_sk", "cs_bill_addr_sk", "cs_ship_customer_sk",
        "cs_ship_cdemo_sk", "cs_ship_hdemo_sk", "cs_ship_addr_sk", "cs_call_center_sk",
        "cs_catalog_page_sk", "cs_ship_mode_sk", "cs_warehouse_sk", "cs_item_sk", "cs_promo_sk",
        "cs_order_number", "cs_quantity", "cs_wholesale_cost", "cs_list_price", "cs_sales_price",
        "cs_ext_discount_amt", "cs_ext_sales_price", "cs_ext_wholesale_cost", "cs_ext_list_price",
        "cs_ext_tax", "cs_coupon_amt", "cs_ext_ship_cost", "cs_net_paid", "cs_net_paid_inc_tax",
        "cs_net_paid_inc_ship", "cs_net_paid_inc_ship_tax", "cs_net_profit", "cache_buster",
    ],
    "date_dim": [
        "d_date_sk", "d_date_id", "d_date", "d_month_seq", "d_week_seq", "d_quarter_seq", "d_year",
        "d_dow", "d_moy", "d_dom", "d_qoy", "d_fy_year", "d_fy_quarter_seq", "d_fy_week_seq",
        "d_day_name", "d_quarter_name", "d_holiday", "d_weekend", "d_following_holiday",
        "d_first_dom", "d_last_dom", "d_same_day_ly", "d_same_day_lq", "d_current_day",
        "d_current_week", "d_current_month", "d_current_quarter", "d_current_year", "d_date_sk_1",
    ],
    "item": [
        "i_item_sk", "i_item_id", "i_rec_start_date", "i_rec_end_date", "i_item_desc",
        "i_current_price", "i_wholesale_cost", "i_brand_id", "i_brand", "i_class_id", "i_class",
        "i_category_id", "i_category", "i_manufact_id", "i_manufact", "i_size", "i_formulation",
        "i_color", "i_units", "i_container", "i_manager_id", "i_product_name",
    ],
    "store": [
        "s_store_sk", "s_store_id", "s_rec_start_date", "s_rec_end_date", "s_closed_date_sk",
        "s_store_name", "s_number_employees", "s_floor_space", "s_hours", "s_manager",
        "s_market_id", "s_geography_class", "s_market_desc", "s_market_manager", "s_division_id",
        "s_division_name", "s_company_id", "s_company_name", "s_street_number", "s_street_name",
        "s_street_type", "s_suite_number", "s_city", "s_county", "s_state", "s_zip", "s_country",
        "s_gmt_offset", "s_tax_percentage",
    ],
    "promotion": [
        "p_promo_sk", "p_promo_id", "p_start_date_sk", "p_end_date_sk", "p_item_sk", "p_cost",
        "p_response_target", "p_promo_name", "p_channel_dmail", "p_channel_email",
        "p_channel_catalog", "p_channel_tv", "p_channel_radio", "p_channel_press",
        "p_channel_event", "p_channel_demo", "p_channel_details", "p_purpose", "p_discount_active",
    ],
    "ship_mode": [
        "sm_ship_mode_sk", "sm_ship_mode_id", "sm_type", "sm_code", "sm_carrier", "sm_contract",
    ],
    "catalog_page": [
        "cp_catalog_page_sk", "cp_catalog_page_id", "cp_start_date_sk", "cp_end_date_sk",
        "cp_department", "cp_catalog_number", "cp_catalog_page_number", "cp_description", "cp_type",
    ],
    "customer_address": [
        "ca_address_sk", "ca_address_id", "ca_street_number", "ca_street_name", "ca_street_type",
        "ca_suite_number", "ca_city", "ca_county", "ca_state", "ca_zip", "ca_country",
        "ca_gmt_offset", "ca_location_type",
    ],
    "customer_demographics": [
        "cd_demo_sk", "cd_gender", "cd_marital_status", "cd_education_status",
        "cd_purchase_estimate", "cd_credit_rating", "cd_dep_count", "cd_dep_employed_count",
        "cd_dep_college_count",
    ],
}

# TPC-DS row counts BEFORE customisation, per the v4.0.0 spec. Dimensions are exact; the facts are
# what dsdgen produces and are checked to within 1%, so a wrong scale factor is caught while a
# build-to-build wobble is not fatal. A MISMATCH HERE IS FATAL: it means the generator did not
# produce TPC-DS.
EXPECTED = {
    1:   {"store_sales": 2_880_404, "catalog_sales": 1_441_548, "catalog_page": 11_718,
          "customer_address": 50_000, "customer_demographics": 1_920_800, "date_dim": 73_049,
          "item": 18_000, "promotion": 300, "ship_mode": 20, "store": 12},
    10:  {"store_sales": 28_800_991, "catalog_sales": 14_401_261, "catalog_page": 12_000,
          "customer_address": 250_000, "customer_demographics": 1_920_800, "date_dim": 73_049,
          "item": 102_000, "promotion": 500, "ship_mode": 20, "store": 102},
    100: {"store_sales": 287_997_024, "catalog_sales": 143_997_065, "catalog_page": 20_400,
          "customer_address": 1_000_000, "customer_demographics": 1_920_800, "date_dim": 73_049,
          "item": 204_000, "promotion": 1_000, "ship_mode": 20, "store": 402},
}

# The paper's Table 4.3.1 -- row counts AFTER customisation, which is the reproduction target. SF1 is
# absent because the paper does not test it, so a SF1 run reports `null` rather than a false verdict.
PAPER_ROWS = {
    10:  {"store_sales": 26_206_837, "catalog_sales": 14_257_451, "catalog_page": 12_000,
          "customer_address": 250_000, "customer_demographics": 1_920_800, "date_dim": 2_191,
          "item": 102_000, "promotion": 500, "ship_mode": 20, "store": 102},
    100: {"store_sales": 262_082_396, "catalog_sales": 142_557_716, "catalog_page": 20_400,
          "customer_address": 1_000_000, "customer_demographics": 1_920_800, "date_dim": 2_191,
          "item": 204_000, "promotion": 1_000, "ship_mode": 20, "store": 402},
}

# 1998-01-01 -> 2021-01-01. The paper reports 2,191 date_dim rows for 2021-2026, and TPC-DS sales
# span 1998-01-01..2003-12-31, which is the same 2,191 days -- so the shift is the gap between the
# two starts and the facts' *_sold_date_sk join d_date_sk_1 onto the recent window.
DATE_SHIFT_DAYS = 8401
DATE_LO, DATE_HI = "2021-01-01", "2026-12-31"

# Every foreign key the semantic model relates, as (fact, column, dimension, key). Probed after the
# customisation and REPORTED: the paper drops null fact rows precisely so referential integrity
# holds, and the .bim sets relyOnReferentialIntegrity on all of these, which permits an inner join.
# A non-zero count would mean the model is silently dropping fact rows in every query.
RELATIONSHIPS = [
    ("store_sales", "ss_sold_date_sk", "date_dim", "d_date_sk_1"),
    ("store_sales", "ss_item_sk", "item", "i_item_sk"),
    ("store_sales", "ss_store_sk", "store", "s_store_sk"),
    ("store_sales", "ss_promo_sk", "promotion", "p_promo_sk"),
    ("store_sales", "ss_addr_sk", "customer_address", "ca_address_sk"),
    ("store_sales", "ss_cdemo_sk", "customer_demographics", "cd_demo_sk"),
    ("catalog_sales", "cs_sold_date_sk", "date_dim", "d_date_sk_1"),
    ("catalog_sales", "cs_item_sk", "item", "i_item_sk"),
    ("catalog_sales", "cs_promo_sk", "promotion", "p_promo_sk"),
    ("catalog_sales", "cs_catalog_page_sk", "catalog_page", "cp_catalog_page_sk"),
    ("catalog_sales", "cs_ship_mode_sk", "ship_mode", "sm_ship_mode_sk"),
    ("catalog_sales", "cs_bill_addr_sk", "customer_address", "ca_address_sk"),
    ("catalog_sales", "cs_bill_cdemo_sk", "customer_demographics", "cd_demo_sk"),
]

# The TPC-DS primary key of each fact -- the key the duckdb models merge on, and what
# `.github/scripts/test_tpcds_landing_sql.py` asserts is unique after the null drop.
FACT_KEYS = {"store_sales": ("ss_item_sk", "ss_ticket_number"),
             "catalog_sales": ("cs_item_sk", "cs_order_number")}

# Byte-identical to the nyc/green/bts/cms logs, so macros/new_parquet_files.sql and
# spark_new_parquet_files.sql read it unchanged even though nothing here needs a pending-file list.
# What differs is how two columns are USED: `source_type` is the TABLE name (ten values, where every
# other dataset has one or two) and `etag` is `sf<N>`, which is how a re-run knows a scale factor is
# already landed and how a reader tells which one a run measured.
LOG_COLUMNS = ("source_type VARCHAR, source_filename VARCHAR, archive_path VARCHAR, "
               "archived_at TIMESTAMPTZ, row_count BIGINT, source_url VARCHAR, "
               "etag VARCHAR, file_stem VARCHAR, columns VARCHAR")

RESULT_PREFIX = "[tpcds] RESULT "


def scale_factor():
    """The scale factor, refused unless it is one this project will run.

    Refuses rather than clamping, for the same reason `datasets.selected()` refuses an unknown
    dataset: `download_limit` means files everywhere else and the form's default is 200, so a value
    that fell through would ask dsdgen for a volume nobody chose and nothing would say so."""
    if SF_RAW not in SCALE_FACTORS:
        raise SystemExit(
            "download_limit is the SCALE FACTOR for tpcds and must be one of "
            + ", ".join(SCALE_FACTORS) + "; got " + repr(SF_RAW)
            + ". SF1000 needs a node this workspace cannot allocate and is out of scope.")
    return int(SF_RAW)


def customise_sql(table, src=None):
    """The SELECT that turns a raw dsdgen table into the paper's table (section 4.5).

    Kept as a pure function of the table name so `.github/scripts/test_tpcds_landing_sql.py` can
    execute the REAL text against a tiny dsdgen in local DuckDB. That test is the one that would
    have caught the cms landing bug for free, and this SQL is the only place in the project where a
    value is edited rather than passed through."""
    src = src or table
    raw = [c for c in COLUMNS[table] if c not in ("cache_buster", "d_date_sk_1")]
    cols = ", ".join('"' + c + '"' for c in raw)
    if table in FACTS:
        # "All nulls in both Fact Tables were removed" -- read as: drop any row with a NULL in any
        # column. The reconstruction is validated by the row counts, not by this comment.
        not_null = " AND ".join('"' + c + '" IS NOT NULL' for c in raw)
        return ("SELECT " + cols + ", CAST(1 AS INTEGER) AS cache_buster "
                "FROM " + src + " WHERE " + not_null)
    if table == "date_dim":
        return ("SELECT " + cols + ", CAST(d_date_sk - " + str(DATE_SHIFT_DAYS)
                + " AS BIGINT) AS d_date_sk_1 FROM " + src
                + " WHERE d_date BETWEEN DATE '" + DATE_LO + "' AND DATE '" + DATE_HI + "'")
    return "SELECT " + cols + " FROM " + src


# --------------------------------------------------------------------------- the notebook half

def _log(msg):
    print("[tpcds] " + msg, flush=True)


def generate(con, sf, work):
    """dsdgen -> customise -> check -> local parquet. Returns the verdict dict."""
    _log("dsdgen(sf=" + str(sf) + ") starting")
    t0 = time.time()
    con.sql("INSTALL tpcds; LOAD tpcds;")
    con.sql("CALL dsdgen(sf=" + str(sf) + ")")
    _log("dsdgen done in " + format(time.time() - t0, ",.0f") + "s")

    produced = {r[0] for r in con.sql("SHOW TABLES").fetchall()}
    missing = [t for t in TABLES if t not in produced]
    if missing:
        raise SystemExit("dsdgen did not produce " + str(missing))
    for t in sorted(produced - set(TABLES)):
        con.sql("DROP TABLE " + t)          # free the other 14 before anything is materialised

    raw_counts, spec = {}, EXPECTED.get(sf, {})
    problems = []
    for t in TABLES:
        raw_counts[t] = con.sql("SELECT count(*) FROM " + t).fetchone()[0]
        want = spec.get(t)
        if want is None:
            continue
        ok = (abs(raw_counts[t] - want) / want <= 0.01) if t in FACTS else (raw_counts[t] == want)
        if not ok:
            problems.append(t + ": got " + str(raw_counts[t]) + ", spec " + str(want))
    if problems:
        raise SystemExit("raw dsdgen counts disagree with the TPC-DS spec -- " + "; ".join(problems))
    _log("raw counts match the spec for all " + str(len(TABLES)) + " tables")

    landed, paper = {}, PAPER_ROWS.get(sf)
    for t in TABLES:
        con.sql("CREATE OR REPLACE TABLE " + t + "_landed AS " + customise_sql(t))
        con.sql("DROP TABLE " + t)
        landed[t] = con.sql("SELECT count(*) FROM " + t + "_landed").fetchone()[0]

    matches = None
    if paper:
        matches = {t: landed[t] == paper[t] for t in TABLES}
        for t in TABLES:
            flag = "ok " if matches[t] else "DIFFERS"
            _log("  " + t.ljust(23) + format(landed[t], ">15,") + "  paper "
                 + format(paper[t], ">15,") + "  " + flag)
        bad = [t for t in TABLES if not matches[t]]
        _log("vs the paper's Table 4.3.1: " + ("all match" if not bad else "DIFFERS on " + str(bad)
             + " -- the null rule is a reconstruction, not a landing fault"))
    else:
        _log("no published row counts for sf" + str(sf) + "; skipping the paper comparison")

    orphans = {}
    for fact, col, dim, key in RELATIONSHIPS:
        n = con.sql("SELECT count(*) FROM " + fact + "_landed f WHERE f." + col
                    + " IS NOT NULL AND NOT EXISTS (SELECT 1 FROM " + dim + "_landed d WHERE d."
                    + key + " = f." + col + ")").fetchone()[0]
        orphans[fact + "." + col] = n
    broken = {k: v for k, v in orphans.items() if v}
    _log("referential integrity over " + str(len(RELATIONSHIPS)) + " relationships: "
         + ("0 orphans everywhere" if not broken else "ORPHANS " + str(broken)
            + " -- relyOnReferentialIntegrity would drop these rows in every query"))

    files = {}
    for t in TABLES:
        d = os.path.join(work, t)
        os.makedirs(d, exist_ok=True)
        if t in FACTS:
            # PER_THREAD_OUTPUT gives several files, which is what a fact of this size wants; the
            # row-group size here is irrelevant to the benchmark (each engine rewrites the layout on
            # the way in) and 1M rows just keeps the downstream read cheap.
            con.sql("COPY " + t + "_landed TO '" + d.replace("\\", "/")
                    + "' (FORMAT parquet, COMPRESSION snappy, ROW_GROUP_SIZE 1000000, "
                      "PER_THREAD_OUTPUT, OVERWRITE)")
            parts = sorted(f for f in os.listdir(d) if f.endswith(".parquet"))
            for i, f in enumerate(parts):
                os.rename(os.path.join(d, f), os.path.join(d, t + "_" + format(i, "04d") + ".parquet"))
        else:
            con.sql("COPY " + t + "_landed TO '" + os.path.join(d, t + ".parquet").replace("\\", "/")
                    + "' (FORMAT parquet, COMPRESSION snappy, ROW_GROUP_SIZE 1000000)")
        con.sql("DROP TABLE " + t + "_landed")
        files[t] = sorted(f for f in os.listdir(d) if f.endswith(".parquet"))
        _log("  wrote " + t.ljust(23) + format(landed[t], ">15,") + " rows in "
             + str(len(files[t])) + " file(s)")

    return {"sf": sf, "raw": raw_counts, "landed": landed, "paper": paper,
            "matches_paper": matches, "orphans": orphans, "files": files}


def land(dr, con, sf, work, verdict):
    """Upload the parquet and rewrite this dataset's rows in the archive log."""
    def wipe(rel):
        """Remove everything under Files/<rel>.

        A re-land at a DIFFERENT scale factor writes a different number of fact parts (SF10 is a
        handful, SF100 dozens), and the models glob the folder -- so leftovers from a bigger scale
        factor would be read as data. `dr.copy(overwrite=True)` replaces same-named files and cannot
        remove ones this run does not write, and obstore's Azure delete is a batch request OneLake
        rejects, which is why the other downloaders swallow its failure. So delete through the DFS
        API, recursively, and let a failure be visible."""
        target = FILES_PATH + "/" + rel
        if not target.startswith("abfss://"):
            shutil.rmtree(target.replace("abfss://", ""), ignore_errors=True)
            return
        import re
        import requests
        from duckrun import auth
        m = re.match(r"abfss://([^@]+)@([^/]+)/(.+)", target)
        fs, host, path = m.group(1), m.group(2), m.group(3)
        r = requests.delete("https://" + host + "/" + fs + "/" + path,
                            params={"recursive": "true"},
                            headers={"Authorization": "Bearer " + auth.get_onelake_token(),
                                     "x-ms-version": "2023-11-03"}, timeout=300)
        if r.status_code not in (200, 202, 404):
            raise SystemExit("could not clear " + target + ": " + str(r.status_code) + " " + r.text[:200])

    log_path = FILES_PATH + "/parquet_raw_archive_log.parquet"
    if con.sql("SELECT count(*) FROM glob('" + log_path + "')").fetchone()[0]:
        con.sql("CREATE OR REPLACE TEMP TABLE _pq_archive_log AS SELECT source_type, "
                "source_filename, archive_path, archived_at, row_count, source_url, etag, "
                "file_stem, columns FROM read_parquet('" + log_path + "') WHERE file_stem IS NOT NULL")
    else:
        con.sql("CREATE OR REPLACE TEMP TABLE _pq_archive_log (" + LOG_COLUMNS + ")")
    # This dataset owns exactly its ten source_types; another dataset's rows in the same lakehouse
    # are left alone, and this run's own previous rows go, so the log describes one scale factor.
    con.sql("DELETE FROM _pq_archive_log WHERE source_type IN ('" + "', '".join(TABLES) + "')")

    import duckdb
    url = "duckdb://tpcds/dsdgen?sf=" + str(sf) + "&duckdb=" + duckdb.__version__
    for t in TABLES:
        wipe("parquet_raw/" + t)
        dr.copy(os.path.join(work, t), "parquet_raw/" + t, overwrite=True)
        for f in verdict["files"][t]:
            stem = f[:-len(".parquet")]
            rows = con.sql("SELECT count(*) FROM read_parquet('"
                           + os.path.join(work, t, f).replace("\\", "/") + "')").fetchone()[0]
            con.sql(
                "INSERT INTO _pq_archive_log VALUES ('" + t + "', '" + f + "', '/" + t + "/" + f
                + "', now(), " + str(rows) + ", '" + url + "', 'sf" + str(sf) + "', '" + stem
                + "', '" + ",".join(COLUMNS[t]) + "')")
        _log("  landed " + t.ljust(23) + str(len(verdict["files"][t])) + " file(s)")

    with tempfile.TemporaryDirectory() as ltmp:
        lp = os.path.join(ltmp, "parquet_raw_archive_log.parquet").replace("\\", "/")
        con.sql("COPY _pq_archive_log TO '" + lp + "' (FORMAT PARQUET)")
        dr.copy(ltmp, "", overwrite=True)
    _log("archive log rewritten: " + str(con.sql(
        "SELECT count(*) FROM _pq_archive_log").fetchone()[0]) + " row(s)")


def run_in_notebook():
    import duckdb
    import duckrun

    sf = scale_factor()
    tmp = os.environ.get("TMPDIR") or tempfile.gettempdir()
    work = os.path.join(tmp, "tpcds_sf" + str(sf))
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work, exist_ok=True)

    free = shutil.disk_usage(tmp).free
    _log("sf=" + str(sf) + " cpus=" + str(os.cpu_count()) + " free disk "
         + format(free / 1024 ** 3, ",.0f") + " GiB at " + tmp)
    # dsdgen has no chunking: it builds every table before returning, so the working set is the
    # whole scale factor. An on-disk database plus a spill directory is what lets SF100 finish on a
    # node that cannot hold it in memory, and this refuses early rather than dying an hour in.
    need_gb = {1: 8, 10: 40, 100: 260}[sf]
    if free < need_gb * 1024 ** 3:
        raise SystemExit("sf" + str(sf) + " needs about " + str(need_gb) + " GiB free under " + tmp
                         + "; found " + format(free / 1024 ** 3, ",.0f")
                         + " GiB. Dispatch with more cores, which is what sizes the node.")

    con = duckdb.connect(os.path.join(work, "gen.duckdb"))
    con.sql("SET threads TO " + str(os.cpu_count()))
    con.sql("SET temp_directory = '" + os.path.join(work, "spill").replace("\\", "/") + "'")
    con.sql("SET preserve_insertion_order = false")
    _log("duckdb " + duckdb.__version__)

    verdict = generate(con, sf, work)
    dr = duckrun.connect(FILES_PATH, read_only=False)
    land(dr, dr.con, sf, work, verdict)
    con.close()
    shutil.rmtree(work, ignore_errors=True)

    # LAST LINE, DELIBERATELY. duckrun hands the runner the notebook's log with the FIRST 400,000
    # characters dropped, and there is no structured result channel, so the verdict has to be
    # scraped from the tail. Anything printed after this could push it out.
    print(RESULT_PREFIX + json.dumps(verdict, default=str), flush=True)


# --------------------------------------------------------------------------- the runner half

def already_landed(sf):
    """True when the log already describes every table at this scale factor."""
    import duckrun
    dr = duckrun.connect(FILES_PATH, read_only=True)
    log_path = FILES_PATH + "/parquet_raw_archive_log.parquet"
    if not dr.con.sql("SELECT count(*) FROM glob('" + log_path + "')").fetchone()[0]:
        return False
    got = {r[0] for r in dr.con.sql(
        "SELECT DISTINCT source_type FROM read_parquet('" + log_path + "') WHERE etag = 'sf"
        + str(sf) + "'").fetchall()}
    return not (set(TABLES) - got)


def relay(sf):
    """Run this same script inside a throwaway Fabric notebook and relay its outcome."""
    import duckrun
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".github", "scripts"))
    import datasets
    import record

    ws = os.environ["WS_ID"]
    cores = int(os.environ.get("FABRIC_CORES", "8"))
    if sf >= 100 and cores < 32:
        raise SystemExit("sf100 needs FABRIC_CORES >= 32 (dsdgen holds the whole scale factor); "
                         "dispatch with cores=32 or more, got " + str(cores))
    # Named for what it is and given a random suffix for the same reason fabric_run.py's is: Fabric
    # keeps a deleted item's display name reserved for minutes and the create is not retried.
    name = "tpcds-gen-sf" + str(sf) + "-" + uuid.uuid4().hex[:8]
    env = {k: os.environ[k] for k in ("FILES_PATH", "download_limit", "DATASET", "TPCDS_FORCE")
           if os.environ.get(k)}
    _log("relaying into Fabric: notebook=" + name + " cores=" + str(cores)
         + " forwarding " + ", ".join(sorted(env)))

    def remember(item_id):
        if not item_id:
            return
        try:
            record.merge({"items": {str(item_id).upper(): {
                "role": "compute", "kind": "Notebook", "name": name,
                # `landing`, not the run's engine: this notebook generates the input every engine
                # then reads, so folding its CU into whichever engine the dispatch happened to
                # build would attribute the generator's cost to one arm of the comparison.
                "engine": "landing", "created": True,
                "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}}})
        except Exception as ex:                                    # noqa: BLE001
            _log("could not record the generator notebook (" + type(ex).__name__ + ": " + str(ex) + ")")

    def confirm_deleted(item_id):
        """Prove the throwaway notebook is gone, and go red if it is not.

        duckrun deletes it in a `finally`, so the delete is attempted on the success and the failure
        path alike -- but `_delete_item` is BEST-EFFORT: a non-2xx or a thrown exception only logs
        `warning: could not delete temp notebook`. An item that is still listed is still billable,
        and a warning inside a green log is not something anyone reads.

        The workflow would eventually catch it -- `provision.py teardown` deletes role `compute` by
        GUID and exits non-zero if it survives -- but leaning on that is wrong twice over. Generating
        is a ONE-OFF meant to be runnable from a laptop with no workflow around it; and even inside a
        run, the notebook would sit billable from `land` until the teardown at the very end, for a
        job that finished an hour earlier.

        So this polls for the 404 itself. It is deliberately NOT `provision.drop_guid`, which does
        exactly this and is the obvious reuse: provision.py reads `sys.argv[1]` at module level and
        runs its whole mode dispatch on import, so importing it here would provision something.
        """
        if not item_id:
            return
        import requests
        from duckrun import auth
        api = "https://api.fabric.microsoft.com/v1/workspaces/" + ws + "/items/" + str(item_id)
        hdr = {"Authorization": "Bearer " + auth.get_fabric_token()}
        try:
            requests.delete(api, headers=hdr, timeout=60)
        except Exception:                                          # noqa: BLE001
            pass                       # duckrun has almost certainly deleted it already; the poll decides
        # Fabric accepts a DELETE asynchronously (202), so the only answer that ends this loop is a
        # 404. A thrown request is read as "not gone yet", never as evidence it went.
        for _ in range(60):
            try:
                if requests.get(api, headers=hdr, timeout=60).status_code == 404:
                    try:
                        record.item(str(item_id).upper(), "compute", "Notebook", name,
                                    deleted=record.now())
                    except Exception:                              # noqa: BLE001
                        pass
                    _log("generator notebook " + str(item_id) + " confirmed deleted")
                    return
            except Exception:                                      # noqa: BLE001
                pass
            time.sleep(5)
        raise SystemExit("generator notebook " + name + " (" + str(item_id) + ") is STILL LISTED "
                         "and therefore STILL BILLABLE -- delete it in the workspace")

    try:
        res = duckrun.workspace(ws).run_python(
            ".", entry="download_tpcds.py", args=["--in-notebook"], name=name,
            lakehouse=datasets.spec()["landing"], env=env, cores=cores, pip=["duckrun>=0.4.50"])
    except BaseException as ex:
        item = getattr(ex, "item_id", None)
        remember(item)
        # Best-effort on the failure path: the run is already going to fail with a real cause, and
        # raising a teardown error here would replace it. The workflow's own teardown is the net.
        try:
            confirm_deleted(item)
        except BaseException:                                      # noqa: BLE001
            _log("WARNING the generator notebook may still be listed; teardown will catch it")
        raise
    remember(res.item_id)
    confirm_deleted(res.item_id)

    verdict = None
    for line in (res.log or "").splitlines():
        if line.startswith(RESULT_PREFIX):
            try:
                verdict = json.loads(line[len(RESULT_PREFIX):])
            except ValueError:
                pass
    if verdict:
        try:
            record.merge({"generator": verdict})
        except Exception:                                          # noqa: BLE001
            pass
        summarise(verdict)
    elif res.success:
        _log("WARNING the notebook succeeded but printed no result line; the record carries no "
             "row counts for this landing")

    _log("generator success=" + str(res.success) + " returncode=" + str(res.returncode))
    return 0 if res.success else 1


def summarise(v):
    """A row-count table in the job summary, so the paper comparison is visible without the log."""
    out = os.environ.get("GITHUB_STEP_SUMMARY")
    lines = ["### TPC-DS sf" + str(v["sf"]) + " landed", "",
             "| table | rows | paper (Table 4.3.1) | match |", "|---|---:|---:|:---:|"]
    for t in TABLES:
        paper = (v.get("paper") or {}).get(t)
        m = (v.get("matches_paper") or {}).get(t)
        lines.append("| `" + t + "` | " + format(v["landed"][t], ",") + " | "
                     + (format(paper, ",") if paper else "n/a") + " | "
                     + ("yes" if m else ("no" if m is False else "n/a")) + " |")
    bad = {k: n for k, n in (v.get("orphans") or {}).items() if n}
    lines += ["", "Referential integrity: "
              + ("no orphans on any of the 13 relationships." if not bad else "**orphans** " + str(bad))]
    text = "\n".join(lines)
    if out:
        with open(out, "a", encoding="utf-8") as fh:
            fh.write(text + "\n")
    sys.stderr.write(text + "\n")


def status():
    """What is landed, per scale factor, without generating anything.

    The generation is a one-off, so the question anyone actually has is "has it already happened",
    and answering it should cost nothing and create nothing."""
    import duckrun
    dr = duckrun.connect(FILES_PATH, read_only=True)
    log_path = FILES_PATH + "/parquet_raw_archive_log.parquet"
    if not dr.con.sql("SELECT count(*) FROM glob('" + log_path + "')").fetchone()[0]:
        _log("no archive log at " + log_path + " -- nothing landed here yet")
        return 0
    rows = dr.con.sql(
        "SELECT etag, count(DISTINCT source_type) AS tables, count(*) AS files, sum(row_count) "
        "AS rows FROM read_parquet('" + log_path + "') WHERE source_type IN ('"
        + "', '".join(TABLES) + "') GROUP BY etag ORDER BY etag").fetchall()
    if not rows:
        _log("the log has no tpcds rows -- nothing landed here yet")
        return 0
    for etag, tables, files, rowcount in rows:
        complete = "complete" if tables == len(TABLES) else "INCOMPLETE (" + str(tables) + "/10)"
        _log(str(etag) + ": " + complete + ", " + str(files) + " file(s), "
             + format(int(rowcount or 0), ",") + " rows")
    return 0


def main():
    if IN_NOTEBOOK:
        run_in_notebook()
        return 0
    if STATUS:
        return status()
    sf = scale_factor()
    _log("sf=" + str(sf) + " files_path=" + FILES_PATH + " force=" + str(FORCE))
    if not FORCE and already_landed(sf):
        _log("sf" + str(sf) + " is already landed for all " + str(len(TABLES))
             + " tables; nothing to do. Set TPCDS_FORCE=1 to regenerate.")
        return 0
    return relay(sf)


if __name__ == "__main__":
    raise SystemExit(main())
