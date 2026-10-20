"""Generate the white paper's TPC-DS subset with tpcgen-cli and land it in OneLake.

THE ONLY GENERATOR HERE. The other five scripts in this directory download a public archive; this
one MAKES its input. Everything else about it follows the same idiom -- an archive-log parquet at the
Files root, one row per landed file, so a re-run only does what is new -- because the models, the
macros and stats.py all read that log and know nothing about where the bytes came from.

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

THE GENERATOR IS tpcgen-cli (datafusion-contrib/tpcgen-rs), PINNED EXACTLY, AND IT REPLACED DuckDB's
`dsdgen` ON 2026-10-21. dsdgen materialises a whole scale factor before returning, so SF100 needed a
throwaway Fabric notebook at `cores=32` with ~260 GiB of disk -- which made `land` the one job here
that spent Fabric compute, and carried a relay, a notebook-deletion poll and a cgroup memory pin.
tpcgen streams one table PART at a time in constant memory (SF10's two facts: 23s, 270 MB RSS on a
free runner), so this runs on the GitHub runner like every other downloader and spends no capacity.
⚠️ **IT IS NOT THE SAME DATA, AND THAT WAS MEASURED BEFORE THE SWITCH.** Runs 39514066032 and
39514324649 put both generators side by side: row counts per table agree with the TPC-DS spec and
with each other, but nearly every fact value, the facts' NULL pattern, and several dimension text
columns differ -- so the null drop keeps a different number of rows. At SF10 tpcgen lands
26,208,141 / 14,257,679 against the paper's 26,206,837 / 14,257,451, which DuckDB's dsdgen
reproduced exactly. Every tpcds record before the switch measured the dsdgen generation; the
mart's `total_rows` moved with it, so the page reads the two as separate generations rather than
blending them. The pin is the data's identity: a different tpcgen version may emit different rows,
so `tpcgen()` refuses a binary that is not exactly `TPCGEN_VERSION`.

THE PAPER'S SCRIPT IS NOT PUBLISHED, so the null rule is a RECONSTRUCTION, and the comparison against
its Table 4.3.1 is REPORTED, never fatal -- see `verdict`. A raw count that disagrees with the TPC-DS
spec IS fatal: that is the generator not producing TPC-DS, or two parts overlapping.

`download_limit` IS THE SCALE FACTOR, not a file count. cms already reinterprets it (program years);
this reinterprets it again, and the `plan` job refuses anything outside SCALE_FACTORS for free,
before a leg spends capacity -- the dispatch form's default is 200, which would otherwise ask for
SF200.

IT IS A ONE-OFF. A scale factor is generated once and read by every engine, every dispatch,
afterwards. The `land` job only calls this when `skip_download=false`, which is off by default; and
it skips itself when the archive log already carries `etag = sf<N>` for all ten tables, so even an
explicit re-dispatch is a no-op unless `TPCDS_FORCE=1` -- the `regenerate` dispatch input.

EVERYTHING IS GENERATED AND CHECKED LOCALLY BEFORE ONELAKE IS TOUCHED. Each fact part is generated,
customised to its landed parquet and its raw file deleted, so the runner's disk holds the landed
scale factor (21.4 GB at SF100) plus one raw part. Only once every count has passed does `land()`
wipe and upload, so a refused generation leaves the previous archive exactly as it was.

THE TABLE UPLOADS GO `overwrite=False`, AND THAT IS THE WHOLE SF100 FIX. duckrun turns
`overwrite=True` on an abfss target into a single-shot `Put Blob` with the file buffered in memory
-- the only thing OneLake honours as an atomic REPLACE -- and OneLake answers a PUT of a ~400 MB
store_sales file with a bare 500. `wipe()` empties the folder immediately before, so there is
nothing to replace and the streaming multipart path is both correct and unbounded. See the block
comment in `land()`; do not "tidy" that keyword back.

THE UPLOAD IS ALSO RETRIED AND THE GENERATION IS NOT, which is the right way round: everything
before `land()` is deterministic and re-runnable, while the upload is the last step and the one that
can fail on something nobody controls. `_retry_onelake()` wraps the three calls that talk to OneLake
-- the recursive clear, the per-TABLE copy and the archive-log rewrite -- with six attempts at a
widening backoff, retrying a 5xx or a dropped connection and never a 4xx.
⚠️ **THE RETRY DID NOT SAVE RUN 34550578120 AND COULD NOT HAVE.** All six attempts re-ran the
identical single-shot PUT and failed identically; obstore's own ten internal retries burned out in
3.45s inside each one. A 500 that repeats at 15/30/45/60/75s is a LIMIT, not weather -- and run
33742041035's "ten OneLake 500s", recorded as a transient, was almost certainly this same limit.
Keep the retry for real transients; it is also what made the deterministic shape legible.

Run it once per scale factor and never think about it again:

    pip install tpcgen-cli==4.0.0
    FILES_PATH=<abfss .../dbt_tpcds_landing/Files> download_limit=10 python download_tpcds.py

    python download_tpcds.py --status      # what is landed, without generating anything

Env in:
  FILES_PATH        the landing lakehouse's Files root (set by `provision.py land`)
  download_limit    the scale factor: 1, 10 or 100
  TPCDS_FORCE       "1" to re-land a scale factor that is already logged
  TMPDIR            where the scale factor is staged before upload (needs ~40 GiB free at SF100)
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

STATUS = "--status" in sys.argv

FILES_PATH = os.environ.get("FILES_PATH", "/tmp/landing").rstrip("/")
SF_RAW = (os.environ.get("download_limit") or "10").strip()
FORCE = os.environ.get("TPCDS_FORCE", "").strip() == "1"

# EXACT, and the data's identity rather than a dependency floor: the rows are whatever this version
# of the generator emits. benchmark.yml installs the same spelling in `checks` and `land`.
TPCGEN_VERSION = "4.0.0"

# The scale factors this project will run. SF1 is the local smoke; SF10 is the paper's smallest
# volume and the default here; SF100 is the volume its Direct Lake findings turn on. SF1000 is
# absent because nothing here has measured a runner landing ~210 GB, not because the generator
# cannot stream it.
SCALE_FACTORS = ("1", "10", "100")

# The ten tables of the paper's subset (section 4.3). TPC-DS has 24; only these are generated.
TABLES = ("store_sales", "catalog_sales", "catalog_page", "customer_address",
          "customer_demographics", "date_dim", "item", "promotion", "ship_mode", "store")
FACTS = ("store_sales", "catalog_sales")

# Parts per fact. tpcgen generates one part at a time (`--parts N --part i`), so this is both the
# landed file count and the runner's working-set divisor: SF100 keeps the 32 files of ~400 MB that
# dsdgen's PER_THREAD_OUTPUT produced at cores=32. SF1 is one part, written whole.
FACT_PARTS = {1: 1, 10: 8, 100: 32}

# The landed schema of every table: the TPC-DS column order, plus the paper's two additions.
# MIRRORS macros/tpcds_columns.sql, and `.github/scripts/test_tpcds_columns.py` asserts the two
# agree column for column and in order.
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

# The landed TYPE of every non-VARCHAR column, pinned to what DuckDB's dsdgen wrote before the
# switch so the parquet the models, the semantic model and three engines read did not change shape
# with the generator. tpcgen emits Int32 for every integer, where dsdgen gave BIGINT for all but
# `cd_dep_college_count`; `customise_sql()` casts every column to this, and
# `test_tpcds_landing_sql.py` asserts the landed DESCRIBE equals it. Anything absent is VARCHAR.
_D72 = "DECIMAL(7,2)"
TYPES = {
    "store_sales": dict({c: "BIGINT" for c in COLUMNS["store_sales"][:11]},
                        **{c: _D72 for c in COLUMNS["store_sales"][11:23]},
                        cache_buster="INTEGER"),
    "catalog_sales": dict({c: "BIGINT" for c in COLUMNS["catalog_sales"][:19]},
                          **{c: _D72 for c in COLUMNS["catalog_sales"][19:34]},
                          cache_buster="INTEGER"),
    "catalog_page": {"cp_catalog_page_sk": "BIGINT", "cp_start_date_sk": "BIGINT",
                     "cp_end_date_sk": "BIGINT", "cp_catalog_number": "BIGINT",
                     "cp_catalog_page_number": "BIGINT"},
    "customer_address": {"ca_address_sk": "BIGINT", "ca_gmt_offset": "DECIMAL(5,2)"},
    "customer_demographics": {"cd_demo_sk": "BIGINT", "cd_purchase_estimate": "BIGINT",
                              "cd_dep_count": "BIGINT", "cd_dep_employed_count": "BIGINT",
                              "cd_dep_college_count": "INTEGER"},
    "date_dim": dict({c: "BIGINT" for c in (
        "d_date_sk", "d_month_seq", "d_week_seq", "d_quarter_seq", "d_year", "d_dow", "d_moy",
        "d_dom", "d_qoy", "d_fy_year", "d_fy_quarter_seq", "d_fy_week_seq", "d_first_dom",
        "d_last_dom", "d_same_day_ly", "d_same_day_lq", "d_date_sk_1")}, d_date="DATE"),
    "item": {"i_item_sk": "BIGINT", "i_rec_start_date": "DATE", "i_rec_end_date": "DATE",
             "i_current_price": _D72, "i_wholesale_cost": _D72, "i_brand_id": "BIGINT",
             "i_class_id": "BIGINT", "i_category_id": "BIGINT", "i_manufact_id": "BIGINT",
             "i_manager_id": "BIGINT"},
    "promotion": {"p_promo_sk": "BIGINT", "p_start_date_sk": "BIGINT", "p_end_date_sk": "BIGINT",
                  "p_item_sk": "BIGINT", "p_cost": "DECIMAL(15,2)", "p_response_target": "BIGINT"},
    "ship_mode": {"sm_ship_mode_sk": "BIGINT"},
    "store": {"s_store_sk": "BIGINT", "s_rec_start_date": "DATE", "s_rec_end_date": "DATE",
              "s_closed_date_sk": "BIGINT", "s_number_employees": "BIGINT",
              "s_floor_space": "BIGINT", "s_market_id": "BIGINT", "s_division_id": "BIGINT",
              "s_company_id": "BIGINT", "s_gmt_offset": "DECIMAL(5,2)",
              "s_tax_percentage": "DECIMAL(5,2)"},
}

# Where tpcgen's column name differs from the landed one. The TPC-DS spec itself spells this column
# `s_tax_precentage` and tpcgen follows the spec; DuckDB's dsdgen corrected it, and the models, the
# semantic model and `macros/tpcds_columns.sql` all carry the corrected name, so it is renamed here.
SOURCE_NAMES = {"store": {"s_tax_percentage": "s_tax_precentage"}}

# TPC-DS row counts BEFORE customisation, per the v4.0.0 spec, checked EXACTLY for every table --
# tpcgen and dsdgen both hit them to the row. A MISMATCH HERE IS FATAL: it means the generator did
# not produce TPC-DS, or two fact parts overlapped.
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

# The paper's Table 4.3.1 -- row counts AFTER customisation. DuckDB's dsdgen hit both facts exactly;
# tpcgen does not (its NULL pattern differs, see the docstring), so the facts now report DIFFERS by
# design and only the dimensions are expected to match. SF1 is absent because the paper does not
# test it, so a SF1 run reports `null` rather than a false verdict.
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


def scale_factor():
    """The scale factor, refused unless it is one this project will run.

    Refuses rather than clamping, for the same reason `datasets.selected()` refuses an unknown
    dataset: `download_limit` means files everywhere else and the form's default is 200, so a value
    that fell through would ask for a volume nobody chose and nothing would say so."""
    if SF_RAW not in SCALE_FACTORS:
        raise SystemExit(
            "download_limit is the SCALE FACTOR for tpcds and must be one of "
            + ", ".join(SCALE_FACTORS) + "; got " + repr(SF_RAW) + ".")
    return int(SF_RAW)


def _landed(table, col):
    """`CAST("<source name>" AS <landed type>) AS "<landed name>"` for one raw column."""
    src = SOURCE_NAMES.get(table, {}).get(col, col)
    return 'CAST("' + src + '" AS ' + TYPES[table].get(col, "VARCHAR") + ') AS "' + col + '"'


def customise_sql(table, src=None):
    """The SELECT that turns a raw tpcgen table into the paper's table (section 4.5).

    Kept as a pure function of the table name so `.github/scripts/test_tpcds_landing_sql.py` can
    execute the REAL text against a tiny generation in local DuckDB. That test is the one that would
    have caught the cms landing bug for free, and this SQL is the only place in the project where a
    value is edited rather than passed through. It also pins the landed names and types, which the
    generator does not choose to match."""
    src = src or table
    raw = [c for c in COLUMNS[table] if c not in ("cache_buster", "d_date_sk_1")]
    cols = ", ".join(_landed(table, c) for c in raw)
    if table in FACTS:
        # "All nulls in both Fact Tables were removed" -- read as: drop any row with a NULL in any
        # column. The reconstruction is validated by the row counts, not by this comment.
        not_null = " AND ".join('"' + SOURCE_NAMES.get(table, {}).get(c, c) + '" IS NOT NULL'
                                for c in raw)
        return ("SELECT " + cols + ", CAST(1 AS INTEGER) AS cache_buster "
                "FROM " + src + " WHERE " + not_null)
    if table == "date_dim":
        return ("SELECT " + cols + ", CAST(d_date_sk - " + str(DATE_SHIFT_DAYS)
                + " AS BIGINT) AS d_date_sk_1 FROM " + src
                + " WHERE d_date BETWEEN DATE '" + DATE_LO + "' AND DATE '" + DATE_HI + "'")
    return "SELECT " + cols + " FROM " + src


def _quote_list(files):
    return "[" + ", ".join("'" + f.replace("\\", "/") + "'" for f in files) + "]"


# --------------------------------------------------------------------------- generate

def _log(msg):
    print("[tpcds] " + msg, flush=True)


# Ten consecutive OneLake 500s is what ended run 33742041035, partway through uploading SF100 --
# after generation, after the customisation and after the whole scale factor had been written to
# local parquet, i.e. the most expensive thing this script does, failing last. THE UPLOAD IS THE ONE PART
# WITH NO NATURAL RETRY: the generate half is deterministic and re-runnable, the archive log is
# rewritten last on purpose, but a `dr.copy` that raises throws away hours of compute over a
# transient.
_UPLOAD_ATTEMPTS = 6


def _retry_onelake(what, fn):
    """Call `fn`, retrying a TRANSIENT OneLake failure with a widening backoff.

    Six attempts at 15s x n is ~5 minutes of patience, which is the shape `fabric_build.py` already
    uses for OneLake commit conflicts. A 4xx is NOT retried: that is a refusal (a bad path, an
    expired token, a permission), and repeating it just spends the budget before failing anyway.
    Anything that is not an HTTP status at all -- a dropped connection, a read timeout -- IS
    retried, because that is the same transient wearing a different exception.

    `what` names the table, so a partial SF100 upload says where it stopped rather than leaving the
    reader to count files in OneLake.
    """
    for attempt in range(1, _UPLOAD_ATTEMPTS + 1):
        try:
            return fn()
        except Exception as ex:                                    # noqa: BLE001
            status = getattr(getattr(ex, "response", None), "status_code", None)
            if status is None:
                status = getattr(ex, "status_code", None)
            if status is not None and 400 <= int(status) < 500:
                raise
            if attempt == _UPLOAD_ATTEMPTS:
                raise
            backoff = 15 * attempt
            _log("  " + what + ": " + type(ex).__name__ + " "
                 + str(ex).splitlines()[0][:160] + " -- retrying in " + str(backoff)
                 + "s (attempt " + str(attempt) + "/" + str(_UPLOAD_ATTEMPTS) + ")")
            time.sleep(backoff)


def tpcgen(sf, out, table, parts=None, part=None):
    """Run the pinned tpcgen-cli for one table (or one part of one) and return its parquet files.

    Refuses a missing binary or any version but `TPCGEN_VERSION`: the rows are a function of the
    generator version, so a silently newer one would land a different dataset under the same etag.
    `--compat c` is the default and is spelled out anyway -- it is the mode tpcgen tests against the
    C dsdgen, and `trino` produces different rows."""
    exe = shutil.which("tpcgen-cli")
    if not exe:
        raise SystemExit("tpcgen-cli is not installed; pip install tpcgen-cli==" + TPCGEN_VERSION)
    got = subprocess.run([exe, "--version"], capture_output=True, text=True, check=True).stdout.split()
    if got[-1:] != [TPCGEN_VERSION]:
        raise SystemExit("tpcgen-cli " + " ".join(got[1:]) + " found, " + TPCGEN_VERSION
                         + " required: another version may generate different rows")
    shutil.rmtree(out, ignore_errors=True)
    cmd = [exe, "tpcds", "parquet", "-s", str(sf), "--tables", table, "--compat", "c",
           "--output-dir", out, "--overwrite", "--quiet", "--no-progress"]
    if parts:
        cmd += ["--parts", str(parts)]
        if part:
            cmd += ["--part", str(part)]
    subprocess.run(cmd, check=True)
    files = sorted(os.path.join(root, f) for root, _d, fs in os.walk(out)
                   for f in fs if f.endswith(".parquet"))
    if not files:
        raise SystemExit("tpcgen-cli wrote no parquet for " + table + " " + " ".join(cmd[3:]))
    return files


def generate(con, sf, work):
    """tpcgen -> customise -> check -> local parquet under <work>/<table>. Returns the verdict.

    Dimensions first, held in memory as `<t>_landed` (the largest, customer_demographics, is 1.9M
    rows at every scale factor) so each fact part can be probed for orphans as it is written. Facts
    are generated ONE PART AT A TIME and the raw part is deleted the moment its landed parquet
    exists, which is what keeps SF100 inside a free runner's disk."""
    raw_counts, landed, files, orphans = {}, {}, {}, {}
    gen = os.path.join(work, "_raw")
    t0 = time.time()
    for t in TABLES:
        d = os.path.join(work, t)
        os.makedirs(d, exist_ok=True)
        if t in FACTS:
            continue
        src = "read_parquet(" + _quote_list(tpcgen(sf, gen, t)) + ")"
        raw_counts[t] = con.sql("SELECT count(*) FROM " + src).fetchall()[0][0]
        con.sql("CREATE OR REPLACE TABLE " + t + "_landed AS " + customise_sql(t, src))
        landed[t] = con.sql("SELECT count(*) FROM " + t + "_landed").fetchall()[0][0]
        con.sql("COPY " + t + "_landed TO '" + os.path.join(d, t + ".parquet").replace("\\", "/")
                + "' (FORMAT parquet, COMPRESSION snappy, ROW_GROUP_SIZE 1000000)")
        files[t] = [t + ".parquet"]

    for fact in FACTS:
        n = FACT_PARTS.get(sf, 1)
        raw_counts[fact] = landed[fact] = 0
        for _f, col, _d, _k in RELATIONSHIPS:
            if _f == fact:
                orphans[fact + "." + col] = 0
        files[fact] = []
        for i in range(1, n + 1):
            parts = tpcgen(sf, gen, fact, parts=n, part=i) if n > 1 else tpcgen(sf, gen, fact)
            src = "read_parquet(" + _quote_list(parts) + ")"
            raw_counts[fact] += con.sql("SELECT count(*) FROM " + src).fetchall()[0][0]
            name = fact + "_" + format(i - 1, "04d") + ".parquet"
            out = os.path.join(work, fact, name).replace("\\", "/")
            con.sql("COPY (" + customise_sql(fact, src) + ") TO '" + out
                    + "' (FORMAT parquet, COMPRESSION snappy, ROW_GROUP_SIZE 1000000)")
            shutil.rmtree(gen, ignore_errors=True)
            part = "read_parquet('" + out + "')"
            landed[fact] += con.sql("SELECT count(*) FROM " + part).fetchall()[0][0]
            for _f, col, dim, key in RELATIONSHIPS:
                if _f == fact:
                    orphans[fact + "." + col] += con.sql(
                        "SELECT count(*) FROM " + part + " f WHERE f." + col + " IS NOT NULL AND "
                        "NOT EXISTS (SELECT 1 FROM " + dim + "_landed d WHERE d." + key + " = f."
                        + col + ")").fetchall()[0][0]
            files[fact].append(name)
            _log("  " + fact + " part " + str(i) + "/" + str(n) + ": " + format(landed[fact], ",")
                 + " landed so far")
    for t in TABLES:
        if t not in FACTS:
            con.sql("DROP TABLE " + t + "_landed")
    _log("generated sf" + str(sf) + " in " + format(time.time() - t0, ",.0f") + "s")

    # EXACT for the facts too, which dsdgen's ±1% never was: tpcgen matched the spec to the row at
    # SF1 and SF10, and generating part by part means an overlap or a gap between parts shows up
    # here as a count off by a part's worth -- the one fault this layout introduces.
    spec = EXPECTED.get(sf, {})
    problems = [t + ": got " + str(raw_counts[t]) + ", spec " + str(spec[t])
                for t in TABLES if t in spec and raw_counts[t] != spec[t]]
    if problems:
        raise SystemExit("raw tpcgen counts disagree with the TPC-DS spec -- " + "; ".join(problems))
    _log("raw counts match the spec for all " + str(len(TABLES)) + " tables")

    matches, paper = None, PAPER_ROWS.get(sf)
    if paper:
        matches = {t: landed[t] == paper[t] for t in TABLES}
        for t in TABLES:
            flag = "ok " if matches[t] else "DIFFERS"
            _log("  " + t.ljust(23) + format(landed[t], ">15,") + "  paper "
                 + format(paper[t], ">15,") + "  " + flag)
        bad = [t for t in TABLES if not matches[t]]
        _log("vs the paper's Table 4.3.1: " + ("all match" if not bad else "DIFFERS on " + str(bad)
             + " -- expected since the tpcgen switch: its NULL pattern is not dsdgen's"))
    else:
        _log("no published row counts for sf" + str(sf) + "; skipping the paper comparison")

    broken = {k: v for k, v in orphans.items() if v}
    _log("referential integrity over " + str(len(RELATIONSHIPS)) + " relationships: "
         + ("0 orphans everywhere" if not broken else "ORPHANS " + str(broken)
            + " -- relyOnReferentialIntegrity would drop these rows in every query"))

    return {"sf": sf, "generator": "tpcgen-cli " + TPCGEN_VERSION, "raw": raw_counts,
            "landed": landed, "paper": paper, "matches_paper": matches, "orphans": orphans,
            "files": files}


# --------------------------------------------------------------------------- land

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
        def _delete():
            r = requests.delete("https://" + host + "/" + fs + "/" + path,
                                params={"recursive": "true"},
                                headers={"Authorization": "Bearer " + auth.get_onelake_token(),
                                         "x-ms-version": "2023-11-03"}, timeout=300)
            # A 5xx is raised so `_retry_onelake` can see it; 4xx stays a SystemExit, because a
            # refusal here means the path or the token is wrong and no amount of waiting fixes it.
            if r.status_code >= 500:
                raise RuntimeError("clear " + target + ": " + str(r.status_code)
                                   + " " + r.text[:200])
            if r.status_code not in (200, 202, 404):
                raise SystemExit("could not clear " + target + ": " + str(r.status_code)
                                 + " " + r.text[:200])

        _retry_onelake("clear " + rel, _delete)

    log_path = FILES_PATH + "/parquet_raw_archive_log.parquet"
    if con.sql("SELECT count(*) FROM glob('" + log_path + "')").fetchall()[0][0]:
        con.sql("CREATE OR REPLACE TEMP TABLE _pq_archive_log AS SELECT source_type, "
                "source_filename, archive_path, archived_at, row_count, source_url, etag, "
                "file_stem, columns FROM read_parquet('" + log_path + "') WHERE file_stem IS NOT NULL")
    else:
        con.sql("CREATE OR REPLACE TEMP TABLE _pq_archive_log (" + LOG_COLUMNS + ")")
    # This dataset owns exactly its ten source_types; another dataset's rows in the same lakehouse
    # are left alone, and this run's own previous rows go, so the log describes one scale factor.
    con.sql("DELETE FROM _pq_archive_log WHERE source_type IN ('" + "', '".join(TABLES) + "')")

    url = "tpcgen://tpcds?sf=" + str(sf) + "&tpcgen-cli=" + TPCGEN_VERSION
    for t in TABLES:
        wipe("parquet_raw/" + t)
        # ⚠️ `overwrite=False`, AND THE DESTINATION IS EMPTY BECAUSE `wipe()` JUST DELETED IT. That
        # pairing is the whole point and inverting either half breaks SF100 outright.
        #
        # duckrun: `single_shot = overwrite and remote.is_abfss(base)`, and `single_shot` means
        # `obstore.put(..., use_multipart=False)` -- the whole file BUFFERED IN MEMORY and sent as
        # one `Put Blob`. That exists because OneLake cannot replace a committed blob any other way
        # (multipart's `Put Block` draws 409, and obstore's delete is upstream-broken there), so it
        # is the right path for REPLACING a file and the wrong one for writing a big new one:
        # store_sales at SF100 is 32 files of roughly 400 MB and OneLake answers a single PUT that
        # size with a bare 500. `overwrite=False` takes duckrun's streaming multipart path, which
        # its own docstring says handles multi-GB blobs.
        #
        # Run 34550578120 is the measurement, and it is also why the retry above did not save it:
        # every one of the six attempts re-ran the identical single-shot PUT and failed identically,
        # with obstore's own ten internal retries burning out in 3.45s each time. A 500 that repeats
        # at 15/30/45/60/75s intervals is not weather. Run 33742041035 -- "ten OneLake 500s" -- was
        # almost certainly this same limit rather than the transient it was recorded as.
        #
        # `overwrite=False` is only safe because the wipe ran: duckrun SKIPS a key that already
        # exists under it, so against a populated folder this would land a stale mix silently. The
        # wipe is what makes the folder empty, and it raises on any status but 200/202/404, so it
        # cannot fail quietly. It also makes the retry genuinely incremental — a second attempt
        # skips what already landed instead of re-sending it.
        #
        # Per TABLE, so a retry that succeeds re-sends one table's bytes and not the scale factor's.
        _retry_onelake("upload " + t,
                       lambda t=t: dr.copy(os.path.join(work, t), "parquet_raw/" + t,
                                           overwrite=False))
        for f in verdict["files"][t]:
            stem = f[:-len(".parquet")]
            rows = con.sql("SELECT count(*) FROM read_parquet('"
                           + os.path.join(work, t, f).replace("\\", "/") + "')").fetchall()[0][0]
            con.sql(
                "INSERT INTO _pq_archive_log VALUES ('" + t + "', '" + f + "', '/" + t + "/" + f
                + "', now(), " + str(rows) + ", '" + url + "', 'sf" + str(sf) + "', '" + stem
                + "', '" + ",".join(COLUMNS[t]) + "')")
        _log("  landed " + t.ljust(23) + str(len(verdict["files"][t])) + " file(s)")

    with tempfile.TemporaryDirectory() as ltmp:
        lp = os.path.join(ltmp, "parquet_raw_archive_log.parquet").replace("\\", "/")
        con.sql("COPY _pq_archive_log TO '" + lp + "' (FORMAT PARQUET)")
        # LAST, and by design: the log is the watermark, so until this lands the archive still
        # describes the previous state rather than a half-landed one. It is also the smallest write
        # here and the one it would be maddening to lose the whole run to.
        #
        # `overwrite=True` STAYS HERE, unlike the table uploads above: this genuinely REPLACES an
        # existing blob, which on OneLake only the single-shot `Put Blob` can do. It is a few KB, so
        # the size limit that rules that path out for a 400 MB fact file does not reach it.
        _retry_onelake("archive log", lambda: dr.copy(ltmp, "", overwrite=True))
    _log("archive log rewritten: " + str(con.sql(
        "SELECT count(*) FROM _pq_archive_log").fetchall()[0][0]) + " row(s)")


def run(sf):
    """Generate and check locally, then land. Runs where it is called -- the GitHub runner in CI."""
    import duckdb
    import duckrun

    tmp = os.environ.get("TMPDIR") or tempfile.gettempdir()
    work = os.path.join(tmp, "tpcds_sf" + str(sf))
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work, exist_ok=True)

    # The whole LANDED scale factor is staged before upload (21.4 GB at SF100) plus one raw part, so
    # this refuses early rather than dying on a full disk partway through the facts.
    free = shutil.disk_usage(tmp).free
    need_gb = {1: 2, 10: 8, 100: 40}.get(sf, 1)
    _log("sf=" + str(sf) + " cpus=" + str(os.cpu_count()) + " free disk "
         + format(free / 1024 ** 3, ",.0f") + " GiB at " + tmp)
    if free < need_gb * 1024 ** 3:
        raise SystemExit("sf" + str(sf) + " needs about " + str(need_gb) + " GiB free under " + tmp
                         + "; found " + format(free / 1024 ** 3, ",.0f") + " GiB. Point TMPDIR "
                         "at a bigger disk.")

    con = duckdb.connect()
    con.sql("SET preserve_insertion_order = false")
    con.sql("SET temp_directory = '" + os.path.join(work, "_spill").replace("\\", "/") + "'")
    verdict = generate(con, sf, work)
    dr = duckrun.connect(FILES_PATH, read_only=False)
    land(dr, dr.con, sf, work, verdict)
    con.close()
    shutil.rmtree(work, ignore_errors=True)
    return verdict


def already_landed(sf):
    """True when the log already describes every table at this scale factor."""
    import duckrun
    dr = duckrun.connect(FILES_PATH, read_only=True)
    log_path = FILES_PATH + "/parquet_raw_archive_log.parquet"
    if not dr.con.sql("SELECT count(*) FROM glob('" + log_path + "')").fetchall()[0][0]:
        return False
    got = {r[0] for r in dr.con.sql(
        "SELECT DISTINCT source_type FROM read_parquet('" + log_path + "') WHERE etag = 'sf"
        + str(sf) + "'").fetchall()}
    return not (set(TABLES) - got)


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
    if not dr.con.sql("SELECT count(*) FROM glob('" + log_path + "')").fetchall()[0][0]:
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
    if STATUS:
        return status()
    sf = scale_factor()
    _log("sf=" + str(sf) + " files_path=" + FILES_PATH + " force=" + str(FORCE))
    if not FORCE and already_landed(sf):
        _log("sf" + str(sf) + " is already landed for all " + str(len(TABLES))
             + " tables; nothing to do. Set TPCDS_FORCE=1 (the `regenerate` input) to regenerate.")
        return 0
    verdict = run(sf)
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".github", "scripts"))
    try:
        import record
        record.merge({"generator": verdict})
    except Exception as ex:                                        # noqa: BLE001
        _log("could not record the verdict (" + type(ex).__name__ + ": " + str(ex) + ")")
    summarise(verdict)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
