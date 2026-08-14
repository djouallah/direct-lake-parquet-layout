"""Land the CMS Open Payments general-payment archive — the fifth dataset, and the WIDE one.

Sibling of download_green_taxi.py, same watermark idiom (an archive-log parquet at the Files root,
so a re-run only fetches what is new), retargeted at CMS's Sunshine Act payment records: every row
is a payment a drug or device manufacturer made to a physician or teaching hospital, with an
amount, a date, a payer and a payee.

WHY THIS DATASET EXISTS HERE. The other four are 5, 17, 20 and 22 columns. This one is NINETY-ONE,
and it is the first with a SPARSE surface: 54 of the 91 columns are more than half NULL, because
CMS models a one-to-many product list as five repeated column groups
(`Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1..5` and its four siblings) plus a
six-wide recipient-type/specialty group. Measured on a 100 MB sample of PY2023: member `_1` is
~7% NULL, `_2` ~83%, `_3` ~95%, `_4` ~98%, `_5` ~99%, and the six-wide groups are 100% NULL past
`_1`. Sparse columns are exactly where an encoding pass has the most to gain, and nothing in aemo,
nyc, bts or green exercises that at all.

It is also the first table carrying BOTH skew regimes at once, which is what makes it a new point
rather than a wider bts. From the same sample: Nature_of_Payment 92% one value, Form_of_Payment
86%, Physician_Ownership 87%, Related_Product 95%, Delay_in_Publication and
Dispute_Status_for_Publication 100% — nyc's extreme-skew regime — beside Covered_Recipient_Specialty
at 302 values / 9.5% top and the manufacturer id at ~1,000 / 10.5% top, which is bts's competing
regime. nyc says nothing about the second and bts nothing about the first.

TWO SOURCE FACTS THAT ARE CODED RATHER THAN TEMPLATED, both verified against the live service:

1. THE DOWNLOAD URL IS NOT TEMPLATABLE. It is
   .../openpayments/PGYR2023_P06302026_06032026/OP_DTL_GNRL_PGYR2023_P06302026_06032026.csv — the
   path segment carries the PUBLICATION and REFRESH dates, which move every June when CMS
   republishes. `resolve_urls()` reads them from the metastore search API instead. Same class of
   trap as bts's obfuscated TranStats spelling: a plausible-looking constant that silently 404s a
   year after it was written. The resolved URL goes into the log's `source_url`.

2. THE WATERMARK UNIT IS THE ANNUAL CSV; THE LANDED UNIT IS THE MONTH. A month cannot be fetched on
   its own, so `download_limit` counts PROGRAM YEARS (oldest first), while the archive is landed as
   monthly parquet so the incremental drain has the same grain as nyc/green/bts. One year therefore
   writes ~12 log rows that share `source_filename` (the watermark) and differ in `file_stem` (what
   the fact stores as `file`). Seven program years, ~88M rows, ~84 files.

   CMS data carries payments dated outside their own program year — a documented source condition,
   not a defect — so months are bucketed by the ACTUAL Date_of_Payment and a stray month is landed
   as itself rather than clamped into the year it was published under.

THE SPLIT IS RECONCILED AT LAND TIME, not by a dbt test, and that is deliberate. Because this
script writes BOTH sides of the split, a dbt assertion that the months sum to the year would be
comparing the downloader against itself. So `land_year()` counts the source CSV once and refuses to
log ANYTHING for a year whose monthly partitions do not sum to it — free, on a runner, before any
Fabric capacity is spent. The dbt test (tests/cms/*/assert_fct_cms_payments_matches_archive_log.sql)
keeps the job it can actually do: checking that what was landed is what was WRITTEN.

Env in:
  FILES_PATH      the landing lakehouse Files root (abfss:// or a local path)
  download_limit  program YEARS to fetch this run — months on every other dataset (oldest first,
                  so a capped drain extends deterministically). Clamped by MAX_YEARS_PER_RUN
                  because a year here is GBs where a month there is MBs.
  CMS_MAX_YEARS   the clamp, default 2
  CMS_START       first program year to consider (default 2019 — the metastore catalog does not
                  serve 2013-2018, which are archived separately; probed 2026-08-15)
"""
import json
import os
import re
import shutil
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import duckrun

FILES_PATH = os.environ.get("FILES_PATH", "/tmp/landing").rstrip("/")
DOWNLOAD_LIMIT = int(os.environ.get("download_limit", "1"))
START_YEAR = int(os.environ.get("CMS_START", "2019").strip())

# ⚠️ `download_limit` COUNTS PROGRAM YEARS HERE AND MONTHS EVERYWHERE ELSE, so the shared dispatch
# input means something an order of magnitude larger on this dataset — and it is clamped rather
# than trusted. A year is 3.3-9.2 GB of CSV; the whole archive is ~50 GB. The form offers values up
# to 200, which is a sane month count on nyc/green/bts and would try to pull the entire archive
# through one runner here, blowing its disk somewhere in the middle with the runner minutes spent.
#
# Two years is what fits comfortably: peak disk is one CSV (~9 GB at the largest year) plus its
# parquet, processed and deleted one year at a time. Four dispatches drain the seven years.
#
# The scheduled path never reaches this — benchmark.yml only runs the download step when
# `github.event_name != 'schedule' && !inputs.skip_download` — so this guards the HAND dispatch,
# which is the one that would pass 200.
MAX_YEARS_PER_RUN = int(os.environ.get("CMS_MAX_YEARS", "2"))

# The metastore search endpoint. Free, unauthenticated, and the ONLY way to learn the current
# publication-dated path — see fact 1 in the module docstring.
CATALOG = ("https://openpaymentsdata.cms.gov/api/1/search/"
           "?fulltext=General%20Payment%20Data&page-size=40")

# Mirrored by macros/cms_payment_columns.sql — one source of truth for all three dialects.
#
# ALL 91, IN FILE ORDER, AND THE ORDER IS THE SOURCE'S. This is the whole source table: the
# instruction was to land everything and shape the star schema afterwards, and landing is the
# irreversible half (the archive is normalised to parquet once, here), so a column dropped now
# costs a re-download of ~50 GB to get back.
#
# THE HEADER IS BYTE-IDENTICAL ACROSS PY2019-2025 — checked by md5 of the literal header row on
# 2019, 2021, 2023 and 2025 — so unlike nyc and green there is no type drift to normalise around.
# The explicit CASTs below and in the macro are kept anyway, as the declaration that all four
# engines store the same schema.
#
# DO NOT BUILD THIS LIST FROM CMS's DATA DICTIONARY. The dictionary's field list spells #78
# `Covered_or_Nonccovered_Indicator_4` (double c). The FILE spells it correctly. Taking the
# dictionary's spelling produces a column that matches nothing, and the guard below would refuse
# every year for a column that does not exist.
CORE_COLUMNS = [
    "Change_Type",
    "Covered_Recipient_Type",
    "Teaching_Hospital_CCN",
    "Teaching_Hospital_ID",
    "Teaching_Hospital_Name",
    "Covered_Recipient_Profile_ID",
    "Covered_Recipient_NPI",
    "Covered_Recipient_First_Name",
    "Covered_Recipient_Middle_Name",
    "Covered_Recipient_Last_Name",
    "Covered_Recipient_Name_Suffix",
    "Recipient_Primary_Business_Street_Address_Line1",
    "Recipient_Primary_Business_Street_Address_Line2",
    "Recipient_City",
    "Recipient_State",
    "Recipient_Zip_Code",
    "Recipient_Country",
    "Recipient_Province",
    "Recipient_Postal_Code",
    "Covered_Recipient_Primary_Type_1",
    "Covered_Recipient_Primary_Type_2",
    "Covered_Recipient_Primary_Type_3",
    "Covered_Recipient_Primary_Type_4",
    "Covered_Recipient_Primary_Type_5",
    "Covered_Recipient_Primary_Type_6",
    "Covered_Recipient_Specialty_1",
    "Covered_Recipient_Specialty_2",
    "Covered_Recipient_Specialty_3",
    "Covered_Recipient_Specialty_4",
    "Covered_Recipient_Specialty_5",
    "Covered_Recipient_Specialty_6",
    "Covered_Recipient_License_State_code1",
    "Covered_Recipient_License_State_code2",
    "Covered_Recipient_License_State_code3",
    "Covered_Recipient_License_State_code4",
    "Covered_Recipient_License_State_code5",
    "Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name",
    "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID",
    "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name",
    "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State",
    "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country",
    "Total_Amount_of_Payment_USDollars",
    "Date_of_Payment",
    "Number_of_Payments_Included_in_Total_Amount",
    "Form_of_Payment_or_Transfer_of_Value",
    "Nature_of_Payment_or_Transfer_of_Value",
    "City_of_Travel",
    "State_of_Travel",
    "Country_of_Travel",
    "Physician_Ownership_Indicator",
    "Third_Party_Payment_Recipient_Indicator",
    "Name_of_Third_Party_Entity_Receiving_Payment_or_Transfer_of_Value",
    "Charity_Indicator",
    "Third_Party_Equals_Covered_Recipient_Indicator",
    "Contextual_Information",
    "Delay_in_Publication_Indicator",
    "Record_ID",
    "Dispute_Status_for_Publication",
    "Related_Product_Indicator",
    "Covered_or_Noncovered_Indicator_1",
    "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1",
    "Product_Category_or_Therapeutic_Area_1",
    "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1",
    "Associated_Drug_or_Biological_NDC_1",
    "Associated_Device_or_Medical_Supply_PDI_1",
    "Covered_or_Noncovered_Indicator_2",
    "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_2",
    "Product_Category_or_Therapeutic_Area_2",
    "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2",
    "Associated_Drug_or_Biological_NDC_2",
    "Associated_Device_or_Medical_Supply_PDI_2",
    "Covered_or_Noncovered_Indicator_3",
    "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_3",
    "Product_Category_or_Therapeutic_Area_3",
    "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3",
    "Associated_Drug_or_Biological_NDC_3",
    "Associated_Device_or_Medical_Supply_PDI_3",
    "Covered_or_Noncovered_Indicator_4",
    "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_4",
    "Product_Category_or_Therapeutic_Area_4",
    "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4",
    "Associated_Drug_or_Biological_NDC_4",
    "Associated_Device_or_Medical_Supply_PDI_4",
    "Covered_or_Noncovered_Indicator_5",
    "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_5",
    "Product_Category_or_Therapeutic_Area_5",
    "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5",
    "Associated_Drug_or_Biological_NDC_5",
    "Associated_Device_or_Medical_Supply_PDI_5",
    "Program_Year",
    "Payment_Publication_Date",
]

# The canonical type every archived year is rewritten to. Mirrored by cms_payment_type() in
# macros/cms_payment_columns.sql.
#
# Record_ID STAYS A STRING, and that is CMS's own declaration rather than a reading of the values.
# Every observed value is 10 numeric digits and BIGINT would be smaller and faster as a merge key,
# but reinterpreting a documented identifier as a number is how leading zeros disappear — and a
# TRY_CAST that failed would produce a NULL merge key, which is worse than a wide one.
#
# The four ids that ARE numeric by declaration take BIGINT rather than INTEGER: an NPI is ten
# digits, which overflows INT32.
CANONICAL = {
    "Date_of_Payment": "DATE",
    "Payment_Publication_Date": "DATE",
    "Total_Amount_of_Payment_USDollars": "DOUBLE",
    "Number_of_Payments_Included_in_Total_Amount": "INTEGER",
    "Program_Year": "INTEGER",
    "Covered_Recipient_NPI": "BIGINT",
    "Covered_Recipient_Profile_ID": "BIGINT",
    "Teaching_Hospital_ID": "BIGINT",
}

# The join key into dim_cms_payer, and the reason this dataset carries a whitespace assertion in
# all three dialects: it is a STRING key crossing engines, and T-SQL pads on comparison where
# DuckDB and Spark do not.
PAYER_ID = "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID"
PAYER_COLUMNS = [PAYER_ID,
                 "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name",
                 "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State",
                 "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country"]


# ⚠️ CMS SERVES DATES AS MM/DD/YYYY, AND A PLAIN `CAST(x AS DATE)` THROWS ON EVERY ROW:
#   Conversion Error: invalid date field format: "06/27/2023", expected format is (YYYY-MM-DD)
# Verified against the real PY2023 file, and it applies to BOTH date columns. Two ways this hides:
# `read_csv(all_varchar = true)` means DuckDB never gets a chance to infer the format, and a probe
# written as `SELECT count(*) FROM (SELECT CAST(...) FROM ...)` PASSES because the projection is
# pruned away — which is exactly how this survived a first look. Force the value out (max(), or a
# GROUP BY) before believing a cast works.
#
# try_strptime, not strptime: an unparseable date then yields NULL and the row lands under
# `cms_<year>-00` rather than raising and taking a whole multi-GB program year with it. Measured 0
# unparseable in 187,750 rows of PY2023, so this is the guard rather than the expected path.
DATE_FORMAT = "%m/%d/%Y"


def canonical_type(col):
    return CANONICAL.get(col, "VARCHAR")


def canonical_expr(col):
    """The SELECT-list expression that lands `col` in its canonical type.

    A per-column EXPRESSION rather than a type, because the two DATE columns need parsing and
    everything else needs a plain cast."""
    if CANONICAL.get(col) == "DATE":
        return f"""CAST(try_strptime("{col}", '{DATE_FORMAT}') AS DATE) AS "{col}\""""
    return f'CAST("{col}" AS {canonical_type(col)}) AS "{col}"'


dr = duckrun.connect(FILES_PATH, read_only=False)
con = dr.con
con.sql("INSTALL httpfs; LOAD httpfs;")
try:
    con.sql("SET GLOBAL azure_transport_option_type='"
            + os.environ.get("AZURE_TRANSPORT_OPTION_TYPE", "default") + "'")
except Exception:
    pass


def push_new(local_folder, rel):
    dr.copy(local_folder, rel, overwrite=False)


def push_replace(local_folder, rel):
    import obstore
    from dbt.adapters.duckrun import objectstore, secret
    base = f"{FILES_PATH}/{rel}" if rel else FILES_PATH
    store = objectstore.build_store(base, secret.refreshed(dr.storage_options))
    for n in os.listdir(local_folder):
        try:
            obstore.delete(store, n)
        except Exception:
            pass
    dr.copy(local_folder, rel, overwrite=True)


LOG_PATH = FILES_PATH + "/parquet_raw_archive_log.parquet"
# Identical column shape to the nyc/green logs, so macros/new_parquet_files.sql and
# spark_new_parquet_files.sql read it unchanged. What differs is only how two columns are USED:
# `source_filename` is the annual CSV (the watermark, repeated across a year's ~12 rows) while
# `file_stem` is the landed month (unique, and what the fact stores as `file`). Every other dataset
# has one row per source file and the two agree; here they deliberately do not.
LOG_COLUMNS = ("source_type VARCHAR, source_filename VARCHAR, archive_path VARCHAR, "
               "archived_at TIMESTAMPTZ, row_count BIGINT, source_url VARCHAR, "
               "etag VARCHAR, file_stem VARCHAR, columns VARCHAR")


def load_log():
    exists = con.sql(f"SELECT count(*) FROM glob('{LOG_PATH}')").fetchone()[0]
    if exists:
        con.sql(f"""
            CREATE OR REPLACE TEMP TABLE _pq_archive_log AS
            SELECT source_type, source_filename, archive_path, archived_at,
                   row_count, source_url, etag, file_stem, columns
            FROM read_parquet('{LOG_PATH}') WHERE file_stem IS NOT NULL
        """)
    else:
        con.sql(f"CREATE OR REPLACE TEMP TABLE _pq_archive_log ({LOG_COLUMNS})")


def save_log():
    with tempfile.TemporaryDirectory() as ltmp:
        lp = os.path.join(ltmp, "parquet_raw_archive_log.parquet").replace("\\", "/")
        con.sql(f"COPY _pq_archive_log TO '{lp}' (FORMAT PARQUET)")
        push_replace(ltmp, "")


def resolve_urls():
    """{program_year: (source_filename, download url)} from the metastore — see fact 1 above.

    Refuses to guess. If the catalog cannot be read, or returns no General Payment Data item with a
    distribution, this raises rather than falling back to a templated path: a stale constant would
    404 mid-drain with the runner minutes already spent, and a WRONG one would silently land another
    year's data under this year's name."""
    req = urllib.request.Request(CATALOG, headers={"User-Agent": "Mozilla/5.0 (dbt-cms)"})
    with urllib.request.urlopen(req, timeout=120) as r:
        doc = json.load(r)
    out = {}
    for item in (doc.get("results") or {}).values():
        m = re.match(r"^(\d{4}) General Payment Data$", (item.get("title") or "").strip())
        if not m:
            continue
        dists = item.get("distribution") or []
        urls = [d.get("downloadURL") for d in dists if (d.get("downloadURL") or "").endswith(".csv")]
        if not urls:
            continue
        year = int(m.group(1))
        # The stem of the CSV is the watermark — it carries the publication dates, so a CMS
        # republication lands as a NEW source_filename and the year is drained again rather than
        # silently skipped as already-present.
        out[year] = (os.path.basename(urls[0]).removesuffix(".csv"), urls[0])
    if not out:
        raise SystemExit(f"no 'NNNN General Payment Data' item with a CSV distribution at {CATALOG}")
    return out


def fetch(url, path):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (dbt-cms)"})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=1800) as r:
                etag = r.headers.get("ETag")
                with open(path, "wb") as f:
                    while True:
                        chunk = r.read(1 << 22)
                        if not chunk:
                            break
                        f.write(chunk)
            return etag
        except urllib.error.HTTPError as e:
            if attempt < 2 and e.code in (429, 500, 502, 503, 504):
                time.sleep(2 ** attempt)
                continue
            raise


def header_of(path):
    """The CSV's own header row, split on commas.

    Read as STRICT UTF-8 and left to raise if it is not — see check_utf8() for why guessing an
    encoding is the wrong repair. The header carries no quoted commas in any published year (all
    seven checked), so a plain split is exact here."""
    with open(path, "rb") as f:
        raw = f.readline()
    return raw.decode("utf-8").strip().split(",")


def check_utf8(path, sample_bytes=1 << 26):
    """Refuse a file that is not valid UTF-8. Best-effort over the first 64 MB.

    THE bts POSTURE, AND THE REASON IS RECORDED IN download_bts_flights.repair_encoding(): reaching
    for encoding='latin-1' makes the leg green while storing a mojibake twin of a real value
    PERMANENTLY, because the archive is normalised to parquet once, here. On bts that inflated the
    exact cardinality the dataset existed to measure. So a file that is not UTF-8 is REFUSED and the
    repair is a separate, measured decision — never a guess made to get past this line.

    PY2019-2025 all decode clean; this is the guard for the year CMS publishes next."""
    with open(path, "rb") as f:
        raw = f.read(sample_bytes)
    # Do not fail on a multi-byte sequence straddling the sample boundary.
    for trim in range(0, 4):
        try:
            (raw[:len(raw) - trim] if trim else raw).decode("utf-8")
            return
        except UnicodeDecodeError as e:
            if trim == 3 or e.start < len(raw) - 4:
                raise SystemExit(
                    f"{os.path.basename(path)} is not valid UTF-8 at byte {e.start}: {e.reason}. "
                    f"REFUSED — do not 'fix' this with encoding='latin-1'; see check_utf8()'s "
                    f"docstring and download_bts_flights.repair_encoding().")


def land_year(year, source_filename, url):
    """Fetch one program year and land it as monthly parquet. Returns [(stem, path, rows)].

    Downloaded to disk rather than streamed from the URL through httpfs: a mid-stream failure on an
    8 GB read would lose a pass of twenty minutes with no retry, and fetch() already has one. Peak
    disk is the CSV (~9 GB at the largest year) plus the parquet (~1 GB), and the CSV is deleted
    before the months are consolidated."""
    with tempfile.TemporaryDirectory() as tmp:
        csv_path = os.path.join(tmp, source_filename + ".csv")
        part_dir = os.path.join(tmp, "parts")
        out_dir = os.path.join(tmp, "out")
        os.makedirs(out_dir)
        print(f"  {year}: fetching {url}", flush=True)
        etag = fetch(url, csv_path)
        print(f"  {year}: {os.path.getsize(csv_path):,} bytes", flush=True)

        check_utf8(csv_path)
        cols = header_of(csv_path)
        missing = [c for c in CORE_COLUMNS if c not in cols]
        if missing:
            # REFUSED, not landed-and-hoped — the green/bts posture. Nothing reaches out/, so the
            # archive only ever holds years every dialect can read with one statement.
            print(f"  REFUSED {year}: missing {missing} (has {len(cols)} columns) — not archived",
                  flush=True)
            return []

        q = csv_path.replace("\\", "/")
        select = ", ".join(canonical_expr(c) for c in CORE_COLUMNS)
        # ONE pass over the CSV — the expensive part — writing DuckDB's own hive partitions, then a
        # cheap consolidation per month below. `columns` is left to the default: this is the LANDING
        # format, not a layout under test, and a hand-picked row-group size here would be a second,
        # invisible geometry knob upstream of the one the dispatch controls.
        #
        # ⚠️ `_ym` PARSES Date_of_Payment ITSELF, and the expression in `select` above does NOT
        # reach it. read_csv(all_varchar=true) means every column arrives VARCHAR, and DuckDB
        # resolves a select-list column reference against the FROM rather than against a sibling
        # alias — so `strftime("Date_of_Payment", …)` here saw the raw VARCHAR and failed to bind:
        #   Could not choose a best candidate function for strftime(VARCHAR, STRING_LITERAL)
        # That killed run 31810902120 after a 5.95 GB download had already been paid for. It is
        # unreachable from a model render check, because it is this script's SQL, not a model's —
        # `test_cms_landing_sql.py` executes this exact statement against a synthetic CSV instead.
        #
        # It must also use the same MM/DD/YYYY parse as canonical_expr(), or the partition and the
        # stored date disagree about which month a row belongs to — silently, since both would
        # still be valid dates. `02/03` is the cheap check: 3 February under %m/%d, 2 March under
        # %d/%m.
        #
        # NULL is ROUTED, not dropped: an unparseable date would otherwise land in
        # __HIVE_DEFAULT_PARTITION__, be skipped, and REFUSE the whole program year over one bad
        # row. `cms_<year>-00` is a real landed file whose name says "no month", so the
        # reconciliation stays exact and nothing is silently lost.
        con.sql(f"""
            COPY (
              SELECT {select},
                     COALESCE(strftime(CAST(try_strptime("Date_of_Payment",
                                                         '{DATE_FORMAT}') AS DATE), '%Y-%m'),
                              "Program_Year" || '-00') AS _ym
              FROM read_csv('{q}', header = true, all_varchar = true,
                            strict_mode = false, parallel = true)
            ) TO '{part_dir.replace(chr(92), '/')}'
              (FORMAT PARQUET, PARTITION_BY (_ym), COMPRESSION ZSTD, OVERWRITE_OR_IGNORE)
        """)
        source_rows = con.sql(
            f"SELECT count(*) FROM read_csv('{q}', header = true, all_varchar = true, "
            f"strict_mode = false)").fetchone()[0]
        os.remove(csv_path)

        landed, total = [], 0
        for entry in sorted(os.listdir(part_dir)):
            if not entry.startswith("_ym="):
                continue
            ym = entry.removeprefix("_ym=")
            if ym in ("", "__HIVE_DEFAULT_PARTITION__"):
                # Unreachable while the COALESCE above holds — kept so that if it ever stops
                # holding, the rows are counted missing and the year is REFUSED, rather than
                # silently dropped and the archive quietly short.
                print(f"  WARN {year}: rows in {entry!r} have no month and are not landed; "
                      f"the reconciliation below will refuse this year", flush=True)
                continue
            if ym.endswith("-00"):
                print(f"  WARN {year}: some rows have an unparseable Date_of_Payment and are "
                      f"landed as cms_{ym}; they will carry a NULL date in the fact", flush=True)
            stem = f"cms_{ym}"
            dst = os.path.join(out_dir, stem + ".parquet")
            src_glob = os.path.join(part_dir, entry, "*.parquet").replace("\\", "/")
            # DuckDB may write more than one file per partition; consolidate to exactly one so the
            # landed unit and the log row are 1:1, as they are on every other dataset.
            con.sql(f"COPY (SELECT * EXCLUDE (_ym) FROM read_parquet('{src_glob}')) "
                    f"TO '{dst.replace(chr(92), '/')}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            rows = con.sql(f"SELECT count(*) FROM read_parquet('{dst.replace(chr(92), '/')}')"
                           ).fetchone()[0]
            landed.append((stem, dst, int(rows), etag))
            total += int(rows)
        shutil.rmtree(part_dir, ignore_errors=True)

        # THE LAND-TIME RECONCILIATION — see the module docstring. This script writes both sides of
        # the split, so this is the only place the split can be checked at all; a dbt test would be
        # comparing the downloader against itself.
        if total != source_rows:
            print(f"  REFUSED {year}: months sum to {total:,} against {source_rows:,} in the "
                  f"source CSV ({source_rows - total:,} unaccounted — most likely a NULL "
                  f"Date_of_Payment) — not archived", flush=True)
            return []

        # `record_id` leads no key on its own, but (file, Record_ID) is the merge key on all four
        # engines and duckrun asserts its source is unique on it. Failing HERE names the file.
        dup = con.sql(
            "SELECT count(*) FROM (SELECT \"Record_ID\" FROM read_parquet('"
            + os.path.join(out_dir, "*.parquet").replace("\\", "/")
            + "') GROUP BY 1 HAVING count(*) > 1)").fetchone()[0]
        if dup:
            print(f"  REFUSED {year}: Record_ID is not unique ({dup:,} repeated values) — "
                  f"not archived", flush=True)
            return []

        # ⚠️ A PROGRAM YEAR CAN EMIT A MONTH BELONGING TO ANOTHER YEAR, and the landed file is named
        # for the MONTH alone. Measured: PY2024 holds 15,498,612 payments dated in 2024 and 75 that
        # are not, so it lands 13 months. If one of those months was already landed by a different
        # program year, push_new(overwrite=False) keeps the FIRST file while this year's rows are
        # dropped — and the log still gains a row claiming them. Rows that never landed, which no
        # downstream model can filter back in, under a log that says otherwise.
        #
        # Refuse rather than write. The alternative is a stem carrying the program year
        # (cms_2024_2019-07), which is the better scheme but costs a ~50 GB re-drain of everything
        # already landed; this makes the failure loud and keeps that decision open. The archive
        # stays consistent either way, which is the property that matters in the irreversible half.
        clash = con.sql(
            "SELECT file_stem, min(source_filename) FROM _pq_archive_log "
            "WHERE source_type = 'cms' AND source_filename <> '" + source_filename + "' "
            "AND file_stem IN ('" + "', '".join(s for s, _d, _r, _e in landed) + "') "
            "GROUP BY file_stem ORDER BY file_stem").fetchall()
        if clash:
            print(f"  REFUSED {year}: {len(clash)} month(s) already landed by another program "
                  f"year — {', '.join(f'{s} (from {o})' for s, o in clash[:5])}"
                  f"{' …' if len(clash) > 5 else ''}. Landing would drop this year's rows for "
                  f"those months while still logging them. Not archived.", flush=True)
            return []

        push_new(out_dir, "parquet_raw/cms")
        now = datetime.now(timezone.utc).isoformat()
        cols_csv = ",".join(cols).replace("'", "''")
        url_sql = url.replace("'", "''")
        etag_sql = "NULL" if not etag else "'" + etag.replace("'", "''") + "'"
        for stem, _dst, rows, _etag in landed:
            con.sql(f"""INSERT INTO _pq_archive_log VALUES (
                'cms', '{source_filename}', '/cms/{stem}.parquet', '{now}'::TIMESTAMPTZ,
                {rows}, '{url_sql}', {etag_sql}, '{stem}', '{cols_csv}')""")
        print(f"  {year}: {total:,} rows over {len(landed)} months", flush=True)
        save_log()
        return landed


def land_payers():
    """The manufacturer/GPO lookup — dim_cms_payer's source, rebuilt from the landed archive.

    CMS publishes no separate manufacturer lookup, so it is DERIVED here rather than downloaded.
    That is the one thing this differs from land_zones() in on green, and the reason it is done at
    land time rather than as a dbt model is that a model would have to scan the whole fact to build
    a ~1,000-row dimension on every engine, every run.

    ONE ROW PER ID IS A HARD REQUIREMENT, not tidiness: dim_cms_payer carries a `unique` test on
    the key, and manufacturers are renamed between program years, so a plain DISTINCT over the four
    columns yields the same id twice with two names. The newest program year wins."""
    root = f"{FILES_PATH}/parquet_raw/cms/*.parquet"
    have = con.sql(f"SELECT count(*) FROM glob('{root}')").fetchone()[0]
    if not have:
        print("  payer lookup: nothing landed yet, skipping")
        return
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "out")
        os.makedirs(out)
        dst = os.path.join(out, "cms_payers.parquet").replace("\\", "/")
        sel = ", ".join(f'"{c}"' for c in PAYER_COLUMNS)
        con.sql(f"""
            COPY (
              SELECT {sel}
              FROM (
                SELECT {sel}, "Program_Year",
                       row_number() OVER (PARTITION BY "{PAYER_ID}"
                                          ORDER BY "Program_Year" DESC) AS rn
                FROM read_parquet('{root}')
                WHERE "{PAYER_ID}" IS NOT NULL
              )
              WHERE rn = 1
              ORDER BY 1
            ) TO '{dst}' (FORMAT PARQUET)
        """)
        rows = con.sql(f"SELECT count(*) FROM read_parquet('{dst}')").fetchone()[0]
        push_replace(out, "parquet_raw/payer")
    con.sql("DELETE FROM _pq_archive_log WHERE source_type = 'payer'")
    now = datetime.now(timezone.utc).isoformat()
    con.sql(f"""INSERT INTO _pq_archive_log VALUES (
        'payer', 'cms_payers', '/payer/cms_payers.parquet', '{now}'::TIMESTAMPTZ,
        {rows}, NULL, NULL, 'cms_payers', '{",".join(PAYER_COLUMNS)}')""")
    print(f"  payer lookup: {rows:,} rows")


print(f"Landing to: {FILES_PATH}")
print(f"Program years from {START_YEAR}, limit {DOWNLOAD_LIMIT}")
load_log()

urls = resolve_urls()
have = {r[0] for r in con.sql(
    "SELECT DISTINCT source_filename FROM _pq_archive_log WHERE source_type = 'cms'").fetchall()}
limit = min(DOWNLOAD_LIMIT, MAX_YEARS_PER_RUN)
todo = [y for y in sorted(urls) if y >= START_YEAR and urls[y][0] not in have][:limit]
print(f"Catalog has {sorted(urls)}; pending {todo}"
      + (f" (download_limit {DOWNLOAD_LIMIT} clamped to {limit} — a program year is GBs, not a "
         f"month; see MAX_YEARS_PER_RUN)" if limit < DOWNLOAD_LIMIT else ""))

landed = months = 0
for year in todo:
    source_filename, url = urls[year]
    got = land_year(year, source_filename, url)
    if got:
        landed += 1
        months += len(got)
if not todo:
    print("  cms: nothing new")

land_payers()
save_log()

con.sql("""SELECT source_type, count(*) AS files, sum(row_count) AS rows
           FROM _pq_archive_log GROUP BY source_type ORDER BY source_type""").show()
print(f"Landed this run: cms years={landed}, months={months}")
print("Done. Now run:  dbt build --target duckrun   (with DATASET=cms)")
