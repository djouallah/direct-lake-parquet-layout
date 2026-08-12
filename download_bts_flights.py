"""Land the US DOT/BTS airline on-time performance archive — the third dataset.

Sibling of download_aemo.py and download_nyc_taxi.py, same shape and the same watermark idiom (an
archive-log parquet at the Files root, so a re-run only fetches what is new), retargeted at a
source that serves ZIPPED CSV: TranStats' PREZIP endpoint, one zip per month since October 1987.

WHY THIS DATASET EXISTS HERE. aemo and nyc are two points on the surface V-Order's worth tracks —
column count x categorical skew — and both sit in regimes where the optimizer never has to choose.
aemo's five narrow near-unique columns give it nothing to reorder; nyc's categoricals are 97-99%
single-value, so extreme that EVERY column stays run-length-friendly under any sort — the columns
do not compete. Flights is the regime between them, and the canonical BI fact shape: many
INDEPENDENT, moderately skewed categoricals (DayOfWeek seven values near-uniform,
Reporting_Airline ~20, Origin/Dest ~350 Zipfian, Tail_Number thousands, CancellationCode ~98%
NULL, the flags binary) where sorting for one buys nothing on the others. V-Order's multi-column
greedy ordering has to divide its budget, and the per-column `runs` table in `layout.ordering` is
what shows which columns it sacrifices.

THE ARCHIVE IS NORMALISED AT LAND TIME, exactly as nyc's is, and for the same reader: BTS serves
CSV in which the flags ship as '1.00', the times as '0745' and ~110 columns of which this project
reads 22 — plus a trailing comma on every line, header included. Each month is decoded once here
and rewritten as parquet with the canonical 22-column schema before upload, so every engine reads
one homogeneous archive with one plain statement and the stored types are identical across all
four outputs. A month whose header lacks a core column is REFUSED, loudly, at the cost of a free
runner minute instead of a mid-write failure with Fabric capacity already spent.

`CORE_COLUMNS` below is mirrored by `macros/bts_flight_columns.sql`, which is what the three model
trees generate their SELECT lists from. `.github/scripts/test_bts_columns.py` asserts the two
never drift.

ONE SOURCE-SPECIFIC WRINKLE: transtats.bts.gov has a history of serving an incomplete TLS chain,
which urllib refuses while browsers quietly repair it. fetch() retries ONCE with verification off,
loudly, for this pinned public host only — the data is public monthly CSV whose row counts are
re-checked against what the models store, so integrity rests on the reconciliation test rather
than the transport.

Env in:
  FILES_PATH      the landing lakehouse Files root (abfss:// or a local path)
  download_limit  months to fetch this run (oldest first, so a capped drain extends deterministically)
  BTS_START       first month to consider, YYYY-MM (default 1987-10 — the first month BTS
                  published; there is nothing before it to land)
"""
import os
import ssl
import tempfile
import time
import urllib.error
import urllib.request
import zipfile
from datetime import date, datetime, timezone

import duckrun

FILES_PATH = os.environ.get("FILES_PATH", "/tmp/landing").rstrip("/")
DOWNLOAD_LIMIT = int(os.environ.get("download_limit", "2"))
START = os.environ.get("BTS_START", "1987-10").strip()

# TranStats' prezipped monthly files. Note the month is NOT zero-padded in the URL, and the zip
# holds one CSV (plus a readme) whose own name carries parentheses — the archived copy is renamed
# to the plain `flights_YYYY-MM` stem the models key on.
ZIP_URL = ("https://transtats.bts.gov/PREZIP/"
           "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{y}_{m}.zip")
# The unique-carrier lookup — the dim_carrier source, BTS's own Code -> Description table.
# The query string is the site's own letter/digit substitution of `Lookup=L_UNIQUE_CARRIERS` —
# TranStats' redesign obfuscates every parameter this way, and the PLAIN spelling now returns the
# HTML homepage with status 200, which is worse than a 404: it reads as success and fails at the
# CSV parse. This exact URL is what the on-time table's own field page links, verified 2026-08-12
# — it serves Code,Description CSV (~54 KB).
CARRIERS = "https://www.transtats.bts.gov/Download_Lookup.asp?Y11x72=Y_haVdhR_PNeeVRef"

# BTS publishes with roughly a three-month lag; skip the tail rather than probe it.
PUBLISH_LAG_MONTHS = 4

# Mirrored by macros/bts_flight_columns.sql — see that file for why it is 22 of BTS's ~110.
CORE_COLUMNS = [
    "DayOfWeek", "FlightDate", "Reporting_Airline", "Tail_Number",
    "Flight_Number_Reporting_Airline", "Origin", "Dest", "CRSDepTime", "DepTime", "DepDelay",
    "DepDel15", "TaxiOut", "TaxiIn", "ArrTime", "ArrDelay", "ArrDel15", "Cancelled",
    "CancellationCode", "Diverted", "AirTime", "Distance", "DistanceGroup",
]

# The canonical type every archived month is rewritten to. Mirrored by bts_flight_type() in
# macros/bts_flight_columns.sql, which is what each dialect CASTs to — so the read is a no-op cast
# over already-canonical data. INTEGER goes through DOUBLE first because the CSV spells the flags
# '1.00' and DuckDB will not parse a decimal string straight to INTEGER.
CANONICAL = {
    "FlightDate": "DATE",
    "Reporting_Airline": "VARCHAR", "Tail_Number": "VARCHAR", "Origin": "VARCHAR",
    "Dest": "VARCHAR", "CancellationCode": "VARCHAR",
    "DayOfWeek": "INTEGER", "Flight_Number_Reporting_Airline": "INTEGER",
    "CRSDepTime": "INTEGER", "DepTime": "INTEGER", "DepDel15": "INTEGER",
    "ArrTime": "INTEGER", "ArrDel15": "INTEGER", "Cancelled": "INTEGER",
    "Diverted": "INTEGER", "DistanceGroup": "INTEGER",
}


def canonical_expr(col):
    t = CANONICAL.get(col, "DOUBLE")
    if t == "INTEGER":
        return f'TRY_CAST(TRY_CAST("{col}" AS DOUBLE) AS INTEGER) AS "{col}"'
    return f'TRY_CAST("{col}" AS {t}) AS "{col}"'


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
# Byte-identical column shape to the nyc log — same filename, same nine columns — which is what
# lets macros/new_parquet_files.sql serve both datasets verbatim. Only the MODEL name over it
# differs (stg_flights_archive_log), and only because dbt patches resolve by name project-wide.
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


def months():
    """Every YYYY-MM from START up to the newest month BTS has plausibly published, oldest first.

    Oldest first is what makes a capped drain reproducible — same reasoning as the other two
    downloaders: run N and run N+1 with the same `download_limit` extend the archive by the same
    months in the same order."""
    y, m = (int(p) for p in START.split("-"))
    today = date.today()
    end_y, end_m = today.year, today.month - PUBLISH_LAG_MONTHS
    while end_m <= 0:
        end_y, end_m = end_y - 1, end_m + 12
    out = []
    while (y, m) <= (end_y, end_m):
        out.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def fetch(url, path):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (dbt-bts)"})
    context = None
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=600, context=context) as r:
                etag = r.headers.get("ETag")
                with open(path, "wb") as f:
                    while True:
                        chunk = r.read(1 << 20)
                        if not chunk:
                            break
                        f.write(chunk)
            return etag
        except urllib.error.URLError as e:
            # transtats serves an incomplete TLS chain on some frontends. One loud downgrade for
            # this pinned public host; see the module docstring for why that is acceptable here.
            if context is None and isinstance(getattr(e, "reason", None), ssl.SSLError):
                print(f"  WARN {url}: TLS chain rejected ({e.reason}); retrying unverified "
                      f"for this pinned host", flush=True)
                context = ssl._create_unverified_context()
                continue
            if attempt < 3 and isinstance(e, urllib.error.HTTPError) \
                    and e.code in (429, 500, 502, 503, 504):
                time.sleep(2 ** attempt)
                continue
            raise


def footer(path):
    """(row_count, column names) from a parquet FOOTER — no scan, no decompression."""
    p = path.replace("\\", "/")
    rows = con.sql(f"SELECT sum(num_rows) FROM parquet_file_metadata('{p}')").fetchone()[0]
    cols = [r[0] for r in con.sql(
        f"SELECT name FROM parquet_schema('{p}') WHERE num_children IS NULL").fetchall()]
    return int(rows or 0), cols


def csv_member(zpath):
    """The one CSV inside a PREZIP zip (it also carries a readme.html)."""
    with zipfile.ZipFile(zpath) as z:
        names = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if len(names) != 1:
            raise RuntimeError(f"{os.path.basename(zpath)}: expected one CSV, found {names}")
        return names[0]


def csv_header(path):
    """The CSV's own column list. BTS ends every line — header included — with a comma, which
    DuckDB reads as one trailing blank-named column ('columnN'); it is reported here as-is so the
    log records what the source really said."""
    p = path.replace("\\", "/")
    r = con.sql(f"SELECT * FROM read_csv('{p}', header=true, all_varchar=true) LIMIT 0")
    return list(r.columns)


def normalise(src, dst):
    """Rewrite one month's CSV as parquet with the canonical 22-column schema — see the module
    docstring. ZSTD to match the nyc archive; the landing format is not a layout under test."""
    con.sql("COPY (SELECT "
            + ", ".join(canonical_expr(c) for c in CORE_COLUMNS)
            + f" FROM read_csv('{src.replace(chr(92), '/')}', header=true, all_varchar=true)) "
            + f"TO '{dst.replace(chr(92), '/')}' (FORMAT PARQUET, COMPRESSION ZSTD)")


def land_flights(limit):
    have = {r[0] for r in con.sql(
        "SELECT source_filename FROM _pq_archive_log WHERE source_type = 'flights'").fetchall()}
    todo = [ym for ym in months() if f"flights_{ym}" not in have][:limit]
    if not todo:
        print("  flights: nothing new")
        return 0, 0
    landed = refused = 0
    # One temp dir per batch so a long drain does not hold the whole archive on the runner disk —
    # a month is a ~25-70 MB zip that inflates to ~200-700 MB of CSV. Sequential rather than the
    # nyc downloader's thread pool: the zips are bigger, TranStats is slower, and the decode (CSV
    # parse + parquet write) dominates the wall clock anyway.
    batch_size = 4
    for i in range(0, len(todo), batch_size):
        batch = todo[i:i + batch_size]
        with tempfile.TemporaryDirectory() as tmp:
            raw_dir, out_dir = os.path.join(tmp, "raw"), os.path.join(tmp, "out")
            os.makedirs(raw_dir)
            os.makedirs(out_dir)
            got = []
            for ym in batch:
                y, m = (int(p) for p in ym.split("-"))
                url = ZIP_URL.format(y=y, m=m)
                zpath = os.path.join(raw_dir, f"flights_{ym}.zip")
                try:
                    etag = fetch(url, zpath)
                    member = csv_member(zpath)
                    with zipfile.ZipFile(zpath) as z:
                        z.extract(member, raw_dir)
                    csv_path = os.path.join(raw_dir, member)
                except Exception as e:
                    print(f"  WARN skip {ym}: {type(e).__name__}: {e}", flush=True)
                    continue
                cols = csv_header(csv_path)
                missing = [c for c in CORE_COLUMNS if c not in cols]
                if missing:
                    # REFUSED, not landed-and-hoped: nothing is written to out/, so the archive
                    # only ever holds months every dialect can read.
                    print(f"  REFUSED {ym}: missing {missing} (has {len(cols)} columns) — "
                          f"not archived", flush=True)
                    refused += 1
                else:
                    name = f"flights_{ym}.parquet"
                    normalise(csv_path, os.path.join(out_dir, name))
                    rows, _ = footer(os.path.join(out_dir, name))
                    got.append((ym, name, rows, cols, etag))
                # The inflated CSV is the disk cost; drop it before the next month's.
                os.remove(csv_path)
                os.remove(zpath)
            if not got:
                continue
            push_new(out_dir, "parquet_raw/flights")
            now = datetime.now(timezone.utc).isoformat()
            for ym, name, rows, cols, etag in got:
                stem = name.removesuffix(".parquet")
                y, m = (int(p) for p in ym.split("-"))
                etag_sql = "NULL" if not etag else "'" + etag.replace("'", "''") + "'"
                con.sql(f"""INSERT INTO _pq_archive_log VALUES (
                    'flights', '{stem}', '/flights/{name}', '{now}'::TIMESTAMPTZ,
                    {rows}, '{ZIP_URL.format(y=y, m=m)}', {etag_sql}, '{stem}',
                    '{",".join(cols)}')""")
                print(f"  flights {ym}: {rows:,} rows", flush=True)
                landed += 1
        save_log()
    return landed, refused


def land_carriers():
    """The unique-carrier lookup — the dim_carrier source. Refreshed at most daily, exactly as the
    taxi zone lookup and the AEMO DUID reference are: a slowly-changing reference table, not part
    of the archive being drained.

    BTS serves it as CSV (Code, Description) and it is landed as PARQUET for the same reason the
    zone lookup is — Fabric Spark cannot read a headered CSV by column name from a path, so
    converting once here makes the read one plain statement in all three dialects. The columns are
    renamed to `code` / `name` at land time: `Description` says nothing, and a lower-case pair
    keeps the dimension's SQL identical across dialects with no quoting."""
    last = con.sql("SELECT max(archived_at) FROM _pq_archive_log "
                   "WHERE source_type = 'carrier'").fetchone()[0]
    if last is not None and (datetime.now(last.tzinfo) - last).total_seconds() < 86400:
        print(f"  carrier lookup fresh ({last}), skipping")
        return
    with tempfile.TemporaryDirectory() as tmp:
        raw = os.path.join(tmp, "raw")
        out = os.path.join(tmp, "out")
        os.makedirs(raw)
        os.makedirs(out)
        src = os.path.join(raw, "carrier_lookup.csv")
        fetch(CARRIERS, src)
        dst = os.path.join(out, "carrier_lookup.parquet")
        con.sql(f"""COPY (
            SELECT CAST("Code" AS VARCHAR) AS code,
                   CAST("Description" AS VARCHAR) AS name
            FROM read_csv('{src.replace(chr(92), '/')}', header = true, all_varchar = true)
            WHERE "Code" IS NOT NULL
        ) TO '{dst.replace(chr(92), '/')}' (FORMAT PARQUET)""")
        rows = con.sql(f"SELECT count(*) FROM read_parquet('{dst.replace(chr(92), '/')}')"
                       ).fetchone()[0]
        push_replace(out, "parquet_raw/carrier")
    con.sql("DELETE FROM _pq_archive_log WHERE source_type = 'carrier'")
    now = datetime.now(timezone.utc).isoformat()
    con.sql(f"""INSERT INTO _pq_archive_log VALUES (
        'carrier', 'carrier_lookup', '/carrier/carrier_lookup.parquet', '{now}'::TIMESTAMPTZ,
        {rows}, '{CARRIERS}', NULL, 'carrier_lookup', 'code,name')""")
    print(f"  carrier lookup: {rows} rows")


print(f"Landing to: {FILES_PATH}")
print(f"Months from {START}, limit {DOWNLOAD_LIMIT}")
load_log()
landed, refused = land_flights(DOWNLOAD_LIMIT)
land_carriers()
save_log()

con.sql("""SELECT source_type, count(*) AS files, sum(row_count) AS rows
           FROM _pq_archive_log GROUP BY source_type ORDER BY source_type""").show()
print(f"Landed this run: flights={landed}" + (f", REFUSED={refused}" if refused else ""))
print("Done. Now run:  dbt build --target duckrun   (with DATASET=bts)")
