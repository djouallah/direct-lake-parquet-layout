"""Land the NYC TLC green-taxi archive — the fourth dataset, and the small arm of the skew pair.

Sibling of download_nyc_taxi.py, same shape, same watermark idiom (an archive-log parquet at the
Files root, so a re-run only fetches what is new), retargeted at the green (outer-borough) fleet.

WHY THIS DATASET EXISTS HERE. The yellow-taxi pair measured V-Order reordering the most repetitive
column 3,371x and shrinking the table 36%. The counter-claim under test is that on GREEN taxi the
same profile produces BIGGER data. Green is the same extreme-skew regime as yellow — RatecodeID and
store_and_fwd_flag at 97-99% single-value, the LocationIDs Zipfian (on Brooklyn and Queens here;
green pickups in Manhattan are legally restricted to the upper zones) — plus two categoricals
yellow does not have: `trip_type` (~98% street-hail) and `ehail_fee` (~all NULL). What differs is
the SIZE: the whole 2014-onward archive is tens of millions of rows against yellow's hundreds, so
this is the same surface on a much smaller table.

THE ARCHIVE IS NORMALISED AT LAND TIME, exactly as yellow's is, and green needs it more: the same
column ships as INT64, INT32 and DOUBLE in different years (RatecodeID, passenger_count,
payment_type, ehail_fee, congestion_surcharge all drift), and Spark refuses a parquet scan whose
files disagree on a column's type. Each month is decoded once here and rewritten with the canonical
20-column schema before upload; a month whose footer lacks a core column is REFUSED, loudly, on a
free runner rather than mid-write with Fabric capacity already spent.

WHY 20 OF TLC'S 21. Probed month by month over the CDN (2014-01, 2015-01, 2016-06, 2019-01,
2022-01, 2025-01): every archived month carries the same 20 columns — green KEEPS
`congestion_surcharge` (present since the start, NULL before 2019), the inverse of yellow, where
the column only appears from 2019 and is excluded. The one exclusion is `cbd_congestion_fee`,
2025-onward only — green's `airport_fee` analogue.

`CORE_COLUMNS` below is mirrored by `macros/green_trip_columns.sql`, which is what the three model
trees generate their SELECT lists from. `.github/scripts/test_green_columns.py` asserts the two
never drift — a divergence would land files the models cannot read, or refuse files they can.

Env in:
  FILES_PATH      the landing lakehouse Files root (abfss:// or a local path)
  download_limit  months to fetch this run (oldest first, so a capped drain extends deterministically)
  GREEN_START     first month to consider, YYYY-MM (default 2014-01 — the fleet launched 2013-08 but
                  the CDN does not serve any 2013 month at all; every one returns a server error,
                  probed 2026-08-13)
"""
import os
import tempfile
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone

import duckrun

FILES_PATH = os.environ.get("FILES_PATH", "/tmp/landing").rstrip("/")
DOWNLOAD_LIMIT = int(os.environ.get("download_limit", "2"))
START = os.environ.get("GREEN_START", "2014-01").strip()

# TLC's CloudFront origin. Monthly files, and a `misc/` folder holding the zone lookup.
CDN = "https://d37ci6vzurychx.cloudfront.net"
TRIPS = CDN + "/trip-data/green_tripdata_{ym}.parquet"
ZONES = CDN + "/misc/taxi_zone_lookup.csv"

# TLC publishes with roughly a two-month lag; asking for a month that does not exist yet is a 403,
# not a 404, so probing is not free. Skip the tail rather than discover it.
PUBLISH_LAG_MONTHS = 3

# Mirrored by macros/green_trip_columns.sql — see that file for why it is 20 and not TLC's 21.
CORE_COLUMNS = [
    "VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime", "store_and_fwd_flag",
    "RatecodeID", "PULocationID", "DOLocationID", "passenger_count", "trip_distance",
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "ehail_fee",
    "improvement_surcharge", "total_amount", "payment_type", "trip_type",
    "congestion_surcharge",
]

# The canonical type every archived month is rewritten to. Mirrored by green_trip_type() in
# macros/green_trip_columns.sql, which is what each dialect CASTs to — so the read is a no-op cast
# over already-canonical data, kept as an explicit declaration that all four engines store the same
# schema no matter what their reader would have inferred.
CANONICAL = {
    "lpep_pickup_datetime": "TIMESTAMP", "lpep_dropoff_datetime": "TIMESTAMP",
    "store_and_fwd_flag": "VARCHAR",
    "VendorID": "INTEGER", "passenger_count": "INTEGER", "RatecodeID": "INTEGER",
    "PULocationID": "INTEGER", "DOLocationID": "INTEGER", "payment_type": "INTEGER",
    "trip_type": "INTEGER",
}


def canonical_type(col):
    return CANONICAL.get(col, "DOUBLE")

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
# Same column shape as the AEMO log except the stem column, which is `file_stem` here rather than
# `csv_filename` — naming a parquet file's stem "csv" is the kind of small lie that survives a year
# — plus `columns`, the footer's own column list, so a schema question can be answered from the log
# without re-reading 150 files. The macros key on `file_stem`; see macros/new_parquet_files.sql.
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
    """Every YYYY-MM from START up to the newest month TLC has plausibly published, oldest first.

    Oldest first is deliberate and is what makes a capped drain reproducible: run N and run N+1 with
    the same `download_limit` extend the archive by the same months in the same order, so two
    dispatches differ by what was changed rather than by which slice of history they happened to
    grab. Newest-first would make every run measure a different workload."""
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
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (dbt-green)"})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=300) as r:
                etag = r.headers.get("ETag")
                with open(path, "wb") as f:
                    while True:
                        chunk = r.read(1 << 20)
                        if not chunk:
                            break
                        f.write(chunk)
            return etag
        except urllib.error.HTTPError as e:
            if attempt < 2 and e.code in (429, 500, 502, 503, 504):
                time.sleep(2 ** attempt)
                continue
            raise


def footer(path):
    """(row_count, column names) from the parquet FOOTER — no scan, no decompression.

    This is both the cheap row count for the log and the schema guard: `parquet_schema` lists the
    file's own columns, so a month whose schema drifted is caught before it is uploaded."""
    p = path.replace("\\", "/")
    rows = con.sql(f"SELECT sum(num_rows) FROM parquet_file_metadata('{p}')").fetchone()[0]
    cols = [r[0] for r in con.sql(
        f"SELECT name FROM parquet_schema('{p}') WHERE num_children IS NULL").fetchall()]
    return int(rows or 0), cols


def normalise(src, dst):
    """Rewrite one month with the canonical 20-column schema — see the module docstring.

    `columns` is left to the default: this is the LANDING format, not a layout under test, and
    giving it a hand-picked row-group size would put a second, invisible geometry knob upstream of
    the one the dispatch controls. Every engine reads the same bytes either way."""
    con.sql("COPY (SELECT "
            + ", ".join(f'CAST("{c}" AS {canonical_type(c)}) AS "{c}"' for c in CORE_COLUMNS)
            + f" FROM read_parquet('{src.replace(chr(92), '/')}')) "
            + f"TO '{dst.replace(chr(92), '/')}' (FORMAT PARQUET, COMPRESSION ZSTD)")


def land_trips(limit):
    have = {r[0] for r in con.sql(
        "SELECT source_filename FROM _pq_archive_log WHERE source_type = 'green'").fetchall()}
    todo = [ym for ym in months() if f"green_tripdata_{ym}" not in have][:limit]
    if not todo:
        print("  green: nothing new")
        return 0, 0
    landed = refused = 0
    # One temp dir per batch, so a long drain does not hold the whole archive on the runner disk at
    # once. Green months are far smaller than yellow's (~5-50 MB against 50-200), but the shape is
    # kept identical. `raw/` holds what TLC served, `out/` the normalised copy that is actually
    # uploaded; only `out/` is ever handed to dr.copy, so a refused month cannot leak in.
    batch_size, max_workers = 4, 4
    for i in range(0, len(todo), batch_size):
        batch = todo[i:i + batch_size]
        with tempfile.TemporaryDirectory() as tmp:
            raw_dir, out_dir = os.path.join(tmp, "raw"), os.path.join(tmp, "out")
            os.makedirs(raw_dir)
            os.makedirs(out_dir)
            got = []
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futs = {}
                for ym in batch:
                    name = f"green_tripdata_{ym}.parquet"
                    futs[ex.submit(fetch, TRIPS.format(ym=ym),
                                   os.path.join(raw_dir, name))] = (ym, name)
                for fut in as_completed(futs):
                    ym, name = futs[fut]
                    try:
                        etag = fut.result()
                    except Exception as e:
                        print(f"  WARN skip {ym}: {type(e).__name__}: {e}")
                        continue
                    path = os.path.join(raw_dir, name)
                    rows, cols = footer(path)
                    missing = [c for c in CORE_COLUMNS if c not in cols]
                    if missing:
                        # REFUSED, not landed-and-hoped: see the module docstring. Nothing is written
                        # to out/, so the archive only ever holds months every dialect can read.
                        print(f"  REFUSED {ym}: missing {missing} (has {len(cols)} columns) — "
                              f"not archived", flush=True)
                        refused += 1
                        continue
                    normalise(path, os.path.join(out_dir, name))
                    got.append((ym, name, rows, cols, etag))
            if not got:
                continue
            push_new(out_dir, "parquet_raw/green")
            now = datetime.now(timezone.utc).isoformat()
            for ym, name, rows, cols, etag in got:
                stem = name.removesuffix(".parquet")
                etag_sql = "NULL" if not etag else "'" + etag.replace("'", "''") + "'"
                con.sql(f"""INSERT INTO _pq_archive_log VALUES (
                    'green', '{stem}', '/green/{name}', '{now}'::TIMESTAMPTZ,
                    {rows}, '{TRIPS.format(ym=ym)}', {etag_sql}, '{stem}',
                    '{",".join(cols)}')""")
                print(f"  green {ym}: {rows:,} rows", flush=True)
                landed += 1
        save_log()
    return landed, refused


def land_zones():
    """The 265-row zone lookup. Refreshed at most daily, exactly as the AEMO DUID reference is —
    it is a slowly-changing reference table, not part of the archive being drained.

    TLC serves it as CSV and it is landed as PARQUET, which is not tidiness. Fabric Spark cannot
    read a headered CSV by column name from a path: `csv.`path`` defaults header=false and yields
    _c0.._c3, and the two routes that would fix that are both closed here — an external CSV table
    with an explicit schema is rejected outright, and the from_csv-over-text idiom the AEMO models
    use costs an explicit schema plus a header-row filter for a 265-row dimension. Converting once,
    here, makes the read one plain statement in all three dialects."""
    last = con.sql("SELECT max(archived_at) FROM _pq_archive_log "
                   "WHERE source_type = 'zone'").fetchone()[0]
    if last is not None and (datetime.now(last.tzinfo) - last).total_seconds() < 86400:
        print(f"  zone lookup fresh ({last}), skipping")
        return
    with tempfile.TemporaryDirectory() as tmp:
        raw = os.path.join(tmp, "raw")
        out = os.path.join(tmp, "out")
        os.makedirs(raw)
        os.makedirs(out)
        src = os.path.join(raw, "taxi_zone_lookup.csv")
        fetch(ZONES, src)
        dst = os.path.join(out, "taxi_zone_lookup.parquet")
        con.sql(f"""COPY (
            SELECT CAST("LocationID" AS INTEGER) AS "LocationID",
                   CAST("Borough" AS VARCHAR) AS "Borough",
                   CAST("Zone" AS VARCHAR) AS "Zone",
                   CAST("service_zone" AS VARCHAR) AS "service_zone"
            FROM read_csv_auto('{src.replace(chr(92), '/')}', header = true)
        ) TO '{dst.replace(chr(92), '/')}' (FORMAT PARQUET)""")
        rows = con.sql(f"SELECT count(*) FROM read_parquet('{dst.replace(chr(92), '/')}')"
                       ).fetchone()[0]
        push_replace(out, "parquet_raw/zone")
    con.sql("DELETE FROM _pq_archive_log WHERE source_type = 'zone'")
    now = datetime.now(timezone.utc).isoformat()
    con.sql(f"""INSERT INTO _pq_archive_log VALUES (
        'zone', 'taxi_zone_lookup', '/zone/taxi_zone_lookup.parquet', '{now}'::TIMESTAMPTZ,
        {rows}, '{ZONES}', NULL, 'taxi_zone_lookup', 'LocationID,Borough,Zone,service_zone')""")
    print(f"  zone lookup: {rows} rows")


print(f"Landing to: {FILES_PATH}")
print(f"Months from {START}, limit {DOWNLOAD_LIMIT}")
load_log()
landed, refused = land_trips(DOWNLOAD_LIMIT)
land_zones()
save_log()

con.sql("""SELECT source_type, count(*) AS files, sum(row_count) AS rows
           FROM _pq_archive_log GROUP BY source_type ORDER BY source_type""").show()
print(f"Landed this run: green={landed}" + (f", REFUSED={refused}" if refused else ""))
print("Done. Now run:  dbt build --target duckrun   (with DATASET=green)")
