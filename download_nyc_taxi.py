"""Land the NYC TLC yellow-taxi archive — the second dataset, and the skewed half of the pair.

Sibling of download_aemo.py, same shape and the same watermark idiom (an archive-log parquet at the
Files root, so a re-run only fetches what is new), retargeted at a source that is already parquet.

WHY THIS DATASET EXISTS HERE. The V-Order result rested on `mart.fct_summary`: 143M rows of five
narrow columns on a perfectly regular 5-minute x DUID grid. V-Order is an ENCODING pass — this repo
measured that it does not reorder rows — so it acts on column count x categorical skew, and that
table supplies neither. Yellow taxi supplies both: 17 columns after the core subset, `RatecodeID` /
`store_and_fwd_flag` / `payment_type` / `VendorID` at 97-99% single-value, and the two LocationIDs
Zipfian on Manhattan and the airports. AEMO is then the near-uniform arm of the same experiment.

THE ARCHIVE IS NORMALISED AT LAND TIME, AND THAT IS THE POINT. TLC has published this data since
2009 and republished all of it as parquet in 2022; the schema still moved over the years, in two
different ways, and only one of them is survivable by a reader:

  columns    `congestion_surcharge` from 2019, `airport_fee` from 2022 and spelled `Airport_fee`
             in some months. A name-based read can ignore these.
  TYPES      `passenger_count` and `RatecodeID` ship as int64 in some years and double in others.
             THIS is the one that cannot be read around: Spark refuses a parquet scan whose files
             disagree on a column's type ("Failed to merge incompatible data types"), with or
             without mergeSchema. DuckDB would unify them and Fabric Warehouse would take its own
             view — so the three engines would not even be reading the same table.

So each month is decoded once here and rewritten with the canonical 17-column schema before it is
uploaded. Two things follow. Every engine reads one homogeneous archive with one plain statement,
and the *stored* types are identical across all four outputs, which is what the parity table
assumes. And a month whose footer lacks a core column is REFUSED, loudly, rather than landed for a
model to discover: a schema surprise costs a free runner minute instead of a mid-write failure with
Fabric capacity already spent.

Normalising the INPUT does not touch what is being measured. The experiment is the layout each
engine WRITES; the source is a source, and this is the same call download_aemo.py already makes by
landing plain CSV because Fabric OPENROWSET cannot read gzip.

`CORE_COLUMNS` below is mirrored by `macros/nyc_trip_columns.sql`, which is what the three model
trees generate their SELECT lists from. `.github/scripts/test_nyc_columns.py` asserts the two never
drift — a divergence would land files the models cannot read, or refuse files they can.

Env in:
  FILES_PATH      the landing lakehouse Files root (abfss:// or a local path)
  download_limit  months to fetch this run (oldest first, so a capped drain extends deterministically)
  NYC_START       first month to consider, YYYY-MM (default 2011-01 — the first month of the stable
                  zone-id schema; 2009-2010 carry pickup/dropoff lat/lon instead and are a different
                  table, not a smaller one)
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
START = os.environ.get("NYC_START", "2011-01").strip()

# TLC's CloudFront origin. Monthly files, and a `misc/` folder holding the zone lookup.
CDN = "https://d37ci6vzurychx.cloudfront.net"
TRIPS = CDN + "/trip-data/yellow_tripdata_{ym}.parquet"
ZONES = CDN + "/misc/taxi_zone_lookup.csv"

# TLC publishes with roughly a two-month lag; asking for a month that does not exist yet is a 403,
# not a 404, so probing is not free. Skip the tail rather than discover it.
PUBLISH_LAG_MONTHS = 3

# Mirrored by macros/nyc_trip_columns.sql — see that file for why it is 17 and not TLC's 19.
CORE_COLUMNS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "trip_distance", "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID",
    "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount",
]

# The canonical type every archived month is rewritten to. Mirrored by nyc_trip_type() in
# macros/nyc_trip_columns.sql, which is what each dialect CASTs to — so the read is a no-op cast
# over already-canonical data, kept as an explicit declaration that all four engines store the same
# schema no matter what their reader would have inferred.
CANONICAL = {
    "tpep_pickup_datetime": "TIMESTAMP", "tpep_dropoff_datetime": "TIMESTAMP",
    "store_and_fwd_flag": "VARCHAR",
    "VendorID": "INTEGER", "passenger_count": "INTEGER", "RatecodeID": "INTEGER",
    "PULocationID": "INTEGER", "DOLocationID": "INTEGER", "payment_type": "INTEGER",
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
# without re-reading 180 files. The macros key on `file_stem`; see macros/new_parquet_files.sql.
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
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (dbt-nyc)"})
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
    """Rewrite one month with the canonical 17-column schema — see the module docstring.

    `columns` is left to the default: this is the LANDING format, not a layout under test, and
    giving it a hand-picked row-group size would put a second, invisible geometry knob upstream of
    the one the dispatch controls. Every engine reads the same bytes either way."""
    con.sql("COPY (SELECT "
            + ", ".join(f'CAST("{c}" AS {canonical_type(c)}) AS "{c}"' for c in CORE_COLUMNS)
            + f" FROM read_parquet('{src.replace(chr(92), '/')}')) "
            + f"TO '{dst.replace(chr(92), '/')}' (FORMAT PARQUET, COMPRESSION ZSTD)")


def land_trips(limit):
    have = {r[0] for r in con.sql(
        "SELECT source_filename FROM _pq_archive_log WHERE source_type = 'yellow'").fetchall()}
    todo = [ym for ym in months() if f"yellow_tripdata_{ym}" not in have][:limit]
    if not todo:
        print("  yellow: nothing new")
        return 0, 0
    landed = refused = 0
    # One temp dir per batch, so a long drain does not hold the whole archive on the runner disk at
    # once — a single month is ~50-200 MB and a full 2011-onward drain is tens of GB. `raw/` holds
    # what TLC served, `out/` the normalised copy that is actually uploaded; only `out/` is ever
    # handed to dr.copy, so a refused month cannot leak into the archive.
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
                    name = f"yellow_tripdata_{ym}.parquet"
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
            push_new(out_dir, "parquet_raw/yellow")
            now = datetime.now(timezone.utc).isoformat()
            for ym, name, rows, cols, etag in got:
                stem = name.removesuffix(".parquet")
                etag_sql = "NULL" if not etag else "'" + etag.replace("'", "''") + "'"
                con.sql(f"""INSERT INTO _pq_archive_log VALUES (
                    'yellow', '{stem}', '/yellow/{name}', '{now}'::TIMESTAMPTZ,
                    {rows}, '{TRIPS.format(ym=ym)}', {etag_sql}, '{stem}',
                    '{",".join(cols)}')""")
                print(f"  yellow {ym}: {rows:,} rows", flush=True)
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
print(f"Landed this run: yellow={landed}" + (f", REFUSED={refused}" if refused else ""))
print("Done. Now run:  dbt build --target duckrun   (with DATASET=nyc)")
