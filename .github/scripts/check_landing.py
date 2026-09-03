"""Refuse a build whose landing archive is EMPTY, in the free job, before any compute is spent.

WHAT THIS COSTS TO NOT HAVE, measured: run 33734219062 was a scheduled `tpcds` cell. `land`
provisioned the lakehouse and reported success, `plan` was green, and the spark leg then acquired a
Livy session and failed 29 models deep on

    [PATH_NOT_FOUND] Path does not exist: abfss://…/Files/landing/parquet_raw/customer_address

because nothing had ever been generated into that archive. Every tpcds slot of the weekly grid does
this — four a week, each spending Fabric compute to discover that its input is not there.

**IT IS THE SCHEDULE THAT MAKES THIS UNREACHABLE, NOT A BUG IN THE GENERATOR.** `download_tpcds.py`
is complete and works; but `land` only runs a downloader when `skip_download` is off, and a
scheduled run FORCES it on (benchmark.yml: `github.event_name == 'schedule' || inputs.skip_download`)
so that every scheduled build measures the same archive. So a dataset whose landing has never been
populated by hand can never populate itself, and the grid rediscovers that weekly.

THE CHECK IS A LISTING, NOT A QUERY, and it is deliberately the dumbest possible one: are there any
bytes under `Files` that are not duckrun's own round-trip directory? It does NOT check the archive
log, the table set, or that the dataset's own files are present — a half-drained archive is a normal
state here (`download_limit` exists to produce one) and refusing it would block ordinary work. The
only thing it refuses is NOTHING AT ALL, which is never a state a build can succeed from.

It runs in `land`, which is a free ubuntu runner that has already resolved the token and installed
duckrun — so the check adds one listing and no dependency. `plan` would be earlier and cheaper still,
but it holds no Fabric credentials, and giving it some to save a runner minute is the wrong trade.
"""
import os
import sys

# duckrun's own `run_python` round-trip — the result and log files a notebook writes back. Never
# input, and `stats.py` excludes it from the landing block for the same reason; a leftover pair from
# an earlier dispatch would otherwise make an empty archive look populated, which is exactly the
# state this refuses.
NOT_ARCHIVE = "duckrun_remote"


def landed_bytes(files_path):
    """`(files, bytes)` under the archive, excluding `NOT_ARCHIVE`."""
    import obstore
    import duckrun
    from dbt.adapters.duckrun import objectstore, secret

    dr = duckrun.connect(files_path, read_only=True)
    store = objectstore.build_store(files_path, secret.refreshed(dr.storage_options))
    files = size = 0
    for batch in obstore.list(store):
        for o in batch:
            path = o["path"]
            folder = path.rsplit("/", 1)[0] if "/" in path else "(root)"
            if folder == NOT_ARCHIVE or folder.startswith(NOT_ARCHIVE + "/"):
                continue
            files += 1
            size += int(o["size"] or 0)
    return files, size


def main():
    files_path = (os.environ.get("FILES_PATH") or "").strip()
    dataset = (os.environ.get("DATASET") or "aemo").strip()
    if not files_path:
        # Not fatal: this runs after `provision.py land` and only that step sets FILES_PATH, so an
        # empty value means the provision step did not run. That failure is already red on its own,
        # and turning it into a second, differently-worded red helps nobody.
        sys.stderr.write("FILES_PATH is unset — nothing to check\n")
        return 0

    try:
        files, size = landed_bytes(files_path)
    except Exception as exc:
        # BEST-EFFORT ON THE READ, FATAL ONLY ON A CONFIRMED EMPTY ARCHIVE. A listing that throws is
        # a question that could not be asked; refusing the build on it would turn a transient OneLake
        # error into a failed dispatch, which is a worse trade than the leg failing later on its own.
        sys.stderr.write(f"could not list {files_path}: {exc}\n")
        return 0

    if files:
        sys.stderr.write(f"{dataset}: archive holds {files:,} file(s), "
                         f"{size / 1048576:,.0f} MB\n")
        return 0

    # THE MESSAGE IS THE POINT. Whoever reads this is looking at a red run on a dataset they did not
    # dispatch, and the remedy is a hand dispatch they have no reason to know exists.
    sys.stderr.write(
        f"\n{dataset}: THE LANDING ARCHIVE IS EMPTY — refusing to build.\n\n"
        f"  {files_path}\n\n"
        "Nothing has ever been landed for this dataset, so every model would fail on a missing\n"
        "path partway through a leg that had already spent Fabric compute. A scheduled run cannot\n"
        "fix this by itself: it forces `skip_download` on, which is what makes every scheduled\n"
        "build measure the same archive.\n\n"
        "Land it once, by hand:\n\n"
        f"  gh workflow run Benchmark -f dataset={dataset} -f skip_download=false \\\n"
        "     -f build=false -f benchmark=false -f download_limit=<N>\n\n"
        "`download_limit` is a file count on most datasets, PROGRAM YEARS on cms, and the dsdgen\n"
        "SCALE FACTOR (1, 10 or 100) on tpcds — where landing generates rather than downloads and\n"
        "is a one-off that spends Fabric compute. See TODO.md before picking a value.\n")
    return 1


if __name__ == "__main__":
    sys.exit(main())
