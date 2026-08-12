"""Assert the (DATASET, target) gating matrix enables exactly the right nodes — offline, free.

This is the standing guard for the trap documented in `dbt_project.yml`: a gate placed on the
PROJECT key rather than a FOLDER key also matches the generic tests declared in `models/<dataset>/
_*.yml`, because a generic test's fqn is the fqn of its **yml file** and carries no dialect segment.
That bug once left `dwh` and `spark` running ZERO tests while every doc said otherwise.

It also guards the second, newer trap: `+enabled` is a scalar, so a deeper folder key CLOBBERS a
shallower one. Splitting `target.type` onto the dialect key and `DATASET` onto the dataset key parses
clean, runs clean, and builds the WRONG DATASET. Nothing goes red — except this.

And the cheapest failure of all: a `DATASET` typo makes every gate false, `dbt build` reports
"Nothing to do", the leg goes GREEN and the run records a layout of nothing.

`dbt parse` needs no credentials and takes seconds, so this runs in the free `checks` job before any
leg spends capacity, and again inside each leg immediately before `dbt build`.

Usage:
    python .github/scripts/check_gating.py                 # the whole matrix (8 parses)
    python .github/scripts/check_gating.py duckrun nyc     # one cell, as a leg runs it
"""
import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))

# target -> the dialect folder it must enable. The adapter `type`s behind these are duckrun/duckdb/
# fabric/fabricspark; this file states the OUTCOME, dbt_project.yml states the condition, and the
# point of the check is that the two agree.
DIALECT = {"duckrun": "duckdb", "iceberg": "duckdb", "dwh": "dwh", "spark": "spark"}

# dataset -> the model names its tree must produce, identical across all three dialects (that
# identity is what lets ref(), the shared patch files and the singular tests be written once).
MODELS = {
    "aemo": {"stg_csv_archive_log", "dim_calendar", "dim_duid",
             "fct_price", "fct_scada", "fct_price_today", "fct_scada_today", "fct_summary"},
    "nyc": {"stg_parquet_archive_log", "dim_date", "dim_zone", "fct_trips"},
    "bts": {"stg_flights_archive_log", "dim_flight_date", "dim_carrier", "fct_flights"},
}

# dataset -> how many data tests must be ENABLED. Generic tests come from models/<dataset>/_*.yml
# and singular ones from tests/<dataset>/<dialect>/. The count is asserted rather than the names
# because the whole failure mode is tests silently vanishing, and a count catches that without
# re-listing the suite in two places.
# bts is 6 like aemo — four generic (unique/not_null on dim_carrier.code and dim_flight_date.date)
# plus two singular (the archive-log reconciliation, and the whitespace guard that every STRING
# join key crossing engines gets).
TESTS = {"aemo": 6, "nyc": 5, "bts": 6}

DATASETS = tuple(MODELS)
TARGETS = tuple(DIALECT)


def parse(target, dataset, tmp):
    """Run `dbt parse` for one cell and return its manifest."""
    env = dict(os.environ, DATASET=dataset)
    # Nothing here connects, but the profile renders env_var() calls; give every one of them a value
    # so a missing var reads as a parse failure of the profile rather than of the gating.
    for k, v in (("ONELAKE_TABLES_PATH", os.path.join(tmp, "warehouse")),
                 ("WAREHOUSE_PATH", os.path.join(tmp, "warehouse")),
                 ("FILES_PATH", os.path.join(tmp, "landing")),
                 ("ONELAKE_TOKEN", "parse-only"), ("ONELAKE_ENDPOINT", "parse-only"),
                 ("FABRIC_DWH_SERVER", "parse-only"), ("FABRIC_DWH_NAME", "parse-only"),
                 # dbt-fabricspark validates these two as UUIDs at profile load, before any
                 # parsing happens — a placeholder string fails the credentials check, not the gate.
                 ("FABRIC_WORKSPACE_ID", "00000000-0000-0000-0000-000000000000"),
                 ("FABRIC_LAKEHOUSE_ID", "00000000-0000-0000-0000-000000000000"),
                 ("FABRIC_LAKEHOUSE_NAME", "parse-only")):
        env.setdefault(k, v)
    target_dir = os.path.join(tmp, f"{dataset}-{target}")
    r = subprocess.run(
        [sys.executable, "-m", "dbt.cli.main", "parse",
         "--target", target, "--profiles-dir", ROOT, "--project-dir", ROOT,
         "--target-path", target_dir, "--no-partial-parse"],
        env=env, cwd=ROOT, capture_output=True, text=True)
    path = os.path.join(target_dir, "manifest.json")
    if not os.path.exists(path):
        sys.exit(f"dbt parse failed for dataset={dataset} target={target}:\n"
                 f"{(r.stdout + r.stderr)[-3000:]}")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def enabled(manifest, kind):
    return {n["name"] for n in manifest["nodes"].values() if n["resource_type"] == kind}


def disabled(manifest, kind):
    return {n["name"] for group in manifest.get("disabled", {}).values()
            for n in group if n["resource_type"] == kind}


def check(target, dataset, tmp):
    """Assert one (target, dataset) cell. Returns a list of failure strings."""
    m = parse(target, dataset, tmp)
    bad = []
    want = MODELS[dataset]
    got = enabled(m, "model")
    if got != want:
        bad.append(f"models enabled {sorted(got)} != expected {sorted(want)}")

    # Every model of the OTHER datasets must be disabled — not merely absent. A dataset whose tree
    # failed to parse at all would also produce an empty enabled set, and that must not read as pass.
    off = disabled(m, "model")
    for other in DATASETS:
        if other == dataset:
            continue
        missing = MODELS[other] - off
        if missing:
            bad.append(f"dataset {other!r} models not disabled: {sorted(missing)}")

    # The point of the whole check: every enabled node's fqn must carry this dataset AND, for the
    # nodes that live in a dialect tree, this target's dialect. A gate that leaked would show up
    # here as a node from the wrong folder.
    for n in m["nodes"].values():
        fqn = n["fqn"]
        if n["resource_type"] == "model":
            if len(fqn) < 3 or fqn[1] != dataset or fqn[2] != DIALECT[target]:
                bad.append(f"model {n['name']} has fqn {fqn} — expected "
                           f"[<project>, {dataset!r}, {DIALECT[target]!r}, ...]")
        elif n["resource_type"] == "test" and len(fqn) > 1 and fqn[1] in DATASETS:
            if fqn[1] != dataset:
                bad.append(f"test {n['name']} belongs to dataset {fqn[1]!r}, not {dataset!r}")

    # Tests, and this is the assertion the original bug would have failed: the generic ones must be
    # enabled on ALL FOUR targets, not just the DuckDB pair.
    n_tests = len(enabled(m, "test"))
    if n_tests != TESTS[dataset]:
        names = sorted(enabled(m, "test"))
        bad.append(f"{n_tests} data tests enabled, expected {TESTS[dataset]}: {names}")
    return bad


def main():
    args = sys.argv[1:]
    if args:
        if len(args) != 2 or args[0] not in TARGETS or args[1] not in DATASETS:
            sys.exit(f"usage: check_gating.py [<target: {'|'.join(TARGETS)}> "
                     f"<dataset: {'|'.join(DATASETS)}>]")
        cells = [(args[0], args[1])]
    else:
        cells = [(t, d) for d in DATASETS for t in TARGETS]

    failures = 0
    with tempfile.TemporaryDirectory() as tmp:
        for target, dataset in cells:
            bad = check(target, dataset, tmp)
            mark = "ok  " if not bad else "FAIL"
            print(f"{mark} dataset={dataset:5s} target={target:8s} "
                  f"-> models/{dataset}/{DIALECT[target]}", flush=True)
            for b in bad:
                print(f"       {b}", flush=True)
            failures += bool(bad)
    if failures:
        sys.exit(f"\n{failures} of {len(cells)} gating cell(s) wrong — see above")
    print(f"\n{len(cells)} gating cell(s) correct")


if __name__ == "__main__":
    main()
