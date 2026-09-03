"""The engine registry for the benchmark — one place that knows what the four engines are.

Deliberately mirrors `.github/scripts/stats.py`'s `ENGINES` / `WRITER`: that script is the parity
dashboard over the same four items, and the two must never disagree about which Fabric item belongs
to which engine. If an item is renamed, both change together.

The one thing stats.py has no opinion about and this does: the deploy MODE, and there are two,
measured as two self-contained PHASES per engine. The **Direct Lake** phase (`dl`, the default) is
THE ranking — an in-memory transcode straight from the Delta files, the only reading in which the
answer is about the *physical layout*. The **DirectQuery** phase (`dq`) deploys the SAME `.bim`
with `mode="direct_query"` under `<prefix><engine>_dq` and is a separate, never-blended column set:
render_report partitions every ranking and side-by-side table on `is_dq()`, because a pushdown
timing is not a slow layout and a report that mixed the two kinds of number invited exactly that
misreading — that used to be the reason no DirectQuery existed here at all, and the partition is
what made it admissible as CONTEXT rather than as a competitor.

Within a phase the mode is still a **premise, not a per-engine setting** — one constant per phase,
never a dict. Both models bind to that phase's SHORTCUT LAKEHOUSE (`<output item>_dl` / `_dq`,
created by `provision.py bench_prepare`), not to the output item: OneLake bills a transaction
against the item hosting the shortcut, so each phase's reads land on its own GUID instead of mixing
into the engine's ETL column. That also makes the item KIND irrelevant at deploy time — dwh's
models read its warehouse Tables through a lakehouse shortcut like everyone else's.

The hot-only path downstream survives and is not about DirectQuery: a model can be missing its
cold and warm numbers because its job died before reporting them, or because the dispatch asked for
fewer than three passes. `render_report._totals` scopes each metric to the models that have it, so a
missing tier is a gap rather than a zero.
"""
import json
import os

# Item names PER DATASET — the same map .github/scripts/datasets.py holds, deliberately duplicated
# rather than imported. This directory is built to be deletable by removing one folder and one
# workflow file, and an import from .github/scripts would end that. The duplication is what keeps
# the deletion free, exactly as `report.py` re-implements a dict-union rather than importing
# record.py. `.github/scripts/test_datasets.py` asserts the two maps agree for every dataset, so
# the copy cannot drift silently — which matters more here than it did with one dataset, because a
# mismatch now deploys a semantic model over the OTHER dataset's lakehouse rather than failing.
DATASET_ITEMS = {
    "aemo": {"duckrun": "dbt_delta", "iceberg": "dbt_iceberg",
             "spark": "dbt_spark", "dwh": "dbt_dwh"},
    "nyc": {"duckrun": "dbt_nyc_delta", "iceberg": "dbt_nyc_iceberg",
            "spark": "dbt_nyc_spark", "dwh": "dbt_nyc_dwh"},
    "bts": {"duckrun": "dbt_bts_delta", "iceberg": "dbt_bts_iceberg",
            "spark": "dbt_bts_spark", "dwh": "dbt_bts_dwh"},
    "green": {"duckrun": "dbt_green_delta", "iceberg": "dbt_green_iceberg",
              "spark": "dbt_green_spark", "dwh": "dbt_green_dwh"},
    "cms": {"duckrun": "dbt_cms_delta", "iceberg": "dbt_cms_iceberg",
            "spark": "dbt_cms_spark", "dwh": "dbt_cms_dwh"},
}

# Which Fabric item KIND each engine writes into — a property of the adapter, not of the dataset,
# so it is not repeated per dataset above.
ENGINE_KIND = {"duckrun": "lakehouses", "iceberg": "lakehouses",
               "spark": "lakehouses", "dwh": "warehouses"}


def dataset():
    """The dataset this run covers. Refuses an unknown name rather than falling back — the same
    rule .github/scripts/datasets.py enforces, and for the same reason: DATASET also drives dbt's
    `+enabled` gates, where a typo silently enables nothing and the leg goes green."""
    name = (os.environ.get("DATASET") or "aemo").strip()
    if name not in DATASET_ITEMS:
        raise SystemExit(f"unknown dataset {name!r}; known: {', '.join(DATASET_ITEMS)}")
    return name


# (engine label, Fabric item display name, item kind) — same triple as stats.py's ENGINES.
ENGINES = [(e, DATASET_ITEMS[dataset()][e], ENGINE_KIND[e])
           for e in ("duckrun", "iceberg", "spark", "dwh")]

ITEM = {e: item for e, item, _ in ENGINES}
KIND = {e: kind for e, _, kind in ENGINES}

# What actually wrote the parquet behind each engine's Delta log (stats.py's WRITER, verbatim).
WRITER = {"duckrun": "delta-rs", "iceberg": "duckdb (iceberg)",
          "spark": "spark", "dwh": "warehouse"}

# How every engine's tables are read, in the spelling duckrun's deploy(mode=) takes. One constant
# PER PHASE, never a dict: within a phase all four engines must be read the same way, so the mode is
# the phase's premise rather than a knob. Independent of the item KIND since duckrun 0.4.36 — a
# warehouse's Tables are Delta in OneLake exactly like a lakehouse's, and both phases read through a
# lakehouse shortcut anyway.
DEPLOY_MODE = "direct_lake"
DEPLOY_MODE_DQ = "direct_query"

# The DirectQuery phase's model-name suffix. `engine_of("aemo_spark_dq")` returns `spark_dq`, so a
# DQ model labels its own column everywhere the render layer derives labels from model names.
DQ_SUFFIX = "_dq"


def is_dq(model):
    """Whether a model name (or bare engine label) belongs to the DirectQuery phase."""
    return str(model).endswith(DQ_SUFFIX)


def phase():
    """Which bench phase this invocation measures, from BENCH_PHASE — `dl` unless the workflow's
    DQ steps say otherwise. Refuses an unknown value for the usual reason: a typo that silently
    meant `dl` would deploy a second Direct Lake model under the DQ name and record its pushdown
    column from a transcode."""
    ph = os.environ.get("BENCH_PHASE") or "dl"
    if ph not in ("dl", "dq"):
        raise SystemExit(f"unknown BENCH_PHASE {ph!r}; use dl or dq")
    return ph


def shortcut_lakehouse(engine, ph=None):
    """The per-phase lakehouse this phase's model binds to: `<output item>_dl` / `_dq`.

    A deliberate copy of provision.py's `bench_lakehouse_name` — same isolation rule as
    DATASET_ITEMS itself, and `.github/scripts/test_datasets.py` pins the two together the same
    way. Computed from the registry at call time, not from the import-time ITEM map, so tests can
    exercise every dataset."""
    return f"{DATASET_ITEMS[dataset()][engine]}_{ph or phase()}"


ALL = [e for e, _, _ in ENGINES]

# Semantic models are named <PREFIX><engine>, and the PREFIX is the dataset. Within one run the DAX
# suite is identical across models, so the model name is the ONLY thing that identifies which
# engine's table a timing came from; across runs the prefix is what keeps two datasets' timings from
# colliding in `benchmark.timings`, which is keyed by model name.
PREFIXES = {"aemo": "aemo_", "nyc": "nyc_", "bts": "bts_", "green": "green_",
            "cms": "cms_"}


def prefix():
    return PREFIXES[dataset()]


def model_name(engine, ph=None):
    """`<prefix><engine>` for the Direct Lake phase, `<prefix><engine>_dq` for DirectQuery.

    The phase defaults to the env (`BENCH_PHASE`), so deploy_models.py and xmla_compare.py agree
    about which model a step touches without either passing it explicitly."""
    return f"{prefix()}{engine}{DQ_SUFFIX if (ph or phase()) == 'dq' else ''}"


def engine_of(model):
    """Inverse of model_name. Tolerates being handed a bare engine label already, and tolerates the
    OTHER dataset's prefix — merge_reports and the render layer read fragments by model name, and a
    hard failure there would lose a whole run's report over a naming question."""
    for p in PREFIXES.values():
        if model.startswith(p):
            return model[len(p):]
    return model


def selected(default=None):
    """The engines this run covers, from BENCH_ENGINES (comma-separated).

    Order decides only the ORDER THEY ARE MEASURED IN — it is the bench matrix's order, and index 0
    is simply the job that skips the idle gap. It used to also name the reference every ratio was
    taken against; there is no reference any more (see render_report's docstring), so no number in
    the report depends on how the dispatch happened to list the engines."""
    raw = (os.environ.get("BENCH_ENGINES") or "").strip()
    if not raw:
        return list(default if default is not None else ALL)
    out = []
    for part in raw.split(","):
        e = part.strip().lower()
        if not e:
            continue
        if e not in ITEM:
            raise SystemExit(f"unknown engine {e!r}; known: {', '.join(ALL)}")
        if e not in out:
            out.append(e)
    if not out:
        raise SystemExit("BENCH_ENGINES was set but named no engine")
    return out


def items():
    """The {engine: {item, kind, guid, writer}} map that resolve_env.py wrote to
    BENCH_ITEMS. Raises with a pointer rather than a KeyError, because every consumer of this needs
    resolve_env.py to have run first."""
    raw = os.environ.get("BENCH_ITEMS")
    if not raw:
        raise SystemExit("BENCH_ITEMS is not set — run benchmark/resolve_env.py first")
    return json.loads(raw)
