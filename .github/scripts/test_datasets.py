"""The dataset -> Fabric item map is written twice — assert the two copies never drift.

`.github/scripts/datasets.py` is what PROVISIONS the items and READS them back (provision.py,
stats.py, fabric_run.py). `benchmark/engines.py` is what DEPLOYS a semantic model over them. The
second deliberately does not import the first: `benchmark/` is built to be deletable by removing one
directory and one workflow file, and that isolation is worth more than sharing a dict.

The cost of the duplication is exactly this test. A divergence is silent and it got worse when the
second dataset arrived: with one dataset a wrong name meant "item not found" and a loud failure;
now `dbt_delta` where `dbt_nyc_delta` was meant is a REAL item that really exists, so the benchmark
would deploy a Direct Lake model over the other dataset's lakehouse and report its timings under
this run — a plausible number for the wrong table.

Same pattern the repo already uses for the stats.py/engines.py mirror, extended to the dataset axis.
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(ROOT, "benchmark"))

import datasets  # noqa: E402
import engines as E  # noqa: E402


def test_every_dataset_is_known_to_both():
    assert set(datasets.DATASETS) == set(E.DATASET_ITEMS)


def test_item_names_agree_for_every_dataset_and_engine():
    for name, spec in datasets.DATASETS.items():
        assert spec["items"] == E.DATASET_ITEMS[name], f"item map differs for dataset {name!r}"


def test_engine_kinds_agree():
    assert datasets.ENGINE_KIND == E.ENGINE_KIND


def test_writers_agree():
    assert datasets.WRITER == E.WRITER


def test_model_prefixes_agree():
    for name, spec in datasets.DATASETS.items():
        assert spec["model_prefix"] == E.PREFIXES[name], f"model prefix differs for {name!r}"


def _names(spec):
    # The bench job's per-phase shortcut lakehouses (`<output item>_dl` / `_dq`) are real items a
    # run creates, so they are in the shadow check like everything else.
    items = list(spec["items"].values())
    return (items + [spec["landing"], spec["dwh_src"]]
            + [f"{i}_{ph}" for i in items for ph in ("dl", "dq")])


def test_no_two_datasets_share_or_shadow_an_item_name():
    # Sharing is the fatal one: provision.ensure() finds-or-creates by display name, so two
    # datasets naming an item the same means the second builds into the first's lakehouse.
    #
    # Shadowing (one name a strict prefix of another dataset's) is the softer one. It is harmless
    # to provision.find(), which compares with ==, but cu/ carried a substring matcher for years and
    # a scheme that works only because nothing loose matches today is a trap for the next reader.
    #
    # WITHIN a dataset a prefix relation is fine and intentional: `dbt_dwh` / `dbt_dwh_src` is the
    # warehouse and the lakehouse that hosts its landing shortcut, and that pair predates all of
    # this. Only the CROSS-dataset case is checked.
    for a_name, a in datasets.DATASETS.items():
        for b_name, b in datasets.DATASETS.items():
            if a_name >= b_name:
                continue
            shared = set(_names(a)) & set(_names(b))
            assert not shared, f"{a_name} and {b_name} share item name(s) {sorted(shared)}"
            for x in _names(a):
                for y in _names(b):
                    assert not y.startswith(x) and not x.startswith(y), \
                        f"{a_name}'s {x!r} shadows {b_name}'s {y!r}"


def test_selected_refuses_an_unknown_dataset():
    # The highest-probability new failure in the whole change: a DATASET typo makes every dbt
    # `+enabled` gate false, so the build reports "Nothing to do" and the leg goes GREEN having
    # built nothing. Every entry point resolves through selected(), so it must raise.
    for bad in ("NYC", "nyc ", "", "taxi"):
        try:
            datasets.selected(bad)
        except SystemExit:
            continue
        raise AssertionError(f"selected({bad!r}) should have raised")


def test_selected_accepts_the_known_names_and_defaults_to_aemo():
    assert datasets.selected("nyc") == "nyc"
    assert datasets.selected("aemo") == "aemo"
    assert datasets.DEFAULT == "aemo"


def test_the_mart_is_one_of_the_datasets_own_tables():
    # stats.py profiles MART's encodings and ordering by looking it up in the stats it just read;
    # a mart that is not in TABLES would silently produce no ordering block at all.
    for name, spec in datasets.DATASETS.items():
        assert spec["mart"] in spec["tables"], f"{name}: mart is not in tables"


def test_bench_lakehouse_names_agree_between_provision_and_engines():
    # provision.py CREATES the per-phase shortcut lakehouses and benchmark/engines.py DEPLOYS the
    # semantic models over them, each computing the name from its own copy of the item map — the
    # same pinned-duplication discipline as test_item_names_agree, and a divergence is the same
    # silent failure: a model deployed over an item that does not exist, or worse, someone else's.
    for name, spec in datasets.DATASETS.items():
        os.environ["DATASET"] = name
        try:
            for engine, item in spec["items"].items():
                for ph in ("dl", "dq"):
                    assert E.shortcut_lakehouse(engine, ph) == f"{item}_{ph}", \
                        f"{name}/{engine}/{ph}: engines.shortcut_lakehouse diverged"
        finally:
            os.environ.pop("DATASET", None)


def _bim_schemas(dataset):
    """schema -> tables as the dataset's semantic-model template declares them, read as DATA —
    benchmark/ stays import-free from here, and a .bim is a JSON file like any other."""
    import json
    templates = {"aemo": "fct_summary", "nyc": "fct_trips", "bts": "fct_flights",
                 "green": "fct_green_trips", "cms": "fct_cms_payments",
                 "tpcds": "store_sales"}
    path = os.path.join(ROOT, "benchmark", f"{templates[dataset]}.SemanticModel", "model.bim")
    with open(path, encoding="utf-8") as f:
        bim = json.load(f)
    out = {}
    for t in bim["model"]["tables"]:
        src = (t.get("partitions") or [{}])[0].get("source") or {}
        if src.get("entityName"):
            out.setdefault(src.get("schemaName"), set()).add(src["entityName"])
    return out


def test_table_schemas_agrees_with_every_semantic_model_template():
    # provision.py builds the per-phase Tables shortcuts from table_schemas(); a table filed under
    # the wrong schema is a shortcut pointing at a path that does not exist, discovered only when
    # the deploy's readiness probe times out on paid capacity.
    for name in datasets.DATASETS:
        declared = {s: set(ts) for s, ts in datasets.table_schemas(name).items()}
        assert declared == _bim_schemas(name), f"{name}: schema split differs from the template"
