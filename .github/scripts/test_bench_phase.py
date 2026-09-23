"""Offline tests for the bench job's two-phase item lifecycle, against a stubbed Fabric.

Each bench phase (Direct Lake, DirectQuery) creates a shortcut lakehouse, deploys a semantic model
over it, measures, and deletes both items — `provision.py bench_drop <phase>` is that delete, and
it shares `drop_guid` with the teardown, so what is tested here is the SELECTION: a phase must
delete its own two items, in model-before-lakehouse order, and nothing else's.

    python -m pytest .github/scripts/test_bench_phase.py -q
"""
import importlib
import json
import os
import sys
import types

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

WS = "00000000-0000-0000-0000-0000000000ws"


class Resp:
    def __init__(self, status=200, payload=None):
        self.status_code = status
        self._payload = payload if payload is not None else {}
        self.text = json.dumps(self._payload)

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise AssertionError(f"unexpected raise_for_status on {self.status_code}")


class Fabric:
    def __init__(self, items, undeletable=()):
        self.items = dict(items)
        self.undeletable = set(undeletable)
        self.deletes = []

    def get(self, url, headers=None, **kw):
        if "/items/" in url:
            guid = url.rsplit("/", 1)[1]
            return Resp(200, {"id": guid}) if guid in self.items else Resp(404)
        raise AssertionError(f"unexpected GET {url}")

    def delete(self, url, headers=None, **kw):
        guid = url.rsplit("/", 1)[1]
        self.deletes.append(guid)
        if guid in self.undeletable:
            return Resp(202)
        self.items.pop(guid, None)
        return Resp(200)

    def post(self, url, headers=None, **kw):
        raise AssertionError(f"bench_drop must not POST: {url}")


FOUR_ITEMS = {
    "SEM-DL": {"role": "semantic_model", "kind": "SemanticModel", "name": "aemo_duckrun"},
    "LH-DL": {"role": "bench_dl", "kind": "Lakehouse", "name": "dbt_delta_dl"},
    "SEM-DQ": {"role": "semantic_model_dq", "kind": "SemanticModel", "name": "aemo_duckrun_dq"},
    "LH-DQ": {"role": "bench_dq", "kind": "Lakehouse", "name": "dbt_delta_dq"},
    "OUT": {"role": "output", "kind": "Lakehouse", "name": "dbt_delta"},
}


def run_bench_drop(tmp_path, monkeypatch, phase, record_items, fabric_items=None,
                   undeletable=(), run_record=True):
    fab = Fabric(fabric_items if fabric_items is not None else
                 {g: it["name"] for g, it in record_items.items()}, undeletable)
    fake_requests = types.SimpleNamespace(get=fab.get, delete=fab.delete, post=fab.post)
    auth = types.ModuleType("duckrun.auth")
    auth.get_fabric_token = lambda: "TEST-TOKEN"
    monkeypatch.setitem(sys.modules, "requests", fake_requests)
    monkeypatch.setitem(sys.modules, "duckrun", types.ModuleType("duckrun"))
    monkeypatch.setitem(sys.modules, "duckrun.auth", auth)
    monkeypatch.setattr("time.sleep", lambda *_: None)

    frag = tmp_path / "record-40-bench-duckrun.json"
    frag.write_text(json.dumps({"items": record_items}), encoding="utf-8")
    monkeypatch.setenv("WS_ID", WS)
    if run_record:
        monkeypatch.setenv("RUN_RECORD", str(frag))
    else:
        monkeypatch.delenv("RUN_RECORD", raising=False)
    monkeypatch.setattr(sys, "argv", ["provision.py", "bench_drop", phase])
    sys.modules.pop("provision", None)
    importlib.import_module("provision")
    written = json.loads(frag.read_text(encoding="utf-8"))
    return fab, written.get("items", {})


def test_the_dl_phase_deletes_its_model_then_its_lakehouse_and_nothing_else(tmp_path, monkeypatch):
    fab, rec = run_bench_drop(tmp_path, monkeypatch, "dl", FOUR_ITEMS)
    assert fab.deletes == ["SEM-DL", "LH-DL"], \
        "the model reads through the lakehouse, so it goes first — and the dq phase's items stay"
    assert rec["SEM-DL"]["deleted"] and rec["LH-DL"]["deleted"]
    assert "deleted" not in rec["SEM-DQ"] and "deleted" not in rec["LH-DQ"]
    assert "deleted" not in rec["OUT"], "the engine's output item is teardown's, not a phase's"


def test_the_dq_phase_deletes_its_own_pair(tmp_path, monkeypatch):
    fab, rec = run_bench_drop(tmp_path, monkeypatch, "dq", FOUR_ITEMS)
    assert fab.deletes == ["SEM-DQ", "LH-DQ"]
    assert rec["SEM-DQ"]["deleted"] and rec["LH-DQ"]["deleted"]


def test_an_already_deleted_item_is_not_re_deleted(tmp_path, monkeypatch):
    items = {**FOUR_ITEMS,
             "SEM-DL": {**FOUR_ITEMS["SEM-DL"], "deleted": "2026-08-13T10:00:00+00:00"}}
    fab, _ = run_bench_drop(tmp_path, monkeypatch, "dl", items)
    assert fab.deletes == ["LH-DL"]


def test_a_failed_delete_warns_and_leaves_the_retry_to_teardown(tmp_path, monkeypatch):
    """bench_drop must NOT fail the job — a red step here would cost the DQ measurement. The item
    keeps no `deleted` stamp and its role is not in TEARDOWN_KEEP, so teardown retries it and goes
    red only if it is still standing at the end of the run."""
    fab, rec = run_bench_drop(tmp_path, monkeypatch, "dl", FOUR_ITEMS, undeletable={"LH-DL"})
    assert "LH-DL" in fab.deletes, "the delete was attempted"
    assert "deleted" not in rec["LH-DL"], "a survivor must not be stamped deleted"
    prov = sys.modules["provision"]
    for role in ("bench_dl", "bench_dq", "semantic_model_dq"):
        assert role not in prov.TEARDOWN_KEEP, f"{role} in TEARDOWN_KEEP removes teardown's retry"


def test_bench_drop_refuses_to_run_without_a_record(tmp_path, monkeypatch):
    """Without the fragment there is no record of what the phase created, and a name-driven delete
    is the failure teardown was built to avoid."""
    with pytest.raises(SystemExit) as ex:
        run_bench_drop(tmp_path, monkeypatch, "dl", FOUR_ITEMS, run_record=False)
    assert "RUN_RECORD" in str(ex.value)


def test_an_unknown_phase_is_refused(tmp_path, monkeypatch):
    with pytest.raises(SystemExit):
        run_bench_drop(tmp_path, monkeypatch, "warm", FOUR_ITEMS)


def test_the_teardown_retries_what_a_phase_failed_to_delete(tmp_path, monkeypatch):
    """The backstop end to end: items a bench_drop left un-stamped are ordinary deletable items to
    the teardown, exactly because their roles are not in TEARDOWN_KEEP."""
    fab = Fabric({"LH-DL": "dbt_delta_dl", "SEM-DQ": "aemo_duckrun_dq"})
    fake_requests = types.SimpleNamespace(get=fab.get, delete=fab.delete,
                                          post=lambda *a, **k: Resp(500))
    auth = types.ModuleType("duckrun.auth")
    auth.get_fabric_token = lambda: "TEST-TOKEN"
    monkeypatch.setitem(sys.modules, "requests", fake_requests)
    monkeypatch.setitem(sys.modules, "duckrun", types.ModuleType("duckrun"))
    monkeypatch.setitem(sys.modules, "duckrun.auth", auth)
    monkeypatch.setattr("time.sleep", lambda *_: None)
    src = tmp_path / "sofar.json"
    src.write_text(json.dumps({"items": {
        "LH-DL": {"role": "bench_dl", "kind": "Lakehouse", "name": "dbt_delta_dl"},
        "SEM-DQ": {"role": "semantic_model_dq", "kind": "SemanticModel",
                   "name": "aemo_duckrun_dq"},
        "LH-DQ": {"role": "bench_dq", "kind": "Lakehouse", "name": "dbt_delta_dq",
                  "deleted": "2026-08-13T10:00:00+00:00"},
    }}), encoding="utf-8")
    monkeypatch.setenv("WS_ID", WS)
    monkeypatch.setenv("RUN_RECORD", str(tmp_path / "frag.json"))
    monkeypatch.setattr(sys, "argv", ["provision.py", "teardown", str(src)])
    sys.modules.pop("provision", None)
    importlib.import_module("provision")
    assert sorted(fab.deletes) == ["LH-DL", "SEM-DQ"], \
        "un-stamped phase items are retried; the stamped one is skipped"


def test_bench_lakehouse_name_is_the_output_item_plus_the_phase(tmp_path, monkeypatch):
    run_bench_drop(tmp_path, monkeypatch, "dl", {})
    prov = sys.modules["provision"]
    assert prov.bench_lakehouse_name("duckrun", "dl") == "dbt_delta_dl"
    assert prov.bench_lakehouse_name("dwh", "dq") == "dbt_dwh_dq"
    with pytest.raises(SystemExit):
        prov.bench_lakehouse_name("duckrun", "warm")


# ---- bench_sync: the `_dq` endpoint's metadata sync, forced and checked -------------------------

NYC = ["mart.dim_date", "mart.dim_zone", "mart.fct_trips", "landing.stg_parquet_archive_log"]


class SyncFabric:
    """A `_dq` lakehouse with an endpoint whose refreshMetadata answers from `rounds`, one per POST:
    each round is {tableName: status}; a name left out is absent from the result."""

    def __init__(self, rounds, lro=True):
        self.rounds = list(rounds)
        self.lro = lro
        self.posts = []
        self._pending = None

    def _result(self):
        rnd = self.rounds.pop(0)
        return {"value": [{"tableName": t, "status": s} for t, s in rnd.items()]}

    def get(self, url, headers=None, **kw):
        if url.endswith(f"/workspaces/{WS}/lakehouses"):
            return Resp(200, {"value": [{"id": "LH-DQ", "displayName": "dbt_nyc_delta_dq"}]})
        if url.endswith("/lakehouses/LH-DQ"):
            return Resp(200, {"properties": {"sqlEndpointProperties": {"id": "EP"}}})
        if url.endswith("/operations/OP/result"):
            return Resp(200, self._pending)
        if url.endswith("/operations/OP"):
            return Resp(200, {"status": "Succeeded"})
        raise AssertionError(f"unexpected GET {url}")

    def post(self, url, headers=None, json=None, **kw):
        assert url.endswith("/sqlEndpoints/EP/refreshMetadata"), url
        self.posts.append(json)
        if not self.lro:
            return Resp(200, self._result())
        self._pending = self._result()
        r = Resp(202)
        r.headers = {"x-ms-operation-id": "OP", "Retry-After": "1"}
        return r

    def delete(self, url, **kw):
        raise AssertionError("bench_sync must not DELETE")


def run_bench_sync(monkeypatch, fab):
    fake_requests = types.SimpleNamespace(get=fab.get, delete=fab.delete, post=fab.post)
    auth = types.ModuleType("duckrun.auth")
    auth.get_fabric_token = lambda: "TEST-TOKEN"
    monkeypatch.setitem(sys.modules, "requests", fake_requests)
    monkeypatch.setitem(sys.modules, "duckrun", types.ModuleType("duckrun"))
    monkeypatch.setitem(sys.modules, "duckrun.auth", auth)
    monkeypatch.setattr("time.sleep", lambda *_: None)
    monkeypatch.setenv("WS_ID", WS)
    monkeypatch.setenv("DATASET", "nyc")
    monkeypatch.delenv("RUN_RECORD", raising=False)
    monkeypatch.setattr(sys, "argv", ["provision.py", "bench_sync", "duckrun"])
    sys.modules.pop("provision", None)
    importlib.import_module("provision")


def test_a_synced_endpoint_passes_through_the_lro(monkeypatch):
    fab = SyncFabric([{t: "Success" for t in NYC}])
    run_bench_sync(monkeypatch, fab)
    assert len(fab.posts) == 1
    scoped = {f"{d['schema']}.{t}" for d in fab.posts[0]["tables"] for t in d["tableNames"]}
    assert scoped == set(NYC), "the refresh is scoped to exactly the tables the model reads"


def test_not_run_counts_as_synced_and_a_200_is_the_result(monkeypatch):
    fab = SyncFabric([{t: "NotRun" for t in NYC}], lro=False)
    run_bench_sync(monkeypatch, fab)
    assert len(fab.posts) == 1


def test_a_missing_table_is_retried_then_passes(monkeypatch):
    fab = SyncFabric([{t: "Success" for t in NYC[1:]}, {t: "Success" for t in NYC}])
    run_bench_sync(monkeypatch, fab)
    assert len(fab.posts) == 2


def test_a_table_that_never_syncs_is_fatal_and_named(monkeypatch):
    fail = {**{t: "Success" for t in NYC}, "mart.fct_trips": "Failure"}
    fab = SyncFabric([fail] * 4)
    with pytest.raises(SystemExit, match="mart.fct_trips"):
        run_bench_sync(monkeypatch, fab)
    assert len(fab.posts) == 4
