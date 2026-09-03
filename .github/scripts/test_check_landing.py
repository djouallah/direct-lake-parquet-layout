"""The empty-archive guard: what it refuses, and the three things it must NOT refuse.

Every assertion here is about a false positive. The guard runs on every dispatch of every dataset,
so a check that is too eager blocks ordinary work — and it sits in `land`, before the paid legs, so
a wrong refusal costs a whole run rather than a leg.
"""
import importlib.util
import pathlib
import sys

import pytest

HERE = pathlib.Path(__file__).resolve().parent


@pytest.fixture
def guard(monkeypatch):
    spec = importlib.util.spec_from_file_location("check_landing", HERE / "check_landing.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    monkeypatch.setenv("FILES_PATH", "abfss://ws@onelake.dfs.fabric.microsoft.com/g/Files")
    monkeypatch.setenv("DATASET", "tpcds")
    return mod


def _listing(guard, monkeypatch, entries):
    monkeypatch.setattr(guard, "landed_bytes",
                        lambda _p: (len(entries), sum(n for _, n in entries)))


def test_an_empty_archive_is_refused(guard, monkeypatch, capsys):
    """THE CASE IT EXISTS FOR — run 33734219062. Non-zero, so `land` goes red and no leg starts."""
    _listing(guard, monkeypatch, [])
    assert guard.main() == 1
    said = capsys.readouterr().err
    assert "THE LANDING ARCHIVE IS EMPTY" in said
    # The remedy has to be IN the message: whoever reads this is looking at a red scheduled run on a
    # dataset they did not dispatch, and the fix is a hand dispatch they have no reason to know about.
    assert "gh workflow run Benchmark" in said and "skip_download=false" in said
    assert "tpcds" in said, "names the dataset, so a grid failure says which cell"
    assert "SCALE FACTOR" in said, "tpcds reinterprets download_limit and a wrong value is expensive"


def test_a_populated_archive_passes(guard, monkeypatch, capsys):
    _listing(guard, monkeypatch, [("csv_raw/x/a.csv", 1048576)])
    assert guard.main() == 0
    assert "1 file(s)" in capsys.readouterr().err


def test_a_half_drained_archive_passes(guard, monkeypatch):
    """`download_limit` EXISTS to produce one, so a partial archive is a normal state and refusing it
    would block the ordinary way of extending a dataset. The guard checks for NOTHING AT ALL, never
    for completeness — it does not read the archive log or the table set."""
    _listing(guard, monkeypatch, [("parquet_raw/store_sales/a.parquet", 512)])
    assert guard.main() == 0


def test_a_listing_that_throws_is_not_fatal(guard, monkeypatch, capsys):
    """BEST-EFFORT ON THE READ, FATAL ONLY ON A CONFIRMED EMPTY ARCHIVE. A transient OneLake error is
    a question that could not be asked; turning it into a failed dispatch is a worse trade than
    letting the leg fail on its own, which it would anyway if the archive really were empty."""
    def boom(_p):
        raise RuntimeError("OneLake said no")
    monkeypatch.setattr(guard, "landed_bytes", boom)
    assert guard.main() == 0
    assert "could not list" in capsys.readouterr().err


def test_no_files_path_is_a_no_op(guard, monkeypatch, capsys):
    """FILES_PATH comes from `provision.py land`, the step immediately above. Absent means THAT step
    did not run, which is already red on its own — a second, differently-worded red helps nobody."""
    monkeypatch.delenv("FILES_PATH", raising=False)
    assert guard.main() == 0
    assert "unset" in capsys.readouterr().err


def test_duckrun_round_trip_files_do_not_count_as_landed(monkeypatch):
    """`Files/duckrun_remote/` is duckrun's `run_python` round-trip — the result and log a notebook
    writes back, two files per run. A leftover pair from an earlier dispatch would make a genuinely
    empty archive look populated, which is the one state this guard must catch. `stats.py` excludes
    the same folder from the landing block, for the same reason.

    Exercises the real `landed_bytes` against a stubbed obstore, so the exclusion is asserted in the
    function that does it rather than in a fixture that agrees with it.
    """
    spec = importlib.util.spec_from_file_location("check_landing2", HERE / "check_landing.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    objects = [{"path": "duckrun_remote/result.json", "size": 12},
               {"path": "duckrun_remote/log.txt", "size": 34},
               {"path": "csv_raw/scada/a.csv", "size": 1000}]
    fake_obstore = type("M", (), {"list": staticmethod(lambda _s: iter([objects])),
                                  "__spec__": None})
    fake_duckrun = type("M", (), {
        "connect": staticmethod(lambda *a, **k: type("D", (), {"storage_options": {}})()),
        "__spec__": None})
    fake_os = type("M", (), {"build_store": staticmethod(lambda *a, **k: object()), "__spec__": None})
    fake_secret = type("M", (), {"refreshed": staticmethod(lambda o: o), "__spec__": None})
    adapters = type("M", (), {"objectstore": fake_os, "secret": fake_secret, "__spec__": None})
    monkeypatch.setitem(sys.modules, "obstore", fake_obstore)
    monkeypatch.setitem(sys.modules, "duckrun", fake_duckrun)
    monkeypatch.setitem(sys.modules, "dbt.adapters.duckrun", adapters)

    files, size = mod.landed_bytes("abfss://ws@onelake.dfs.fabric.microsoft.com/g/Files")
    assert (files, size) == (1, 1000), "only the real archive file counts"
