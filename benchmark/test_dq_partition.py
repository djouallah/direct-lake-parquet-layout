"""The Direct Lake / DirectQuery partition in the render layer, offline.

A pushdown timing is not a slow layout: mixing the two kinds of number in one ranking is the
misreading that once had the DirectQuery leg removed outright, and the partition is what makes it
admissible again as context. These tests pin that no `_dq` model can reach a Direct Lake table or
ranking, and that the DQ models get their own.

    python -m pytest benchmark/test_dq_partition.py -q
"""
import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import render_report as rr  # noqa: E402
import render_summary as rs  # noqa: E402


def _q(cold, warm, hot):
    return {"cold_ms": cold, "warm_ms": warm, "hot_median_ms": hot,
            "all_hot_ms": [hot] * 3, "hot_spread_pct": 1.0, "rows": 1, "tier": "probe"}


TIMINGS = {
    "aemo_duckrun": {"probe_mw": _q(100.0, 20.0, 10.0), "probe_rowcount": _q(50.0, 10.0, 5.0)},
    "aemo_spark": {"probe_mw": _q(200.0, 40.0, 20.0), "probe_rowcount": _q(80.0, 15.0, 8.0)},
    "aemo_duckrun_dq": {"probe_mw": _q(900.0, 800.0, 700.0),
                        "probe_rowcount": _q(500.0, 400.0, 300.0)},
    "aemo_spark_dq": {"probe_mw": _q(950.0, 850.0, 750.0),
                      "probe_rowcount": _q(550.0, 450.0, 350.0)},
}


def test_split_timings_partitions_on_the_suffix():
    dl, dq = rr.split_timings(TIMINGS)
    assert set(dl) == {"aemo_duckrun", "aemo_spark"}
    assert set(dq) == {"aemo_duckrun_dq", "aemo_spark_dq"}


def test_the_direct_lake_ranking_never_names_a_dq_model():
    analysis = rr.compute_analysis({"timings": TIMINGS})
    for metric, rows in analysis["ranking"].items():
        engines = [r["engine"] for r in rows]
        assert engines and not any(e.endswith("_dq") for e in engines), \
            f"{metric}: {engines} — a pushdown total placed in the layout ranking"


def test_the_dq_models_get_their_own_ranking():
    analysis = rr.compute_analysis({"timings": TIMINGS})
    assert "ranking_dq" in analysis
    for metric, rows in analysis["ranking_dq"].items():
        engines = {r["engine"] for r in rows}
        assert engines == {"duckrun_dq", "spark_dq"}, f"{metric}: {engines}"
    # A DL-only report carries no empty ranking_dq key at all — absent, not {}.
    dl_only = {m: v for m, v in TIMINGS.items() if not m.endswith("_dq")}
    assert "ranking_dq" not in rr.compute_analysis({"timings": dl_only})


def test_cold_column_cost_covers_direct_lake_models_only():
    # The marginal probe cost measures transcode; a DQ model has no VertiPaq store to transcode
    # into, so a row for it would be a number wearing the wrong meaning.
    analysis = rr.compute_analysis({"timings": TIMINGS})
    assert set(analysis["cold_column_cost"]) == {"aemo_duckrun", "aemo_spark"}


def test_verify_ranking_holds_the_dq_ranking_to_the_same_invariants():
    analysis = rr.compute_analysis({"timings": TIMINGS})
    errs, _notes = rs.verify_ranking({"timings": TIMINGS}, analysis)
    assert not errs
    # Corrupt the DQ ranking's order: still fatal — a DirectQuery table naming the slower engine
    # the winner is exactly as bad as the Direct Lake one doing it.
    bad = {**analysis, "ranking_dq": {"COLD": [
        {"engine": "spark_dq", "rank": 1, "total_ms": 2000.0, "x_fastest": 1.0, "query_wins": 0},
        {"engine": "duckrun_dq", "rank": 2, "total_ms": 1000.0, "x_fastest": 2.0,
         "query_wins": 2}]}}
    errs, _notes = rs.verify_ranking({"timings": TIMINGS}, bad)
    assert errs and any("(DQ)" in e for e in errs)


def test_verify_ranking_catches_a_dq_model_leaked_into_the_direct_lake_ranking():
    analysis = rr.compute_analysis({"timings": TIMINGS})
    leaked = {**analysis, "ranking": {**analysis["ranking"], "COLD": [
        {"engine": "duckrun_dq", "rank": 1, "total_ms": 100.0, "x_fastest": 1.0,
         "query_wins": 1}]}}
    errs, _notes = rs.verify_ranking({"timings": TIMINGS}, leaked)
    assert any("leaked" in e for e in errs)
