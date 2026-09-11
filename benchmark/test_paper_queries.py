"""The vendored white-paper capture: that it is the capture, and that one edit is the only edit.

The whole value of `paper_queries.json` is that the DAX is byte-for-byte what the paper's own load
test issued. Every assertion here is about that staying true — a paraphrase would look identical in
a run log and would quietly turn a reproduction back into a reconstruction.
"""
import json
import pathlib
import re

HERE = pathlib.Path(__file__).resolve().parent
DOC = json.loads((HERE / "paper_queries.json").read_text(encoding="utf-8"))


def test_the_provenance_is_recorded_with_a_commit():
    """A VENDORED FILE WITHOUT A COMMIT CANNOT BE RE-FETCHED OR DIFFED, which is the only thing that
    makes vendoring better than copying. The repo was renamed once already — the PDF's appendix
    cites `Fab-DL-DB-DQ-Whitepaper`, which 404s — so the name alone is not enough to find it again.
    """
    src = DOC["source"]
    assert src["repo"].startswith("https://github.com/")
    assert re.fullmatch(r"[0-9a-f]{40}", src["commit"]), "a full commit sha, not a branch"
    assert src["path"].endswith(".json") and src["fetched"]


def test_it_is_the_fifteen_VISUAL_queries_and_the_slicer_ones_are_absent():
    """FIFTEEN, NOT TWENTY-FOUR. The paper's reported results exclude slicer interactions, and the
    three queries needing a `Time Unit` field-parameter table are all slicer queries — so dropping
    them costs nothing measured and keeps a table out of the semantic model that would exist only to
    serve a query we never report."""
    assert DOC["capture"]["dax_events"] == 24
    assert len(DOC["queries"]) == 15 == DOC["capture"]["visual_queries"]
    numbers = {q["query_number"] for q in DOC["queries"]}
    assert not (numbers & set(DOC["capture"]["slicer_events"]))
    assert not any("'Time Unit'" in q["dax"] for q in DOC["queries"])


def test_five_visuals_across_three_progressive_slicer_rounds():
    """THE AXIS IS HOW MANY SLICERS, NOT WHETHER. The reconstruction this replaced modelled the
    paper's scenarios as "unfiltered" and "all six slicers"; there is NO unfiltered round. Every
    captured query filters at least `ca_state` and an education status, round 2 adds `s_manager`,
    round 3 adds `cp_type` and `sm_carrier`. Getting that wrong meant the old suite's cheap tier was
    measuring something the paper never ran."""
    assert DOC["capture"]["rounds"] == ["AR", "CO", "CA"]
    by_visual = {}
    for q in DOC["queries"]:
        by_visual.setdefault(q["visual"], []).append(q["slicer_state"])
    assert len(by_visual) == 5, sorted(by_visual)
    for visual, states in by_visual.items():
        assert sorted(states) == ["AR", "CA", "CO"], visual
    # Progressive, and asserted on the queries themselves rather than on the round label.
    for q in DOC["queries"]:
        assert "'customer_address'[ca_state]" in q["dax"], q["name"]
        if q["slicer_state"] != "AR":
            assert "'store'[s_manager]" in q["dax"], q["name"]
        if q["slicer_state"] == "CA":
            assert "'ship_mode'[sm_carrier]" in q["dax"], q["name"]


def test_the_cache_buster_predicate_is_left_exactly_as_captured():
    """The paper's load tester rewrites `< 2` to a random bound per virtual user to defeat the query
    cache. This harness measures cold, warm and hot ON PURPOSE — defeating the cache would destroy
    two of its three tiers — so the literal stays, and nothing here may start substituting it."""
    assert any("cache_buster" in q["dax"] for q in DOC["queries"])
    for q in DOC["queries"]:
        for lit in re.findall(r"cache_buster'?\]\s*<\s*(\d+)", q["dax"]):
            assert lit == "2", f"{q['name']}: cache_buster bound was rewritten to {lit}"


def test_the_only_edit_is_the_measure_qualifier():
    """ONE NORMALISATION, TEXTUAL, AND NOTHING ELSE. The capture qualifies measures against the
    report's measure-holder table, `'Measures 1'[Store Revenue]`; our model carries the identical
    measures on their own fact tables, so the qualifier is stripped and DAX resolves the bare name
    regardless of home table. Asserted by rebuilding the shipped DAX from the raw file: if anyone
    ever edits a query in passing, the two stop matching."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("_xc", HERE / "xmla_compare.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    built = {name: dax for _tier, name, dax in mod._paper_queries()}
    assert len(built) == 15
    for q in DOC["queries"]:
        assert built[q["name"]] == q["dax"].replace("'Measures 1'[", "["), q["name"]
        assert "'Measures 1'" not in built[q["name"]]


def test_the_suite_carries_them_as_its_composite_tier():
    import importlib.util
    spec = importlib.util.spec_from_file_location("_xc2", HERE / "xmla_compare.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    names = [n for tier, n, _ in mod.TPCDS_QUERIES if tier == "composite"]
    for q in DOC["queries"]:
        assert q["name"] in names, q["name"]
    # `probe_rowcount` must stay LAST among the probes — the marginal-column-cost table subtracts it
    # — and inserting a tier ahead of the probes would break that. Cheap to assert here too.
    probes = [n for tier, n, _ in mod.TPCDS_QUERIES if tier == "probe"]
    assert probes[-1] == "probe_rowcount"
