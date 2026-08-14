"""The nightly's weekday rotation is written THREE times — assert the three agree.

benchmark.yml carries the cron lines under `schedule:`, a `DATASET` env chain at workflow level,
and a byte-identical `RUNIN_DATASET` chain inside the `record` step. CLAUDE.md says the two chains
"must stay IDENTICAL" and nothing enforced it, which was survivable at four datasets and four cron
lines and is not at five and five.

EVERY FAILURE THIS CATCHES IS SILENT AND SPENDS CAPACITY:

  a cron with no branch          -> that weekday falls through to the fallback and quietly builds
                                    aemo instead of the dataset the comment claims
  the two chains disagreeing     -> the BUILD uses one dataset and the RECORD says another, so the
                                    run's layout is filed under the wrong dataset's history and the
                                    dashboard compares it against tables it was never built from
  a branch naming an unknown cron-> dead branch, same fall-through as the first
  a dataset with no slot         -> added to the registry, never measured, and nothing says so

The comparisons in the workflow are EXACT STRING matches against the cron as written, whitespace
included, so this test compares the same way rather than parsing cron semantics. A cron rewritten
from '17 7 * * 3,6' to '17 7 * * 6,3' fires on the same days and matches nothing — which is
precisely the class of edit that looks harmless in review.
"""
import os
import re

import datasets

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
WORKFLOW = os.path.join(ROOT, ".github", "workflows", "benchmark.yml")


def _src():
    return open(WORKFLOW, encoding="utf-8").read()


def _crons():
    """[(cron string, the dataset its trailing comment claims)] in file order."""
    block = re.search(r"^  schedule:\n((?:    - cron:.*\n)+)", _src(), re.M)
    assert block, "no schedule: block of cron lines in benchmark.yml"
    out = []
    for line in block.group(1).splitlines():
        m = re.match(r'\s*- cron: "([^"]+)"\s*#\s*(\S+)', line)
        assert m, f"cron line without a trailing dataset comment: {line!r}"
        out.append((m.group(1), m.group(2)))
    return out


def _chain(var):
    """[(cron string, dataset)] for one env chain, plus its fallback, in file order."""
    m = re.search(
        rf"{var}: \$\{{\{{ github\.event_name != 'schedule' && inputs\.dataset(.*?)\}}\}}",
        _src(), re.S)
    assert m, f"no {var} chain in benchmark.yml"
    body = m.group(1)
    pairs = re.findall(r"github\.event\.schedule == '([^']+)' && '([^']+)'", body)
    fallback = re.findall(r"\|\|\s*'([^']+)'\s*$", body.strip())
    assert fallback, f"{var} has no trailing fallback dataset"
    return pairs, fallback[-1]


def test_the_two_env_chains_are_identical():
    """DATASET drives the BUILD; RUNIN_DATASET drives the RECORD. If they disagree the run builds
    one dataset and files the result under another — and both halves look fine on their own."""
    assert _chain("DATASET") == _chain("RUNIN_DATASET")


def test_every_cron_is_reachable():
    """A cron with no matching branch is not an error anywhere — it just silently becomes the
    fallback dataset, on the one day a week that was meant to measure something else."""
    pairs, fallback = _chain("DATASET")
    mapped = dict(pairs)
    for cron, claimed in _crons():
        resolved = mapped.get(cron, fallback)
        assert resolved == claimed, (
            f"cron {cron!r} is commented {claimed!r} but the DATASET chain resolves it to "
            f"{resolved!r}" + ("" if cron in mapped else " (no branch matches it, so it falls "
                               "through to the fallback)"))


def test_no_branch_names_a_cron_that_does_not_exist():
    """The mirror of the above: a branch left behind after a cron was rewritten is dead code that
    reads as live configuration."""
    crons = {c for c, _ in _crons()}
    for cron, ds in _chain("DATASET")[0]:
        assert cron in crons, f"the chain maps {cron!r} -> {ds!r}, but no such cron line exists"


def test_every_dataset_named_is_a_real_one():
    """A typo here is the DATASET-typo trap arriving by a route the `choice` input cannot guard:
    every `+enabled` gate goes false, `dbt build` reports "Nothing to do" and exits 0."""
    pairs, fallback = _chain("DATASET")
    for _cron, ds in pairs:
        assert ds in datasets.DATASETS, f"{ds!r} is not a known dataset"
    assert fallback in datasets.DATASETS, f"fallback {fallback!r} is not a known dataset"
    for _cron, claimed in _crons():
        assert claimed in datasets.DATASETS, f"cron comment names unknown dataset {claimed!r}"


def test_every_dataset_gets_a_nightly_slot():
    """Adding a dataset and forgetting the rotation leaves it measured only when someone remembers
    to dispatch it by hand — which, on a page whose whole argument is comparing datasets, reads as
    'this one has no data' rather than 'nobody ran it'."""
    covered = {claimed for _cron, claimed in _crons()}
    missing = set(datasets.DATASETS) - covered
    assert not missing, f"no nightly slot for {sorted(missing)}"


def test_the_week_is_covered_exactly_once():
    """Seven days, no gaps and no double-bookings. Two crons firing on the same weekday would start
    two Benchmark runs at the same minute — which the concurrency group turns into one cancelled
    run, but only after both have been queued, and which CLAUDE.md forbids outright because two
    concurrent runs share one Fabric capacity and throttle each other into wrong numbers."""
    seen = {}
    for cron, claimed in _crons():
        minute, hour, dom, mon, dow = cron.split()
        assert (dom, mon) == ("*", "*"), f"{cron!r}: the rotation is weekday-only"
        for d in dow.split(","):
            assert d not in seen, (
                f"weekday {d} is claimed by both {seen[d]!r} and {claimed!r}")
            seen[d] = claimed
    assert set(seen) == {str(d) for d in range(7)}, \
        f"the week is not fully covered: have {sorted(seen)}"
