"""The nightly schedule is a 5x4 GRID — assert the cron lines and the env chains agree about it.

`benchmark.yml` carries 20 cron lines under `schedule:`, each commented with the cell it means
(`# <dataset> <config>`), and three workflow-level env chains that turn `github.event.schedule`
back into that cell: `DATASET` (exact whole-cron matching), `BENCH_ENGINES` and
`SPARK_RESOURCE_PROFILE` (hour-prefix matching). Nothing else in the run knows which cell fired.

EVERY FAILURE THIS CATCHES IS SILENT AND SPENDS CAPACITY:

  a cron with no branch          -> falls through to the chain's fallback and quietly builds aemo
                                    with duckrun instead of the cell the comment claims
  a branch naming a dead cron    -> the mirror of the above: configuration that reads as live
  a missing cell                 -> one (dataset, config) is never measured and nothing says so
  a duplicated cell              -> two runs of one cell, and another cell silently dropped to fit
  two crons at one (day, time)   -> two Benchmark runs at the same minute, which the concurrency
                                    group turns into one queued and one run, on ONE Fabric capacity
  slots too close together       -> an overrunning run queues the next, and a second overrun EVICTS
                                    the pending one (cancel-in-progress: false keeps one pending
                                    slot, not two), losing a cell for that week
  a slot in East US work hours   -> interactive CU on a shared capacity while people are on it, which
                                    the run cannot report: it goes green, just slower and dearer
  a chain restated instead of
    referenced                   -> the BUILD uses one cell and the RECORD files another

`DATASET` compares EXACT STRINGS against the cron as written, whitespace included, so this test
compares the same way rather than parsing cron semantics: a cron rewritten from '17 3 * * 0' to
'17 03 * * 0' fires on the same minute and matches nothing — precisely the class of edit that looks
harmless in review. The two hour-keyed chains use `startsWith`, so this test simulates that instead
of comparing text, and separately asserts the prefixes cannot be ambiguous.
"""
import itertools
import os
import re

import datasets

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
WORKFLOW = os.path.join(ROOT, ".github", "workflows", "benchmark.yml")

# The config axis of the grid, keyed by the token each cron line's comment carries. The value is
# what the two hour-keyed chains must resolve that cron to. `spark_default` is writeHeavy — the
# workspace default, no V-Order — and is the other half of the pair `spark_vorder` makes.
CONFIGS = {
    "duckrun":       ("duckrun", "writeHeavy"),
    "dwh":           ("dwh", "writeHeavy"),
    "spark_vorder":  ("spark", "readHeavyForPBI"),
    "spark_default": ("spark", "writeHeavy"),
}

# Minutes a same-day pair of slots must stay apart. The longest run ever recorded in history/runs/
# is 84 minutes (the median is 32), and runs must stay serial — one Fabric capacity.
MIN_GAP_MINUTES = 100

# Every slot must run while East US is off work, on BOTH sides of the DST boundary — the capacity is
# shared and the query passes are INTERACTIVE CU, the class of usage a capacity admin notices. This
# is asserted rather than left to the comment because it is exactly what drifted: the grid sat two
# hours later for a while and its last slot, 10:17 UTC, is 06:17 EDT. That is morning, it read as a
# night slot in every comment, and run 31941551767 fired there.
#
# The window is AFTER WORK rather than dead of night, which is what makes the whole run fit and not
# just its start: at a 23:00 floor the earliest legal grid ended its last slot 06:41 EDT on a
# worst-case run, and that overrun had to be accepted. NIGHT_END is still the binding side.
US_EAST_OFFSETS = {"EST": -5, "EDT": -4}   # America/New_York, the East US region's clock
NIGHT_START, NIGHT_END = 22 * 60, 6 * 60   # local 22:00-06:00
MAX_RUN_MINUTES = 84                       # longest run in history/runs/; the median is 32

_BRANCH = re.compile(
    r"\|\|\s*(?:github\.event\.schedule == '(?P<exact>[^']+)'"
    r"|startsWith\(github\.event\.schedule, '(?P<prefix>[^']+)'\))"
    r"\s*&& '(?P<value>[^']+)'")


def _src():
    return open(WORKFLOW, encoding="utf-8").read()


def _crons():
    """[(cron string, dataset, config token)] in file order."""
    block = re.search(r"^  schedule:\n((?:    (?:-|#).*\n)+)", _src(), re.M)
    assert block, "no schedule: block of cron lines in benchmark.yml"
    out = []
    for line in block.group(1).splitlines():
        if re.match(r"\s*#", line):          # the hour->config legend above the lines
            continue
        m = re.match(r'\s*- cron: "([^"]+)"\s*#\s*(\S+)\s+(\S+)\s*$', line)
        assert m, f"cron line without a `# <dataset> <config>` comment: {line!r}"
        out.append((m.group(1), m.group(2), m.group(3)))
    return out


def _chain(var, input_name):
    """([(kind, key, value)], fallback) for one workflow-level env chain, in file order."""
    m = re.search(
        rf"^  {var}: \$\{{\{{ github\.event_name != 'schedule' && inputs\.{input_name}\n(.*?)\}}\}}",
        _src(), re.S | re.M)
    assert m, f"no {var} chain in benchmark.yml (or it no longer opens with the dispatch branch)"
    body = m.group(1)
    branches = [("exact", b.group("exact"), b.group("value")) if b.group("exact")
                else ("prefix", b.group("prefix"), b.group("value"))
                for b in _BRANCH.finditer(body)]
    fallback = re.findall(r"\|\|\s*'([^']+)'\s*$", body.strip())
    assert fallback, f"{var} has no trailing fallback value"
    return branches, fallback[-1]


def _resolve(chain, cron):
    """What GitHub would evaluate the chain to for this cron: first match wins, else the fallback."""
    branches, fallback = chain
    for kind, key, value in branches:
        if (kind == "exact" and cron == key) or (kind == "prefix" and cron.startswith(key)):
            return value
    return fallback


def _minutes(cron):
    minute, hour, _dom, _mon, _dow = cron.split()
    return int(hour) * 60 + int(minute)


def _weekdays(cron):
    return cron.split()[4].split(",")


def test_every_cron_resolves_to_the_cell_its_comment_claims():
    """The comment is documentation; the three chains are what the run actually reads. A cron whose
    branch was dropped or mistyped falls through to the fallback and builds a different cell —
    green, on paid capacity, filed under the wrong name."""
    ds_chain = _chain("DATASET", "dataset")
    eng_chain = _chain("BENCH_ENGINES", "engines")
    prof_chain = _chain("SPARK_RESOURCE_PROFILE", "spark_resource_profile")
    for cron, dataset, config in _crons():
        assert config in CONFIGS, f"cron {cron!r} names unknown config {config!r}"
        engine, profile = CONFIGS[config]
        assert _resolve(ds_chain, cron) == dataset, (
            f"cron {cron!r} is commented {dataset!r} but DATASET resolves it to "
            f"{_resolve(ds_chain, cron)!r}")
        assert _resolve(eng_chain, cron) == engine, (
            f"cron {cron!r} is commented {config!r} but BENCH_ENGINES resolves it to "
            f"{_resolve(eng_chain, cron)!r}")
        assert _resolve(prof_chain, cron) == profile, (
            f"cron {cron!r} is commented {config!r} but SPARK_RESOURCE_PROFILE resolves it to "
            f"{_resolve(prof_chain, cron)!r}")


def test_the_grid_is_complete_and_fires_each_cell_once():
    """Every dataset against every config, exactly once a week. A missing cell is an engine the page
    compares against others while nothing refreshes it; a duplicated one spends a run twice."""
    cells = [(ds, cfg) for _cron, ds, cfg in _crons()]
    want = set(itertools.product(datasets.DATASETS, CONFIGS))
    assert set(cells) == want, (
        f"missing {sorted(want - set(cells))}, unexpected {sorted(set(cells) - want)}")
    dupes = {c for c in cells if cells.count(c) > 1}
    assert not dupes, f"cells scheduled more than once: {sorted(dupes)}"


def test_no_branch_names_a_cron_that_does_not_exist():
    """A branch left behind after a cron was rewritten is dead code that reads as live config — and
    a PREFIX that matches nothing is the same failure wearing a shape grep will not find."""
    crons = [c for c, _ds, _cfg in _crons()]
    for var, inp in (("DATASET", "dataset"), ("BENCH_ENGINES", "engines"),
                     ("SPARK_RESOURCE_PROFILE", "spark_resource_profile")):
        for kind, key, value in _chain(var, inp)[0]:
            hits = [c for c in crons
                    if (c == key if kind == "exact" else c.startswith(key))]
            assert hits, f"{var} maps {kind} {key!r} -> {value!r}, but no cron line matches it"


def test_hour_prefixes_cannot_be_ambiguous():
    """The two hour-keyed chains lean on `startsWith`. If one prefix were a prefix of another the
    earlier branch would silently swallow the later hour's slots — '17 1' would eat both 01:17 and
    10:17 — and every affected run would build the wrong engine without erroring."""
    for var, inp in (("BENCH_ENGINES", "engines"),
                     ("SPARK_RESOURCE_PROFILE", "spark_resource_profile")):
        prefixes = [k for kind, k, _v in _chain(var, inp)[0] if kind == "prefix"]
        for a, b in itertools.permutations(prefixes, 2):
            assert not b.startswith(a), f"{var}: prefix {a!r} swallows {b!r}"


def test_every_value_named_is_a_real_one():
    """A dataset typo is the DATASET-typo trap arriving by a route the `choice` input cannot guard:
    every `+enabled` gate goes false, `dbt build` reports "Nothing to do" and exits 0."""
    branches, fallback = _chain("DATASET", "dataset")
    for _kind, _key, ds in branches:
        assert ds in datasets.DATASETS, f"{ds!r} is not a known dataset"
    assert fallback in datasets.DATASETS, f"fallback {fallback!r} is not a known dataset"
    for _cron, ds, _cfg in _crons():
        assert ds in datasets.DATASETS, f"cron comment names unknown dataset {ds!r}"

    engines = {e for e, _p in CONFIGS.values()}
    eng_branches, eng_fallback = _chain("BENCH_ENGINES", "engines")
    for _kind, _key, e in eng_branches + [(None, None, eng_fallback)]:
        assert e in engines, f"BENCH_ENGINES names {e!r}, which is not one of {sorted(engines)}"


def test_no_two_crons_share_a_weekday_and_time():
    """Two crons at one minute start two Benchmark runs at once. The concurrency group makes one
    queue rather than run, but both were dispatched and one cell's measurement is at the mercy of
    the other's length — and CLAUDE.md forbids concurrent runs outright: one Fabric capacity."""
    seen = {}
    for cron, ds, cfg in _crons():
        for d in _weekdays(cron):
            slot = (d, _minutes(cron))
            assert slot not in seen, (
                f"weekday {d} at {_minutes(cron) // 60:02d}:{_minutes(cron) % 60:02d} is claimed by "
                f"both {seen[slot]} and {(ds, cfg)}")
            seen[slot] = (ds, cfg)


def test_same_day_slots_stay_far_enough_apart():
    """Runs are serial. `cancel-in-progress: false` keeps ONE pending run, so an overrun makes the
    next slot wait — but a second overrun evicts that pending run and the cell is simply not
    measured that week. The gap is the whole guard; the old one-cron-per-weekday rule gave it for
    free and the grid does not."""
    per_day = {}
    for cron, _ds, _cfg in _crons():
        for d in _weekdays(cron):
            per_day.setdefault(d, []).append(_minutes(cron))
    for day, mins in per_day.items():
        mins.sort()
        for a, b in zip(mins, mins[1:]):
            assert b - a >= MIN_GAP_MINUTES, (
                f"weekday {day}: slots at {a // 60:02d}:{a % 60:02d} and {b // 60:02d}:{b % 60:02d} "
                f"are {b - a} min apart, under the {MIN_GAP_MINUTES} min the longest recorded run "
                f"needs")


def test_every_slot_runs_to_completion_while_east_us_is_off_work():
    """The whole reason the times are not round. A slot landing in East US working hours puts
    interactive CU on a shared capacity while people are on it — and the failure is invisible from
    the run itself, which goes green having simply been slower and more expensive.

    BOTH ENDPOINTS are checked, not just the start: a slot that begins at 04:17 and runs 84 minutes
    has spent half its query passes in the morning. The window is 8 hours and the worst run is 84
    minutes, so start and end inside it means the whole run is.

    And at BOTH UTC offsets, because the grid outlives the DST boundary and a slot legal in winter
    can be an hour into the morning in summer — precisely how 10:17 UTC (05:17 EST, 06:17 EDT)
    stood as a night slot for as long as it did."""
    def local(minutes, offset):
        return (minutes + offset * 60) % (24 * 60)

    for cron, ds, cfg in _crons():
        start = _minutes(cron)
        for name, offset in US_EAST_OFFSETS.items():
            for label, at in (("starts", local(start, offset)),
                              ("ends", local(start + MAX_RUN_MINUTES, offset))):
                assert at >= NIGHT_START or at < NIGHT_END, (
                    f"{cron!r} ({ds} {cfg}) is {start // 60:02d}:{start % 60:02d} UTC and {label} at "
                    f"{at // 60:02d}:{at % 60:02d} {name} on the {MAX_RUN_MINUTES}-minute worst case, "
                    f"outside the {NIGHT_START // 60:02d}:00-{NIGHT_END // 60:02d}:00 East US window")


def test_the_rotation_is_weekday_only():
    """A day-of-month restriction alongside a weekday one is ORed by cron, not ANDed — the grid
    would fire cells it never claimed."""
    for cron, _ds, _cfg in _crons():
        _minute, _hour, dom, mon, _dow = cron.split()
        assert (dom, mon) == ("*", "*"), f"{cron!r}: the grid is weekday-only"


def test_the_record_and_the_plan_read_the_env_back_rather_than_restating_it():
    """The scheduled dataset, engine and spark profile are each spelled ONCE, in the workflow-level
    env. Every other consumer references it. A restated chain is a chain that can drift, and the
    drift is silent: the build takes one cell and the record files another."""
    src = _src()
    for line in ("RUNIN_DATASET: ${{ env.DATASET }}",
                 "RUN_ENGINE: ${{ env.BENCH_ENGINES }}",
                 "RUNIN_SPARK_RESOURCE_PROFILE: ${{ env.SPARK_RESOURCE_PROFILE }}",
                 "ENGINES: ${{ env.BENCH_ENGINES }}"):
        assert line in src, f"expected {line!r} — a restated chain would drift from the build"
    # Exactly three chains branch on the cron. A fourth is a copy someone will forget to edit.
    owners = re.findall(r"^  (\w+): \$\{\{ github\.event_name != 'schedule'", src, re.M)
    assert set(owners) == {"DATASET", "BENCH_ENGINES", "SPARK_RESOURCE_PROFILE"}, owners
    # Prose mentions it too, so count only lines that are not comments.
    live = "\n".join(l for l in src.splitlines() if not l.lstrip().startswith("#"))
    assert len(re.findall(r"github\.event\.schedule", live)) == sum(
        len(_chain(v, i)[0]) for v, i in (("DATASET", "dataset"),
                                          ("BENCH_ENGINES", "engines"),
                                          ("SPARK_RESOURCE_PROFILE", "spark_resource_profile"))), \
        "github.event.schedule is read somewhere outside the three env chains"
