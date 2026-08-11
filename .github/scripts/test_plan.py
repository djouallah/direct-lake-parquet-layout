"""Run the `plan` job's REAL script, extracted from benchmark.yml, across the input matrix.

WHY THIS EXISTS. `plan` is the job that validates free-text dispatch inputs before any leg spends
capacity — and it was the only substantial code in this repo with no offline coverage at all,
because it lives as a heredoc inside a YAML string. It crashed on `row_group_size=auto` with
`invalid literal for int() with base 10: 'auto'`: the validation correctly EXEMPTED `auto` from
being an integer, and the log line three lines below parsed it anyway. `plan` is a `needs:` gate, so
every dispatch died before `land` — in the one job whose entire purpose is to catch bad input for
free.

It got through because it had been checked against a hand-copied paraphrase of the script rather
than the script. So this executes the bytes the workflow runs: it reads the heredoc out of the YAML
and runs it, which means it cannot drift from what CI does short of the extraction itself failing —
and that failure is loud.

Costs about a second, needs no credentials, and runs in the free `checks` job with everything else.
"""
import io
import os
import re
import subprocess
import sys
import textwrap

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
WORKFLOW = os.path.join(ROOT, ".github", "workflows", "benchmark.yml")


def plan_script():
    """The `plan` job's python, dedented, exactly as the runner sees it."""
    src = io.open(WORKFLOW, encoding="utf-8").read()
    m = re.search(r"python3 - >> \"\$GITHUB_OUTPUT\" <<'EOF'\n(.*?)\n\s*EOF\n", src, re.S)
    assert m, "could not find the plan job's heredoc — the extraction, not the script, is broken"
    return textwrap.dedent(m.group(1))


def run(dataset="aemo", engines="duckrun", row_group_size="auto", sort_by="auto"):
    env = dict(os.environ, DATASET=dataset, ENGINES=engines,
               ROW_GROUP_SIZE=row_group_size, SORT_BY=sort_by)
    return subprocess.run([sys.executable, "-c", plan_script()], env=env, cwd=ROOT,
                          capture_output=True, text=True)


# (label, kwargs, should the job succeed)
CASES = [
    # `auto` on both knobs is the DEFAULT dispatch, and it is what crashed.
    ("auto everywhere", {}, True),
    ("pinned aemo layout", {"row_group_size": "2000000", "sort_by": "date,time,price"}, True),
    ("auto on nyc", {"dataset": "nyc", "engines": "spark"}, True),
    ("pinned nyc layout",
     {"dataset": "nyc", "row_group_size": "2000000", "sort_by": "pickup_date,PULocationID"}, True),
    # Blank sort is the only way to ask for NO sort, so it must stay legal.
    ("unsorted", {"sort_by": ""}, True),
    ("AUTO uppercase", {"row_group_size": "AUTO", "sort_by": "AUTO"}, True),
    # ...and the refusals, each of which saves a dispatch.
    ("non-numeric geometry", {"row_group_size": "abc"}, False),
    ("zero geometry", {"row_group_size": "0"}, False),
    ("the other dataset's sort key", {"dataset": "nyc", "sort_by": "date,time,price"}, False),
    ("malformed sort key", {"sort_by": "date;time"}, False),
    ("whitespace sort key", {"sort_by": "   "}, False),
    ("unknown engine", {"engines": "nope"}, False),
    ("unknown dataset", {"dataset": "taxi"}, False),
]


@pytest.mark.parametrize("label,kwargs,ok", CASES, ids=[c[0] for c in CASES])
def test_plan_accepts_and_refuses_the_right_inputs(label, kwargs, ok):
    r = run(**kwargs)
    assert (r.returncode == 0) is ok, (
        f"{label}: exit {r.returncode}\n--- stdout ---\n{r.stdout}\n--- stderr ---\n{r.stderr}")


def test_plan_emits_the_three_outputs_the_matrix_reads():
    """`engines`, `fabric` and `server` on stdout — the workflow appends stdout to $GITHUB_OUTPUT,
    and a missing key gives an empty matrix, which builds NOTHING and reports success."""
    r = run(engines="duckrun,spark")
    assert r.returncode == 0, r.stderr
    keys = {ln.split("=", 1)[0] for ln in r.stdout.splitlines() if "=" in ln}
    assert {"engines", "fabric", "server"} <= keys, r.stdout


def test_plan_keeps_stdout_clean():
    """Everything on stdout becomes a $GITHUB_OUTPUT line. Diagnostics go to stderr — a stray print
    here corrupts the outputs the matrices are built from."""
    r = run()
    assert r.returncode == 0, r.stderr
    for ln in r.stdout.splitlines():
        assert re.fullmatch(r"[a-z_]+=.*", ln), f"not a KEY=VALUE line: {ln!r}"
