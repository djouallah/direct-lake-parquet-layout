"""Benchmark ONE engine's semantic model by replaying a USER SESSION against it over the XMLA
endpoint and timing every query. One process, one engine, no comparison of any kind.

Every model exposes identical tables, columns and measures over the SAME 143M-row `mart.fct_summary`
— one copy per engine, at row-count parity. So this is NOT a correctness check: the numbers are
identical by construction. What differs is how each engine physically wrote the table, which changes
how much the Direct Lake engine has to transcode and scan. We measure that as query wall-clock.

**The session is the measurement, and nothing is ever cleared.** `deploy_models.py` deletes and
recreates the semantic model, so it starts with an empty VertiPaq store; this script then walks the
whole suite `BENCH_RUNS` times and the PASS NUMBER is the tier:

    pass 1     -> cold   first visit; pays the whole Delta->memory transcode, once
    pass 2     -> warm   second visit
    pass 3..N  -> hot    settled; median + spread over N-2 samples

with BENCH_THINK_SECONDS of idle between consecutive queries, because a person reads a visual before
clicking the next one. The pause is outside every timed region.

Those labels are positions in a session, not engine states. Microsoft uses the same words more
narrowly (warm = data resident, VertiScan caches empty; hot = resident AND caches populated), and by
that definition pass 2 is arguably already hot because pass 1 populated the caches too. A TMSL
`clearCache` between passes would manufacture the strict warm state — it clears query caches without
evicting resident columns, which is exactly that transition. It is DELIBERATELY NOT USED: this
reproduces user behaviour rather than testing the engine, and a user's second visit is simply their
second visit. Do not add it to make the label technically precise.

What this replaced: a per-query dehydrate (`clearValues` + `full` before EVERY cold-tier query). No
user is ever in that state, and `clearValues` clears the data cache — TMSL defines it as no more than
"Clear values in this object and all its dependents" — which is not a statement about transcoding
cost. Nothing here issues a refresh after readiness.

**Nothing touches the model between readiness and pass 1.** That is why the top-DUID resolve happens
AFTER pass 1 (it transcodes DUID and mw, the very columns probe_duid/probe_mw measure) and why the
readiness probe reads a tiny dimension instead of `COUNTROWS(fct_summary)`, which is byte-identical
to probe_rowcount — the control the marginal-column-cost decomposition subtracts.

What is under test: **identical DAX, identical semantic models, four dbt adapters.** Every model is
Direct Lake over its own adapter's copy of the same tables, so every timing is a Delta→memory
transcode and an in-memory scan — shaped by the physical layout that adapter wrote, which is the only
thing that differs. `dwh` included: duckrun 0.4.36's `deploy(mode=)` reads a warehouse's Tables as the
Delta they are, so it is no longer measured as SQL-endpoint pushdown to a different engine. A pushdown
time is not a slow layout, and mixing the two kinds of number in one table invited exactly that
misreading.

Uses the XMLA endpoint (ADOMD.NET), NOT the throttled /executeQueries REST endpoint.
Run headless — see .github/workflows/benchmark.yml.

**`BENCH_ENGINES` must name exactly one engine, and this script refuses more.** The workflow runs one
job per engine, because a Fabric/XMLA token lives about an hour and a four-model pass with two 600s
gaps in it does not fit inside one — the expiry would land mid-measurement on whichever engine went
last. So each job mints its own token minutes before using it, writes its own report fragment, and
COMPUTES NOTHING: every number that involves more than one engine is produced by the render layer from
the merged fragments. There is no in-process comparison path here any more (there was one, for running
this from a laptop; the laptop is not a supported way to spend this capacity, and keeping a second
orchestration shape alive to serve it meant two answers to the same question).

Env in:
  BENCH_ENGINES  — exactly ONE engine label. More than one is an error, not a comparison.
  BENCH_TOP_DUID — optional; pins the value the hot_only ladder filters on instead of resolving
                   it from the data. Named for AEMO's DUID, which is what it pinned when there
                   was one dataset; on nyc it pins the pickup zone. See SUITES.
                   Unset is fine: every engine holds the same rows, so each job resolves the same
                   DUID, and the value is recorded per model for the render layer to check.
  PBI_WORKSPACE  — workspace *display name* (XMLA data source uses the name, not the id)
  PBI_TOKEN      — optional; else self-acquired via duckrun (analysis.windows.net/powerbi/api)
  ADOMD_DIR      — folder containing Microsoft.AnalysisServices.AdomdClient.dll
  BENCH_RUNS     — PASSES over the whole suite (default 6): pass 1 cold, pass 2 warm, the rest hot.
                   At 6 the hot median is over 4 samples and the hot spread is real. Below 3 there
                   is no hot tier at all, which the render layer scopes out per metric rather than
                   guessing at.
  BENCH_THINK_SECONDS
                 — idle seconds BETWEEN queries (default 4): a person reading a visual before
                   clicking the next one. Firing the suite back-to-back is the one thing left in
                   this that no user does. It sits outside every timed region, so it changes what
                   is reproduced and not what is measured — but it is idle inside the token's ~1h
                   life, so raising it eats the headroom the per-engine job split exists to protect.
                   0 disables it.

Cold and warm are single samples by construction — there is exactly one first visit and one second
visit per deployed model — so neither carries a spread. More cold samples means more dispatches, not
a bigger number here.

Exit 0 always — this is a benchmark, not a pass/fail gate.
"""
import glob
import json
import os
import statistics
import sys
import time

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import engines as E  # noqa: E402
import report  # noqa: E402

# The session's query suite. Each entry is (tier, name, dax). Adding a query = adding a tuple.
#
# EVERY query runs in EVERY pass — the tier is descriptive, not a switch, and no query is measured
# differently from any other. (It used to gate a per-query dehydrate; that is gone.)
#   probe      — one column, full scan, scalar result.
#   composite  — realistic multi-column workloads over the mart.
#   raw        — one query per RAW landing table, so every table in the model is measured and none
#                is dead weight. `raw_scada_mw` is the heaviest measurement here.
#   hot_only   — selectivity ladder on the sort-key column. "{key}" is filled at runtime with the
#                busiest value of that column (see SUITES), and unless BENCH_TOP_DUID is pinned
#                that resolve happens after pass 1 — so these join the session from pass 2 and
#                have no cold number.
#
# ORDER IS LOAD-BEARING, in one specific way: `probe_rowcount` must stay LAST among the probes.
# render_summary's marginal-column-cost table is `probe_<col>` minus `probe_rowcount`, and that
# subtraction only means "the cost of touching one more column" if each probe is the first query to
# touch its column and the rowcount control runs once everything is already resident. Reordering the
# probes silently changes what that table measures. test_verdicts.py pins it.
#
# Every table, column and measure referenced below exists in BOTH templates — benchmark/
# test_templates.py asserts the two semantic surfaces are identical, and that identity is what makes
# one suite portable across every engine's model — they are structurally identical by construction.
AEMO_QUERIES = [
    # --- Tier 1: per-column probes (rowcount LAST — see the note above) ---
    ("probe", "probe_mw",       'EVALUATE ROW("x", SUM(fct_summary[mw]))'),
    ("probe", "probe_price",    'EVALUATE ROW("x", SUM(fct_summary[price]))'),
    ("probe", "probe_duid",     'EVALUATE ROW("x", DISTINCTCOUNT(fct_summary[DUID]))'),
    ("probe", "probe_date",     'EVALUATE ROW("x", COUNTROWS(VALUES(fct_summary[date])))'),
    ("probe", "probe_time",     'EVALUATE ROW("x", COUNTROWS(VALUES(fct_summary[time])))'),
    ("probe", "probe_rowcount", 'EVALUATE ROW("x", COUNTROWS(fct_summary))'),
    # --- Tier 2: composite workloads ---
    ("composite", "region_x_year",
     'EVALUATE SUMMARIZECOLUMNS(dim_duid[Region], dim_calendar[year], '
     '"MWh", [Total MWh], "AvgP", [Avg Price], "Gens", [Generator Count])'),
    ("composite", "fuel_x_region",
     'EVALUATE SUMMARIZECOLUMNS(dim_duid[FuelSourceDescriptor], dim_duid[Region], '
     '"MWh", [Total MWh], "MW", [Total MW])'),
    ("composite", "timeofday_x_region",
     'EVALUATE SUMMARIZECOLUMNS(fct_summary[time], dim_duid[Region], '
     '"MWh", [Total MWh], "AvgP", [Avg Price])'),
    ("composite", "duid_x_month",
     'EVALUATE SUMMARIZECOLUMNS(fct_summary[DUID], dim_calendar[year], dim_calendar[month], '
     '"MWh", [Total MWh])'),
    ("composite", "filtered_nsw_2024_by_duid",
     'EVALUATE CALCULATETABLE('
     'SUMMARIZECOLUMNS(fct_summary[DUID], "MWh", [Total MWh], "AvgP", [Avg Price]), '
     'dim_duid[Region] = "NSW1", dim_calendar[year] = 2024)'),
    ("composite", "scalar_weighted_full_scan",
     'EVALUATE ROW('
     '"RevenueProxy", SUMX(fct_summary, fct_summary[mw] * fct_summary[price]), '
     '"DistinctDUID", DISTINCTCOUNT(fct_summary[DUID]), '
     '"Rows", COUNTROWS(fct_summary))'),
    ("composite", "topn_duid_by_mwh",
     'EVALUATE TOPN(50, SUMMARIZECOLUMNS(fct_summary[DUID], dim_calendar[year], '
     '"MWh", [Total MWh]), [MWh], DESC)'),
    # --- Tier 2 (cont.): column-width at fixed shape (cold scaling with touched columns) ---
    ("composite", "wide_all_measures",
     'EVALUATE SUMMARIZECOLUMNS(dim_calendar[year], "a", [Total MWh], "b", [Avg Price], '
     '"c", [Total MW], "d", [Generator Count])'),
    ("composite", "narrow_one_measure",
     'EVALUATE SUMMARIZECOLUMNS(dim_calendar[year], "a", [Total MWh])'),
    # --- Tier 3: the RAW tables, one query per table so nothing in the model goes unmeasured ---
    # These are why the semantic model carries all eight tables and not just the mart three. The
    # first is the single heaviest measurement in the suite: fct_scada is the largest table in the
    # project, so a cold SUM over one of its columns is the biggest Delta->memory transcode any
    # engine here has to do, and it is where a layout difference has the most room to show.
    ("raw", "raw_scada_mw", 'EVALUATE ROW("x", [Scada MW])'),
    ("raw", "raw_scada_x_region_year",
     'EVALUATE SUMMARIZECOLUMNS(dim_duid[Region], dim_calendar[year], '
     '"MW", [Scada MW], "Rows", [Scada Rows])'),
    ("raw", "raw_price_x_region_year",
     'EVALUATE SUMMARIZECOLUMNS(fct_price[REGIONID], dim_calendar[year], '
     '"AvgRRP", [Avg RRP], "Demand", SUM(fct_price[TOTALDEMAND]))'),
    ("raw", "raw_intraday_scada",
     'EVALUATE SUMMARIZECOLUMNS(fct_scada_today[DUID], '
     '"MW", [Scada Today MW], "Rows", [Scada Today Rows])'),
    ("raw", "raw_intraday_price",
     'EVALUATE SUMMARIZECOLUMNS(fct_price_today[REGIONID], '
     '"AvgRRP", AVERAGE(fct_price_today[RRP]), "Rows", [Price Today Rows])'),
    ("raw", "raw_archive_log",
     'EVALUATE SUMMARIZECOLUMNS(stg_csv_archive_log[source_type], '
     '"Files", [Archive Files], "Rows", [Archive Source Rows])'),
    # --- Tier 4: selectivity ladder (SUMX lifts work above the XMLA noise floor) ---
    ("hot_only", "sel_1yr",
     'EVALUATE ROW("r", CALCULATE(SUMX(fct_summary, fct_summary[mw] * fct_summary[price]), '
     'dim_calendar[year] = 2024))'),
    ("hot_only", "sel_1mo",
     'EVALUATE ROW("r", CALCULATE(SUMX(fct_summary, fct_summary[mw] * fct_summary[price]), '
     'dim_calendar[year] = 2024, dim_calendar[month] = 6))'),
    ("hot_only", "sel_1duid",
     'EVALUATE ROW("r", CALCULATE(SUMX(fct_summary, fct_summary[mw] * fct_summary[price]), '
     'fct_summary[DUID] = {key}))'),
    ("hot_only", "sel_1duid_1mo",
     'EVALUATE ROW("r", CALCULATE(SUMX(fct_summary, fct_summary[mw] * fct_summary[price]), '
     'fct_summary[DUID] = {key}, dim_calendar[year] = 2024, dim_calendar[month] = 6))'),
]

# ---------------------------------------------------------------------------- the NYC taxi suite
#
# THE SAME SHAPE, NOT THE SAME QUERIES — and the shape is what makes the two datasets comparable.
# Probes first with `probe_rowcount` LAST, then composites over the star, then one query per raw
# table, then a hot-only selectivity ladder. Read the AEMO block above for why each tier exists;
# everything said there applies here unchanged.
#
# What differs is what the columns ARE, and that is the point of the dataset. AEMO's mart is five
# narrow columns on a regular grid; this one has 17, of which store_and_fwd_flag, RatecodeID,
# payment_type and VendorID sit at 97-99% single-value and the two LocationIDs are Zipfian. The
# probe tier is therefore where the interesting result lives: a per-column cold cost on a 99%
# single-value string is exactly what an encoding pass should move, and fct_summary had nothing of
# the kind to measure.
#
# The probes cover the skewed categoricals and both ends of the width range, NOT all 17. Every probe
# is paid for in every pass of every engine, and a column whose cold cost nobody would read is
# capacity spent for nothing.
NYC_QUERIES = [
    # --- Tier 1: per-column probes (rowcount LAST — see the note above) ---
    ("probe", "probe_fare",       'EVALUATE ROW("x", SUM(fct_trips[fare_amount]))'),
    ("probe", "probe_distance",   'EVALUATE ROW("x", SUM(fct_trips[trip_distance]))'),
    ("probe", "probe_pulocation", 'EVALUATE ROW("x", DISTINCTCOUNT(fct_trips[PULocationID]))'),
    ("probe", "probe_dolocation", 'EVALUATE ROW("x", DISTINCTCOUNT(fct_trips[DOLocationID]))'),
    ("probe", "probe_paytype",    'EVALUATE ROW("x", DISTINCTCOUNT(fct_trips[payment_type]))'),
    # The two most extreme columns in the table, and the reason this dataset exists: ~99% one value
    # and ~97% one value. If V-Order does what an encoding pass should, it does it here.
    ("probe", "probe_storefwd",
     'EVALUATE ROW("x", DISTINCTCOUNT(fct_trips[store_and_fwd_flag]))'),
    ("probe", "probe_ratecode",   'EVALUATE ROW("x", DISTINCTCOUNT(fct_trips[RatecodeID]))'),
    ("probe", "probe_pickup",
     'EVALUATE ROW("x", COUNTROWS(VALUES(fct_trips[tpep_pickup_datetime])))'),
    ("probe", "probe_rowcount",   'EVALUATE ROW("x", COUNTROWS(fct_trips))'),
    # --- Tier 2: composite workloads over the star ---
    ("composite", "borough_x_year",
     'EVALUATE SUMMARIZECOLUMNS(dim_zone[Borough], dim_date[year], '
     '"Fare", [Total Fare], "Trips", [Total Trips], "Dist", [Avg Distance])'),
    ("composite", "paytype_x_borough",
     'EVALUATE SUMMARIZECOLUMNS(fct_trips[payment_type], dim_zone[Borough], '
     '"Fare", [Total Fare], "Tips", [Total Tips])'),
    ("composite", "dow_x_borough",
     'EVALUATE SUMMARIZECOLUMNS(dim_date[day_of_week], dim_zone[Borough], '
     '"Trips", [Total Trips], "Fare", [Total Fare])'),
    ("composite", "zone_x_month",
     'EVALUATE SUMMARIZECOLUMNS(dim_zone[Zone], dim_date[year], dim_date[month], '
     '"Fare", [Total Fare])'),
    ("composite", "filtered_manhattan_2019_by_zone",
     'EVALUATE CALCULATETABLE('
     'SUMMARIZECOLUMNS(dim_zone[Zone], "Fare", [Total Fare], "Trips", [Total Trips]), '
     'dim_zone[Borough] = "Manhattan", dim_date[year] = 2019)'),
    ("composite", "scalar_weighted_full_scan",
     'EVALUATE ROW('
     '"TipRate", DIVIDE(SUMX(fct_trips, fct_trips[tip_amount]), '
     'SUMX(fct_trips, fct_trips[fare_amount])), '
     '"DistinctZones", DISTINCTCOUNT(fct_trips[PULocationID]), '
     '"Rows", COUNTROWS(fct_trips))'),
    ("composite", "topn_zone_by_fare",
     'EVALUATE TOPN(50, SUMMARIZECOLUMNS(dim_zone[Zone], dim_date[year], '
     '"Fare", [Total Fare]), [Fare], DESC)'),
    # Column-width at fixed shape — cold scaling with the number of columns touched.
    ("composite", "wide_all_measures",
     'EVALUATE SUMMARIZECOLUMNS(dim_date[year], "a", [Total Fare], "b", [Total Amount], '
     '"c", [Total Tips], "d", [Total Passengers], "e", [Avg Distance])'),
    ("composite", "narrow_one_measure",
     'EVALUATE SUMMARIZECOLUMNS(dim_date[year], "a", [Total Fare])'),
    # --- Tier 3: the RAW table. NYC has ONE where AEMO has five — the star is four tables, not
    #     eight, so this tier is correspondingly smaller rather than padded out to match.
    ("raw", "raw_archive_log",
     'EVALUATE SUMMARIZECOLUMNS(stg_parquet_archive_log[source_type], '
     '"Files", [Archive Files], "Rows", [Archive Source Rows])'),
    # --- Tier 4: selectivity ladder (SUMX lifts work above the XMLA noise floor) ---
    ("hot_only", "sel_1yr",
     'EVALUATE ROW("r", CALCULATE(SUMX(fct_trips, '
     'fct_trips[trip_distance] * fct_trips[fare_amount]), dim_date[year] = 2019))'),
    ("hot_only", "sel_1mo",
     'EVALUATE ROW("r", CALCULATE(SUMX(fct_trips, '
     'fct_trips[trip_distance] * fct_trips[fare_amount]), '
     'dim_date[year] = 2019, dim_date[month] = 6))'),
    ("hot_only", "sel_1zone",
     'EVALUATE ROW("r", CALCULATE(SUMX(fct_trips, '
     'fct_trips[trip_distance] * fct_trips[fare_amount]), '
     'fct_trips[PULocationID] = {key}))'),
    ("hot_only", "sel_1zone_1mo",
     'EVALUATE ROW("r", CALCULATE(SUMX(fct_trips, '
     'fct_trips[trip_distance] * fct_trips[fare_amount]), '
     'fct_trips[PULocationID] = {key}, dim_date[year] = 2019, dim_date[month] = 6))'),
]

# ---------------------------------------------------------------------------- the BTS flights suite
#
# THE SAME SHAPE AGAIN — probes with `probe_rowcount` LAST, composites over the star, one query per
# raw table, then the hot-only selectivity ladder. Read the AEMO block for why each tier exists.
#
# What differs is WHERE the interesting result lives. nyc's probe tier measures 97-99% single-value
# columns — the easy case, where every column can be run-friendly at once. Here the probes walk the
# CARDINALITY LADDER of independent categoricals that compete for V-Order's one sort: DayOfWeek
# (7, near-uniform), Reporting_Airline (~20), Origin/Dest (~350, Zipfian), CRSDepTime (~1,200,
# clustered), Tail_Number (thousands), CancellationCode (~98% NULL). A per-column cold cost across
# that ladder, put beside layout.ordering's per-column `runs`, is what says which columns the
# greedy ordering sacrificed and whether the sacrifice shows up in transcode time.
#
# The ladder and the year-filtered composites use 1988: the archive drains OLDEST FIRST from
# 1987-10, so 1988 is complete after even the first 40-month drain — a later year would silently
# filter to nothing on a young archive, and a filter matching nothing is a very fast query, which
# this benchmark would read as a result. It was 1995 for one commit, which is WORSE than a recent
# year: TranStats' PREZIP endpoint does not serve the 1990s at all
# (download_bts_flights.GAP_YEARS), so 1995 filters to nothing on a FULLY drained archive too.
BTS_QUERIES = [
    # --- Tier 1: per-column probes (rowcount LAST — see the note above) ---
    ("probe", "probe_distance",   'EVALUATE ROW("x", SUM(fct_flights[Distance]))'),
    ("probe", "probe_depdelay",   'EVALUATE ROW("x", SUM(fct_flights[DepDelay]))'),
    ("probe", "probe_dow",        'EVALUATE ROW("x", DISTINCTCOUNT(fct_flights[DayOfWeek]))'),
    ("probe", "probe_carrier",
     'EVALUATE ROW("x", DISTINCTCOUNT(fct_flights[Reporting_Airline]))'),
    ("probe", "probe_origin",     'EVALUATE ROW("x", DISTINCTCOUNT(fct_flights[Origin]))'),
    ("probe", "probe_dest",       'EVALUATE ROW("x", DISTINCTCOUNT(fct_flights[Dest]))'),
    ("probe", "probe_crsdep",
     'EVALUATE ROW("x", COUNTROWS(VALUES(fct_flights[CRSDepTime])))'),
    ("probe", "probe_tail",       'EVALUATE ROW("x", DISTINCTCOUNT(fct_flights[Tail_Number]))'),
    # The most extreme column in the table: ~98% NULL, the store_and_fwd_flag analogue.
    ("probe", "probe_cancelcode",
     'EVALUATE ROW("x", DISTINCTCOUNT(fct_flights[CancellationCode]))'),
    ("probe", "probe_rowcount",   'EVALUATE ROW("x", COUNTROWS(fct_flights))'),
    # --- Tier 2: composite workloads over the star ---
    ("composite", "carrier_x_year",
     'EVALUATE SUMMARIZECOLUMNS(dim_carrier[name], dim_flight_date[year], '
     '"Flights", [Total Flights], "AvgDep", [Avg Dep Delay], "Dist", [Total Distance])'),
    ("composite", "origin_x_dow",
     'EVALUATE SUMMARIZECOLUMNS(fct_flights[Origin], fct_flights[DayOfWeek], '
     '"Flights", [Total Flights], "AvgArr", [Avg Arr Delay])'),
    ("composite", "carrier_x_month",
     'EVALUATE SUMMARIZECOLUMNS(dim_carrier[name], dim_flight_date[year], '
     'dim_flight_date[month], "Flights", [Total Flights])'),
    ("composite", "filtered_1988_by_origin",
     'EVALUATE CALCULATETABLE('
     'SUMMARIZECOLUMNS(fct_flights[Origin], "Flights", [Total Flights], '
     '"AvgDep", [Avg Dep Delay]), dim_flight_date[year] = 1988)'),
    ("composite", "scalar_weighted_full_scan",
     'EVALUATE ROW('
     '"DelayPerMile", DIVIDE(SUMX(fct_flights, fct_flights[DepDelay] * fct_flights[Distance]), '
     'SUMX(fct_flights, fct_flights[Distance])), '
     '"DistinctTails", DISTINCTCOUNT(fct_flights[Tail_Number]), '
     '"Rows", COUNTROWS(fct_flights))'),
    ("composite", "topn_origin_by_flights",
     'EVALUATE TOPN(50, SUMMARIZECOLUMNS(fct_flights[Origin], dim_flight_date[year], '
     '"Flights", [Total Flights]), [Flights], DESC)'),
    # Column-width at fixed shape — cold scaling with the number of columns touched.
    ("composite", "wide_all_measures",
     'EVALUATE SUMMARIZECOLUMNS(dim_flight_date[year], "a", [Total Flights], '
     '"b", [Total Distance], "c", [Avg Dep Delay], "d", [Avg Arr Delay], '
     '"e", [Cancelled Flights])'),
    ("composite", "narrow_one_measure",
     'EVALUATE SUMMARIZECOLUMNS(dim_flight_date[year], "a", [Total Flights])'),
    # --- Tier 3: the RAW table. Like nyc, the star is four tables, so this tier is one query. ---
    ("raw", "raw_archive_log",
     'EVALUATE SUMMARIZECOLUMNS(stg_flights_archive_log[source_type], '
     '"Files", [Archive Files], "Rows", [Archive Source Rows])'),
    # --- Tier 4: selectivity ladder (SUMX lifts work above the XMLA noise floor) ---
    ("hot_only", "sel_1yr",
     'EVALUATE ROW("r", CALCULATE(SUMX(fct_flights, '
     'fct_flights[Distance] * fct_flights[DepDelay]), dim_flight_date[year] = 1988))'),
    ("hot_only", "sel_1mo",
     'EVALUATE ROW("r", CALCULATE(SUMX(fct_flights, '
     'fct_flights[Distance] * fct_flights[DepDelay]), '
     'dim_flight_date[year] = 1988, dim_flight_date[month] = 6))'),
    ("hot_only", "sel_1origin",
     'EVALUATE ROW("r", CALCULATE(SUMX(fct_flights, '
     'fct_flights[Distance] * fct_flights[DepDelay]), '
     'fct_flights[Origin] = {key}))'),
    ("hot_only", "sel_1origin_1mo",
     'EVALUATE ROW("r", CALCULATE(SUMX(fct_flights, '
     'fct_flights[Distance] * fct_flights[DepDelay]), '
     'fct_flights[Origin] = {key}, dim_flight_date[year] = 1988, '
     'dim_flight_date[month] = 6))'),
]

# ---------------------------------------------------------------------------- the green taxi suite
#
# THE SAME SHAPE AS NYC, one probe longer — green is the same extreme-skew regime on a much smaller
# table, plus two columns yellow does not have: trip_type (~98% street-hail, probed below) and
# ehail_fee (~all NULL — not probed: DISTINCTCOUNT over a ~single-valued NULL column is byte-alike
# with probe_storefwd and the extra probe is paid in every pass of every engine).
#
# The ladder and the year-filtered composite use 2014: the archive drains OLDEST FIRST from 2014-01
# (the CDN serves no 2013 month at all), so 2014 is complete after even the first year's drain — a
# later year would silently filter to nothing on a young archive, and a filter matching nothing is
# a very fast query, which this benchmark would read as a result. The borough filter is Brooklyn,
# not Manhattan: green pickups in Manhattan are legally restricted to the upper zones, so yellow's
# filter would select against this fleet's grain rather than through it.
GREEN_QUERIES = [
    # --- Tier 1: per-column probes (rowcount LAST — see the note above) ---
    ("probe", "probe_fare",       'EVALUATE ROW("x", SUM(fct_green_trips[fare_amount]))'),
    ("probe", "probe_distance",   'EVALUATE ROW("x", SUM(fct_green_trips[trip_distance]))'),
    ("probe", "probe_pulocation",
     'EVALUATE ROW("x", DISTINCTCOUNT(fct_green_trips[PULocationID]))'),
    ("probe", "probe_dolocation",
     'EVALUATE ROW("x", DISTINCTCOUNT(fct_green_trips[DOLocationID]))'),
    ("probe", "probe_paytype",
     'EVALUATE ROW("x", DISTINCTCOUNT(fct_green_trips[payment_type]))'),
    # The most extreme columns in the table: ~99% one value, ~97% one value, ~98% one value. If
    # V-Order does what an encoding pass should, it does it here.
    ("probe", "probe_storefwd",
     'EVALUATE ROW("x", DISTINCTCOUNT(fct_green_trips[store_and_fwd_flag]))'),
    ("probe", "probe_ratecode",
     'EVALUATE ROW("x", DISTINCTCOUNT(fct_green_trips[RatecodeID]))'),
    ("probe", "probe_triptype",
     'EVALUATE ROW("x", DISTINCTCOUNT(fct_green_trips[trip_type]))'),
    ("probe", "probe_pickup",
     'EVALUATE ROW("x", COUNTROWS(VALUES(fct_green_trips[lpep_pickup_datetime])))'),
    ("probe", "probe_rowcount",   'EVALUATE ROW("x", COUNTROWS(fct_green_trips))'),
    # --- Tier 2: composite workloads over the star ---
    ("composite", "borough_x_year",
     'EVALUATE SUMMARIZECOLUMNS(dim_green_zone[Borough], dim_green_date[year], '
     '"Fare", [Total Fare], "Trips", [Total Trips], "Dist", [Avg Distance])'),
    ("composite", "paytype_x_borough",
     'EVALUATE SUMMARIZECOLUMNS(fct_green_trips[payment_type], dim_green_zone[Borough], '
     '"Fare", [Total Fare], "Tips", [Total Tips])'),
    ("composite", "dow_x_borough",
     'EVALUATE SUMMARIZECOLUMNS(dim_green_date[day_of_week], dim_green_zone[Borough], '
     '"Trips", [Total Trips], "Fare", [Total Fare])'),
    ("composite", "zone_x_month",
     'EVALUATE SUMMARIZECOLUMNS(dim_green_zone[Zone], dim_green_date[year], '
     'dim_green_date[month], "Fare", [Total Fare])'),
    ("composite", "filtered_brooklyn_2014_by_zone",
     'EVALUATE CALCULATETABLE('
     'SUMMARIZECOLUMNS(dim_green_zone[Zone], "Fare", [Total Fare], "Trips", [Total Trips]), '
     'dim_green_zone[Borough] = "Brooklyn", dim_green_date[year] = 2014)'),
    ("composite", "scalar_weighted_full_scan",
     'EVALUATE ROW('
     '"TipRate", DIVIDE(SUMX(fct_green_trips, fct_green_trips[tip_amount]), '
     'SUMX(fct_green_trips, fct_green_trips[fare_amount])), '
     '"DistinctZones", DISTINCTCOUNT(fct_green_trips[PULocationID]), '
     '"Rows", COUNTROWS(fct_green_trips))'),
    ("composite", "topn_zone_by_fare",
     'EVALUATE TOPN(50, SUMMARIZECOLUMNS(dim_green_zone[Zone], dim_green_date[year], '
     '"Fare", [Total Fare]), [Fare], DESC)'),
    # Column-width at fixed shape — cold scaling with the number of columns touched.
    ("composite", "wide_all_measures",
     'EVALUATE SUMMARIZECOLUMNS(dim_green_date[year], "a", [Total Fare], "b", [Total Amount], '
     '"c", [Total Tips], "d", [Total Passengers], "e", [Avg Distance])'),
    ("composite", "narrow_one_measure",
     'EVALUATE SUMMARIZECOLUMNS(dim_green_date[year], "a", [Total Fare])'),
    # --- Tier 3: the RAW table. Like nyc, the star is four tables, so this tier is one query. ---
    ("raw", "raw_archive_log",
     'EVALUATE SUMMARIZECOLUMNS(stg_green_archive_log[source_type], '
     '"Files", [Archive Files], "Rows", [Archive Source Rows])'),
    # --- Tier 4: selectivity ladder (SUMX lifts work above the XMLA noise floor) ---
    ("hot_only", "sel_1yr",
     'EVALUATE ROW("r", CALCULATE(SUMX(fct_green_trips, '
     'fct_green_trips[trip_distance] * fct_green_trips[fare_amount]), '
     'dim_green_date[year] = 2014))'),
    ("hot_only", "sel_1mo",
     'EVALUATE ROW("r", CALCULATE(SUMX(fct_green_trips, '
     'fct_green_trips[trip_distance] * fct_green_trips[fare_amount]), '
     'dim_green_date[year] = 2014, dim_green_date[month] = 6))'),
    ("hot_only", "sel_1zone",
     'EVALUATE ROW("r", CALCULATE(SUMX(fct_green_trips, '
     'fct_green_trips[trip_distance] * fct_green_trips[fare_amount]), '
     'fct_green_trips[PULocationID] = {key}))'),
    ("hot_only", "sel_1zone_1mo",
     'EVALUATE ROW("r", CALCULATE(SUMX(fct_green_trips, '
     'fct_green_trips[trip_distance] * fct_green_trips[fare_amount]), '
     'fct_green_trips[PULocationID] = {key}, dim_green_date[year] = 2014, '
     'dim_green_date[month] = 6))'),
]

# CMS Open Payments — the WIDE and SPARSE suite.
#
# THIS SUITE HAS ONE THING THE OTHER FOUR DO NOT: A MATCHED SPARSE PAIR. probe_product_head and
# probe_product_tail are the SAME column family, the same semantic type and the same DAX, over a
# column that is ~7% NULL and one that is ~99% NULL — because CMS models a one-to-many product list
# as five repeated groups and almost every payment names one product.
#
# ⚠️ THE PAIR DOES NOT ISOLATE SPARSITY, AND AN EARLIER VERSION OF THIS COMMENT CLAIMED IT DID.
# NULL rate and cardinality move TOGETHER here, necessarily: a column that is 99% NULL has far fewer
# distinct values than its 7% sibling (~127 against ~3,194 in a 187,750-row sample of PY2023),
# because the non-NULL rows are all there is to be distinct over. So the pair holds family, type and
# query constant and varies BOTH — the measured gap is their combined effect and cannot be
# attributed to sparsity alone. First measurement, run 31862268079 on 87.7M rows: 571 ms cold /
# 128 ms warm for the head against 263 / 60 for the tail, so ~2.2x and ~2.1x cheaper.
#
# Separating the two would need a third probe — a mostly-populated column at the tail's cardinality,
# or a sparse one at the head's — and no column family here offers that. Keep both of these
# adjacent, and read the gap as "sparse and low-cardinality together", which is what it is.
#
# The rest of the probes cover the two skew regimes this dataset carries at once — probe_nature and
# probe_form are the 92%/86% single-value columns (nyc's regime, where every column can win the sort)
# and probe_specialty, probe_payer and probe_state are the 302/~1,000/56-value competing ones (bts's
# regime, where they cannot). probe_recipient is deliberately the most expensive: a DISTINCTCOUNT
# over a near-unique id, which is the case a column store is worst at and which no other suite has.
#
# The ladder and the year-filtered composite use 2019: the archive drains OLDEST FIRST from PY2019
# (CMS's catalog serves nothing earlier), so 2019 is complete after even the first year's drain — a
# later year would silently filter to nothing on a young archive, and a filter matching nothing is a
# very fast query, which this benchmark would read as a result. Same rule as bts pinning 1988 and
# green pinning 2014; do not move it forward because the archive "should" have caught up.
CMS_QUERIES = [
    # --- Tier 1: per-column probes (rowcount LAST — see the note above) ---
    ("probe", "probe_amount",
     'EVALUATE ROW("x", SUM(fct_cms_payments[Total_Amount_of_Payment_USDollars]))'),
    # nyc's regime: 92% and 86% single-value. If V-Order does what an encoding pass should, here.
    ("probe", "probe_nature",
     'EVALUATE ROW("x", DISTINCTCOUNT(fct_cms_payments[Nature_of_Payment_or_Transfer_of_Value]))'),
    ("probe", "probe_form",
     'EVALUATE ROW("x", DISTINCTCOUNT(fct_cms_payments[Form_of_Payment_or_Transfer_of_Value]))'),
    # bts's regime: hundreds to thousands of competing values that cannot all win one sort.
    ("probe", "probe_specialty",
     'EVALUATE ROW("x", DISTINCTCOUNT(fct_cms_payments[Covered_Recipient_Specialty_1]))'),
    ("probe", "probe_payer",
     'EVALUATE ROW("x", DISTINCTCOUNT(fct_cms_payments'
     '[Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID]))'),
    ("probe", "probe_state",
     'EVALUATE ROW("x", DISTINCTCOUNT(fct_cms_payments[Recipient_State]))'),
    # THE SPARSE PAIR — read these two together or neither. Same family, same type, same query;
    # ~7% NULL against ~99% NULL. This is the measurement the dataset was added for.
    ("probe", "probe_product_head",
     'EVALUATE ROW("x", DISTINCTCOUNT(fct_cms_payments'
     '[Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1]))'),
    ("probe", "probe_product_tail",
     'EVALUATE ROW("x", DISTINCTCOUNT(fct_cms_payments'
     '[Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5]))'),
    # Near-unique id: the worst case for a column store, and the only probe of its kind here.
    ("probe", "probe_recipient",
     'EVALUATE ROW("x", DISTINCTCOUNT(fct_cms_payments[Covered_Recipient_Profile_ID]))'),
    ("probe", "probe_date",
     'EVALUATE ROW("x", COUNTROWS(VALUES(fct_cms_payments[Date_of_Payment])))'),
    ("probe", "probe_rowcount", 'EVALUATE ROW("x", COUNTROWS(fct_cms_payments))'),
    # --- Tier 2: composite workloads over the star ---
    ("composite", "payer_x_year",
     'EVALUATE SUMMARIZECOLUMNS(dim_cms_payer[payer_name], dim_cms_date[year], '
     '"Amt", [Total Amount], "Pmts", [Total Payments], "Avg", [Avg Payment])'),
    ("composite", "nature_x_year",
     'EVALUATE SUMMARIZECOLUMNS(fct_cms_payments[Nature_of_Payment_or_Transfer_of_Value], '
     'dim_cms_date[year], "Amt", [Total Amount], "Pmts", [Total Payments])'),
    ("composite", "specialty_x_state",
     'EVALUATE SUMMARIZECOLUMNS(fct_cms_payments[Covered_Recipient_Specialty_1], '
     'fct_cms_payments[Recipient_State], "Amt", [Total Amount])'),
    ("composite", "payer_x_month",
     'EVALUATE SUMMARIZECOLUMNS(dim_cms_payer[payer_name], dim_cms_date[year], '
     'dim_cms_date[month], "Amt", [Total Amount])'),
    ("composite", "filtered_2019_by_nature",
     'EVALUATE CALCULATETABLE('
     'SUMMARIZECOLUMNS(fct_cms_payments[Nature_of_Payment_or_Transfer_of_Value], '
     '"Amt", [Total Amount], "Pmts", [Total Payments]), '
     'dim_cms_payer[payer_country] = "United States", dim_cms_date[year] = 2019)'),
    ("composite", "scalar_weighted_full_scan",
     'EVALUATE ROW('
     '"PerPayment", DIVIDE('
     'SUMX(fct_cms_payments, fct_cms_payments[Total_Amount_of_Payment_USDollars]), '
     'SUMX(fct_cms_payments, fct_cms_payments'
     '[Number_of_Payments_Included_in_Total_Amount])), '
     '"DistinctPayers", DISTINCTCOUNT(fct_cms_payments'
     '[Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID]), '
     '"Rows", COUNTROWS(fct_cms_payments))'),
    ("composite", "topn_payer_by_amount",
     'EVALUATE TOPN(50, SUMMARIZECOLUMNS(dim_cms_payer[payer_name], dim_cms_date[year], '
     '"Amt", [Total Amount]), [Amt], DESC)'),
    # Column-width at fixed shape — cold scaling with the number of columns touched.
    ("composite", "wide_all_measures",
     'EVALUATE SUMMARIZECOLUMNS(dim_cms_date[year], "a", [Total Amount], "b", [Total Payments], '
     '"c", [Avg Payment], "d", [Payment Count], "e", [Distinct Recipients], '
     '"f", [Distinct Products])'),
    ("composite", "narrow_one_measure",
     'EVALUATE SUMMARIZECOLUMNS(dim_cms_date[year], "a", [Total Amount])'),
    # THE SPARSE GROUP AT WIDTH — five mostly-NULL columns of one family in one grouping. The
    # single-column pair above says what a sparse column costs alone; this says what happens when a
    # query opens the whole group, which is the shape a real report over this data would take.
    ("composite", "wide_sparse_group",
     'EVALUATE SUMMARIZECOLUMNS('
     'fct_cms_payments[Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1], '
     'fct_cms_payments[Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_2], '
     'fct_cms_payments[Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_3], '
     'fct_cms_payments[Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_4], '
     'fct_cms_payments[Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_5], '
     '"Amt", [Total Amount])'),
    # --- Tier 3: the RAW table. Like nyc, the star is four tables, so this tier is one query. ---
    ("raw", "raw_archive_log",
     'EVALUATE SUMMARIZECOLUMNS(stg_cms_archive_log[source_type], '
     '"Files", [Archive Files], "Rows", [Archive Source Rows])'),
    # --- Tier 4: selectivity ladder (SUMX lifts work above the XMLA noise floor) ---
    ("hot_only", "sel_1yr",
     'EVALUATE ROW("r", CALCULATE(SUMX(fct_cms_payments, '
     'fct_cms_payments[Total_Amount_of_Payment_USDollars] * '
     'fct_cms_payments[Number_of_Payments_Included_in_Total_Amount]), '
     'dim_cms_date[year] = 2019))'),
    ("hot_only", "sel_1mo",
     'EVALUATE ROW("r", CALCULATE(SUMX(fct_cms_payments, '
     'fct_cms_payments[Total_Amount_of_Payment_USDollars] * '
     'fct_cms_payments[Number_of_Payments_Included_in_Total_Amount]), '
     'dim_cms_date[year] = 2019, dim_cms_date[month] = 6))'),
    ("hot_only", "sel_1payer",
     'EVALUATE ROW("r", CALCULATE(SUMX(fct_cms_payments, '
     'fct_cms_payments[Total_Amount_of_Payment_USDollars] * '
     'fct_cms_payments[Number_of_Payments_Included_in_Total_Amount]), '
     'fct_cms_payments[Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID] = {key}))'),
    ("hot_only", "sel_1payer_1mo",
     'EVALUATE ROW("r", CALCULATE(SUMX(fct_cms_payments, '
     'fct_cms_payments[Total_Amount_of_Payment_USDollars] * '
     'fct_cms_payments[Number_of_Payments_Included_in_Total_Amount]), '
     'fct_cms_payments[Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID] = {key}, '
     'dim_cms_date[year] = 2019, dim_cms_date[month] = 6))'),
]

# ---------------------------------------------------------------------------- the TPC-DS suite
#
# THE ONLY SUITE HERE THAT REPRODUCES SOMEONE ELSE'S QUERIES RATHER THAN ASKING ITS OWN. The other
# five exist to measure a surface; this one exists so the Data Leaps / Microsoft white paper *Modern
# Power BI Architecture Choices for Reporting on Azure Databricks* can be re-run against a different
# Delta layout, alongside the Databricks arm in c:/dbx_vertipaq. So the composite tier IS the
# paper's five test queries and nothing else, and the probes and the ladder around them are the
# harness's own instrument, kept in the shape every other suite uses.
#
# ⚠️ THE PAPER'S LITERAL DAX IS NOT AVAILABLE, AND THESE ARE A RECONSTRUCTION. Section 6.3 names the
# five queries and says their full text is in Appendix 4; section 11 shows Appendix 4 is an external
# attachment, and it is not published -- the companion repository is gone. What the paper DOES carry
# is enough to fix every degree of freedom that changes what the engine does:
#
#   * Figure 4.4.1, the model diagram, names the measures verbatim -- Catalog Revenue, Catalog Sales
#     Quantity, Catalog Sales Same Period LY, Catalog Sales YoY, Store Distinct Customers, Store Net
#     Profit, Store Profit % by Item Category, Store Revenue -- and shows the relationships and each
#     table's column subset.
#   * Figure 6.1.1, the test report page, shows the five visuals and what each groups by: a Customer
#     Count card; a "Store Profit % Split by Category" donut over i_category; a "Catalog Revenue
#     Ranked by Promotion" bar over p_promo_name; a "Catalog Sales Quantity YoY" combo over quarters
#     with three named series; and a "Store Revenue Time Analysis" matrix of category by quarter
#     carrying revenue, YoY and YTD.
#
# What is NOT recoverable is the Performance Analyzer boilerplate. That is why these are written as
# plain EVALUATEs: pretending to a capture we do not have would be worse than saying so here.
#
# ONE THING THE MISSING TEXT COSTS NOTHING. Query 3 is described as "rank based on sum, implemented
# as a visual calculation". A visual calculation is evaluated on the visual's RESULT SET, not by the
# storage engine, so the captured query underneath it is the plain sorted aggregation written below
# -- the rank adds no engine work in either version.
#
# THE PAPER'S FILTER SCENARIOS ARE 1 AND 3 HERE. Its scenario 1 is the unfiltered page and its
# scenario 3 applies all six slicers; both are below, as `q<N>_...` and `q<N>_filtered_...`. Its
# scenario 2 filters only the columns its Composite Model's aggregation tables cover, and there is
# no Composite Model in this project, so that scenario has no counterpart and is deliberately absent.
#
# THE YEAR PIN IS 2022, and it is pinned for the opposite reason to bts's 1988 and green's 2014.
# Those pin the OLDEST year because their archives drain oldest-first and a recent year would filter
# to nothing. dsdgen emits 2021-2026 whole, so nothing is missing -- but Q5's year-over-year term
# needs a FULL PRIOR YEAR inside the window, and 2021 has none. 2022 is the first year where the
# comparison is real rather than a divide by blank.
TPCDS_QUERIES = [
    # --- Tier 1: per-column probes over the mart (rowcount LAST -- see the note at the top) ---
    ("probe", "probe_ext_sales_price",
     'EVALUATE ROW("x", SUM(store_sales[ss_ext_sales_price]))'),
    ("probe", "probe_quantity", 'EVALUATE ROW("x", SUM(store_sales[ss_quantity]))'),
    ("probe", "probe_item", 'EVALUATE ROW("x", DISTINCTCOUNT(store_sales[ss_item_sk]))'),
    ("probe", "probe_customer", 'EVALUATE ROW("x", DISTINCTCOUNT(store_sales[ss_customer_sk]))'),
    ("probe", "probe_store", 'EVALUATE ROW("x", DISTINCTCOUNT(store_sales[ss_store_sk]))'),
    ("probe", "probe_sold_date",
     'EVALUATE ROW("x", COUNTROWS(VALUES(store_sales[ss_sold_date_sk])))'),
    ("probe", "probe_rowcount", 'EVALUATE ROW("x", COUNTROWS(store_sales))'),

    # --- Tier 2: THE PAPER'S FIVE QUERIES, scenario 1 (no slicer selected) ---
    # Q1 Distinct Count -- the "Customer Count" card. The paper names this its hardest query for
    # Direct Lake and Direct Lake over Mirrored, so it is the one to read first on any comparison.
    ("composite", "q1_distinct_count",
     'EVALUATE ROW("Customer Count", [Store Distinct Customers])'),
    # Q2 Percentage Share -- the "Store Profit % Split by Category" donut.
    ("composite", "q2_pct_share",
     'EVALUATE SUMMARIZECOLUMNS(item[i_category], '
     '"Share", [Store Profit % by Item Category], "Profit", [Store Net Profit])'),
    # Q3 Rank based on sum -- the "Catalog Revenue Ranked by Promotion" bar, sorted descending.
    # TOPN(1001) is the window a bar chart of this size requests; the rank itself is a visual
    # calculation and never reaches the engine.
    ("composite", "q3_rank_by_sum",
     'EVALUATE TOPN(1001, SUMMARIZECOLUMNS(promotion[p_promo_name], '
     '"Catalog Revenue", [Catalog Revenue]), [Catalog Revenue], DESC)'),
    # Q4 YoY on the SECOND fact -- the "Catalog Sales Quantity YoY" combo chart, by quarter, with
    # exactly the three series the paper's legend names.
    ("composite", "q4_yoy_second_fact",
     'EVALUATE SUMMARIZECOLUMNS(date_dim[d_quarter_name], '
     '"Catalog Sales Quantity", [Catalog Sales Quantity], '
     '"Catalog Sales Same Period LY", [Catalog Sales Same Period LY], '
     '"Catalog Sales YoY", [Catalog Sales YoY])'),
    # Q5 YoY and YTD on the LARGEST fact -- the "Store Revenue Time Analysis" matrix, category by
    # quarter. The heaviest query in the suite and the one the paper's SF100/SF1000 findings lean on.
    ("composite", "q5_yoy_ytd_large_fact",
     'EVALUATE SUMMARIZECOLUMNS(item[i_category], date_dim[d_quarter_name], '
     '"Store Revenue", [Store Revenue], "Store Revenue YoY", [Store Revenue YoY], '
     '"Store Revenue YTD", [Store Revenue YTD])'),

    # --- Tier 2 (cont.): the same five under the paper's SCENARIO 3, all six slicers applied ---
    # The slicers are the ones on its report page: Year, Customer State, Customer Education, Store
    # Manager, Carrier and Catalog Type. Filtering through six dimensions at once is what makes the
    # cold transcode pay for the dimension key columns as well as the fact's, which is the whole
    # reason the paper measures a filtered scenario separately.
    ("composite", "q1_filtered_distinct_count",
     'EVALUATE CALCULATETABLE(ROW("Customer Count", [Store Distinct Customers]), '
     'date_dim[d_year] = 2022, customer_address[ca_state] = "CA", '
     'customer_demographics[cd_education_status] = "College")'),
    ("composite", "q2_filtered_pct_share",
     'EVALUATE CALCULATETABLE(SUMMARIZECOLUMNS(item[i_category], '
     '"Share", [Store Profit % by Item Category]), '
     'date_dim[d_year] = 2022, customer_address[ca_state] = "CA", '
     'customer_demographics[cd_education_status] = "College")'),
    ("composite", "q3_filtered_rank_by_sum",
     'EVALUATE CALCULATETABLE(TOPN(1001, SUMMARIZECOLUMNS(promotion[p_promo_name], '
     '"Catalog Revenue", [Catalog Revenue]), [Catalog Revenue], DESC), '
     'date_dim[d_year] = 2022, ship_mode[sm_carrier] = "UPS", catalog_page[cp_type] = "bi-annual")'),
    ("composite", "q4_filtered_yoy_second_fact",
     'EVALUATE CALCULATETABLE(SUMMARIZECOLUMNS(date_dim[d_quarter_name], '
     '"Catalog Sales Quantity", [Catalog Sales Quantity], '
     '"Catalog Sales YoY", [Catalog Sales YoY]), '
     'ship_mode[sm_carrier] = "UPS", catalog_page[cp_type] = "bi-annual")'),
    ("composite", "q5_filtered_yoy_ytd_large_fact",
     'EVALUATE CALCULATETABLE(SUMMARIZECOLUMNS(item[i_category], date_dim[d_quarter_name], '
     '"Store Revenue", [Store Revenue], "Store Revenue YTD", [Store Revenue YTD]), '
     'customer_address[ca_state] = "CA", '
     'customer_demographics[cd_education_status] = "College")'),

    # --- Tier 2 (cont.): the harness's own two, so column-width scaling is comparable across
    #     datasets. Not the paper's; kept because every other suite carries them.
    ("composite", "wide_all_measures",
     'EVALUATE SUMMARIZECOLUMNS(date_dim[d_year], "a", [Store Revenue], "b", [Store Net Profit], '
     '"c", [Store Distinct Customers], "d", [Catalog Revenue], "e", [Catalog Sales Quantity])'),
    ("composite", "narrow_one_measure",
     'EVALUATE SUMMARIZECOLUMNS(date_dim[d_year], "a", [Store Revenue])'),
    # The catalog side of the star, which the paper's five queries reach only through promotion and
    # the date. Without these, ship_mode and catalog_page would be tables the suite never touches.
    ("composite", "catalog_by_ship_mode_x_year",
     'EVALUATE SUMMARIZECOLUMNS(ship_mode[sm_type], date_dim[d_year], '
     '"Revenue", [Catalog Revenue], "Qty", [Catalog Sales Quantity])'),
    ("composite", "catalog_by_page_type",
     'EVALUATE SUMMARIZECOLUMNS(catalog_page[cp_type], '
     '"Revenue", [Catalog Revenue], "Profit", [Catalog Net Profit])'),

    # --- Tier 3: the RAW table. One, because the star is eleven tables of which only the log is
    #     landing-tier -- the same rule as nyc, not aemo's six.
    ("raw", "raw_archive_log",
     'EVALUATE SUMMARIZECOLUMNS(stg_tpcds_archive_log[source_type], '
     '"Files", [Archive Files], "Rows", [Archive Source Rows])'),

    # --- Tier 4: selectivity ladder on the store key (SUMX lifts the work above the XMLA floor) ---
    ("hot_only", "sel_1yr",
     'EVALUATE ROW("r", CALCULATE(SUMX(store_sales, '
     'store_sales[ss_quantity] * store_sales[ss_ext_sales_price]), date_dim[d_year] = 2022))'),
    ("hot_only", "sel_1mo",
     'EVALUATE ROW("r", CALCULATE(SUMX(store_sales, '
     'store_sales[ss_quantity] * store_sales[ss_ext_sales_price]), '
     'date_dim[d_year] = 2022, date_dim[d_moy] = 6))'),
    ("hot_only", "sel_1store",
     'EVALUATE ROW("r", CALCULATE(SUMX(store_sales, '
     'store_sales[ss_quantity] * store_sales[ss_ext_sales_price]), '
     'store_sales[ss_store_sk] = {key}))'),
    ("hot_only", "sel_1store_1mo",
     'EVALUATE ROW("r", CALCULATE(SUMX(store_sales, '
     'store_sales[ss_quantity] * store_sales[ss_ext_sales_price]), '
     'store_sales[ss_store_sk] = {key}, date_dim[d_year] = 2022, date_dim[d_moy] = 6))'),
]


# The ladder's filter value is resolved from the data AFTER pass 1, per dataset. Three things per
# suite: the queries, the DAX that finds the busiest key, and what to CALL it in the log and the
# report — because "top DUID: 132" on a taxi run is exactly the quiet mislabel this repo is against.
#
# `quote` is not decoration. AEMO's DUID is a string and must be quoted into the filter; NYC's
# LocationID is an integer and must NOT be, or the filter compares an int column to text and matches
# nothing — silently, and as a very fast query, which this benchmark would read as a result.
SUITES = {
    "aemo": {
        "queries": AEMO_QUERIES,
        "label": "top DUID",
        "resolve": 'EVALUATE TOPN(1, SUMMARIZECOLUMNS(fct_summary[DUID], "m", [Total MWh]), '
                   '[m], DESC)',
        "quote": True,
        "ready": 'EVALUATE ROW("n", COUNTROWS(dim_calendar))',
    },
    "nyc": {
        "queries": NYC_QUERIES,
        "label": "busiest pickup zone",
        "resolve": 'EVALUATE TOPN(1, SUMMARIZECOLUMNS(fct_trips[PULocationID], '
                   '"m", [Total Trips]), [m], DESC)',
        "quote": False,
        "ready": 'EVALUATE ROW("n", COUNTROWS(dim_date))',
    },
    # bts's Origin is a string like aemo's DUID, so it IS quoted into the filter — see the note on
    # `quote` above: an unquoted string filter compares text to nothing and matches silently.
    "bts": {
        "queries": BTS_QUERIES,
        "label": "busiest origin airport",
        "resolve": 'EVALUATE TOPN(1, SUMMARIZECOLUMNS(fct_flights[Origin], '
                   '"m", [Total Flights]), [m], DESC)',
        "quote": True,
        "ready": 'EVALUATE ROW("n", COUNTROWS(dim_flight_date))',
    },
    # green's PULocationID is an integer like nyc's, so it is NOT quoted; the readiness probe reads
    # this dataset's OWN date dimension — a hardcoded dim_date here is exactly the failure the
    # SUITES dict exists to prevent.
    "green": {
        "queries": GREEN_QUERIES,
        "label": "busiest pickup zone",
        "resolve": 'EVALUATE TOPN(1, SUMMARIZECOLUMNS(fct_green_trips[PULocationID], '
                   '"m", [Total Trips]), [m], DESC)',
        "quote": False,
        "ready": 'EVALUATE ROW("n", COUNTROWS(dim_green_date))',
    },
    # cms's payer id is a STRING like aemo's DUID and bts's Origin, so it IS quoted into the filter.
    # It resolves on [Total Amount] rather than a row count: the biggest payer by DOLLARS is the one
    # a reader of this data cares about, and a count would resolve to whoever bought the most meals.
    "cms": {
        "queries": CMS_QUERIES,
        "label": "top paying manufacturer",
        "resolve": 'EVALUATE TOPN(1, SUMMARIZECOLUMNS(fct_cms_payments'
                   '[Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID], '
                   '"m", [Total Amount]), [m], DESC)',
        "quote": True,
        "ready": 'EVALUATE ROW("n", COUNTROWS(dim_cms_date))',
    },
    # tpcds's ss_store_sk is a BIGINT surrogate key, so it is NOT quoted -- see the note on `quote`
    # above; an unquoted string would match nothing silently, and a quoted integer is the same trap
    # from the other side. It resolves on [Store Revenue] rather than a row count because that is
    # the measure the paper's own report ranks by, and the readiness probe reads THIS dataset's date
    # dimension, which is `date_dim` and not any of the five `dim_*` spellings above.
    "tpcds": {
        "queries": TPCDS_QUERIES,
        "label": "busiest store",
        "resolve": 'EVALUATE TOPN(1, SUMMARIZECOLUMNS(store_sales[ss_store_sk], '
                   '"m", [Store Revenue]), [m], DESC)',
        "quote": False,
        "ready": 'EVALUATE ROW("n", COUNTROWS(date_dim))',
    },
}

# EVERY DAX STRING IN THIS FILE IS REACHABLE FROM `SUITES`, and that is the invariant, not a
# convenience. `warm_up`'s readiness probe and `top_key`'s resolver are DAX that never appears in
# `queries`, so a test checking only the query list checks two thirds of the file — which is exactly
# how a hardcoded `dim_calendar` reached an NYC model that has `dim_date`, and cost the whole
# benchmark job to sixteen readiness retries. benchmark/test_templates.py now walks
# queries + resolve + ready for BOTH datasets against their own templates. Any new DAX belongs in
# this dict, not in a function body.
DAX_KEYS = ("resolve", "ready")

SUITE = SUITES[E.dataset()]
QUERIES = SUITE["queries"]


def resolve_queries(key):
    """Fill the ladder's "{key}" placeholder with the resolved value.

    If nothing could be resolved, DROP the key-dependent ladder queries rather than run a broken
    filter — a filter matching nothing is a very fast query, and a fast query is precisely what this
    benchmark reads as a result."""
    out = []
    for tier, name, dax in QUERIES:
        if "{key}" in dax:
            if key is None or key == "":
                continue
            dax = dax.replace("{key}", '"' + str(key) + '"' if SUITE["quote"] else str(key))
        out.append((tier, name, dax))
    return out


def _load_adomd(adomd_dir: str):
    """Make Microsoft.AnalysisServices.AdomdClient importable via pythonnet."""
    import clr  # pythonnet
    hits = glob.glob(os.path.join(adomd_dir, "**", "Microsoft.AnalysisServices.AdomdClient.dll"),
                     recursive=True)
    if not hits:
        sys.exit(f"ADOMD client DLL not found under {adomd_dir!r}")
    hits.sort(key=lambda p: ("netcore" not in p.lower() and "net6" not in p.lower(), len(p)))
    d = os.path.dirname(hits[0])
    if d not in sys.path:
        sys.path.append(d)
    clr.AddReference("Microsoft.AnalysisServices.AdomdClient")
    print(f"Loaded ADOMD from {hits[0]}")


def open_conn(workspace: str, model: str, token: str, tries=5, delay=15):
    """Open an XMLA connection, retrying transient drops. The XMLA endpoint can forcibly close an
    idle connection (SocketException 10054) — especially after the idle gap or under capacity
    throttling — so one blip shouldn't kill the run."""
    from Microsoft.AnalysisServices.AdomdClient import AdomdConnection
    conn_str = (
        f"Data Source=powerbi://api.powerbi.com/v1.0/myorg/{workspace};"
        f"Initial Catalog={model};User ID=;Password={token};"
    )
    last = None
    for i in range(1, tries + 1):
        try:
            conn = AdomdConnection(conn_str)
            conn.Open()
            return conn
        except Exception as e:
            last = e
            print(f"  open_conn {i}/{tries} failed ({str(e).splitlines()[0][:100]}); "
                  f"retrying in {delay}s...", flush=True)
            time.sleep(delay)
    raise last


def _refresh(conn, model, kind):
    from Microsoft.AnalysisServices.AdomdClient import AdomdCommand
    tmsl = json.dumps({"refresh": {"type": kind, "objects": [{"database": model}]}})
    AdomdCommand(tmsl, conn).ExecuteNonQuery()


def warm_up(conn, model, tries=16, delay=30):
    """A freshly-created Direct Lake model can't read its OneLake source until security
    propagates — the first refresh/query fails with 'source tables ... do not exist or access
    was denied'. Reframe (full) + probe a trivial query, looping until it actually reads data
    (or we give up). Returns True once queryable.

    This is the ONLY refresh the run issues, and the only query before pass 1. It matters more now
    that the model is deleted and recreated every run: a brand-new item is exactly the propagation
    case this exists for.

    The probe reads a TINY DIMENSION, not `COUNTROWS(fct_summary)` as it once did. That was
    byte-identical to the `probe_rowcount` query — the zero-column control that render_summary's
    marginal-column-cost table subtracts from every other probe — so the readiness check was
    pre-warming the very control it would later be measured against. dim_calendar is a few thousand
    rows and proves the same thing: the model can reach its OneLake source.

    WHICH dimension is per dataset — `dim_calendar` on aemo, `dim_date` on nyc — and it comes from
    SUITES rather than a literal here. It was a literal, and on the NYC model it named a table that
    does not exist, so warm-up burned its full 16x30s and the leg never ran a single query.

    The refresh is BEST-EFFORT and its failure is explicitly NOT a readiness signal — a model can be
    perfectly queryable while a refresh against it is rejected. Only the probe decides. Retrying the
    pair as one unit spent 16×30s and then skipped the leg entirely."""
    probe = SUITE["ready"]
    for i in range(1, tries + 1):
        try:
            _refresh(conn, model, "full")   # (re)frame Direct Lake against the current Delta
        except Exception as e:
            print(f"  warm-up {i}: refresh unavailable ({str(e).splitlines()[0][:90]}) — "
                  "probing anyway", flush=True)
        try:
            run_query(conn, probe)          # confirm it can actually transcode/read the data
            print(f"  warm-up: queryable after {i} attempt(s)", flush=True)
            return True
        except Exception as e:
            print(f"  warm-up {i}/{tries}: not ready ({str(e).splitlines()[0][:110]})"
                  + (f"; waiting {delay}s..." if i < tries else ""), flush=True)
            if i < tries:
                time.sleep(delay)
    print("  warm-up: model never became queryable — skipping it", flush=True)
    return False


def run_query(conn, dax: str):
    """Execute dax, drain all rows, return (elapsed_ms, row_count)."""
    from Microsoft.AnalysisServices.AdomdClient import AdomdCommand
    t0 = time.perf_counter()
    reader = AdomdCommand(dax, conn).ExecuteReader()
    rows = 0
    try:
        fc = reader.FieldCount
        while reader.Read():
            for i in range(fc):
                reader.GetValue(i)
            rows += 1
    finally:
        reader.Close()
    return (time.perf_counter() - t0) * 1000.0, rows


def run_scalar(conn, dax):
    """Execute dax and return the first cell of the first row (or None)."""
    from Microsoft.AnalysisServices.AdomdClient import AdomdCommand
    reader = AdomdCommand(dax, conn).ExecuteReader()
    try:
        if reader.Read() and reader.FieldCount:
            return reader.GetValue(0)
    finally:
        reader.Close()
    return None


def top_key(conn):
    """The busiest value of this dataset's ladder column — the DUID with the largest Total MWh on
    aemo, the pickup zone with the most trips on nyc. Same underlying data in every engine, so every
    job resolves the same value and the ladder rows stay comparable across a run."""
    v = run_scalar(conn, SUITE["resolve"])
    return None if v is None else str(v)


def _tier_of(pass_no):
    """The tier a pass belongs to. The pass NUMBER is the tier — that is the whole design."""
    return "cold" if pass_no == 1 else ("warm" if pass_no == 2 else "hot")


def _finalize(by_pass, tier, rows):
    """One query's samples, keyed by pass number, reduced to the reported metrics.

    cold and warm are single samples and carry no spread: there is exactly one first visit and one
    second visit per deployed model. Hot is passes 3+, reported as a MEDIAN — a single capacity spike
    (a 2.5s blip among 110ms runs) blows up a mean and fabricates a winner.

    A query can legitimately be missing pass 1: unless BENCH_TOP_DUID is pinned, the ladder joins the
    session at pass 2, so it has warm and hot numbers and no cold one. The render layer scopes each
    metric to the engines and queries that have it, so a missing tier is a gap, not a zero.
    """
    res = {"tier": tier, "rows": rows, "ms_by_pass": {str(p): v for p, v in sorted(by_pass.items())}}
    if 1 in by_pass:
        res["cold_ms"] = by_pass[1]
    if 2 in by_pass:
        res["warm_ms"] = by_pass[2]
    hot = [v for p, v in sorted(by_pass.items()) if p >= 3]
    if hot:
        res["all_hot_ms"] = hot
        res["hot_median_ms"] = statistics.median(hot)
        hlo, hhi, hmed = min(hot), max(hot), statistics.median(hot)
        res["hot_spread_pct"] = 100.0 * (hhi - hlo) / hmed if hmed else 0.0
    return res


def bench_model(workspace, model, token, runs, pinned_duid=None, think_seconds=0.0):
    """Replay `runs` passes of the whole suite against one model and return (timings, top_duid).

    The ORDER here is the measurement. Readiness, then pass 1 with nothing in between — no refresh,
    no DMV probe, no DUID resolve — because anything that touches a fact column first would spend
    the cold pass before it starts.

    `think_seconds` pauses BETWEEN queries — a person reading a visual before clicking the next one.
    It is outside every timed region (`run_query` starts its clock after the pause), so it changes
    what is being reproduced without changing what is being measured.
    """
    print(f"\n=== Benchmarking {model} ({runs} passes: 1 cold, 2 warm, 3+ hot; "
          f"{think_seconds}s think time) ===")
    conn = open_conn(workspace, model, token)
    if not warm_up(conn, model):
        conn.Close()
        return None, None

    td = pinned_duid
    queries = resolve_queries(td)   # 25 when the DUID is pinned, 21 when it is resolved below
    samples, rows_of, tier_of = {}, {}, {}
    first = True
    try:
        for p in range(1, runs + 1):
            tier = _tier_of(p)
            print(f"\n  --- pass {p}/{runs} ({tier}) — {len(queries)} queries ---", flush=True)
            for tier_name, name, dax in queries:
                # Between queries, not before the first: a session opens with a query, and the pause
                # carries across the pass boundary because the user does not know where that is.
                if think_seconds and not first:
                    time.sleep(think_seconds)
                first = False
                t, rows = run_query(conn, dax)
                samples.setdefault(name, {})[p] = t
                rows_of[name] = rows
                tier_of[name] = tier_name
                print(f"    [{tier_name}] {name}: {t:,.1f}ms (rows={rows})", flush=True)
            if p == 1 and not td:
                # Only now — this transcodes DUID and mw, which probe_duid and probe_mw measure.
                # Free at this point, because pass 1 has already touched both.
                td = top_key(conn)
                queries = resolve_queries(td)
                print(f"  {SUITE['label']} resolved after the cold pass: {td} "
                      f"— the ladder joins from pass 2 ({len(queries)} queries)", flush=True)
    finally:
        conn.Close()

    results = {n: _finalize(by_pass, tier_of[n], rows_of[n]) for n, by_pass in samples.items()}
    return results, td


def _write_timings(model, res):
    # res is already keyed by query with the final report keys (tier, rows, ms_by_pass, cold_ms,
    # warm_ms, all_hot_ms, hot_median_ms, hot_spread_pct) — merge as-is.
    report.merge({"timings": {model: res}})


def main():
    # Engine selection FIRST, before a token is minted or the workspace is read: a misconfigured
    # dispatch should fail in a second, not after an auth round trip.
    picked = E.selected()
    if len(picked) != 1:
        sys.exit(f"BENCH_ENGINES must name exactly ONE engine, got {picked}. This script measures "
                 "one model per process by design — the workflow runs one job per engine so that "
                 "each mints its own token, and every comparison is made by the render layer from "
                 "the merged report.")
    engine = picked[0]
    model = E.model_name(engine)

    workspace = os.environ["PBI_WORKSPACE"].strip()
    from duckrun import auth
    token = os.environ.get("PBI_TOKEN") or auth.get_powerbi_token()  # self-acquire the XMLA token
    adomd_dir = os.environ.get("ADOMD_DIR", ".")
    runs = int(os.environ.get("BENCH_RUNS", "6"))
    think = float(os.environ.get("BENCH_THINK_SECONDS", "4"))
    # Pinning the DUID skips the resolve entirely, so the ladder runs from pass 1 like everything
    # else. Unpinned, it is resolved after the cold pass and the ladder joins at pass 2 — see
    # bench_model. Either way the value is recorded per model, so the render layer can warn if two
    # engines disagreed instead of assuming they did not.
    pinned_duid = (os.environ.get("BENCH_TOP_DUID") or "").strip() or None

    _load_adomd(adomd_dir)
    print(f"Workspace : {workspace}")
    print(f"Engine    : {engine} -> {model} (written by {E.WRITER.get(engine, '?')}, "
          f"phase {E.phase()})")
    # Think time is idle, so it costs no CU — but it is idle INSIDE the token's ~1 hour life, which
    # is the whole reason the workflow runs one job per engine. Say what it adds up to, so a raised
    # value that would run the token out is visible before the measurement rather than after.
    print(f"Passes    : {runs} (1 cold, 2 warm, 3+ hot)   "
          f"Top DUID: {pinned_duid or '(resolved after the cold pass)'}")
    print(f"Think time: {think}s between queries "
          f"(~{think * (len(QUERIES) * runs - 1) / 60:.0f} min of idle across this session)")

    res, td = bench_model(workspace, model, token, runs, pinned_duid, think)
    if res is None:
        sys.exit(f"[{engine}] {model!r} never became queryable — nothing measured.")
    _write_timings(model, res)
    # `top_duid` is the historical key and every committed report carries it, so it stays.
    # `ladder` is the honest one: the same value with the label this dataset calls it by, so a
    # taxi run's report does not print "top DUID: 132". render_summary prefers `ladder`.
    report.merge({"top_duid": {model: td},
                  "ladder": {model: {"label": SUITE["label"], "value": td}}})
    print(f"\n[{engine}] measured {len(res)} queries over {runs} passes "
          f"-> {os.environ.get('RUN_REPORT', 'run_report.json')}")


if __name__ == "__main__":
    main()
