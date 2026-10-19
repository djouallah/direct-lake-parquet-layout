// node --test dashboard/data.test.mjs — offline, tokenless, about a second.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import * as d from "./data.js";
import * as r from "./report.js";
import { PROBE_COLUMNS } from "./probe_columns.js";
import { source, moduleText } from "./probe_columns.mjs";

// A minimal record: one engine, one mart, a handful of queries.
function rec({ id, engine = "duckrun", ds = "aemo", rows = 1000, rgs = 10, mb = 100, cfg = {}, ord = {},
  cold = 1000, warm = 100, hot = 50, dq = null, extra = {}, enc = null, compression = "SNAPPY" }) {
  const qs = (scale) => ({
    probe_rowcount: { cold_ms: 10 * scale, warm_ms: 5 * scale, hot_median_ms: 4 * scale },
    probe_mw: { cold_ms: cold * scale, warm_ms: warm * scale, hot_median_ms: hot * scale },
    probe_price: { cold_ms: cold * scale, warm_ms: warm * scale, hot_median_ms: hot * scale },
    ...extra,
  });
  const timings = { [`${ds}_${engine}`]: qs(1) };
  if (dq) timings[`${ds}_${engine}_dq`] = qs(dq);
  return {
    engine, _file: `${id}.json`, run: { id: String(id), started: "2026-10-01T00:00:00Z" },
    inputs: { dataset: ds },
    benchmark: { timings },
    layout: {
      config: { [engine]: cfg },
      ordering: { [engine]: { ...ord, columns: { mw: { runs: 500 }, price: { runs: 50 } }, sample: { rows: 1000 } } },
      stats: { [engine]: { [`mart.${d.DATASETS[ds].mart}`]: { total_rows: rows, num_row_groups: rgs, num_files: 1, size_mb: mb, compression } } },
      encodings: { [engine]: enc || { mw: { encodings: ["PLAIN", "RLE_DICTIONARY"], mb: mb / 2, dict_pages: rgs, chunks: rgs },
        price: { encodings: ["PLAIN", "RLE_DICTIONARY"], mb: mb / 4, dict_pages: rgs, chunks: rgs } } },
    },
  };
}

// ------------------------------------------------------------------------------------- writers

test("writers are named for what they wrote", () => {
  assert.equal(d.writerOf(rec({ id: 1, cfg: { sorted: "true" } })).key, "deltars_sorted");
  assert.equal(d.writerOf(rec({ id: 1 })).key, "deltars_unsorted");
  assert.equal(d.writerOf(rec({ id: 1, engine: "spark", cfg: { resource_profile: "readHeavyForPBI" } })).key, "spark_vorder");
  assert.equal(d.writerOf(rec({ id: 1, engine: "spark", cfg: { resource_profile: "writeHeavy" } })).key, "spark_plain");
  // No profile recorded: not guessed at.
  assert.equal(d.writerOf(rec({ id: 1, engine: "spark" })).key, "spark_unknown");
  // The warehouse's own readback beats its declared config.
  assert.equal(d.writerOf(rec({ id: 1, engine: "dwh", cfg: { vorder: "true" }, ord: { vorder_enabled: false } })).key, "dwh_novorder");
  assert.equal(d.writerOf(rec({ id: 1, engine: "dwh" })).key, "dwh");
});

test("a record with no dataset is aemo, as every record was before the input existed", () => {
  const x = rec({ id: 1 });
  delete x.inputs;
  assert.equal(d.datasetOf(x), "aemo");
});

// ------------------------------------------------------------------------------------- flatten

test("flattenRun reads the mart's shape whatever its schema prefix", () => {
  const f = d.flattenRun(rec({ id: 7, rows: 2000, rgs: 4, mb: 50 }));
  assert.equal(f.rows, 2000);
  assert.equal(f.rgs, 4);
  assert.equal(f.rowsPerRg, 500);
  assert.equal(f.mb, 50);
  assert.equal(f.sampleRows, 1000);
});

test("DirectQuery timings are the _dq model and nothing else", () => {
  const t = d.timingsOf(rec({ id: 1, dq: 10 }));
  assert.equal(t.dl.probe_mw.cold_ms, 1000);
  assert.equal(t.dq.probe_mw.cold_ms, 10000);
});

test("only the biggest source generation is kept, and an unmeasured run is not dropped", () => {
  const recs = [rec({ id: 1, rows: 1000 }), rec({ id: 2, rows: 500 }), rec({ id: 3, rows: 1000 })];
  const nocount = rec({ id: 4 });
  delete nocount.layout.stats;
  const b = d.build([...recs, nocount], {});
  assert.deepEqual(b.runs.map((x) => x.id).sort(), ["1", "3", "4"]);
  assert.equal(b.excluded.length, 1);
  assert.equal(b.excluded[0].id, "2");
});

test("the query set is what most runs carry, and a run missing one gets no total", () => {
  const extra = { probe_extra: { cold_ms: 1, warm_ms: 1 } };
  const recs = [rec({ id: 1, extra }), rec({ id: 2, extra }), rec({ id: 3, extra }), rec({ id: 4 })];
  const b = d.build(recs, {});
  assert.ok(b.perDataset.aemo.queriesDl.includes("probe_extra"));
  const four = b.runs.find((x) => x.id === "4");
  assert.equal(four.dl.cold, null);
  assert.equal(b.runs.find((x) => x.id === "1").dl.cold, 10 + 1000 + 1000 + 1);
});

test("runs from before the DirectQuery phase do not veto its queries", () => {
  const recs = [rec({ id: 1 }), rec({ id: 2 }), rec({ id: 3 }), rec({ id: 4, dq: 10 })];
  const b = d.build(recs, {});
  assert.equal(b.perDataset.aemo.queriesDq.length, 3);
  assert.ok(b.runs.find((x) => x.id === "4").dq.warm > 0);
});

test("a column's cost is its probe minus the row-count probe, matched case-insensitively", () => {
  const b = d.build([rec({ id: 1 })], { fct_summary: { mw: "mw", duid: "DUID" } });
  const mw = b.columns.find((c) => c.column === "mw");
  assert.equal(mw.cold, 1000 - 10);
  assert.equal(mw.warm, 100 - 5);
  assert.equal(mw.mb, 50);
  assert.equal(mw.dictFull, true);
  assert.equal(mw.runsPer1k, 500);
  // DUID has no probe timing in this record, so no row rather than a row of nulls.
  assert.equal(b.columns.find((c) => c.column === "DUID"), undefined);
});

test("a dictionary gap is a column whose dictionary pages are fewer than its row groups", () => {
  const enc = { mw: { encodings: ["PLAIN", "PLAIN_DICTIONARY"], mb: 60, dict_pages: 7, chunks: 10 } };
  const b = d.build([rec({ id: 1, enc })], { fct_summary: { mw: "mw" } });
  assert.equal(b.columns[0].dictFull, false);
  assert.equal(b.columns[0].encodings, "PLAIN+PLAIN_DICTIONARY");
});

// --------------------------------------------------------------------------------- probe map

test("the committed probe map is exactly what the DAX suite says", () => {
  assert.deepEqual(PROBE_COLUMNS, d.probeMapFrom(source()));
  const text = readFileSync(new URL("./probe_columns.js", import.meta.url), "utf8").split("\r\n").join("\n");
  assert.equal(text, moduleText(PROBE_COLUMNS));
});

test("the probe map covers every dataset's mart and never maps the row-count control", () => {
  for (const ds of Object.values(d.DATASETS)) assert.ok(PROBE_COLUMNS[ds.mart], ds.mart);
  for (const m of Object.values(PROBE_COLUMNS)) assert.equal(m.rowcount, undefined);
  assert.equal(PROBE_COLUMNS.fct_trips.paytype, "payment_type");
});

// ----------------------------------------------------------------------------------- statistics

test("spearman: monotone is 1, reversed is -1, ties averaged, too few is null", () => {
  assert.equal(d.spearman([1, 2, 3, 4, 5], [10, 20, 30, 40, 50]), 1);
  assert.equal(d.spearman([1, 2, 3, 4, 5], [5, 4, 3, 2, 1]), -1);
  assert.ok(Math.abs(d.spearman([1, 2, 2, 3, 4], [1, 2, 3, 4, 5]) - 0.9747) < 1e-3);
  assert.equal(d.spearman([1, 2, 3, 4], [1, 2, 3, 4]), null);
});

test("strength words never call a weak correlation a relationship", () => {
  assert.equal(d.strength(0.85), "strong");
  assert.equal(d.strength(-0.5), "moderate");
  assert.equal(d.strength(0.3), "weak");
  assert.equal(d.strength(0.1), "none");
});

test("verdict: clear only when every run of the winner beats every run of the rest", () => {
  assert.deepEqual(d.verdict({ a: [1, 2, 3], b: [4, 5, 6] }), { status: "clear", best: "a" });
  assert.equal(d.verdict({ a: [1, 2, 5], b: [4, 5, 6] }).status, "noise");
  assert.equal(d.verdict({ a: [1, 2], b: [4, 5, 6] }).status, "few");
  assert.equal(d.verdict({ a: [1, 2, 3] }).status, "few");
});

test("formatters", () => {
  assert.equal(d.fmtMs(450), "450 ms");
  assert.equal(d.fmtMs(4500), "4.5 s");
  assert.equal(d.fmtMs(45000), "45 s");
  assert.equal(d.fmtMb(0.01), "10 KB");
  assert.equal(d.fmtMb(2048), "2.0 GB");
  assert.equal(d.fmtRows(999888), "1.0M");
  assert.equal(d.fmtRows(16000000), "16M");
});

// ---------------------------------------------------------------------------------- findings

function vorderFixture() {
  const out = [];
  let id = 0;
  for (const ds of ["aemo", "nyc", "bts", "green"]) {
    for (let i = 0; i < 3; i++) {
      out.push(rec({ id: ++id, ds, engine: "spark", cfg: { resource_profile: "readHeavyForPBI" }, cold: 100 + i, warm: 50 + i * 20, mb: 80 }));
      out.push(rec({ id: ++id, ds, engine: "spark", cfg: { resource_profile: "writeHeavy" }, cold: 200 + i, warm: 50 + i * 20 + 5, mb: 100 }));
    }
  }
  return out;
}

// The same fixture with a ledger: V-Order reads at 1,000 CU, without at 2,000.
function withCu(recs) {
  const items = {};
  for (const x of recs) {
    const g = `sm-${x.run.id}`;
    x.items = { [g]: { role: "semantic_model" } };
    items[g] = { Query: x.layout.config.spark.resource_profile === "readHeavyForPBI" ? 1000 + Number(x.run.id) : 2000 + Number(x.run.id) };
  }
  return { recs, ledger: { items } };
}

// ---------------------------------------------------------------------------- layout properties

test("slope per doubling: y = x^0.5 reads ×√2, and a property that never moved is not measurable", () => {
  const xs = [1, 2, 4, 8, 16, 32], ys = xs.map((x) => Math.sqrt(x));
  assert.ok(Math.abs(2 ** r.slopeLog2(xs, ys) - Math.SQRT2) < 1e-9);
  assert.equal(r.slopeLog2([5, 5, 5, 5, 5], [1, 2, 3, 4, 5]), null);
});

test("every dataset weighs the same: one with ten times the runs does not move the global number", () => {
  const g = r.acrossDatasets({ aemo: 0.5, nyc: 0.9, bts: 0.95 });
  assert.equal(g.med, 0.9);
  assert.deepEqual([g.n, g.down, g.up], [3, 3, 0]);
});

test("Spark V-Order card: the read-CU ratio per dataset, its median, and the agreement", () => {
  const { recs, ledger } = withCu(vorderFixture());
  const f = r.propSparkVOrder(r.context(recs, {}, ledger));
  assert.equal(f.status, "clear");
  assert.match(f.head[0], /^−50% CU$/);
  assert.match(f.html, /4 of 4 datasets agree/);
});

test("an effect inside ±10% is reported as no measurable effect, not dropped", () => {
  const recs = [];
  let id = 0;
  for (const ds of ["aemo", "nyc", "bts"]) for (const files of [1, 2, 4, 8, 16]) {
    const x = rec({ id: ++id, ds, cfg: { sorted: "true" } });
    x.layout.stats.duckrun["mart." + d.DATASETS[ds].mart].num_files = files;
    recs.push(x);
  }
  const f = r.propFileCount(r.context(recs));
  assert.equal(f.status, "none");
  assert.match(f.claim || f.html, /no measurable effect/);
});

test("row-group card: a column's cold cost per doubling of rows per row group, within the same column", () => {
  const recs = [];
  let id = 0;
  for (const ds of ["aemo", "nyc", "bts"]) {
    for (const rgs of [8, 16, 32, 64, 128]) {
      // Fewer, bigger row groups (rgs small) load cheaper cold.
      // Probes for each mart's own columns, so every dataset has columns to fit (nyc and bts map
      // probe_fare / probe_distance / probe_depdelay, aemo maps probe_mw / probe_price).
      const extra = Object.fromEntries(["probe_fare", "probe_distance", "probe_depdelay"].map((q) =>
        [q, { cold_ms: 500 + rgs * 20, warm_ms: 400 - rgs, hot_median_ms: 300 }]));
      recs.push(rec({ id: ++id, ds, rgs, rows: 128e6, cfg: { sorted: "true" }, cold: 500 + rgs * 20, warm: 400 - rgs, extra }));
    }
  }
  const f = r.propRowGroups(r.context(recs));
  assert.match(f.head[0], /^−\d+% cold$/);
  assert.equal(f.status, "clear");
});

test("home is one card per layout property; a card opens that property alone; no dataset switcher", () => {
  const recs = vorderFixture();
  const home = r.render(recs, {});
  // Levers and writer switches are cards; the secondary group is a compact list of the rest.
  const compact = r.PROPERTY_GROUPS.filter((g) => g.compact).flatMap((g) => g.ids).length;
  assert.equal((home.html.match(/class="card"/g) || []).length, r.PROPERTIES.length - compact);
  assert.equal((home.html.match(/<ul class="secondary"><li>/g) || []).length, 1);
  assert.match(home.html, /read CU/);
  assert.match(home.html, /dictionary encoding.*sorting.*row groups/s);
  // Dictionary leads the levers.
  assert.ok(home.html.indexOf('href="?finding=dictionary"') < home.html.indexOf('href="?finding=sort"'));
  assert.doesNotMatch(home.html, /dataset=/);
  const one = r.render(recs, { finding: "spark-vorder" });
  assert.match(one.html, /id="spark-vorder"/);
  assert.match(one.html, /href="\?">← All properties/);
  assert.doesNotMatch(one.html, /class="card"/);
  for (const page of ["method", "how", "data"]) assert.match(r.render(recs, { page }).html, new RegExp(`id="${page}"`));
});

test("a session whose model was never billed, or whose suite did not finish, has no read CU", () => {
  const x = rec({ id: 1 });
  x.items = { b: { role: "bench_dl", name: "dbt_delta_dl" } };
  assert.equal(d.readCu(x, { items: { b: { "OneLake Read": 15 } } }).dl, null);
  const y = rec({ id: 2 });
  delete y.benchmark.timings.aemo_duckrun.probe_mw.cold_ms;
  y.items = { m: { role: "semantic_model" } };
  const b = d.build([y, rec({ id: 3 }), rec({ id: 4 })], {}, { items: { m: { Query: 500 } } });
  assert.equal(b.runs.find((r2) => r2.id === "2").cu.dl, null);
});

test("every card carries a headline number and the finding's own sentence", () => {
  const { found, html } = r.render(vorderFixture(), {});
  for (const f of found) {
    assert.ok(Array.isArray(f.head) && f.head.length === 2, f.id);
    assert.ok(html.includes(r.esc(f.head[1])), f.id);
  }
});

test("read CU: a phase's model, shortcut lakehouse and that lakehouse's endpoint, nothing else", () => {
  const x = rec({ id: 1 });
  x.items = { a: { role: "semantic_model", name: "aemo_duckrun" }, b: { role: "bench_dl", name: "dbt_delta_dl" },
    c: { role: "sql_endpoint", name: "dbt_delta_dl" }, e: { role: "sql_endpoint", name: "dbt_delta" },
    f: { role: "semantic_model_dq" }, g: { role: "output", name: "dbt_delta" } };
  const ledger = { items: { a: { "Query": 100 }, b: { "OneLake Read": 10 }, c: { "SQL Endpoint Query": 5 },
    e: { "SQL Endpoint Query": 999 }, g: { "Notebook": 5000 }, f: { "Query": 7 } } };
  assert.deepEqual(d.readCu(x, ledger), { dl: 115, dq: 7 });
  assert.deepEqual(d.readCu(x, null), { dl: null, dq: null });
});

test("no records is a sentence, not an empty page", () => {
  assert.match(r.render([], {}).html, /No run records found/);
});

// -------------------------------------------------------------------------------------- charts

test("scatter drops non-positive values on a log axis and draws one mark per point", () => {
  const svg = r.scatter({ points: [{ x: 1, y: 1, family: "spark" }, { x: 10, y: 10, family: "spark", filled: true },
    { x: 0, y: 5, family: "spark" }], x: { label: "x", log: true }, y: { label: "y", log: true } });
  assert.equal((svg.match(/class="pt"/g) || []).length, 2);
  assert.match(r.scatter({ points: [{ x: 1, y: 1 }], x: { label: "x" }, y: { label: "y" } }), /Not enough measured points/);
});

test("strips draw one dot per run and one median tick per row", () => {
  const svg = r.strips({ rows: [{ label: "a", values: [1, 2, 3], family: "spark" }, { label: "b", values: [4, 5], family: "spark" }],
    x: { label: "t" } });
  assert.equal((svg.match(/class="pt"/g) || []).length, 5);
  assert.equal((svg.match(/class="median"/g) || []).length, 2);
});

test("log ticks fill a narrow decade instead of leaving only its ends", () => {
  assert.ok(r.logTicks(57000, 157000).length >= 3);
  assert.deepEqual(r.logTicks(1, 1000), [1, 10, 100, 1000]);
});

test("cards speak plainly: no correlation coefficients on the home page", () => {
  const { html } = r.render(vorderFixture(), {});
  assert.doesNotMatch(html, /ρ/);
});

test("dictionary: with the footer declaration recorded, remap and hashing are measured apart", () => {
  const recs = [];
  for (let i = 0; i < 8; i++) {
    const remap = i % 2 === 0;
    const enc = {
      mw: { encodings: ["PLAIN", "RLE_DICTIONARY"], mb: 50, dict_pages: 10, chunks: 10, declared: 10, dict_declared: remap ? 10 : 6 },
      price: { encodings: ["PLAIN", "RLE_DICTIONARY"], mb: 25, dict_pages: 10, chunks: 10, declared: 10, dict_declared: remap ? 10 : 0 },
    };
    recs.push(rec({ id: i + 1, enc, cold: remap ? 400 : 1600, cfg: { sorted: "true" } }));
  }
  const f = r.findDictionary(r.context(recs));
  assert.equal(f.status, "clear");
  assert.match(f.html, /Measured: remap against hashing/);
  assert.match(f.html, /slower, over 8 and 8 column-runs/);
});

test("dictionary: without the declaration it says so and is only partly measured", () => {
  const f = r.findDictionary(r.context([rec({ id: 1 })]));
  assert.equal(f.status, "partial");
  assert.match(f.short, /do not yet capture the footer declaration/);
});

test("row-group band: octaves pooled across datasets, both tiers within 10%, stated as a range", () => {
  // Cold slow at small groups, warm slow at big ones, both fine from 2M to 8M; 32M is thin.
  const pts = [];
  const shape = { 1: [1.3, 0.9], 2: [1.02, 0.95], 4: [0.98, 1.0], 8: [1.0, 1.05], 16: [1.0, 1.3], 32: [1.0, 1.0] };
  for (const [m, [cold, warm]] of Object.entries(shape)) {
    const n = m === "32" ? 4 : 12;
    for (let i = 0; i < n; i++) pts.push({ ds: i % 2 ? "aemo" : "nyc", x: Number(m) * 1e6, cold, warm, run: {} });
  }
  const t = r.bandTrend(pts);
  assert.equal(r.bandText(t), "2M to 8M");
  assert.ok(t.rows.find((x) => x.x === 32e6).thin);
  assert.match(r.bandPanels(pts, t), /2M to 8M/);
});

test("csv quotes what needs quoting and reads nested keys", () => {
  const text = r.csvOf([{ a: "x,y", b: { c: 2 } }], ["a", "b.c"]);
  assert.equal(text, 'a,b.c\n"x,y",2');
});

// --------------------------------------------------------------------------------------- build

test("the build inlines every module and leaves no relative import behind", async () => {
  const { execFileSync } = await import("node:child_process");
  const { mkdtempSync } = await import("node:fs");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");
  const out = join(mkdtempSync(join(tmpdir(), "dash-")), "index.html");
  execFileSync(process.execPath, [new URL("./build.mjs", import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, "$1"), "--out", out]);
  const html = readFileSync(out, "utf8");
  assert.doesNotMatch(html, /^import .* from "\.\//m);
  assert.match(html, /<script id="snapshot" type="application\/json"><\/script>/);
  assert.match(html, /export function propSparkVOrder/);
});

// --------------------------------------------------------------------------------- performance tab

test("performance tab: read CU only, writer x dataset, 1.0x is the cheapest writer on that dataset", () => {
  const { recs, ledger } = withCu(vorderFixture());
  for (const x of recs) {
    const t = x.benchmark.timings, k = Object.keys(t)[0];
    t[`${k}_dq`] = Object.fromEntries(Object.entries(t[k]).map(([q, v]) => [q, { cold_ms: v.cold_ms * 7, warm_ms: v.warm_ms * 7, hot_median_ms: 1 }]));
  }
  const page = r.render(recs, { page: "perf" }, ledger).html;
  assert.match(page, /id="perf"/);
  assert.equal((page.match(/<table class="grid">/g) || []).length, 1);  // read CU only
  assert.doesNotMatch(page, /Warm|Cold/);
  for (const ds of ["AEMO", "NYC taxi", "BTS flights", "Green taxi"]) assert.match(page, new RegExp(`<th>${ds}</th>`));
  // The ledger bills V-Order ~1,000 CU against ~2,000 without, on every dataset: named in plain words.
  assert.match(page, /Cheapest to read: <b>Spark, V-Order<\/b> on 4 of 4 datasets/);
  assert.equal((page.match(/class="best"/g) || []).length, 4);
  assert.match(page, /<th scope="row">Spark, no V-Order<\/th><td class="[^"]*"[^>]*><b>2\.0×/);
  const ctx = r.context(recs, {}, ledger);
  // DirectQuery reads the dq totals and never the Direct Lake ones.
  // The DirectQuery totals are the dq timings, never the Direct Lake ones.
  const run = ctx.runs[0];
  assert.equal(run.dq.cold, run.dl.cold * 7);
  assert.match(r.render(recs, { page: "perf", phase: "dq" }, ledger).html, /class="pill on"[^>]*>DirectQuery/);
});

test("performance tab: unsorted delta-rs, a warehouse with V-Order turned off and readHeavyForSpark are left off", () => {
  const recs = [rec({ id: 1, cfg: { sorted: "true" } }), rec({ id: 2 }),
    rec({ id: 3, engine: "dwh" }), rec({ id: 4, engine: "dwh", ord: { vorder_enabled: false } }),
    rec({ id: 5, engine: "spark", cfg: { resource_profile: "readHeavyForSpark" } })];
  recs.forEach((x, i) => { x.items = { [`sm-${i}`]: { role: "semantic_model" } }; });
  const ledger = { items: Object.fromEntries(recs.map((x, i) => [`sm-${i}`, { Query: 100 }])) };
  const groups = r.perfGroups(r.context(recs, {}, ledger), "dl");
  assert.deepEqual(groups.map((g) => g.key), ["deltars_sorted", "dwh"]);
});

test("performance tab: a cell is the median of the writer's LAST two runs, newest by start time", () => {
  // Five runs of one writer; CU 100..500, oldest first. The last two are 400/500 -> median 450.
  const recs = [1, 2, 3, 4, 5].map((i) => {
    const x = rec({ id: i, cfg: { sorted: "true" } });
    x.run.started = `2026-10-0${i}T00:00:00Z`;
    x.items = { [`sm-${i}`]: { role: "semantic_model" } };
    return x;
  });
  const ledger = { items: Object.fromEntries(recs.map((x, i) => [`sm-${i + 1}`, { Query: (i + 1) * 100 }])) };
  const [g] = r.perfGroups(r.context(recs, {}, ledger), "dl");
  assert.equal(g.cu, 450);
  assert.equal(g.runs, 2);
  assert.equal(g.total, 5);
  assert.deepEqual(g.ids, ["5", "4"]);
});

test("performance tab: the DirectQuery grid counts only sessions since PERF_SINCE, Direct Lake all of them", () => {
  const mk = (id, engine, started) => {
    const x = rec({ id, engine, cfg: engine === "duckrun" ? { sorted: "true" } : {}, dq: 7 });
    x.run.started = started;
    x.items = { [`sm-${id}`]: { role: "semantic_model" }, [`dq-${id}`]: { role: "semantic_model_dq" } };
    return x;
  };
  // duckrun has an October DQ session, dwh only a September one.
  const recs = [mk(1, "duckrun", "2026-10-05T00:00:00Z"), mk(2, "dwh", "2026-09-05T00:00:00Z")];
  const ledger = { items: Object.fromEntries(recs.flatMap((x) =>
    [[`sm-${x.run.id}`, { Query: 100 }], [`dq-${x.run.id}`, { Query: 1000 }]])) };
  const ctx = r.context(recs, {}, ledger);
  assert.deepEqual(r.perfGroups(ctx, "dq").map((g) => g.key), ["deltars_sorted"]);
  assert.deepEqual(r.perfGroups(ctx, "dl").map((g) => g.key), ["deltars_sorted", "dwh"]);
  // The history keeps the old session: it is where the price step shows.
  assert.equal(r.perfHistoryPoints(ctx, "dq")[0].lines.length, 2);
});

test("performance tab: history averages a writer's sessions per day", () => {
  const recs = [1, 2, 3].map((i) => {
    const x = rec({ id: i, cfg: { sorted: "true" } });
    x.run.started = i < 3 ? `2026-10-01T0${i}:00:00Z` : "2026-10-02T00:00:00Z";
    x.items = { [`sm-${i}`]: { role: "semantic_model" } };
    return x;
  });
  const ledger = { items: Object.fromEntries(recs.map((x) => [`sm-${x.run.id}`, { Query: Number(x.run.id) * 100 }])) };
  const ctx = r.context(recs, {}, ledger);
  const [p] = r.perfHistoryPoints(ctx, "dl");
  assert.deepEqual(p.lines[0].pts.map((q) => [q.day, q.cu, q.n]), [["2026-10-01", 150, 2], ["2026-10-02", 300, 1]]);
  const html = r.perfHistory(ctx, "dl");
  assert.equal((html.match(/<path d="M/g) || []).length, 1);
  assert.match(html, /2026-10-01 · avg 150 CU over 2 sessions/);
});

test("performance tab: history has one panel per dataset and every day as a point, oldest first", () => {
  const recs = [3, 1, 2].map((i) => {
    const x = rec({ id: i, cfg: { sorted: "true" } });
    x.run.started = `2026-10-0${i}T00:00:00Z`;
    x.items = { [`sm-${i}`]: { role: "semantic_model" } };
    return x;
  });
  const ledger = { items: Object.fromEntries(recs.map((x) => [`sm-${x.run.id}`, { Query: Number(x.run.id) * 100 }])) };
  const ctx = r.context(recs, {}, ledger);
  const [p] = r.perfHistoryPoints(ctx, "dl");
  assert.equal(p.ds, "aemo");
  assert.deepEqual(p.lines[0].pts.map((q) => q.cu), [100, 200, 300]);
  const html = r.perfHistory(ctx, "dl");
  assert.equal((html.match(/class="facet"/g) || []).length, 1);
  assert.equal((html.match(/data-tip="delta-rs, autosort · AEMO · 2026-10-0\d · avg/g) || []).length, 3);
  // The CU axis fits the panel's range (100–300), so it does not start at zero.
  assert.ok(!/text-anchor="end">0</.test(html));
  assert.ok(/text-anchor="end">100</.test(html));
  // A record with no run.started falls back to the time in its file name.
  assert.equal(r.runTime("2026-10-17T1526Z-39042898921.json"), Date.parse("2026-10-17T15:26:00Z"));
});

test("performance tab: no timed runs is a sentence, not an empty grid", () => {
  const recs = vorderFixture();
  for (const x of recs) x.benchmark.timings = {};
  assert.match(r.render(recs, { page: "perf" }).html, /No timed runs yet/);
});
