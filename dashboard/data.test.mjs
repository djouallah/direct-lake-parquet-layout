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
  assert.equal(r.linkWord(0.3), "only a weak link");
  assert.equal(r.linkWord(null), "too few points to judge a link");
});

test("verdict: clear only when every run of the winner beats every run of the rest", () => {
  assert.deepEqual(d.verdict({ a: [1, 2, 3], b: [4, 5, 6] }), { status: "clear", best: "a" });
  assert.equal(d.verdict({ a: [1, 2, 5], b: [4, 5, 6] }).status, "noise");
  assert.equal(d.verdict({ a: [1, 2], b: [4, 5, 6] }).status, "few");
  assert.equal(d.verdict({ a: [1, 2, 3] }).status, "few");
});

test("the sign note is only said when it means something", () => {
  assert.match(r.signNote(5, 5), /1 time in 16/);
  assert.equal(r.signNote(4, 5), "");
  assert.equal(r.signNote(3, 3), "");
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

test("V-Order: a separated cold and an overlapping warm are reported as exactly that", () => {
  const ctx = r.context(vorderFixture());
  const f = r.findVOrder(ctx);
  assert.equal(f.status, "clear");
  assert.match(f.short, /cold was faster on 4 of 4 datasets \(4 clearly\)/);
  assert.match(f.short, /table smaller on 4/);
  assert.match(f.html, /1 time in 8/);
});

test("a finding the data cannot support says so instead of vanishing", () => {
  const ctx = r.context([rec({ id: 1 })]);
  const f = r.findVOrder(ctx);
  assert.equal(f.status, "few");
  assert.match(f.short, /No dataset has Spark runs with and without V-Order yet/);
  assert.equal(r.findRowGroups(ctx).status, "few");
});

test("row groups are judged with the writer held fixed", () => {
  const recs = [];
  let id = 0;
  for (const [setting, rgs] of [["2000000", 50], ["6000000", 20], ["16000000", 8]]) {
    for (let i = 0; i < 3; i++) recs.push(rec({ id: ++id, rgs, cfg: { sorted: "true", row_group_size: setting }, cold: 1000 + i * 300, warm: 100 + i }));
  }
  // A spark run at a wildly different row-group count must not enter the delta-rs comparison.
  recs.push(rec({ id: 99, engine: "spark", cfg: { resource_profile: "writeHeavy" }, rgs: 500, cold: 99999 }));
  const f = r.findRowGroups(r.context(recs));
  assert.equal(f.status, "noise");
  assert.match(f.html, /written at 3 different row-group counts/);
  assert.doesNotMatch(f.html.split("Across every writer")[0], /99,999|100 s/);
});

test("the page renders every section and the dataset filter narrows it", () => {
  const recs = vorderFixture();
  const all = r.render(recs, {});
  for (const id of ["short", "method", "how", "cold-warm", "column-size", "vorder", "directquery", "takeaway", "data"]) {
    assert.match(all.html, new RegExp(`id="${id}"`), id);
  }
  const one = r.render(recs, { dataset: "nyc" });
  assert.deepEqual(one.ctx.datasets, ["nyc"]);
  assert.doesNotMatch(one.html.split('id="data"')[1], /AEMO/);
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
  assert.match(html, /export function findVOrder/);
});
