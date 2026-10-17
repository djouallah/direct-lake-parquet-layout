// The report: what in a parquet file decides how fast Power BI reads it. Every sentence that states a
// number is generated from `history/runs/*.json` at view time, so the page rewrites itself after
// every Benchmark run and can never quote a number the records no longer support.
//
// The home page is one card per finding — a headline number, the claim, a status — and a card opens
// that finding's evidence (`?finding=<id>`). Method, how Power BI reads parquet and the raw data are
// pages of their own (`?page=`). Speed AND read CU: what the layout costs to query is half the answer.
import { DATASETS, MIN_RUNS, build, median, spearman, strength, verdict, fmtInt, fmtMs, fmtMb, fmtX, fmtRows, fmtCu, loadRemote, DEFAULTS } from "./data.js";
import { PROBE_COLUMNS } from "./probe_columns.js";

// --------------------------------------------------------------------------------------- marks
//
// COLOUR IS THE WRITER FAMILY, SHAPE FILL IS WHETHER IT REORDERED THE ROWS. A scatter puts every
// colour beside every other, so the palette is validated all-pairs, and that caps it at three hues;
// the fourth family (Iceberg, a handful of runs) is the neutral grey with its own square marker.
// Filled = V-Order or a global sort, hollow = neither — the one physical property every chart marks
// the same way, so a reader learns it once.

export const FAMILIES = {
  "delta-rs": { label: "delta-rs (duckrun)", css: "var(--c1)", shape: "circle" },
  spark: { label: "Spark", css: "var(--c2)", shape: "circle" },
  warehouse: { label: "Fabric Warehouse", css: "var(--c3)", shape: "circle" },
  "duckdb-iceberg": { label: "DuckDB → Iceberg", css: "var(--c4)", shape: "square" },
};
const fam = (f) => FAMILIES[f] || { label: f, css: "var(--c4)", shape: "square" };

export const esc = (s) => String(s ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;")
  .replace(/>/g, "&gt;").replace(/"/g, "&quot;");

function mark(x, y, p, r = 4.5) {
  const f = fam(p.family);
  const fill = p.filled ? f.css : "var(--bg)";
  const stroke = p.filled ? "var(--bg)" : f.css;
  const sw = p.filled ? 1.5 : 2;
  const shape = p.shape || f.shape;
  const tip = p.tip ? ` data-tip="${esc(p.tip)}"` : "";
  let body;
  if (shape === "square") {
    body = `<rect x="${(x - r).toFixed(1)}" y="${(y - r).toFixed(1)}" width="${2 * r}" height="${2 * r}" rx="1" fill="${fill}" stroke="${stroke}" stroke-width="${sw}"/>`;
  } else if (shape === "triangle") {
    const h = r * 1.25;
    body = `<path d="M${x.toFixed(1)},${(y - h).toFixed(1)}L${(x + h).toFixed(1)},${(y + h * 0.8).toFixed(1)}L${(x - h).toFixed(1)},${(y + h * 0.8).toFixed(1)}Z" fill="${fill}" stroke="${stroke}" stroke-width="${sw}" stroke-linejoin="round"/>`;
  } else {
    body = `<circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="${r}" fill="${fill}" stroke="${stroke}" stroke-width="${sw}"/>`;
  }
  // The hit target is bigger than the mark, so a tooltip does not demand pixel aim.
  return `<g class="pt"${tip}>${body}<circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="10" fill="transparent"/></g>`;
}

// --------------------------------------------------------------------------------------- scales

export function logTicks(lo, hi) {
  const out = [];
  for (let e = Math.floor(Math.log10(lo)); e <= Math.ceil(Math.log10(hi)); e++) {
    for (const m of [1, 2, 5]) {
      const v = m * 10 ** e;
      if (v >= lo && v <= hi) out.push(v);
    }
  }
  if (out.length > 7) return out.filter((v) => Math.abs(Math.log10(v) % 1) < 1e-9);
  if (out.length >= 3) return out;
  // A narrow decade (57 s to 157 s) holds one or two of 1-2-5; fill it in finer steps.
  const fine = [];
  for (let e = Math.floor(Math.log10(lo)); e <= Math.ceil(Math.log10(hi)); e++) {
    for (const m of [1, 1.5, 2, 3, 4, 5, 6, 8]) {
      const v = m * 10 ** e;
      if (v >= lo && v <= hi) fine.push(v);
    }
  }
  return fine.length >= 2 ? fine : [lo, hi];
}

export function linTicks(lo, hi, n = 5) {
  const span = hi - lo || Math.abs(hi) || 1;
  const step0 = span / n, mag = 10 ** Math.floor(Math.log10(step0));
  const step = [1, 2, 5, 10].map((m) => m * mag).find((s) => s >= step0) || mag * 10;
  const out = [];
  for (let v = Math.ceil(lo / step) * step; v <= hi + 1e-9; v += step) out.push(+v.toFixed(10));
  return out;
}

function scale(values, log, lo, hi) {
  let a = Math.min(...values), b = Math.max(...values);
  if (log) {
    a = a / 1.25; b = b * 1.25;
    const la = Math.log10(a), lb = Math.log10(b);
    return { f: (v) => lo + ((Math.log10(v) - la) / (lb - la || 1)) * (hi - lo), ticks: logTicks(a, b), a, b };
  }
  const pad = (b - a) * 0.08 || Math.abs(a) * 0.1 || 1;
  a = Math.min(0, a - pad) === 0 && a >= 0 ? 0 : a - pad; b = b + pad;
  return { f: (v) => lo + ((v - a) / (b - a || 1)) * (hi - lo), ticks: linTicks(a, b), a, b };
}

// --------------------------------------------------------------------------------------- charts

/**
 * A scatter as an SVG string. `points` are `{x, y, family, filled, shape?, tip}`; non-positive values
 * are dropped on a log axis (the caller says how many in its note). `guides` are `y = k·x` lines,
 * which only mean something when both axes share a unit.
 */
export function scatter({ points, x, y, w = 520, h = 320, guides = [], trend = false }) {
  const pts = points.filter((p) => Number.isFinite(p.x) && Number.isFinite(p.y)
    && (!x.log || p.x > 0) && (!y.log || p.y > 0));
  if (pts.length < 2) return `<p class="empty">Not enough measured points yet.</p>`;
  const m = { l: 62, r: 14, t: 10, b: 42 };
  const sx = scale(pts.map((p) => p.x), x.log, m.l, w - m.r);
  const sy = scale(pts.map((p) => p.y), y.log, h - m.b, m.t);
  const fx = x.fmt || String, fy = y.fmt || String;
  const parts = [`<svg viewBox="0 0 ${w} ${h}" class="chart" role="img" aria-label="${esc(`${y.label} against ${x.label}`)}">`];
  for (const t of sy.ticks) {
    const yy = sy.f(t);
    parts.push(`<line x1="${m.l}" x2="${w - m.r}" y1="${yy.toFixed(1)}" y2="${yy.toFixed(1)}" class="grid"/>`,
      `<text x="${m.l - 6}" y="${(yy + 3.5).toFixed(1)}" class="tick" text-anchor="end">${esc(fy(t))}</text>`);
  }
  for (const t of sx.ticks) {
    const xx = sx.f(t);
    parts.push(`<line x1="${xx.toFixed(1)}" x2="${xx.toFixed(1)}" y1="${m.t}" y2="${h - m.b}" class="grid"/>`,
      `<text x="${xx.toFixed(1)}" y="${h - m.b + 15}" class="tick" text-anchor="middle">${esc(fx(t))}</text>`);
  }
  for (const k of guides) {
    // Clip the line y = k·x to the plot box on a log-log plot, where it is straight.
    const x0 = Math.max(sx.a, sy.a / k), x1 = Math.min(sx.b, sy.b / k);
    if (x1 <= x0) continue;
    const X0 = sx.f(x0), Y0 = sy.f(x0 * k), X1 = sx.f(x1), Y1 = sy.f(x1 * k);
    parts.push(`<line x1="${X0.toFixed(1)}" y1="${Y0.toFixed(1)}" x2="${X1.toFixed(1)}" y2="${Y1.toFixed(1)}" class="guide"/>`,
      `<text x="${(X1 - 4).toFixed(1)}" y="${(Y1 + 12).toFixed(1)}" class="guide-label" text-anchor="end">${k === 1 ? "equal" : `${k}×`}</text>`);
  }
  parts.push(`<text x="${(m.l + w - m.r) / 2}" y="${h - 6}" class="axis" text-anchor="middle">${esc(x.label)}</text>`,
    `<text transform="translate(13,${(m.t + h - m.b) / 2}) rotate(-90)" class="axis" text-anchor="middle">${esc(y.label)}</text>`);
  // A median line through the cloud: points binned by x (octave bins on a log axis), the median
  // y of each bin with at least two points. It is what makes a noisy trend readable at a glance.
  if (trend) {
    const bins = new Map();
    for (const p of pts) {
      const k = x.log ? Math.round(Math.log2(p.x)) : Math.round(p.x);
      (bins.get(k) || bins.set(k, []).get(k)).push(p);
    }
    const line = [...bins.values()].filter((b) => b.length >= 2)
      .map((b) => [median(b.map((p) => p.x)), median(b.map((p) => p.y))]).sort((a, b) => a[0] - b[0]);
    if (line.length >= 2) {
      parts.push(`<polyline points="${line.map(([a, b]) => `${sx.f(a).toFixed(1)},${sy.f(b).toFixed(1)}`).join(" ")}" class="trend"/>`);
    }
  }
  // Hollow marks first, so a filled mark sitting on one is never hidden behind it.
  const ordered = [...pts].sort((a, b) => Number(!!a.filled) - Number(!!b.filled));
  for (const p of ordered) parts.push(mark(sx.f(p.x), sy.f(p.y), p));
  parts.push("</svg>");
  return parts.join("");
}

/**
 * Horizontal strips: one row per group, every run a dot, a line from the fastest run to the slowest
 * and a tick at the median. The chart for "did these two groups separate, or overlap?".
 */
export function strips({ rows, x, w = 760, rowH = 24 }) {
  const live = rows.filter((r) => r.values.some((v) => v > 0));
  if (!live.length) return `<p class="empty">Not enough measured points yet.</p>`;
  const m = { l: 168, r: 16, t: 8, b: 40 };
  const h = m.t + m.b + live.length * rowH;
  const all = live.flatMap((r) => r.values.filter((v) => v > 0));
  const sx = scale(all, x.log !== false, m.l, w - m.r);
  const fx = x.fmt || String;
  const parts = [`<svg viewBox="0 0 ${w} ${h}" class="chart strip" role="img" aria-label="${esc(x.label)}">`];
  for (const t of sx.ticks) {
    const xx = sx.f(t);
    parts.push(`<line x1="${xx.toFixed(1)}" x2="${xx.toFixed(1)}" y1="${m.t}" y2="${h - m.b}" class="grid"/>`,
      `<text x="${xx.toFixed(1)}" y="${h - m.b + 15}" class="tick" text-anchor="middle">${esc(fx(t))}</text>`);
  }
  live.forEach((r, i) => {
    const yy = m.t + rowH * (i + 0.5);
    const v = r.values.filter((n) => n > 0);
    const lo = Math.min(...v), hi = Math.max(...v), md = median(v);
    if (r.gap) parts.push(`<line x1="${m.l - 160}" x2="${w - m.r}" y1="${(yy - rowH / 2).toFixed(1)}" y2="${(yy - rowH / 2).toFixed(1)}" class="sep"/>`);
    parts.push(`<text x="${m.l - 10}" y="${(yy + 4).toFixed(1)}" class="row-label" text-anchor="end">${esc(r.label)}</text>`,
      `<line x1="${sx.f(lo).toFixed(1)}" x2="${sx.f(hi).toFixed(1)}" y1="${yy.toFixed(1)}" y2="${yy.toFixed(1)}" class="range" style="stroke:${fam(r.family).css}"/>`);
    for (const n of v) {
      parts.push(mark(sx.f(n), yy, { family: r.family, filled: r.filled,
        tip: `${r.label}: ${fmtMsOr(n, x)} · median ${fmtMsOr(md, x)} · ${v.length} run${v.length === 1 ? "" : "s"}` }, 3.6));
    }
    parts.push(`<line x1="${sx.f(md).toFixed(1)}" x2="${sx.f(md).toFixed(1)}" y1="${(yy - 9).toFixed(1)}" y2="${(yy + 9).toFixed(1)}" class="median"/>`);
  });
  parts.push(`<text x="${(m.l + w - m.r) / 2}" y="${h - 6}" class="axis" text-anchor="middle">${esc(x.label)}</text></svg>`);
  return parts.join("");
}
const fmtMsOr = (v, x) => (x.fmt ? x.fmt(v) : String(v));

/**
 * Runs of one dataset binned by row-group count (octave bins, at least two runs each), with the
 * median cold, warm and Direct Lake read CU per bin. Every engine is pooled: Power BI sees the file,
 * not who wrote it (finding "Power BI never sees the writer").
 */
export function rowGroupBins(runs) {
  const bins = new Map();
  for (const r of runs) {
    if (!r.rgs || !r.dl.cold || !r.dl.warm) continue;
    const k = Math.round(Math.log2(r.rgs));
    (bins.get(k) || bins.set(k, []).get(k)).push(r);
  }
  const rows = [...bins].filter(([, v]) => v.length >= 2).sort((a, b) => a[0] - b[0]).map(([, v]) => ({
    n: v.length, rgs: median(v.map((r) => r.rgs)), lo: Math.min(...v.map((r) => r.rgs)), hi: Math.max(...v.map((r) => r.rgs)),
    rowsPerRg: median(v.map((r) => r.rowsPerRg)), cold: median(v.map((r) => r.dl.cold)), warm: median(v.map((r) => r.dl.warm)),
    cu: median(v.map((r) => r.cu.dl).filter(Boolean)) }));
  if (rows.length < 2) return null;
  const best = (k) => Math.min(...rows.map((x) => x[k]).filter((v) => v > 0));
  const bc = best("cold"), bw = best("warm"), bu = best("cu");
  for (const x of rows) {
    x.relCold = x.cold / bc; x.relWarm = x.warm / bw; x.relCu = x.cu > 0 ? x.cu / bu : null;
    x.worst = Math.max(x.relCold, x.relWarm);
  }
  const sweet = rows.reduce((a, b) => (b.worst < a.worst ? b : a));
  return { rows, sweet };
}

// ---------------------------------------------------------------- every dataset on one axis
//
// Row-group COUNT is not comparable across tables of different sizes; ROWS PER ROW GROUP is. So the
// cross-dataset chart bins every run by rows per row group (octaves: 0.5M, 1M, 2M, 4M, 8M, 16M) and
// draws each dataset as one line, as a multiple of that dataset's own best bin. Where the lines all
// sit near 1.0x at once is the interval the datasets converge on.

// ------------------------------------------------------------- the row-group band, every dataset
//
// A row-group size that works both cold and warm, from the data alone. Every run is placed at its
// own average rows per row group and its cold and warm are divided by its dataset's median run, so
// datasets share one axis. Runs are grouped by OCTAVE of rows per row group (1M, 2M, 4M, 8M, 16M);
// an octave counts when it holds at least `BAND_MIN_RUNS` runs from at least two datasets. The band
// is the longest run of consecutive counting octaves where cold AND warm are both within
// `BAND_WITHIN` of the median. It is stated as a range, never as one number.

export const BAND_WITHIN = 1.10;
export const BAND_MIN_RUNS = 10;
const DS_ORDER = ["aemo", "nyc", "bts", "green", "cms", "tpcds"];
const octave = (x) => Math.round(Math.log2(x / 1e6));
const octaveRows = (k) => 2 ** k * 1e6;

export function bandPoints(ctx) {
  const pts = [];
  for (const ds of dsList(ctx)) {
    const R = ctx.runs.filter((r) => r.ds === ds && r.rowsPerRg && r.dl.cold && r.dl.warm);
    if (R.length < 5) continue;
    const mc = median(R.map((r) => r.dl.cold)), mw = median(R.map((r) => r.dl.warm));
    for (const r of R) pts.push({ ds, run: r, x: r.rowsPerRg, cold: r.dl.cold / mc, warm: r.dl.warm / mw });
  }
  return pts;
}

function octaves(pts) {
  const bins = new Map();
  for (const p of pts) (bins.get(octave(p.x)) || bins.set(octave(p.x), []).get(octave(p.x))).push(p);
  return [...bins].sort((a, b) => a[0] - b[0]).map(([k, v]) => ({ k, x: octaveRows(k), n: v.length,
    datasets: new Set(v.map((p) => p.ds)).size, cold: median(v.map((p) => p.cold)), warm: median(v.map((p) => p.warm)) }));
}

export function bandTrend(pts) {
  const rows = octaves(pts).filter((x) => x.datasets >= 2);
  if (rows.length < 2) return null;
  for (const x of rows) x.thin = x.n < BAND_MIN_RUNS;
  let best = [], cur = [];
  for (const x of rows.filter((r) => !r.thin)) {
    const ok = Math.max(x.cold, x.warm) <= BAND_WITHIN && (!cur.length || x.k === cur[cur.length - 1].k + 1);
    if (Math.max(x.cold, x.warm) <= BAND_WITHIN) cur = ok ? [...cur, x] : [x];
    else cur = [];
    if (cur.length > best.length) best = [...cur];
  }
  const band = best.length ? { from: best[0], to: best[best.length - 1] } : null;
  return { rows, band };
}

const octaveText = (x) => fmtRows(x).replace(/\.0(?=[KMB])/, "");
export const bandText = (t) => (t && t.band ? `${octaveText(t.band.from.x)} to ${octaveText(t.band.to.x)}` : null);

/** Per dataset: its own octaves (at least two runs each), cold and warm against its median run. */
export function datasetLines(pts) {
  const out = {};
  for (const ds of DS_ORDER) {
    const rows = octaves(pts.filter((p) => p.ds === ds)).filter((x) => x.n >= 2);
    if (rows.length >= 2) out[ds] = rows;
  }
  return out;
}

/** One panel: every dataset's own line for one tier, all datasets together as a bold line, the band. */
export function bandPanelSvg(lines, t, tier, xr, { w = 470, h = 360 } = {}) {
  const m = { l: 50, r: 78, t: 30, b: 50 };
  const [lo, hi] = xr;
  const fx = (v) => m.l + ((Math.log2(v) - Math.log2(lo)) / (Math.log2(hi) - Math.log2(lo))) * (w - m.l - m.r);
  const bot = 0.6, top = 1.8;
  const fy = (v) => h - m.b - ((Math.log(Math.min(Math.max(v, bot), top)) - Math.log(bot)) / (Math.log(top) - Math.log(bot))) * (h - m.b - m.t);
  const parts = [`<svg viewBox="0 0 ${w} ${h}" class="chart" role="img" aria-label="${tier} against rows per row group, one line per dataset">`,
    `<text x="${m.l}" y="16" class="panel-title">${tier}</text>`];
  if (t.band) {
    // The band covers its octaves whole: from half an octave below the first to half above the last.
    const x0 = fx(t.band.from.x / Math.SQRT2), x1 = fx(t.band.to.x * Math.SQRT2);
    parts.push(`<rect x="${x0.toFixed(1)}" y="${m.t}" width="${(x1 - x0).toFixed(1)}" height="${h - m.b - m.t}" class="sweet"/>`,
      `<text x="${((x0 + x1) / 2).toFixed(1)}" y="${m.t + 14}" class="sweet-label big" text-anchor="middle">${esc(bandText(t))}</text>`);
  }
  for (const v of [0.75, 1, 1.25, 1.5]) {
    parts.push(`<line x1="${m.l}" x2="${w - m.r}" y1="${fy(v).toFixed(1)}" y2="${fy(v).toFixed(1)}" class="grid${v === 1 ? " one" : ""}"/>`,
      `<text x="${m.l - 7}" y="${(fy(v) + 4).toFixed(1)}" class="tick" text-anchor="end">${v}×</text>`);
  }
  for (let k = Math.ceil(Math.log2(lo / 1e6)); octaveRows(k) <= hi; k++) {
    parts.push(`<line x1="${fx(octaveRows(k)).toFixed(1)}" x2="${fx(octaveRows(k)).toFixed(1)}" y1="${m.t}" y2="${h - m.b}" class="grid"/>`,
      `<text x="${fx(octaveRows(k)).toFixed(1)}" y="${h - m.b + 16}" class="tick" text-anchor="middle">${fmtRows(octaveRows(k))}</text>`);
  }
  parts.push(`<text x="${(m.l + w - m.r) / 2}" y="${h - 10}" class="axis" text-anchor="middle">rows per row group (fewer, bigger groups →)</text>`);
  const labels = [];
  for (const ds of DS_ORDER) {
    const rows = lines[ds];
    if (!rows) continue;
    const css = `var(--d${DS_ORDER.indexOf(ds) + 1})`;
    parts.push(`<polyline points="${rows.map((x) => `${fx(x.x).toFixed(1)},${fy(x[tier]).toFixed(1)}`).join(" ")}" class="tline thin" style="stroke:${css}"/>`);
    for (const x of rows) {
      parts.push(`<g class="pt" data-tip="${esc(`${label(ds)} · ~${fmtRows(x.x)} rows per group · ${tier} ${fmtX(x[tier])} its median · ${x.n} runs`)}"><circle cx="${fx(x.x).toFixed(1)}" cy="${fy(x[tier]).toFixed(1)}" r="3.4" fill="${css}" stroke="var(--bg)" stroke-width="1.2"/><circle cx="${fx(x.x).toFixed(1)}" cy="${fy(x[tier]).toFixed(1)}" r="9" fill="transparent"/></g>`);
    }
    const last = rows[rows.length - 1];
    labels.push({ x: fx(last.x) + 8, y: fy(last[tier]) + 4, name: label(ds), cls: "small" });
  }
  // Every dataset together: the median of all runs per octave, the line the band is computed from.
  parts.push(`<polyline points="${t.rows.map((x) => `${fx(x.x).toFixed(1)},${fy(x[tier]).toFixed(1)}`).join(" ")}" class="tline all"/>`);
  for (const x of t.rows) {
    parts.push(`<g class="pt" data-tip="${esc(`all datasets · ~${fmtRows(x.x)} rows per group · ${tier} ${fmtX(x[tier])} the median · ${x.n} runs from ${x.datasets} datasets${x.thin ? " · too few runs to count toward the band" : ""}`)}"><circle cx="${fx(x.x).toFixed(1)}" cy="${fy(x[tier]).toFixed(1)}" r="5" fill="${x.thin ? "var(--bg)" : "var(--fg)"}" stroke="var(--fg)" stroke-width="2"/><circle cx="${fx(x.x).toFixed(1)}" cy="${fy(x[tier]).toFixed(1)}" r="11" fill="transparent"/></g>`);
  }
  const last = t.rows[t.rows.length - 1];
  labels.push({ x: fx(last.x) + 8, y: fy(last[tier]) + 4, name: "all", cls: "strong" });
  labels.sort((a, b) => a.y - b.y);
  for (let i = 1; i < labels.length; i++) labels[i].y = Math.max(labels[i].y, labels[i - 1].y + 12);
  for (const l of labels) parts.push(`<text x="${l.x.toFixed(1)}" y="${l.y.toFixed(1)}" class="row-label ${l.cls}">${esc(l.name)}</text>`);
  parts.push("</svg>");
  return parts.join("");
}

export function bandPanels(pts, t) {
  if (!t) return `<p class="empty">Not enough datasets measured at different row-group sizes yet.</p>`;
  const lines = datasetLines(pts);
  const xs = [...Object.values(lines).flatMap((rows) => rows.map((x) => x.x)), ...t.rows.map((x) => x.x)];
  const xr = [Math.min(...xs) / 1.6, Math.max(...xs) * 1.6];
  return `<div class="facets two">${["cold", "warm"].map((tier) => `<figure class="facet">${bandPanelSvg(lines, t, tier, xr)}</figure>`).join("")}</div>`;
}

const TENSION = [["relCold", "cold", "var(--c1)", ""], ["relWarm", "warm", "var(--c2)", "6 4"], ["relCu", "read CU", "var(--c3)", "2 3"]];

/** The row-group tension chart: cold, warm and read CU as multiples of their own best bin. */
export function tensionSvg(bins, { w = 460, h = 300 } = {}) {
  if (!bins) return `<p class="empty">Not enough runs at different row-group counts yet.</p>`;
  const { rows, sweet } = bins;
  const m = { l: 52, r: 62, t: 16, b: 44 };
  const xs = rows.map((x) => x.rgs);
  const sx = scale(xs, true, m.l, w - m.r);
  const ys = rows.flatMap((x) => [x.relCold, x.relWarm, x.relCu]).filter((v) => v > 0);
  const top = Math.max(1.5, Math.max(...ys) * 1.05);
  const fy = (v) => h - m.b - ((v - 1) / (top - 1)) * (h - m.b - m.t);
  const parts = [`<svg viewBox="0 0 ${w} ${h}" class="chart" role="img" aria-label="cold, warm and read CU against row groups">`];
  // The sweet spot: the bin whose worse tier is closest to its own best.
  const half = (x) => (rows.length > 1 ? Math.abs(sx.f(x * 1.41) - sx.f(x)) : 20);
  parts.push(`<rect x="${(sx.f(sweet.rgs) - half(sweet.rgs)).toFixed(1)}" y="${m.t}" width="${(2 * half(sweet.rgs)).toFixed(1)}" height="${h - m.b - m.t}" class="sweet"/>`,
    `<text x="${sx.f(sweet.rgs).toFixed(1)}" y="${m.t + 12}" class="sweet-label" text-anchor="middle">sweet spot</text>`);
  for (const t of linTicks(1, top, 4).filter((v) => v >= 1)) {
    const yy = fy(t);
    parts.push(`<line x1="${m.l}" x2="${w - m.r}" y1="${yy.toFixed(1)}" y2="${yy.toFixed(1)}" class="grid"/>`,
      `<text x="${m.l - 6}" y="${(yy + 3.5).toFixed(1)}" class="tick" text-anchor="end">${t.toFixed(t % 1 ? 1 : 0)}×</text>`);
  }
  for (const x of rows) {
    parts.push(`<text x="${sx.f(x.rgs).toFixed(1)}" y="${h - m.b + 15}" class="tick" text-anchor="middle">${fmtInt(x.rgs)}</text>`,
      `<text x="${sx.f(x.rgs).toFixed(1)}" y="${h - m.b + 27}" class="tick dimtick" text-anchor="middle">${fmtRows(x.rowsPerRg)}</text>`);
  }
  const labels = [];
  for (const [k, name, css, dash] of TENSION) {
    const pts = rows.filter((x) => x[k] > 0);
    if (pts.length < 2) continue;
    parts.push(`<polyline points="${pts.map((x) => `${sx.f(x.rgs).toFixed(1)},${fy(x[k]).toFixed(1)}`).join(" ")}" class="tline" style="stroke:${css}"${dash ? ` stroke-dasharray="${dash}"` : ""}/>`);
    for (const x of pts) {
      parts.push(`<g class="pt" data-tip="${esc(`${name}: ${fmtX(x[k])} its best · ${fmtInt(x.lo)}–${fmtInt(x.hi)} row groups (~${fmtRows(x.rowsPerRg)} rows each) · ${x.n} runs · median ${name === "read CU" ? fmtCu(x.cu) : fmtMs(name === "cold" ? x.cold : x.warm)}`)}"><circle cx="${sx.f(x.rgs).toFixed(1)}" cy="${fy(x[k]).toFixed(1)}" r="4" fill="${css}" stroke="var(--bg)" stroke-width="1.5"/><circle cx="${sx.f(x.rgs).toFixed(1)}" cy="${fy(x[k]).toFixed(1)}" r="10" fill="transparent"/></g>`);
    }
    const last = pts[pts.length - 1];
    labels.push({ x: sx.f(last.rgs) + 8, y: fy(last[k]) + 4, name });
  }
  // End labels pushed apart so two lines finishing at the same value stay readable.
  labels.sort((p, q) => p.y - q.y);
  for (let i = 1; i < labels.length; i++) labels[i].y = Math.max(labels[i].y, labels[i - 1].y + 12);
  for (const l of labels) parts.push(`<text x="${l.x.toFixed(1)}" y="${l.y.toFixed(1)}" class="row-label">${esc(l.name)}</text>`);
  parts.push(`<text x="${(m.l + w - m.r) / 2}" y="${h - 3}" class="axis" text-anchor="middle">row groups in the table · rows per row group</text>`,
    `<text transform="translate(12,${(m.t + h - m.b) / 2}) rotate(-90)" class="axis" text-anchor="middle">× its own best</text></svg>`);
  return parts.join("");
}

export function legend(families, { reorder = true, extra = "" } = {}) {
  const items = families.map((f) => `<span class="key"><svg viewBox="0 0 12 12" width="12" height="12">${fam(f).shape === "square"
    ? `<rect x="1.5" y="1.5" width="9" height="9" rx="1" fill="${fam(f).css}"/>`
    : `<circle cx="6" cy="6" r="4.5" fill="${fam(f).css}"/>`}</svg>${esc(fam(f).label)}</span>`);
  if (reorder) {
    items.push(`<span class="key"><svg viewBox="0 0 12 12" width="12" height="12"><circle cx="6" cy="6" r="4.5" fill="var(--fg)"/></svg>filled: rows reordered (V-Order or a global sort)</span>`,
      `<span class="key"><svg viewBox="0 0 12 12" width="12" height="12"><circle cx="6" cy="6" r="4" fill="none" stroke="var(--fg)" stroke-width="1.6"/></svg>hollow: written as it came</span>`);
  }
  return `<div class="legend">${items.join("")}${extra}</div>`;
}

const familiesIn = (rows) => Object.keys(FAMILIES).filter((f) => rows.some((r) => r.family === f));

function facets(items, cls = "") {
  const live = items.filter((i) => i.html);
  if (!live.length) return `<p class="empty">Not enough measured points yet.</p>`;
  return `<div class="facets${live.length === 1 ? " one" : ""}${cls ? ` ${cls}` : ""}">${live.map((i) =>
    `<figure class="facet"><figcaption>${esc(i.title)}</figcaption>${i.html}</figure>`).join("")}</div>`;
}

export function table(head, rows, cls = "") {
  return `<div class="tablewrap"><table class="${cls}"><thead><tr>${head.map((h) =>
    `<th>${h}</th>`).join("")}</tr></thead><tbody>${rows.map((r) =>
    `<tr>${r.map((c) => `<td>${c ?? "—"}</td>`).join("")}</tr>`).join("")}</tbody></table></div>`;
}

const STATUS = { clear: "clear", noise: "inside the noise", few: "too few runs", partial: "partly measured" };
const badge = (s) => `<span class="badge ${s}">${STATUS[s]}</span>`;
const rhoText = (r) => (r === null ? "—" : `${r >= 0 ? "" : "−"}${Math.abs(r).toFixed(2)}`);
const range = (xs, f) => {
  const v = xs.filter((n) => typeof n === "number" && Number.isFinite(n));
  if (!v.length) return "—";
  const lo = Math.min(...v), hi = Math.max(...v);
  return f(lo) === f(hi) ? f(lo) : `${f(lo)}–${f(hi)}`;
};
/**
 * A sign test in words: when every dataset moved the same way, how often that happens by chance if
 * the switch did nothing. Stated only for 4+ datasets all agreeing, where it is worth saying.
 */
export function signNote(agree, n) {
  if (n < 4 || (agree !== n && agree !== 0)) return "";
  return `All ${n} datasets moved the same way; if it made no difference, that would happen by chance about 1 time in ${fmtInt(1 / (2 * 0.5 ** n))}. `;
}

/** "a strong link", "only a weak link", "no link" — so a claim never calls ρ 0.2 a relationship. */
export function linkWord(rho) {
  const s = strength(rho);
  return s === "strong" ? "a strong link" : s === "moderate" ? "a moderate link" : s === "weak" ? "only a weak link"
    : s === "none" ? "no link" : "too few points to judge a link";
}
const label = (ds) => (DATASETS[ds] || {}).label || ds;
const dsList = (ctx) => Object.keys(DATASETS).filter((ds) => ctx.datasets.includes(ds));

function runTip(r, extra = "") {
  return `${r.writerLabel} · ${label(r.ds)} · run ${r.id} (${r.when})${extra}`;
}

// ------------------------------------------------------------------------------------- findings
//
// Each finding returns `{id, title, short, status, html, head}`. `head` is the card's big number and
// its caption, `short` the card's sentence; both are generated, so a rule the data stops supporting
// says so on the card too.

/** "1.6k CU vs 3.8k CU" plus its verdict — the read-cost cell every paired comparison carries. */
function cuPair(a, b, phase) {
  const x = a.map((r) => r.cu[phase]).filter(Boolean), y = b.map((r) => r.cu[phase]).filter(Boolean);
  if (!x.length || !y.length) return "—";
  return `${fmtCu(median(x))} vs ${fmtCu(median(y))} ${badge(verdict({ a: x, b: y }).status)}`;
}

export function findColdWarm(ctx) {
  const per = dsList(ctx).map((ds) => {
    const R = ctx.runs.filter((r) => r.ds === ds && r.dl.cold && r.dl.warm);
    const D = ctx.runs.filter((r) => r.ds === ds && r.dq.cold && r.dq.warm);
    return { ds, R, D,
      ratio: median(R.map((r) => r.dl.cold / r.dl.warm)),
      dqRatio: median(D.map((r) => r.dq.cold / r.dq.warm)),
      rho: spearman(R.map((r) => r.dl.cold), R.map((r) => r.dl.warm)) };
  }).filter((p) => p.R.length);
  const ratios = per.map((p) => p.ratio), dqr = per.map((p) => p.dqRatio).filter(Boolean);
  const rhos = per.map((p) => p.rho).filter((r) => r !== null);
  const short = per.length
    ? `In Direct Lake, cold costs ${range(ratios, fmtX)} warm; the parquet layout matters for cold.`
    : "No run carries both a cold and a warm Direct Lake time yet.";
  const chart = facets(per.map((p) => ({
    title: `${label(p.ds)} · cold ${fmtX(p.ratio)} warm`,
    html: scatter({
      points: p.R.map((r) => ({ x: r.dl.warm, y: r.dl.cold, family: r.family, filled: r.reorders,
        tip: runTip(r, ` · cold ${fmtMs(r.dl.cold)} · warm ${fmtMs(r.dl.warm)}`) })),
      x: { label: "warm query suite total", log: true, fmt: fmtMs },
      y: { label: "cold query suite total", log: true, fmt: fmtMs },
      guides: [1, 10], w: 420, h: 290 }) })));
  const rows = per.map((p) => [label(p.ds), fmtInt(p.R.length), fmtX(p.ratio), rhoText(p.rho),
    p.dqRatio ? fmtX(p.dqRatio) : "—"]);
  const html = `
    <p class="claim">${per.length ? `In Direct Lake, cold cost <b>${range(ratios, fmtX)}</b> warm, median per dataset. Within a dataset a run's cold time says ${rhos.length ? `${strength(median(rhos)) === "strong" ? "a lot" : "little"} about its warm time (ρ ${range(rhos, rhoText)})` : "nothing measurable yet about its warm time"}. ${dqr.length ? `DirectQuery has no transcode step: its cold cost only ${range(dqr, fmtX)} its warm.` : ""}` : short}</p>
    <p>Cold is the transcode: VertiPaq reading each column out of parquet and building its own segments. Warm is a second visit with those segments already in memory. They answer different questions, so every finding below reports them separately.</p>
    ${legend(familiesIn(per.flatMap((p) => p.R)))}
    ${chart}
    ${table(["dataset", "runs", "cold ÷ warm (Direct Lake)", "ρ cold vs warm", "cold ÷ warm (DirectQuery)"], rows, "num")}`;
  return { id: "cold-warm", title: "Cold against warm", short, status: per.length ? "clear" : "few", html,
    head: [range(ratios, fmtX), "slower cold than warm"],
    card: "Cold is where the parquet layout matters. Warm, the data is already in VertiPaq." };
}

export function findColumnSize(ctx) {
  const per = dsList(ctx).map((ds) => {
    const C = ctx.columns.filter((c) => c.ds === ds && c.mb > 0 && c.cold !== null);
    return { ds, C,
      rho: spearman(C.map((c) => c.mb), C.map((c) => c.cold)),
      rhoWarm: spearman(C.map((c) => c.mb), C.map((c) => c.warm)),
      dropped: C.filter((c) => !(c.cold > 0)).length };
  }).filter((p) => p.C.length >= 5);
  const rc = per.map((p) => p.rho).filter((r) => r !== null);
  const rw = per.map((p) => p.rhoWarm).filter((r) => r !== null);
  const short = rc.length
    ? `A column's compressed size predicts its cold cost (ρ ${range(rc, rhoText)} across datasets) and not its warm cost (ρ ${range(rw, rhoText)}). Small columns load fast.`
    : "Not enough probed columns with a recorded size yet.";
  const chart = facets(per.map((p) => ({
    title: `${label(p.ds)} · ρ ${rhoText(p.rho)}`,
    html: scatter({
      points: p.C.filter((c) => c.cold > 0).map((c) => ({ x: c.mb, y: c.cold, family: c.family,
        filled: c.reorders, shape: c.dictFull === false ? "triangle" : undefined,
        tip: `${c.column} · ${c.writerLabel} · ${fmtMb(c.mb)} · cold +${fmtMs(c.cold)} · warm ${fmtMs(c.warm)}${c.dictFull === false ? ` · dictionary in ${c.dictPages}/${c.chunks} row groups` : ""}` })),
      x: { label: "column compressed size", log: true, fmt: fmtMb },
      y: { label: "extra cold time to load it", log: true, fmt: fmtMs },
      w: 420, h: 290, trend: true }) })));
  const dropped = per.reduce((s, p) => s + p.dropped, 0);
  const html = `
    <p class="claim">${rc.length ? `On every dataset, a bigger column cost more to load cold (<b>ρ ${range(rc, rhoText)}</b>). Once loaded, its size made no measurable difference (ρ ${range(rw, rhoText)}).` : short}</p>
    <p>Each point is one column in one run. The y value is that column's probe query minus the row-count probe in the same pass, which isolates loading the column from the fixed cost of any query. The same column appears once per writer, so the spread along a row of points is what the writer did to it.</p>
    ${legend(familiesIn(per.flatMap((p) => p.C)), { extra: `<span class="key"><svg viewBox="0 0 12 12" width="12" height="12"><path d="M6,1L11,10L1,10Z" fill="var(--fg)"/></svg>triangle: some row groups have no dictionary</span>` })}
    ${chart}
    ${table(["dataset", "column-runs", "ρ size vs cold", "ρ size vs warm"],
      per.map((p) => [label(p.ds), fmtInt(p.C.length), `${rhoText(p.rho)} <span class="dim">${strength(p.rho)}</span>`,
        `${rhoText(p.rhoWarm)} <span class="dim">${strength(p.rhoWarm)}</span>`]), "num")}
    ${dropped ? `<p class="note">${fmtInt(dropped)} column-runs read faster than the row-count probe itself, so they have no positive extra cost to plot on a log axis. They count in ρ.</p>` : ""}`;
  return { id: "column-size", title: "Small columns load fast", short, status: rc.length ? "clear" : "few", html,
    head: [rc.length ? `${rc.filter((x) => x >= 0.4).length} of ${rc.length}` : "—", "datasets where bigger columns loaded slower"],
    card: "Small columns are fast cold. The less space a column takes on disk, the lower its cold cost." };
}

/**
 * How much of a column's cold cost the LAYOUT explains, and what is left for the writer's name.
 *
 * Per (dataset, column), over every run that probed it: least squares of log(cold) on log(column MB)
 * and log(row groups in the table). Each column-run's residual is what the layout did not predict;
 * the median residual per writer family is how much more (or less) Power BI charged that writer than
 * its layout alone says. Near 1.0× means the engine could not tell who wrote the file.
 */
export function layoutModel(ctx, dss) {
  const rg = Object.fromEntries(ctx.runs.map((r) => [r.id, r.rgs]));
  const groups = {};
  for (const c of ctx.columns) {
    if (!dss.includes(c.ds) || !(c.cold > 0) || !(c.mb > 0) || !rg[c.runId]) continue;
    (groups[`${c.ds}|${c.column}`] = groups[`${c.ds}|${c.column}`] || []).push(c);
  }
  const points = [], r2 = [];
  for (const cs of Object.values(groups)) {
    if (cs.length < 8 || new Set(cs.map((c) => c.family)).size < 2) continue;
    const y = cs.map((c) => Math.log(c.cold)), a = cs.map((c) => Math.log(c.mb)), b = cs.map((c) => Math.log(rg[c.runId]));
    const mean = (v) => v.reduce((s, x) => s + x, 0) / v.length;
    const my = mean(y), ma = mean(a), mb = mean(b);
    const sab = a.reduce((s, x, i) => s + (x - ma) * (b[i] - mb), 0);
    const saa = a.reduce((s, x) => s + (x - ma) ** 2, 0), sbb = b.reduce((s, x) => s + (x - mb) ** 2, 0);
    const say = a.reduce((s, x, i) => s + (x - ma) * (y[i] - my), 0), sby = b.reduce((s, x, i) => s + (x - mb) * (y[i] - my), 0);
    const det = saa * sbb - sab * sab;
    // A column whose size or row-group count never varied cannot be modelled on it.
    if (!(det > 1e-9)) continue;
    const ka = (sbb * say - sab * sby) / det, kb = (saa * sby - sab * say) / det;
    const sst = y.reduce((s, v) => s + (v - my) ** 2, 0);
    let sse = 0;
    cs.forEach((c, i) => {
      const pred = my + ka * (a[i] - ma) + kb * (b[i] - mb);
      sse += (y[i] - pred) ** 2;
      points.push({ ...c, predicted: Math.exp(pred), raw: Math.exp(y[i] - my), resid: Math.exp(y[i] - pred) });
    });
    if (sst > 0) r2.push(1 - sse / sst);
  }
  const fams = Object.keys(FAMILIES).filter((f) => points.some((p) => p.family === f)).map((f) => {
    const mine = points.filter((p) => p.family === f);
    return { family: f, n: mine.length, raw: Math.exp(median(mine.map((p) => Math.log(p.raw)))),
      resid: Math.exp(median(mine.map((p) => Math.log(p.resid)))) };
  });
  return { points, fams, r2: median(r2), columns: r2.length };
}

const pctOff = (x) => `${x >= 1 ? "+" : "−"}${Math.abs((x - 1) * 100).toFixed(0)}%`;

export function findWriterBlind(ctx) {
  const m = layoutModel(ctx, dsList(ctx));
  const enough = m.fams.filter((f) => f.n >= 20);
  const blind = enough.filter((f) => Math.abs(f.resid - 1) <= 0.1);
  const left = enough.filter((f) => Math.abs(f.resid - 1) > 0.1);
  const short = enough.length >= 2
    ? `Power BI never sees who wrote the file. Once a column's size and the table's row-group count are known, ${blind.map((f) => fam(f.family).label).join(", ")} ${blind.length === 1 ? "lands" : "land"} within 10% of what the layout predicts${left.length ? `; ${left.map((f) => `${fam(f.family).label} keeps ${pctOff(f.resid)}`).join(", ")}` : ""}.`
    : "Not enough columns probed under two writers yet.";
  const byDs = dsList(ctx).map((ds) => ({ ds, pts: m.points.filter((p) => p.ds === ds) })).filter((x) => x.pts.length >= 5);
  const html = `
    <p class="claim">${enough.length >= 2 ? `The writer's name is not something Power BI can see; only the file is. Predict each column's cold load from two layout facts alone, its compressed size and the table's row-group count, and ${blind.map((f) => `<b>${esc(fam(f.family).label)}</b> (${pctOff(f.resid)})`).join(", ")} ${blind.length === 1 ? "sits" : "sit"} on the prediction.${left.length ? ` ${left.map((f) => `<b>${esc(fam(f.family).label)}</b> keeps ${pctOff(f.resid)}`).join(", ")}, which means its files carry a layout property these two do not capture.` : ""}` : short}</p>
    <p>Each point is one column in one run. Across: what size and row-group count predict for that column, fitted per column over every writer at once. Up: what it actually cost. Points from different writers falling on the same diagonal is the claim. Before the layout is accounted for the writers look different (the "raw" column below). After, the differences shrink to what the layout explains. The two facts explain a median ${(m.r2 * 100).toFixed(0)}% of the run-to-run variation in a column's cold cost, over ${m.columns} columns. The rest is capacity load and what the records do not measure (dictionary declaration, below).</p>
    ${legend(familiesIn(m.points))}
    ${facets(byDs.map((x) => ({ title: label(x.ds), html: scatter({
      points: x.pts.map((p) => ({ x: p.predicted, y: p.cold, family: p.family, filled: p.reorders,
        tip: `${p.column} · ${p.writerLabel} · predicted ${fmtMs(p.predicted)} · actual ${fmtMs(p.cold)}` })),
      x: { label: "cold cost the layout predicts", log: true, fmt: fmtMs },
      y: { label: "actual cold cost", log: true, fmt: fmtMs }, guides: [1], w: 420, h: 290 }) })))}
    ${table(["writer", "column-runs", "raw: against the column's median", "after column size and row groups"],
      m.fams.map((f) => [esc(fam(f.family).label), fmtInt(f.n), pctOff(f.raw), `<b>${pctOff(f.resid)}</b>`]), "num")}`;
  return { id: "writer-blind", title: "Power BI never sees the writer, only the layout", short, status: enough.length >= 2 ? "clear" : "few", html,
    head: [enough.length >= 2 ? `${blind.length} of ${enough.length}` : "—", "engines cost exactly what their layout predicts"],
    card: "Power BI does not know which engine wrote a file. The same layout costs the same, whoever produced it." };
}

export function findDictionary(ctx) {
  const C = ctx.columns.filter((c) => dsList(ctx).includes(c.ds) && c.chunks);
  const fams = familiesIn(C);
  const rows = fams.map((f) => {
    const mine = C.filter((c) => c.family === f);
    const encs = {};
    for (const c of mine) if (c.encodings) encs[c.encodings] = (encs[c.encodings] || 0) + 1;
    const top = Object.entries(encs).sort((a, b) => b[1] - a[1]).slice(0, 2).map(([e]) => `<code>${esc(e)}</code>`).join("<br>");
    const pages = mine.reduce((s, c) => s + (c.dictPages || 0), 0), chunks = mine.reduce((s, c) => s + (c.chunks || 0), 0);
    return [esc(fam(f).label), top || "—", chunks ? `${((pages / chunks) * 100).toFixed(chunks - pages ? 1 : 0)}%` : "—"];
  });
  const gaps = C.filter((c) => c.dictFull === false);
  // MEASURED, where the run recorded its footers' encoding_stats: each column-run is on the remap path
  // (every chunk declared fully dictionary-encoded) or the hashing path. Cold cost is compared against
  // that column's own median, so a timestamp and a flag can share one comparison.
  const measured = C.filter((c) => c.remap !== null && c.cold > 0);
  const colMed = {};
  for (const c of measured) (colMed[`${c.ds}|${c.column}`] = colMed[`${c.ds}|${c.column}`] || []).push(c.cold);
  for (const k of Object.keys(colMed)) colMed[k] = median(colMed[k]);
  const rel = (c) => c.cold / colMed[`${c.ds}|${c.column}`];
  const remapRel = measured.filter((c) => c.remap).map(rel), hashRel = measured.filter((c) => !c.remap).map(rel);
  const gap = remapRel.length && hashRel.length ? median(hashRel) / median(remapRel) : null;
  const declRows = fams.map((f) => {
    const mine = C.filter((c) => c.family === f && c.declared !== null);
    if (!mine.length) return [esc(fam(f).label), "not recorded yet", "—"];
    const ch = mine.reduce((s2, c) => s2 + (c.chunks || 0), 0);
    const dcl = mine.reduce((s2, c) => s2 + (c.declared || 0), 0), dd = mine.reduce((s2, c) => s2 + (c.dictDeclared || 0), 0);
    return [esc(fam(f).label), `${((dcl / ch) * 100).toFixed(0)}%`, `${((dd / ch) * 100).toFixed(0)}%`];
  });
  const m = layoutModel(ctx, dsList(ctx));
  const ice = m.fams.find((f) => f.family === "duckdb-iceberg");
  const short = `Direct Lake loads a column chunk two ways: a cheap remap when the chunk is entirely dictionary-encoded AND its footer's encoding_stats say so, or hashing every value when not. ${gaps.length ? `${fmtInt(gaps.length)} column-runs had chunks with no dictionary page at all.` : ""} ${measured.length ? "" : "The records do not yet capture the footer declaration."}`;
  const html = `
    <p class="claim">Direct Lake transcodes a parquet column chunk one of <b>two ways</b>. If every page of the chunk is dictionary-encoded and the footer's <code>encoding_stats</code> prove it, it <b>remaps</b> the parquet dictionary IDs straight onto VertiPaq's: a cheap ID swap. Otherwise it <b>hashes every value</b> into a new dictionary. That includes a chunk that does have a dictionary but whose footer does not declare it.</p>
    <p>The footer's <code>encodings</code> list cannot settle it. A dictionary-encoded chunk lists <code>PLAIN</code> for its dictionary page, and so does one whose pages fell back to <code>PLAIN</code> midway. Only <code>encoding_stats</code> (<code>PageEncodingStats</code>) says which pages are which, without decoding them. How much the declaration is worth: <a href="https://github.com/duckdb/duckdb/pull/24957">duckdb#24957</a> added it to DuckDB's writer and measured a 142M-row dictionary string column's cold first-touch falling from <b>10,857.5 ms to 689.3 ms</b>. Same pages, one footer list.</p>
    ${measured.length ? `<h4>Measured: remap against hashing</h4>
    <p>${gap !== null ? `Columns whose every chunk was declared fully dictionary-encoded loaded at a median <b>${fmtX(median(remapRel))}</b> of that column's typical cold cost; columns on the hashing path at <b>${fmtX(median(hashRel))}</b>, ${gap >= 1 ? `${fmtX(gap)} slower` : `${fmtX(1 / gap)} faster`}, over ${fmtInt(remapRel.length)} and ${fmtInt(hashRel.length)} column-runs.` : `${fmtInt(measured.length)} column-runs carry the footer declaration, all on the ${remapRel.length ? "remap" : "hashing"} path, so there is nothing to compare yet.`}</p>
    ${table(["writer", "chunks that declare encoding_stats", "chunks declared fully dictionary-encoded"], declRows, "num")}` : ""}
    <h4>What the run records can and cannot say</h4>
    <p>Every run records, per column chunk, whether it has a dictionary page; a chunk with none is certainly on the hashing path. Runs from 2026-10-17 on also record the footer's <code>encoding_stats</code> (the table above); older runs do not, so for them a chunk with a dictionary page could be on either path. Every DuckDB → Iceberg run in the history was written before the DuckDB build carrying #24957 was pinned, so its dictionaries were undeclared${ice ? `. That is the likely reason it is the one writer still <b>${pctOff(ice.resid)}</b> off the layout's prediction in finding "Power BI never sees the writer"` : ""}.</p>
    ${table(["writer", "encodings it writes (most common)", "chunks with a dictionary page"], rows)}
    ${gaps.length ? `<h4>Column-runs with chunks that have no dictionary page: certain hashing</h4>
    ${table(["column-run", "chunks with a dictionary page", "size", "extra cold time"],
      gaps.slice(0, 12).map((g) => [`<code>${esc(g.column)}</code> · ${esc(label(g.ds))} · ${esc(g.writerLabel)}`, `${g.dictPages} of ${g.chunks}`, fmtMb(g.mb), fmtMs(g.cold)]), "num")}` : ""}
    <p class="note"><code>stats.py</code> reads each mart file's footer Thrift directly for this (neither DuckDB's <code>parquet_metadata()</code> nor pyarrow exposes <code>encoding_stats</code>) and records, per column, the chunks that declare their encodings and the chunks declared entirely dictionary-encoded.</p>`;
  const status = !measured.length ? "partial" : gap === null || remapRel.length < MIN_RUNS || hashRel.length < MIN_RUNS ? "few"
    : gap >= 1.3 ? "clear" : "noise";
  return { id: "dictionary", title: "Dictionary encoding, declared", short, status, html,
    head: ["2 ways", "to load a column: reuse its dictionary, or rebuild it"],
    card: "A column stored fully as a dictionary, with the file saying so, loads fast. Otherwise Power BI rebuilds the dictionary value by value." };
}

export function findSort(ctx) {
  const per = dsList(ctx).map((ds) => {
    const C = ctx.columns.filter((c) => c.ds === ds && c.runsPer1k > 0 && c.mb > 0);
    // Within ONE column, across writers: does fewer runs mean fewer bytes? Pooling columns would
    // mostly measure that a timestamp has more distinct values than a flag.
    const byCol = {};
    for (const c of C) (byCol[c.column] = byCol[c.column] || []).push(c);
    const within = Object.values(byCol).map((cs) => spearman(cs.map((c) => c.runsPer1k), cs.map((c) => c.mb)))
      .filter((r) => r !== null);
    return { ds, C, within };
  }).filter((p) => p.C.length >= 5);
  const within = per.flatMap((p) => p.within);
  const pos = within.filter((r) => r > 0.4).length;
  // delta-rs sorted against unsorted, the one place a sort is switched on and off by itself.
  const pairs = dsList(ctx).map((ds) => {
    const s = ctx.runs.filter((r) => r.ds === ds && r.writer === "deltars_sorted" && r.dl.cold);
    const u = ctx.runs.filter((r) => r.ds === ds && r.writer === "deltars_unsorted" && r.dl.cold);
    return { ds, s, u };
  }).filter((p) => p.s.length && p.u.length);
  const short = within.length
    ? `Within the same column, fewer runs of repeated values meant a smaller column on ${pos} of ${within.length} columns, and smaller columns load faster cold.`
    : "No sort-run measurements yet.";
  const pairRows = pairs.flatMap((p) => {
    const out = [];
    for (const [tier, f] of [["cold", (r) => r.dl.cold], ["warm", (r) => r.dl.warm]]) {
      out.push({ label: `${label(p.ds)} sorted · ${tier}`, values: p.s.map(f), family: "delta-rs", filled: true, gap: tier === "cold" && out.length === 0 && pairs.indexOf(p) > 0 },
        { label: `${label(p.ds)} unsorted · ${tier}`, values: p.u.map(f), family: "delta-rs", filled: false });
    }
    return out;
  });
  const pairTable = pairs.map((p) => {
    const vc = verdict({ s: p.s.map((r) => r.dl.cold), u: p.u.map((r) => r.dl.cold) });
    const vw = verdict({ s: p.s.map((r) => r.dl.warm), u: p.u.map((r) => r.dl.warm) });
    return [label(p.ds), `${p.s.length} / ${p.u.length}`,
      `${fmtMs(median(p.s.map((r) => r.dl.cold)))} vs ${fmtMs(median(p.u.map((r) => r.dl.cold)))} ${badge(vc.status)}`,
      `${fmtMs(median(p.s.map((r) => r.dl.warm)))} vs ${fmtMs(median(p.u.map((r) => r.dl.warm)))} ${badge(vw.status)}`,
      `${fmtMb(median(p.s.map((r) => r.mb)))} vs ${fmtMb(median(p.u.map((r) => r.mb)))}`,
      cuPair(p.s, p.u, "dl")];
  });
  const html = `
    <p class="claim">${within.length ? `Within the same column, the writer that left fewer runs of repeated values produced a smaller column on <b>${pos} of ${within.length}</b> columns (median within-column ρ ${rhoText(median(within))}).` : short}</p>
    <p>A run is a stretch of identical adjacent values. Run-length encoding stores a run as one value and a count, so a column whose equal values sit together compresses far better. A global sort and V-Order both create runs; writing rows in arrival order does not. The count is taken over the first 4M rows of the largest file and shown per 1,000 rows.</p>
    ${legend(familiesIn(per.flatMap((p) => p.C)))}
    ${facets(per.map((p) => ({ title: label(p.ds), html: scatter({
      points: p.C.map((c) => ({ x: c.runsPer1k, y: c.mb, family: c.family, filled: c.reorders,
        tip: `${c.column} · ${c.writerLabel} · ${c.runsPer1k.toFixed(1)} runs per 1,000 rows · ${fmtMb(c.mb)}` })),
      x: { label: "runs of repeated values per 1,000 rows", log: true, fmt: (v) => (v >= 10 ? fmtInt(v) : v >= 1 ? v.toFixed(0) : String(+v.toPrecision(1))) },
      y: { label: "column compressed size", log: true, fmt: fmtMb }, w: 420, h: 290 }) })))}
    ${pairs.length ? `<h4>delta-rs with the sort switched on and off</h4>
      <p>The one comparison where a sort is the only thing that changes.</p>
      ${strips({ rows: pairRows, x: { label: "query suite total", fmt: fmtMs } })}
      ${table(["dataset", "runs sorted / unsorted", "cold", "warm", "table size", "Direct Lake read CU"], pairTable, "num")}` : ""}`;
  return { id: "sort", title: "Sorting shrinks columns", short, status: within.length ? "clear" : "few", html,
    head: [within.length ? `${pos} of ${within.length}` : "—", "columns got smaller when equal values sat together"],
    card: "Sorting puts equal values next to each other, so columns shrink, and smaller columns load faster." };
}

/**
 * For one dataset: how each probed column's cold cost, and each query's warm and hot time, move with
 * the row-group count. Measured PER COLUMN and PER QUERY, never on suite totals: a total adds up
 * columns and queries that move in opposite directions and fixed costs that do not move at all, which
 * is how a real trade-off reads as "no effect".
 */
export function rowGroupTrade(ctx, ds, keep = () => true) {
  const R = ctx.runs.filter((r) => r.ds === ds && r.rgs && keep(r));
  const rg = Object.fromEntries(R.map((r) => [r.id, r.rgs]));
  const byCol = {};
  for (const c of ctx.columns) if (c.ds === ds && rg[c.runId] && c.cold !== null) (byCol[c.column] = byCol[c.column] || []).push(c);
  const cols = Object.entries(byCol).map(([column, cs]) => ({ column,
    rho: spearman(cs.map((c) => rg[c.runId]), cs.map((c) => c.cold)) })).filter((x) => x.rho !== null);
  const timed = R.filter((r) => r.dl.warm);
  const qs = (ctx.perDataset[ds] || {}).queriesDl || [];
  const perQuery = (tier) => qs.map((q) => spearman(timed.map((r) => r.rgs),
    timed.map((r) => ((r.timings.dl[q] || {})[tier]) ?? NaN))).filter((x) => x !== null);
  const warm = perQuery("warm_ms"), hot = perQuery("hot_median_ms");
  const withCu = R.filter((r) => r.cu.dl);
  const cuRho = spearman(withCu.map((r) => r.rgs), withCu.map((r) => r.cu.dl));
  const tally = (xs) => ({ up: xs.filter((x) => x > 0.2).length, down: xs.filter((x) => x < -0.2).length,
    n: xs.length, med: median(xs) });
  return { ds, R, timed, distinct: new Set(R.map((r) => r.rgs)).size,
    lo: R.length ? Math.min(...R.map((r) => r.rgs)) : null, hi: R.length ? Math.max(...R.map((r) => r.rgs)) : null,
    cols, cold: tally(cols.map((c) => c.rho)), warm: tally(warm), hot: tally(hot), withCu, cuRho };
}

// Which way a dataset leans: more row groups SLOWER to load cold, and FASTER warm.
const coldSlower = (t) => t.cold.n >= 3 && t.cold.med > 0.2 && t.cold.up > t.cold.down;
const warmFaster = (t) => t.warm.n >= 3 && t.warm.med < -0.1 && t.warm.down > t.warm.up;
const warmSlower = (t) => t.warm.n >= 3 && t.warm.med > 0.1 && t.warm.up > t.warm.down;

export function findRowGroups(ctx) {
  const all = dsList(ctx).map((ds) => rowGroupTrade(ctx, ds)).filter((t) => t.distinct >= 3 && t.timed.length >= 5);
  const fixed = dsList(ctx).map((ds) => rowGroupTrade(ctx, ds, (r) => r.family === "delta-rs"))
    .filter((t) => t.distinct >= 3 && t.timed.length >= 5);
  const nCold = all.filter(coldSlower), nWarm = all.filter(warmFaster), against = all.filter(warmSlower);
  const nCu = all.filter((t) => t.cuRho !== null && t.cuRho > 0.2), cuKnown = all.filter((t) => t.cuRho !== null);
  const bandPts = bandPoints(ctx), bandT = bandTrend(bandPts);
  const short = all.length
    ? `Row groups are a trade. More of them made columns slower to load cold on ${nCold.length} of ${all.length} datasets${cuKnown.length ? ` and cost more Direct Lake CU on ${nCu.length} of ${cuKnown.length}` : ""}, but made warm queries faster on ${nWarm.length}${against.length ? ` (slower on ${against.map((t) => label(t.ds)).join(", ")})` : ""}.`
    : "Not enough runs at different row-group counts yet.";
  const tallyCell = (t, kind) => {
    const x = t[kind];
    if (!x.n) return "—";
    return `${kind === "cold" ? `${x.up} slower · ${x.down} faster` : `${x.down} faster · ${x.up} slower`} <span class="dim">of ${x.n}, median ρ ${rhoText(x.med)}</span>`;
  };
  const rowsOf = (ts) => ts.map((t) => [label(t.ds), fmtInt(t.timed.length), `${fmtInt(t.lo)}–${fmtInt(t.hi)}`,
    tallyCell(t, "cold"), tallyCell(t, "warm"), tallyCell(t, "hot"),
    t.cuRho === null ? "—" : `ρ ${rhoText(t.cuRho)} <span class="dim">${t.cuRho > 0.2 ? "more CU" : t.cuRho < -0.2 ? "less CU" : "no link"}</span>`]);
  const head = ["dataset", "runs", "row groups", "columns, cold load (with more row groups)",
    "queries, warm (with more row groups)", "queries, hot (with more row groups)", "Direct Lake read CU"];
  // Cold per column, relative to that column's own median so every column shares an axis, beside the
  // warm suite total. Same x, two panels, per dataset.
  const charts = all.flatMap((t) => {
    const byId = Object.fromEntries(t.R.map((r) => [r.id, r]));
    const cs = ctx.columns.filter((c) => c.ds === t.ds && byId[c.runId] && c.cold > 0);
    const med = {};
    for (const c of cs) (med[c.column] = med[c.column] || []).push(c.cold);
    for (const k of Object.keys(med)) med[k] = median(med[k]);
    return [{
      title: `${label(t.ds)} · cold, per column · median ρ ${rhoText(t.cold.med)}`,
      html: scatter({
        points: cs.map((c) => ({ x: byId[c.runId].rgs, y: c.cold / med[c.column], family: c.family, filled: c.reorders,
          tip: `${c.column} · ${c.writerLabel} · ${byId[c.runId].rgs} row groups · cold +${fmtMs(c.cold)} (${fmtX(c.cold / med[c.column])} its median)` })),
        x: { label: "row groups in the table", log: true, fmt: fmtInt },
        y: { label: "column cold cost ÷ that column's median", log: true, fmt: (v) => fmtX(v) },
        w: 420, h: 270, trend: true }),
    }, {
      title: `${label(t.ds)} · warm · ${t.warm.down} of ${t.warm.n} queries faster with more`,
      html: scatter({
        points: t.timed.map((r) => ({ x: r.rgs, y: r.dl.warm, family: r.family, filled: r.reorders,
          tip: runTip(r, ` · ${r.rgs} row groups · warm ${fmtMs(r.dl.warm)} · hot ${fmtMs(r.dl.hot)}`) })),
        x: { label: "row groups in the table", log: true, fmt: fmtInt },
        y: { label: "warm query suite total", log: true, fmt: fmtMs },
        w: 420, h: 270, trend: true }),
    }, {
      title: `${label(t.ds)} · Direct Lake read CU · ρ ${rhoText(t.cuRho)}`,
      html: scatter({
        points: t.withCu.map((r) => ({ x: r.rgs, y: r.cu.dl, family: r.family, filled: r.reorders,
          tip: runTip(r, ` · ${r.rgs} row groups · ${fmtCu(r.cu.dl)}`) })),
        x: { label: "row groups in the table", log: true, fmt: fmtInt },
        y: { label: "Direct Lake read CU, whole session", log: true, fmt: fmtCu },
        w: 420, h: 270, trend: true }),
    }];
  });
  const html = `
    <p class="claim">${bandT && bandT.band ? `Across every dataset, <b>${esc(bandText(bandT))} rows per row group</b> keeps both cold and warm within ${Math.round((BAND_WITHIN - 1) * 100)}% of the median. ` : ""}${all.length ? `More row groups made each column <b>slower to load cold</b> on ${nCold.length} of ${all.length} datasets${cuKnown.length ? ` and the Direct Lake session <b>cost more CU</b> on ${nCu.length} of ${cuKnown.length}` : ""}, while warm queries got <b>faster</b> on ${nWarm.length} of ${all.length}${against.length ? `. The exception is ${against.map((t) => esc(label(t.ds))).join(" and ")}, where warm got slower too` : ""}. Fewer, bigger row groups favour cold and CU; more, smaller ones favour warm.` : short}</p>
    <h4>A band that works cold and warm</h4>
    ${bandPanels(bandPts, bandT)}
    <p>Each thin line is one dataset: its runs grouped by octave of rows per row group, cold or warm divided by that dataset's median run so the datasets share the axis. The bold line is every dataset's runs together, and the band is computed from it: the octaves with at least ${BAND_MIN_RUNS} runs from two or more datasets where cold and warm are both within ${Math.round((BAND_WITHIN - 1) * 100)}% of the median. A hollow point is an octave with too few runs to count.${bandT && bandT.band ? ` Below the band cold gets slower; above it warm does.` : ""}</p>
    <h4>Why, and measured column by column</h4>
    <p>A row group becomes one VertiPaq segment. Cold, every segment of a column is transcoded separately, so each extra segment adds its own fixed cost. Warm, the storage engine scans a column's segments in parallel, one thread per segment. A column held in nine segments can keep at most nine cores busy, however many the capacity has, so more segments let a scan use more of the machine.</p>
    <p>This is measured per column for cold (that column's probe minus the row-count probe) and per query for warm and hot. A suite total adds up columns and queries that move in opposite directions, plus fixed costs that do not move at all, and that is how a real trade-off can read as no effect. A count is a column or query whose ρ against the row-group count is beyond ±0.2.</p>
    ${legend(familiesIn(all.flatMap((t) => t.R)))}
    ${facets(charts, "three")}
    ${table(head, rowsOf(all), "num")}
    <h4>With the writer held fixed: delta-rs only</h4>
    <p>Across writers, row-group count travels with everything else a writer does: Fabric Warehouse writes many small, uncompressed row groups, delta-rs a few big ones. Within delta-rs, row-group size is a setting someone chose and nothing else about the writer moves. It is the cleaner test, on fewer runs.</p>
    ${fixed.length ? table(head, rowsOf(fixed), "num") : `<p class="empty">No dataset has delta-rs runs at three or more row-group counts yet.</p>`}`;
  const status = !all.length ? "few" : nCold.length * 2 > all.length && nWarm.length * 2 > all.length ? "clear" : "noise";
  return { id: "row-groups", title: "Row groups: a band that works cold and warm", short, status, html,
    head: [bandText(bandT) || "—", "rows per row group: works cold and warm"],
    card: "Smaller row groups slow cold down, bigger ones slow warm down. In between is a band that works for both." };
}

export function findVOrder(ctx) {
  const pairs = dsList(ctx).map((ds) => ({ ds,
    v: ctx.runs.filter((r) => r.ds === ds && r.writer === "spark_vorder" && r.dl.cold),
    p: ctx.runs.filter((r) => r.ds === ds && r.writer === "spark_plain" && r.dl.cold) }))
    .filter((x) => x.v.length && x.p.length);
  const tiers = [["cold", (r) => r.dl.cold], ["warm", (r) => r.dl.warm], ["DirectQuery warm", (r) => r.dq.warm]];
  const verdicts = pairs.map((x) => Object.fromEntries(tiers.map(([t, f]) => {
    const v = verdict({ v: x.v.map(f).filter(Boolean), p: x.p.map(f).filter(Boolean) });
    return [t, v];
  })));
  const coldWins = verdicts.filter((v) => v.cold.status === "clear" && v.cold.best === "v").length;
  const coldFaster = pairs.filter((x) => median(x.v.map((r) => r.dl.cold)) < median(x.p.map((r) => r.dl.cold))).length;
  const warmFaster = pairs.filter((x) => median(x.v.map((r) => r.dl.warm)) < median(x.p.map((r) => r.dl.warm))).length;
  const smaller = pairs.filter((x) => median(x.v.map((r) => r.mb)) < median(x.p.map((r) => r.mb))).length;
  const cuRatios = pairs.map((x) => median(x.p.map((r) => r.cu.dl)) / median(x.v.map((r) => r.cu.dl)))
    .filter((v) => Number.isFinite(v) && v > 0);
  const cheaper = cuRatios.filter((v) => v > 1).length;
  const cuClear = pairs.filter((x) => {
    const v = verdict({ v: x.v.map((r) => r.cu.dl).filter(Boolean), p: x.p.map((r) => r.cu.dl).filter(Boolean) });
    return v.status === "clear" && v.best === "v";
  }).length;
  const short = pairs.length
    ? `Spark with V-Order (readHeavyForPBI) against without: ${cuRatios.length ? `Direct Lake read CU lower on ${cheaper} of ${cuRatios.length} datasets (${cuClear} clearly), ` : ""}cold faster on ${coldFaster} of ${pairs.length}, warm faster on only ${warmFaster}.`
    : "No dataset has Spark runs with and without V-Order yet.";
  const rows = pairs.flatMap((x, i) => tiers.slice(0, 2).flatMap(([t, f], j) => [
    { label: `${label(x.ds)} · V-Order · ${t}`, values: x.v.map(f), family: "spark", filled: true, gap: i > 0 && j === 0 },
    { label: `${label(x.ds)} · no V-Order · ${t}`, values: x.p.map(f), family: "spark", filled: false }]));
  const tbl = pairs.map((x, i) => [label(x.ds), `${x.v.length} / ${x.p.length}`,
    ...tiers.map(([t, f]) => {
      const a = median(x.v.map(f)), b = median(x.p.map(f));
      return a && b ? `${fmtMs(a)} vs ${fmtMs(b)} ${badge(verdicts[i][t].status)}` : "—";
    }),
    `${fmtMb(median(x.v.map((r) => r.mb)))} vs ${fmtMb(median(x.p.map((r) => r.mb)))}`]);
  const dwh = dsList(ctx).map((ds) => ({ ds,
    v: ctx.runs.filter((r) => r.ds === ds && r.writer === "dwh" && r.dl.cold),
    p: ctx.runs.filter((r) => r.ds === ds && r.writer === "dwh_novorder" && r.dl.cold) })).filter((x) => x.v.length && x.p.length);
  const html = `
    <p class="claim">${pairs.length ? `With V-Order on, Spark's cold time was lower on <b>${coldFaster} of ${pairs.length}</b> datasets, ${coldWins ? `${coldWins} of them clearly (every V-Order run beat every run without it)` : "none of them clearly on its own (some runs overlap)"}, and its table was smaller on ${smaller}. ${signNote(coldFaster, pairs.length)}${cuRatios.length ? `Reading it in Direct Lake cost <b>${range(cuRatios, fmtX)} less CU</b> on ${cheaper} of ${cuRatios.length} datasets, ${cuClear} of them clearly. ` : ""}Warm was lower on ${warmFaster} of ${pairs.length}: once the columns are in memory, V-Order made no consistent difference.` : short}</p>
    <p>V-Order is a Fabric write-time pass: it sorts rows within each row group, sets the row-group geometry and dictionary-encodes, all aimed at VertiPaq. In Spark it comes with the <code>readHeavyForPBI</code> resource profile. The workspace default, <code>writeHeavy</code>, leaves it off. Fabric Warehouse has it on by default.</p>
    ${strips({ rows, x: { label: "Direct Lake query suite total", fmt: fmtMs } })}
    <h4>Direct Lake read CU, with and without V-Order</h4>
    ${strips({ rows: pairs.flatMap((x, i) => [
      { label: `${label(x.ds)} · V-Order`, values: x.v.map((r) => r.cu.dl).filter(Boolean), family: "spark", filled: true, gap: i > 0 },
      { label: `${label(x.ds)} · no V-Order`, values: x.p.map((r) => r.cu.dl).filter(Boolean), family: "spark", filled: false }]),
      x: { label: "Direct Lake read CU, whole session", fmt: fmtCu } })}
    ${table(["dataset", "runs on / off", "cold (on vs off)", "warm (on vs off)", "DirectQuery warm", "table size", "Direct Lake CU", "DirectQuery CU"],
      tbl.map((row, i) => [...row, cuPair(pairs[i].v, pairs[i].p, "dl"), cuPair(pairs[i].v, pairs[i].p, "dq")]), "num")}
    ${dwh.length ? `<h4>Fabric Warehouse with V-Order turned off</h4>${table(["dataset", "runs on / off", "cold (on vs off)", "warm (on vs off)", "table size"],
      dwh.map((x) => [label(x.ds), `${x.v.length} / ${x.p.length}`,
        `${fmtMs(median(x.v.map((r) => r.dl.cold)))} vs ${fmtMs(median(x.p.map((r) => r.dl.cold)))} ${badge(verdict({ a: x.v.map((r) => r.dl.cold), b: x.p.map((r) => r.dl.cold) }).status)}`,
        `${fmtMs(median(x.v.map((r) => r.dl.warm)))} vs ${fmtMs(median(x.p.map((r) => r.dl.warm)))} ${badge(verdict({ a: x.v.map((r) => r.dl.warm), b: x.p.map((r) => r.dl.warm) }).status)}`,
        `${fmtMb(median(x.v.map((r) => r.mb)))} vs ${fmtMb(median(x.p.map((r) => r.mb)))}`]), "num")}
      <p class="note">Turning Warehouse V-Order off is irreversible on a warehouse, so it has been measured rarely.</p>` : ""}`;
  return { id: "vorder", title: "V-Order", short, status: coldWins || cuClear ? "clear" : pairs.length ? "noise" : "few", html,
    head: [cuRatios.length ? `up to ${fmtX(Math.max(...cuRatios))}` : "—", "less capacity to read Spark tables written with V-Order"],
    card: "In Spark, turning V-Order on made the tables cheaper to read and faster cold." };
}

export function findTableSize(ctx) {
  const per = dsList(ctx).map((ds) => {
    const R = ctx.runs.filter((r) => r.ds === ds && r.mb && r.dl.cold && r.dl.warm);
    const C = R.filter((r) => r.cu.dl);
    return { ds, R, rc: spearman(R.map((r) => r.mb), R.map((r) => r.dl.cold)),
      rw: spearman(R.map((r) => r.mb), R.map((r) => r.dl.warm)),
      rcu: spearman(C.map((r) => r.mb), C.map((r) => r.cu.dl)) };
  }).filter((p) => p.R.length >= 5);
  const rc = per.map((p) => p.rc).filter((r) => r !== null), rw = per.map((p) => p.rw).filter((r) => r !== null);
  const fams = familiesIn(per.flatMap((p) => p.R));
  const sizeRows = per.map((p) => [label(p.ds), ...fams.map((f) => {
    const mine = p.R.filter((r) => r.family === f);
    if (!mine.length) return "—";
    const comp = mine[mine.length - 1].compression;
    return `${fmtMb(median(mine.map((r) => r.mb)))}${comp && comp !== "SNAPPY" ? ` <span class="dim">${esc(comp.toLowerCase())}</span>` : ""}`;
  })]);
  const short = rc.length
    ? `Across writers, the whole table's size shows ${linkWord(median(rc))} to cold (ρ ${range(rc, rhoText)}) and ${linkWord(median(rw))} to warm (ρ ${range(rw, rhoText)}); the column sizes are the sharper signal.`
    : "Not enough runs with a size and timings yet.";
  const html = `
    <p class="claim">${rc.length ? `Across writers, the whole table's size shows ${linkWord(median(rc))} to the cold query (ρ ${range(rc, rhoText)} per dataset) and ${linkWord(median(rw))} to the warm one (ρ ${range(rw, rhoText)}). It is weaker than the column-level link because one big, rarely-queried column inflates a table without slowing the queries that skip it.` : short}</p>
    <p>The whole table's size is the sum of the column sizes in the column finding, so it shows the same thing with less detail. Its value is a quick check of a writer before looking at individual columns. Compression is listed where it is not Snappy: Fabric Warehouse writes uncompressed parquet, so its files are bigger on disk than its encodings would suggest.</p>
    ${legend(fams)}
    ${facets(per.map((p) => ({ title: `${label(p.ds)} · ρ cold ${rhoText(p.rc)} · warm ${rhoText(p.rw)}`, html: scatter({
      points: p.R.map((r) => ({ x: r.mb, y: r.dl.cold, family: r.family, filled: r.reorders,
        tip: runTip(r, ` · ${fmtMb(r.mb)} · ${r.compression || "?"} · cold ${fmtMs(r.dl.cold)}`) })),
      x: { label: "table size on disk", log: true, fmt: fmtMb },
      y: { label: "cold query suite total", log: false, fmt: fmtMs }, w: 420, h: 270, trend: true }) })))}
    ${table(["median table size", ...fams.map((f) => esc(fam(f).label))], sizeRows, "num")}
    ${table(["dataset", "ρ size vs cold", "ρ size vs warm", "ρ size vs Direct Lake read CU"],
      per.map((p) => [label(p.ds), rhoText(p.rc), rhoText(p.rw), rhoText(p.rcu)]), "num")}`;
  const st = rc.length ? (["strong", "moderate"].includes(strength(median(rc))) ? "clear" : "noise") : "few";
  return { id: "table-size", title: "Table size and compression", short, status: st, html,
    head: [rc.length ? strength(median(rc)) : "—", "link between whole-table size and speed"],
    card: "The size of the whole table says less than the size of the columns your queries actually touch." };
}

export function findDirectQuery(ctx) {
  const per = dsList(ctx).map((ds) => {
    const R = ctx.runs.filter((r) => r.ds === ds && r.dl.warm && r.dq.warm);
    const byW = {};
    for (const r of ctx.runs.filter((r) => r.ds === ds && r.dq.warm)) (byW[r.writer] = byW[r.writer] || []).push(r);
    const v = verdict(Object.fromEntries(Object.entries(byW).map(([w, rs]) => [w, rs.map((r) => r.dq.warm)])));
    const vDl = verdict(Object.fromEntries(Object.entries(byW).map(([w, rs]) => [w, rs.map((r) => r.dl.warm).filter(Boolean)])));
    return { ds, R, byW, v, vDl,
      cold: median(R.map((r) => r.dq.cold / r.dl.cold).filter(Number.isFinite)),
      warm: median(R.map((r) => r.dq.warm / r.dl.warm)) };
  }).filter((p) => p.R.length);
  const warm = per.map((p) => p.warm);
  const winners = per.filter((p) => p.v.status === "clear");
  const wlabel = (p, id) => (p.byW[id] || [{}])[0].writerLabel || id;
  // Which query shapes suffer most: median DQ ÷ DL warm per query, over runs carrying both.
  const queries = {};
  for (const p of per) for (const r of p.R) for (const [q, t] of Object.entries(r.timings.dq)) {
    const dl = r.timings.dl[q];
    if (t && dl && t.warm_ms > 0 && dl.warm_ms > 0) ((queries[p.ds] = queries[p.ds] || {})[q] = queries[p.ds][q] || []).push(t.warm_ms / dl.warm_ms);
  }
  const qRows = [];
  for (const [ds, qs] of Object.entries(queries)) {
    const sorted = Object.entries(qs).map(([q, v]) => [q, median(v)]).sort((a, b) => b[1] - a[1]);
    qRows.push([label(ds), sorted.slice(0, 3).map(([q, x]) => `<code>${esc(q)}</code> ${fmtX(x)}`).join("<br>"),
      sorted.slice(-3).reverse().map(([q, x]) => `<code>${esc(q)}</code> ${fmtX(x)}`).join("<br>")]);
  }
  const short = per.length
    ? `DirectQuery was ${range(warm, fmtX)} slower than Direct Lake on the same warm queries, whoever wrote the parquet${winners.length ? `; within DirectQuery, ${winners.map((p) => `${wlabel(p, p.v.best)} won clearly on ${label(p.ds)}`).join(", ")}` : ""}.`
    : "No run carries both Direct Lake and DirectQuery times yet.";
  const html = `
    <p class="claim">${per.length ? `The same warm queries took <b>${range(warm, fmtX)}</b> longer through DirectQuery than through Direct Lake, median per dataset. ${winners.length ? `Among writers in DirectQuery, ${winners.map((p) => `<b>${esc(wlabel(p, p.v.best))}</b> was clearly fastest on ${esc(label(p.ds))}`).join("; ")}; everywhere else the writers were inside the noise.` : "No writer was clearly faster than another in DirectQuery."}` : short}</p>
    <p>DirectQuery sends every visual's query to the SQL analytics endpoint, which reads the parquet each time, so there is no in-memory copy to warm up. That is why its cold and warm times sit close together. The parquet layout still matters there, through the endpoint's own scan.</p>
    ${table(["dataset", "runs", "DirectQuery ÷ Direct Lake, cold", "DirectQuery ÷ Direct Lake, warm", "fastest writer, DirectQuery warm", "fastest writer, Direct Lake warm"],
      per.map((p) => [label(p.ds), fmtInt(p.R.length), fmtX(p.cold), fmtX(p.warm),
        p.v.best ? `${esc(wlabel(p, p.v.best))} ${badge(p.v.status)}` : badge("few"),
        p.vDl.best ? `${esc(wlabel(p, p.vDl.best))} ${badge(p.vDl.status)}` : badge("few")]), "num")}
    <h4>Which queries suffer most</h4>
    ${table(["dataset", "largest DirectQuery penalty", "smallest"], qRows)}`;
  return { id: "directquery", title: "Direct Lake against DirectQuery", short, status: per.length ? "clear" : "few", html,
    head: [per.length ? range(warm, fmtX) : "—", "slower in DirectQuery than in Direct Lake"],
    card: "DirectQuery reads the files again on every query, so it is much slower than Direct Lake, whichever engine wrote them." };
}

export function findReadCu(ctx) {
  const per = dsList(ctx).map((ds) => {
    const R = ctx.runs.filter((r) => r.ds === ds);
    const byW = {};
    for (const r of R) (byW[r.writer] = byW[r.writer] || []).push(r);
    const groups = (phase) => Object.fromEntries(Object.entries(byW)
      .map(([w, rs]) => [w, rs.map((r) => r.cu[phase]).filter(Boolean)]).filter(([, v]) => v.length));
    const both = R.filter((r) => r.cu.dl && r.cu.dq);
    const C = R.filter((r) => r.cu.dl && r.dl.cold && r.dl.warm);
    return { ds, R, byW, vDl: verdict(groups("dl")), vDq: verdict(groups("dq")), gDl: groups("dl"), gDq: groups("dq"),
      ratio: median(both.map((r) => r.cu.dq / r.cu.dl)),
      rCold: spearman(C.map((r) => r.dl.cold), C.map((r) => r.cu.dl)),
      rWarm: spearman(C.map((r) => r.dl.warm), C.map((r) => r.cu.dl)) };
  }).filter((p) => Object.keys(p.gDl).length);
  const ratios = per.map((p) => p.ratio).filter(Boolean);
  const rc = per.map((p) => p.rCold).filter((x) => x !== null), rw = per.map((p) => p.rWarm).filter((x) => x !== null);
  const wl = (p, id) => ((p.byW[id] || [{}])[0]).writerLabel || id;
  const dlWins = per.filter((p) => p.vDl.status === "clear");
  const short = per.length
    ? `Reading through DirectQuery cost ${range(ratios, fmtX)} the CU of Direct Lake. In Direct Lake the bill shows ${linkWord(median(rc))} to the cold load (ρ ${range(rc, rhoText)}) and ${linkWord(median(rw))} to the warm queries (ρ ${range(rw, rhoText)}).`
    : "The CU ledger has not read any benchmark session yet.";
  const writers = [...new Set(per.flatMap((p) => Object.keys(p.gDl)))];
  const wlabel = Object.fromEntries(per.flatMap((p) => Object.entries(p.byW).map(([w, rs]) => [w, rs[0].writerLabel])));
  const stripRows = per.flatMap((p, i) => Object.entries(p.gDl)
    .sort((a, b) => median(a[1]) - median(b[1]))
    .map(([w, v], j) => ({ label: `${label(p.ds)} · ${wlabel[w]}`, values: v, family: p.byW[w][0].family,
      filled: p.byW[w][0].reorders, gap: i > 0 && j === 0 })));
  const html = `
    <p class="claim">${per.length ? `The same queries cost <b>${range(ratios, fmtX)} the capacity units</b> through DirectQuery as through Direct Lake. In Direct Lake, a run's CU shows ${linkWord(median(rc))} to its cold load (ρ ${range(rc, rhoText)} per dataset) and ${linkWord(median(rw))} to its warm queries (ρ ${range(rw, rhoText)}): where the link holds, the transcode is what you pay for, and the layout that loads fast is also the one that bills less. ${dlWins.length ? `Cheapest writer in Direct Lake, clearly: ${dlWins.map((p) => `<b>${esc(wl(p, p.vDl.best))}</b> on ${esc(label(p.ds))}`).join(", ")}.` : "No writer was clearly cheapest in Direct Lake on its own."}` : short}</p>
    <p>A capacity unit is what a Fabric capacity bills. These are the CU spent by each benchmark session's own items: the semantic model, the shortcut lakehouse it reads through, and that lakehouse's SQL endpoint. They are read from the Capacity Metrics app after the run. The figure covers the whole session (cold, warm and hot passes together), so it has no cold/warm split of its own. Building the tables is not included.</p>
    ${legend(familiesIn(per.flatMap((p) => p.R)))}
    <h4>Direct Lake read CU per writer</h4>
    ${strips({ rows: stripRows, x: { label: "Direct Lake read CU, whole session", fmt: fmtCu } })}
    ${facets(per.map((p) => ({ title: `${label(p.ds)} · CU against cold · ρ ${rhoText(p.rCold)}`, html: scatter({
      points: p.R.filter((r) => r.cu.dl && r.dl.cold).map((r) => ({ x: r.dl.cold, y: r.cu.dl, family: r.family, filled: r.reorders,
        tip: runTip(r, ` · cold ${fmtMs(r.dl.cold)} · ${fmtCu(r.cu.dl)}`) })),
      x: { label: "cold query suite total", log: true, fmt: fmtMs },
      y: { label: "Direct Lake read CU", log: true, fmt: fmtCu }, w: 420, h: 270, trend: true }) })))}
    ${table(["dataset", ...writers.map((w) => esc(wlabel[w])), "cheapest, Direct Lake", "cheapest, DirectQuery", "DirectQuery ÷ Direct Lake CU"],
      per.map((p) => [label(p.ds), ...writers.map((w) => {
        const dl = p.gDl[w], dq = p.gDq[w];
        return dl ? `${fmtCu(median(dl))}${dq ? ` <span class="dim">· DQ ${fmtCu(median(dq))}</span>` : ""}` : "—";
      }), p.vDl.best ? `${esc(wl(p, p.vDl.best))} ${badge(p.vDl.status)}` : "—",
      p.vDq.best ? `${esc(wl(p, p.vDq.best))} ${badge(p.vDq.status)}` : "—", fmtX(p.ratio)]), "num")}`;
  return { id: "read-cu", title: "What reading costs", short, status: per.length ? "clear" : "few", html,
    head: [ratios.length ? range(ratios, fmtX) : "—", "more capacity to answer the same queries in DirectQuery"],
    card: "DirectQuery costs far more CU than Direct Lake. In Direct Lake, most of the bill is cold." };
}

// --------------------------------------------------------------------------------- frame sections

const DIAGRAM = `<svg viewBox="0 0 760 250" class="diagram" role="img" aria-label="How Direct Lake and DirectQuery read parquet">
  <defs><marker id="arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0L10,5L0,10Z" fill="var(--dim)"/></marker></defs>
  <rect x="10" y="78" width="130" height="94" rx="8" class="box"/>
  <text x="75" y="104" class="dt" text-anchor="middle">parquet in OneLake</text>
  <text x="75" y="124" class="ds" text-anchor="middle">row groups</text>
  <text x="75" y="140" class="ds" text-anchor="middle">encodings · dictionary</text>
  <text x="75" y="156" class="ds" text-anchor="middle">sort order</text>
  <text x="200" y="22" class="dh">Direct Lake</text>
  <path d="M140,105 C190,105 190,55 240,55" class="flow" marker-end="url(#arr)"/>
  <rect x="240" y="30" width="160" height="50" rx="8" class="box hl"/>
  <text x="320" y="51" class="dt" text-anchor="middle">transcode, first touch</text>
  <text x="320" y="68" class="ds" text-anchor="middle">this is COLD</text>
  <path d="M400,55 L450,55" class="flow" marker-end="url(#arr)"/>
  <rect x="450" y="30" width="150" height="50" rx="8" class="box"/>
  <text x="525" y="51" class="dt" text-anchor="middle">VertiPaq segments</text>
  <text x="525" y="68" class="ds" text-anchor="middle">resident in memory</text>
  <path d="M600,55 L650,55" class="flow" marker-end="url(#arr)"/>
  <rect x="650" y="30" width="100" height="50" rx="8" class="box"/>
  <text x="700" y="51" class="dt" text-anchor="middle">queries</text>
  <text x="700" y="68" class="ds" text-anchor="middle">WARM, HOT</text>
  <text x="200" y="152" class="dh">DirectQuery</text>
  <path d="M140,145 C190,145 190,195 240,195" class="flow" marker-end="url(#arr)"/>
  <rect x="240" y="170" width="360" height="50" rx="8" class="box"/>
  <text x="420" y="191" class="dt" text-anchor="middle">SQL analytics endpoint reads the parquet</text>
  <text x="420" y="208" class="ds" text-anchor="middle">on every query, nothing kept in memory</text>
  <path d="M600,195 L650,195" class="flow" marker-end="url(#arr)"/>
  <rect x="650" y="170" width="100" height="50" rx="8" class="box"/>
  <text x="700" y="191" class="dt" text-anchor="middle">queries</text>
  <text x="700" y="208" class="ds" text-anchor="middle">every one pays</text>
</svg>`;

function methodSection(ctx) {
  const rows = dsList(ctx).map((ds) => {
    const R = ctx.runs.filter((r) => r.ds === ds);
    const p = ctx.perDataset[ds] || {};
    const writers = [...new Set(R.map((r) => r.writerLabel))].sort();
    return [`<b>${esc(label(ds))}</b><br><span class="dim">${esc(DATASETS[ds].blurb)}</span>`,
      fmtRows(p.rows), fmtInt(R.length), fmtInt(R.filter((r) => r.dl.cold).length), fmtInt((p.queriesDl || []).length),
      esc(writers.join(", "))];
  });
  return `
    <p>The same source data is written by four engines: <b>delta-rs</b> (through duckrun, DuckDB's dbt adapter for Delta), <b>Spark</b> (Fabric, with and without V-Order), <b>Fabric Warehouse</b>, and <b>DuckDB writing Iceberg</b>. One Power BI semantic model, the same for every writer, is then deployed over each output in Direct Lake mode and in DirectQuery mode. The same DAX suite runs against it six times in a row:</p>
    <ul>
      <li><b>cold</b>: pass 1, right after the model is created, so every column is transcoded from parquet;</li>
      <li><b>warm</b>: pass 2, with the columns that pass 1 touched now in memory;</li>
      <li><b>hot</b>: passes 3 to 6, median.</li>
    </ul>
    <p><b>Read CU</b> is the capacity units each session's own items billed: the semantic model, the shortcut lakehouse it reads through, and that lakehouse's SQL endpoint, read from the Capacity Metrics app. It covers the whole session (all six passes), in Direct Lake and in DirectQuery separately. The CU spent building the tables is not part of this report.</p>
    <p>A total is the sum over the queries a dataset's runs have in common. Runs are compared only within a dataset and only at the same source row count. A difference is called <b>clear</b> only when every run of the winner beat every run of the loser, with at least ${MIN_RUNS} runs each. Otherwise it is <b>inside the noise</b> or <b>too few runs</b>, because one Fabric capacity runs everything and its load varies from run to run.</p>
    <p>A writer changes several things at once: encoding, sort, row-group size, compression. Two findings hold the writer fixed (row-group size, and sorted against unsorted delta-rs) and one holds it to a single switch (Spark with V-Order on and off). Everything else is a correlation across writers, labelled as one.</p>
    ${table(["dataset", "rows", "runs", "timed runs", "queries", "writers"], rows)}`;
}

// ------------------------------------------------------------------------------------- the page

// Card order on the home page: the frame first (cold vs warm, what reading costs), then the layout
// knobs from strongest evidence to weakest, then the DirectQuery comparison.
export const FINDINGS = [findColdWarm, findReadCu, findColumnSize, findRowGroups, findVOrder, findSort,
  findWriterBlind, findDictionary, findDirectQuery];
// Reachable by link (`?finding=table-size`) but not a card: whole-table size is the column-size
// finding with less detail, and its link to speed is weak.
export const EXTRA_FINDINGS = [findTableSize];

export function context(records, opts = {}, ledger = null) {
  const b = build(records, PROBE_COLUMNS, ledger);
  const datasets = Object.keys(b.perDataset).filter((ds) => !opts.dataset || ds === opts.dataset);
  return { ...b, datasets };
}

function dataSection(ctx) {
  const R = ctx.runs.filter((r) => ctx.datasets.includes(r.ds));
  const head = ["dataset", "writer", "run", "date", "files", "row groups", "rows / RG", "size", "compression",
    "DL cold", "DL warm", "DL hot", "DQ cold", "DQ warm", "DL read CU", "DQ read CU"];
  const rows = R.map((r) => [label(r.ds), esc(r.writerLabel),
    r.url ? `<a href="${esc(r.url)}">${esc(r.id)}</a>` : esc(r.id), esc(r.when), fmtInt(r.files), fmtInt(r.rgs),
    fmtRows(r.rowsPerRg), fmtMb(r.mb), esc(r.compression || "—"),
    fmtMs(r.dl.cold), fmtMs(r.dl.warm), fmtMs(r.dl.hot), fmtMs(r.dq.cold), fmtMs(r.dq.warm), fmtCu(r.cu.dl), fmtCu(r.cu.dq)]);
  const ex = ctx.excluded.filter((r) => ctx.datasets.includes(r.ds));
  return `
    <p>Every run behind the charts. Click a column heading to sort, or download the flat tables.</p>
    <p><button type="button" data-csv="runs">Download runs (CSV)</button> <button type="button" data-csv="columns">Download columns (CSV)</button></p>
    ${table(head, rows, "num sortable")}
    ${ex.length ? `<p class="note">${ex.length} run${ex.length === 1 ? " was" : "s were"} left out because ${ex.length === 1 ? "it was" : "they were"} measured on a smaller copy of the source: ${ex.map((r) => `${esc(r.writerLabel)} ${esc(r.id)} (${esc(r.reason)})`).join("; ")}.</p>` : ""}`;
}

export function csvOf(rows, keys) {
  const cell = (v) => {
    const s = v === null || v === undefined ? "" : typeof v === "object" ? JSON.stringify(v) : String(v);
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  return [keys.join(","), ...rows.map((r) => keys.map((k) => cell(k.split(".").reduce((o, p) => (o || {})[p], r))).join(","))].join("\n");
}
export const RUN_KEYS = ["ds", "writerLabel", "id", "when", "engine", "vorder", "sorted", "rgSetting", "rows",
  "files", "rgs", "rowsPerRg", "mb", "compression", "dl.cold", "dl.warm", "dl.hot", "dq.cold", "dq.warm", "dq.hot",
  "cu.dl", "cu.dq"];
export const COLUMN_KEYS = ["ds", "writerLabel", "runId", "column", "type", "mb", "encodings", "dictPages",
  "chunks", "runsPer1k", "cold", "warm", "coldRaw", "warmRaw"];

const HOW = `
  <p>Direct Lake does not import data. On a column's first use it reads that column's parquet and transcodes it into VertiPaq's own segments, one per row group, and keeps them in memory. Everything about the file that makes that read cheaper (smaller columns, dictionaries it can reuse, long runs of equal values, fewer segments) pays off at that moment, in time and in capacity units. After it, queries run against VertiPaq's copy. DirectQuery keeps nothing and goes back to the parquet through the SQL endpoint for every query.</p>
  ${DIAGRAM}`;

const PAGES = { method: "Method", how: "How Power BI reads parquet", data: "The data" };

/** `?a=b&c=d` from the params that are set, so a link keeps the dataset it was clicked under. */
function href(params) {
  const q = Object.entries(params).filter(([, v]) => v).map(([k, v]) => `${k}=${encodeURIComponent(v)}`);
  return q.length ? `?${q.join("&")}` : "?";
}

function card(f, i, opts) {
  const [big, sub] = f.head || ["—", ""];
  return `<a class="card" href="${href({ finding: f.id, dataset: opts.dataset })}">
    <div class="ctop"><span class="cnum">${i + 1}</span>${badge(f.status)}</div>
    <div class="big">${esc(big)}</div><div class="bigsub">${esc(sub)}</div>
    <h3>${esc(f.title)}</h3><p>${esc(f.card || f.short)}</p>
    <span class="more">See the evidence →</span></a>`;
}

/**
 * The page: the home grid of finding cards, one finding (`?finding=<id>`), or one of the frame pages
 * (`?page=method|how|data`). Every view is computed from the same context, so a card's headline and
 * the detail behind it can never disagree.
 */
export function render(records, opts = {}, ledger = null) {
  const ctx = context(records, opts, ledger);
  if (!ctx.runs.length) return { html: `<p class="empty">No run records found.</p>`, ctx };
  const found = FINDINGS.map((f) => f(ctx));
  const extra = EXTRA_FINDINGS.map((f) => f(ctx));
  const here = { finding: opts.finding, page: opts.page };
  const switcher = `<nav class="switch">Dataset: ${[["", "all"], ...Object.keys(ctx.perDataset).map((d) => [d, label(d)])]
    .map(([d, l]) => (d === (opts.dataset || "") ? `<b>${esc(l)}</b>` : `<a href="${href({ ...here, dataset: d })}">${esc(l)}</a>`)).join(" · ")}</nav>`;
  const back = `<a class="back" href="${href({ dataset: opts.dataset })}">← All findings</a>`;
  const frame = `<p class="framelinks">${Object.entries(PAGES).map(([k, v]) =>
    `<a href="${href({ page: k, dataset: opts.dataset })}">${esc(v)}</a>`).join(" · ")}</p>`;
  const idx = found.findIndex((f) => f.id === opts.finding);
  const side = extra.find((f) => f.id === opts.finding);
  let body;
  if (side) {
    body = `${back}${switcher}<section id="${side.id}" class="finding"><h2>${esc(side.title)} ${badge(side.status)}</h2>${side.html}</section>`;
  } else if (idx >= 0) {
    const f = found[idx];
    const prev = found[idx - 1], next = found[idx + 1];
    const pager = `<nav class="pager">${prev ? `<a href="${href({ finding: prev.id, dataset: opts.dataset })}">← ${esc(prev.title)}</a>` : "<span></span>"}${next ? `<a href="${href({ finding: next.id, dataset: opts.dataset })}">${esc(next.title)} →</a>` : ""}</nav>`;
    body = `${back}${switcher}<section id="${f.id}" class="finding"><h2><span class="num">${idx + 1}</span>${esc(f.title)} ${badge(f.status)}</h2>${f.html}</section>${pager}`;
  } else if (PAGES[opts.page]) {
    const inner = opts.page === "method" ? methodSection(ctx) : opts.page === "how" ? HOW : dataSection(ctx);
    body = `${back}${switcher}<section id="${opts.page}"><h2>${esc(PAGES[opts.page])}</h2>${inner}</section>`;
  } else {
    body = `${switcher}<div class="cards">${found.map((f, i) => card(f, i, opts)).join("")}</div>${frame}
      <p class="note">Also measured: <a href="${href({ finding: "table-size", dataset: opts.dataset })}">whole-table size and compression</a>, a weaker version of the column-size finding.</p>`;
  }
  return { html: body, ctx, found };
}

// ----------------------------------------------------------------------------------------- boot

function wire(root, doc, ctx) {
  const tip = doc.createElement("div");
  tip.className = "tooltip";
  doc.body.appendChild(tip);
  root.addEventListener("mouseover", (e) => {
    const g = e.target.closest && e.target.closest("[data-tip]");
    if (!g) return;
    tip.textContent = g.getAttribute("data-tip");
    tip.style.display = "block";
  });
  root.addEventListener("mousemove", (e) => {
    if (tip.style.display !== "block") return;
    const x = Math.min(e.clientX + 14, doc.documentElement.clientWidth - tip.offsetWidth - 8);
    tip.style.left = `${x}px`;
    tip.style.top = `${e.clientY + 14}px`;
  });
  root.addEventListener("mouseout", (e) => {
    if (e.target.closest && e.target.closest("[data-tip]")) tip.style.display = "none";
  });
  root.addEventListener("click", (e) => {
    const btn = e.target.closest && e.target.closest("[data-csv]");
    if (btn) {
      const which = btn.getAttribute("data-csv");
      const text = which === "runs" ? csvOf(ctx.runs.filter((r) => ctx.datasets.includes(r.ds)), RUN_KEYS)
        : csvOf(ctx.columns.filter((c) => ctx.datasets.includes(c.ds)), COLUMN_KEYS);
      const a = doc.createElement("a");
      a.href = URL.createObjectURL(new Blob([text], { type: "text/csv" }));
      a.download = `${which}.csv`;
      a.click();
      return;
    }
    const th = e.target.closest && e.target.closest("table.sortable th");
    if (!th) return;
    const tbl = th.closest("table"), idx = [...th.parentNode.children].indexOf(th);
    const dir = th.dataset.dir === "asc" ? "desc" : "asc";
    for (const h of th.parentNode.children) delete h.dataset.dir;
    th.dataset.dir = dir;
    const key = (tr) => {
      const t = tr.children[idx].textContent.trim();
      const n = parseFloat(t.replace(/,/g, ""));
      const unit = /\bGB\b/.test(t) ? 1024 : /\bs$/.test(t) && !/ms$/.test(t) ? 1000 : /M$/.test(t) ? 1e6 : /K$/.test(t) ? 1e3 : /B$/.test(t) ? 1e9 : 1;
      return Number.isFinite(n) ? n * unit : t;
    };
    const body = tbl.tBodies[0];
    [...body.rows].sort((a, b) => {
      const x = key(a), y = key(b);
      const c = typeof x === "number" && typeof y === "number" ? x - y : String(x).localeCompare(String(y));
      return dir === "asc" ? c : -c;
    }).forEach((r) => body.appendChild(r));
  });
}

export async function boot(doc = document, loc = location) {
  const app = doc.getElementById("app"), status = doc.getElementById("status");
  const q = new URLSearchParams(loc.search);
  const opts = { dataset: DATASETS[q.get("dataset")] ? q.get("dataset") : "",
    finding: q.get("finding") || "", page: q.get("page") || "",
    repo: q.get("repo") || DEFAULTS.repo, ref: q.get("ref") || DEFAULTS.ref };
  try {
    let records, ledger, note;
    const snap = doc.getElementById("snapshot");
    if (snap && snap.textContent.trim()) {
      const s = JSON.parse(snap.textContent);
      records = s.records;
      ledger = s.ledger;
      note = `Offline copy, frozen at ${s.built || "?"}.`;
    } else {
      if (status) status.textContent = "Reading the run records from GitHub…";
      ({ records, ledger } = await loadRemote(opts));
      note = `Live: ${records.length} run records read from ${opts.repo}@${opts.ref}. Reload for new runs.`;
    }
    const { html, ctx } = render(records, opts, ledger);
    app.innerHTML = html;
    if (status) status.textContent = note;
    wire(app, doc, ctx);
  } catch (ex) {
    app.innerHTML = `<p class="empty"><b>Could not read the run records.</b> ${esc(ex && ex.message || ex)}. ` +
      `The page reads <code>history/runs/</code> from raw.githubusercontent.com at view time.</p>`;
    if (status) status.textContent = "";
  }
}

if (typeof document !== "undefined" && document.getElementById("app")) boot();
