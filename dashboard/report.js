// The report: what in a parquet file decides how fast Power BI reads it. Every sentence that states a
// number is generated from `history/runs/*.json` at view time, so the page rewrites itself after
// every Benchmark run and can never quote a number the records no longer support.
//
// The home page is one card per finding — a headline number, the claim, a status — and a card opens
// that finding's evidence (`?finding=<id>`). Method, how Power BI reads parquet and the raw data are
// pages of their own (`?page=`). Speed AND read CU: what the layout costs to query is half the answer.
import { DATASETS, MIN_RUNS, WRITERS, build, median, spearman, strength, verdict, fmtInt, fmtMs, fmtMb, fmtX, fmtRows, fmtCu, loadRemote, DEFAULTS } from "./data.js";
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

export function bandTrend(pts, minDatasets = 2) {
  // One dataset alone has a fraction of the runs, so an octave counts there from `MIN_RUNS` runs.
  const minRuns = minDatasets > 1 ? BAND_MIN_RUNS : MIN_RUNS;
  const lines = datasetLines(pts);
  const ks = [...new Set(Object.values(lines).flatMap((rows) => rows.map((x) => x.k)))].sort((a, b) => a - b);
  const rows = ks.map((k) => {
    const per = Object.values(lines).map((rs) => rs.find((x) => x.k === k)).filter(Boolean);
    return { k, x: octaveRows(k), n: per.reduce((t, x) => t + x.n, 0), datasets: per.length,
      cold: median(per.map((x) => x.cold)), warm: median(per.map((x) => x.warm)) };
  }).filter((x) => x.datasets >= minDatasets);
  if (rows.length < 2) return null;
  for (const x of rows) x.thin = x.n < minRuns;
  const trusted = rows.filter((x) => !x.thin);
  // Runs of consecutive trusted octaves where both tiers are within `BAND_WITHIN` of the median.
  const runs = [];
  for (const x of trusted) {
    const pass = Math.max(x.cold, x.warm) <= BAND_WITHIN;
    const last = runs[runs.length - 1];
    if (pass && last && last.open && x.k === last.to.k + 1) last.to = x;
    else if (pass) runs.push({ from: x, to: x, open: true });
    else if (last) last.open = false;
  }
  // A band is claimed only when it is ONE stretch and something outside it is worse. Two separate
  // stretches, or every octave passing, means the data cannot place a band yet, and the page says so.
  const band = runs.length === 1 && runs[0].to.k - runs[0].from.k + 1 < trusted.length ? runs[0] : null;
  const worst = trusted.length ? Math.max(...trusted.map((x) => Math.max(x.cold, x.warm))) : null;
  return { rows, band, trusted, worst };
}

const octaveText = (x) => fmtRows(x).replace(/\.0(?=[KMB])/, "");
export const bandText = (t) => (!t || !t.band ? null : t.band.from.k === t.band.to.k ? `around ${octaveText(t.band.from.x)}`
  : `${octaveText(t.band.from.x)} to ${octaveText(t.band.to.x)}`);

/** Per dataset: its own octaves (at least two runs each), cold and warm against its median run. */
export function datasetLines(pts) {
  const out = {};
  for (const ds of DS_ORDER) {
    const rows = octaves(pts.filter((p) => p.ds === ds)).filter((x) => x.n >= 2);  // k, x, n, cold, warm
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

export function bandPanels(pts, t, perDataset = true) {
  if (!t) return `<p class="empty">Not enough runs at different row-group sizes yet.</p>`;
  const lines = perDataset ? datasetLines(pts) : {};
  const xs = [...Object.values(lines).flatMap((rows) => rows.map((x) => x.x)), ...t.rows.map((x) => x.x)];
  const xr = [Math.min(...xs) / 1.6, Math.max(...xs) * 1.6];
  return `<div class="facets two">${["cold", "warm"].map((tier) => `<figure class="facet">${bandPanelSvg(lines, t, tier, xr)}</figure>`).join("")}</div>`;
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

const STATUS = { clear: "clear", noise: "inside the noise", few: "too few datasets", partial: "partly measured",
  none: "no measurable effect", parity: "same within noise" };
const badge = (s) => `<span class="badge ${s}">${STATUS[s]}</span>`;
const rhoText = (r) => (r === null ? "—" : `${r >= 0 ? "" : "−"}${Math.abs(r).toFixed(2)}`);
const range = (xs, f) => {
  const v = xs.filter((n) => typeof n === "number" && Number.isFinite(n));
  if (!v.length) return "—";
  const lo = Math.min(...v), hi = Math.max(...v);
  return f(lo) === f(hi) ? f(lo) : `${f(lo)}–${f(hi)}`;
};
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
  const short = `Direct Lake loads a column chunk two ways: a cheap remap when the chunk is entirely dictionary-encoded AND its footer's encoding_stats say so, or hashing every value when not. ${gaps.length ? `${fmtInt(gaps.length)} column-runs had chunks with no dictionary page at all.` : ""} ${measured.length ? "" : "The records do not yet capture the footer declaration."}`;
  const html = `
    <p class="claim">Direct Lake transcodes a parquet column chunk one of <b>two ways</b>. If every page of the chunk is dictionary-encoded and the footer's <code>encoding_stats</code> prove it, it <b>remaps</b> the parquet dictionary IDs straight onto VertiPaq's: a cheap ID swap. Otherwise it <b>hashes every value</b> into a new dictionary. That includes a chunk that does have a dictionary but whose footer does not declare it.</p>
    <p>The footer's <code>encodings</code> list cannot settle it. A dictionary-encoded chunk lists <code>PLAIN</code> for its dictionary page, and so does one whose pages fell back to <code>PLAIN</code> midway. Only <code>encoding_stats</code> (<code>PageEncodingStats</code>) says which pages are which, without decoding them. How much the declaration is worth: <a href="https://github.com/duckdb/duckdb/pull/24957">duckdb#24957</a> added it to DuckDB's writer and measured a 142M-row dictionary string column's cold first-touch falling from <b>10,857.5 ms to 689.3 ms</b>. Same pages, one footer list.</p>
    ${measured.length ? `<h4>Measured: remap against hashing</h4>
    <p>${gap !== null ? `Columns whose every chunk was declared fully dictionary-encoded loaded at a median <b>${fmtX(median(remapRel))}</b> of that column's typical cold cost; columns on the hashing path at <b>${fmtX(median(hashRel))}</b>, ${gap >= 1 ? `${fmtX(gap)} slower` : `${fmtX(1 / gap)} faster`}, over ${fmtInt(remapRel.length)} and ${fmtInt(hashRel.length)} column-runs.` : `${fmtInt(measured.length)} column-runs carry the footer declaration, all on the ${remapRel.length ? "remap" : "hashing"} path, so there is nothing to compare yet.`}</p>
    ${table(["writer", "chunks that declare encoding_stats", "chunks declared fully dictionary-encoded"], declRows, "num")}` : ""}
    <h4>What the run records can and cannot say</h4>
    <p>Every run records, per column chunk, whether it has a dictionary page; a chunk with none is certainly on the hashing path. Runs from 2026-10-17 on also record the footer's <code>encoding_stats</code> (the table above); older runs do not, so for them a chunk with a dictionary page could be on either path. On DuckDB's own writer the declaration is measured directly: the iceberg leg's DuckDB wheel moved from <code>1.6.0.dev365</code> (before #24957) to <code>1.6.0.dev379</code> (after it) between two aemo runs that wrote the same 3 files, 53 row groups, 1,130 MB and the same dictionary pages on every column. Only the footer list changed.</p>
    ${table(["aemo, DuckDB → Iceberg", "dev365, undeclared", "dev379, declared", "change"], [
      ["cold, whole suite", "80.5 s", "40.7 s", "<b>2.0×</b>"],
      ["Direct Lake read CU", "7,923", "3,132", "<b>2.5×</b>"],
      ["warm, whole suite", "4.1 s", "3.0 s", "1.4×"],
      ["<code>DUID</code> cold, string, fully dictionary", "11,377 ms", "543 ms", "<b>21×</b>"],
      ["<code>mw</code> cold, 10 of 53 chunks <code>PLAIN</code>", "6,266 ms", "4,387 ms", "1.4×"]], "num")}
    <p>The fully dictionary-encoded columns collapse; <code>mw</code>, whose <code>PLAIN</code> chunks cannot be remapped whatever the footer says, barely moves. One run per arm (32444969823, 33700004854), with geometry and dictionary pages identical. This page used to say the declaration did not move the cost: it compared the 2026-10-17 run with 2026-09-03 runs that were already on <code>dev379</code>, i.e. the fix against itself.</p>
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

// ----------------------------------------------------------------------------- layout properties
//
// THE QUESTION: which layout properties matter, and by how much. Power BI reads parquet and sees only
// the layout, so every card is one PROPERTY of the files, never an engine.
//
// ONE METHOD, fixed before the numbers were looked at:
//   - continuous property: per dataset, least-squares slope of log2(metric) on log2(property), shown
//     as "×N per doubling" (2^slope);
//   - on/off property: per dataset, median(on) / median(off);
//   - the global number is the MEDIAN ACROSS DATASETS, so every dataset weighs the same however many
//     runs it has, and each card says how many datasets agree on the direction;
//   - an effect counts when the global number is beyond ±10% and most datasets agree; otherwise the
//     card says there is no measurable effect, which is a result too.
// Read CU leads: it is what the capacity pays. Cold follows; warm is context, fast with any layout.

export const EFFECT_MIN = 0.10;

/** Least-squares slope of log2(y) on log2(x); null below 5 points or when x barely moved. */
export function slopeLog2(xs, ys) {
  const p = xs.map((x, i) => [x, ys[i]]).filter(([x, y]) => x > 0 && y > 0);
  if (p.length < 5) return null;
  const X = p.map(([x]) => Math.log2(x)), Y = p.map(([, y]) => Math.log2(y));
  const mx = X.reduce((a, b) => a + b, 0) / X.length, my = Y.reduce((a, b) => a + b, 0) / Y.length;
  const sxx = X.reduce((s, x) => s + (x - mx) ** 2, 0);
  if (sxx < 0.5) return null;
  return X.reduce((s, x, i) => s + (x - mx) * (Y[i] - my), 0) / sxx;
}

/** `{ds: effect}` → the global summary every card prints. */
export function acrossDatasets(per) {
  const v = Object.entries(per).filter(([, e]) => e !== null && Number.isFinite(e));
  if (!v.length) return { med: null, n: 0, up: 0, down: 0 };
  const med = median(v.map(([, e]) => e));
  return { med, n: v.length, up: v.filter(([, e]) => e > 1).length, down: v.filter(([, e]) => e < 1).length };
}

/** Per dataset: metric per doubling of a run-level property. */
export function runSlope(ctx, prop, metric, keep = () => true) {
  const out = {};
  for (const ds of dsList(ctx)) {
    const R = ctx.runs.filter((r) => r.ds === ds && keep(r) && prop(r) > 0 && metric(r) > 0);
    const b = slopeLog2(R.map(prop), R.map(metric));
    out[ds] = b === null ? null : 2 ** b;
  }
  return acrossDatasets(out);
}

/** Per dataset: the median within-column slope, so a timestamp and a flag never share one line. */
export function columnSlope(ctx, prop, metric) {
  const out = {};
  for (const ds of dsList(ctx)) {
    const byCol = {};
    for (const c of ctx.columns) {
      if (c.ds === ds && prop(c) > 0 && metric(c) > 0) (byCol[c.column] = byCol[c.column] || []).push(c);
    }
    const bs = Object.values(byCol).map((cs) => slopeLog2(cs.map(prop), cs.map(metric))).filter((b) => b !== null);
    out[ds] = bs.length ? 2 ** median(bs) : null;
  }
  return acrossDatasets(out);
}

/** Per dataset: median(on) / median(off), at least two runs a side. */
export function ratioEffect(ctx, on, off, metric) {
  const out = {};
  for (const ds of dsList(ctx)) {
    const a = ctx.runs.filter((r) => r.ds === ds && on(r) && metric(r) > 0).map(metric);
    const b = ctx.runs.filter((r) => r.ds === ds && off(r) && metric(r) > 0).map(metric);
    out[ds] = a.length >= 2 && b.length >= 2 ? median(a) / median(b) : null;
  }
  return acrossDatasets(out);
}

const M = {
  cu: { label: "Direct Lake read CU", get: (r) => r.cu.dl },
  dq: { label: "DirectQuery read CU", get: (r) => r.cu.dq },
  cold: { label: "cold", get: (r) => r.dl.cold },
  warm: { label: "warm", get: (r) => r.dl.warm },
  mb: { label: "table size", get: (r) => r.mb },
};

/** "×1.37" / "0.75×", or a dash. */
const eff = (s) => (s && s.med !== null ? (s.med >= 1 ? `×${s.med.toFixed(2)}` : `${s.med.toFixed(2)}×`) : "—");
/** "−25%" / "+37%". */
const pct = (s) => (s && s.med !== null ? `${s.med >= 1 ? "+" : "−"}${Math.abs((s.med - 1) * 100).toFixed(0)}%` : "—");
const agree = (s) => (s && s.n ? `${Math.max(s.up, s.down)} of ${s.n} datasets agree` : "not measurable");
const counts = (s) => !!s && s.med !== null && Math.abs(s.med - 1) > EFFECT_MIN && Math.max(s.up, s.down) * 2 > s.n && s.n >= 2;
const verdictOf = (s) => (!s || s.n < 2 ? "few" : counts(s) ? "clear" : "none");

const EVIDENCE = {
  controlled: "controlled: one switch, everything else equal",
  observational: "observational: compared across runs and engines",
  confounded: "confounded: moves together with the engine",
};
const evidenceTag = (k) => `<span class="evidence ${k}">${esc(EVIDENCE[k])}</span>`;

function effectTable(rows) {
  return table(["measure", "effect", "datasets"], rows.filter(([, s]) => s).map(([lab, s, unit]) =>
    [esc(lab), `<b>${eff(s)}</b>${unit ? ` <span class="dim">${esc(unit)}</span>` : ""} ${badge(verdictOf(s))}`, esc(agree(s))]), "num");
}

/** Each run divided by its dataset's median, for a property and a metric: one shared axis. */
function normalisedPoints(ctx, prop, metric, keep = () => true) {
  const pts = [];
  for (const ds of dsList(ctx)) {
    const R = ctx.runs.filter((r) => r.ds === ds && keep(r) && prop(r) > 0 && metric(r) > 0);
    if (R.length < 5) continue;
    const mp = median(R.map(prop)), mm = median(R.map(metric));
    for (const r of R) pts.push({ r, x: prop(r) / mp, y: metric(r) / mm });
  }
  return pts;
}

function normalisedScatter(ctx, prop, metric, xLabel, yLabel, keep) {
  const pts = normalisedPoints(ctx, prop, metric, keep);
  return scatter({ points: pts.map((p) => ({ x: p.x, y: p.y, family: p.r.family, filled: p.r.reorders,
    tip: `${p.r.writerLabel} · ${label(p.r.ds)} · ${xLabel} ${fmtX(p.x)} · ${yLabel} ${fmtX(p.y)}` })),
  x: { label: `${xLabel} ÷ its dataset's median`, log: true, fmt: fmtX },
  y: { label: `${yLabel} ÷ its dataset's median`, log: true, fmt: fmtX }, w: 640, h: 360, trend: true });
}

/** Strips of groups of runs, each run divided by its dataset's median over every group shown. */
function groupStrips(ctx, groups, metric, xLabel) {
  const rows = groups.map((g) => ({ ...g, values: [] }));
  for (const ds of dsList(ctx)) {
    const all = ctx.runs.filter((r) => r.ds === ds && metric(r) > 0 && groups.some((g) => g.keep(r)));
    if (all.length < 3) continue;
    const mm = median(all.map(metric));
    groups.forEach((g, i) => { for (const r of all) if (g.keep(r)) rows[i].values.push(metric(r) / mm); });
  }
  return strips({ rows: rows.filter((r) => r.values.length), x: { label: `${xLabel} ÷ its dataset's median`, fmt: fmtX } });
}

function propertyCard(o) {
  return { status: o.status, id: o.id, title: o.title, head: o.head, card: o.card, short: o.card,
    html: `<p class="claim">${o.claim}</p><p>${evidenceTag(o.evidence)}</p>${o.body}` };
}

// ---- the properties, ranked by what they do to read CU

export function propTableSize(ctx) {
  const s = { cu: runSlope(ctx, M.mb.get, M.cu.get), cold: runSlope(ctx, M.mb.get, M.cold.get),
    warm: runSlope(ctx, M.mb.get, M.warm.get), dq: runSlope(ctx, M.mb.get, M.dq.get),
    colCold: columnSlope(ctx, (c) => c.mb, (c) => c.cold), colWarm: columnSlope(ctx, (c) => c.mb, (c) => c.warmRaw) };
  return propertyCard({ id: "table-size", title: "Bytes on disk", evidence: "observational", status: verdictOf(s.cu),
    head: [s.cu.med !== null ? `${pct(s.cu)} CU` : "—", "Direct Lake read CU each time the table doubles in size"],
    card: "The fewer bytes on disk, the less capacity Power BI spends reading them: true of the whole table, and of each column cold.",
    claim: s.cu.med !== null ? `Each doubling of the table's size on disk cost <b>${pct(s.cu)} Direct Lake read CU</b> and ${pct(s.cold)} cold time (${agree(s.cu)}). Column by column it holds too: twice the bytes cost <b>${pct(s.colCold)} cold</b> (${agree(s.colCold)}), ${pct(s.colWarm)} warm.` : "Not measurable yet.",
    body: `${effectTable([["Direct Lake read CU, per doubling of the table", s.cu], ["cold, per doubling of the table", s.cold], ["warm, per doubling of the table", s.warm], ["DirectQuery read CU, per doubling of the table", s.dq],
      ["column cold, per doubling of the column", s.colCold], ["column warm, per doubling of the column", s.colWarm]])}
      <h3>The table</h3>
      ${legend(familiesIn(ctx.runs))}
      ${normalisedScatter(ctx, M.mb.get, M.cu.get, "table size", "Direct Lake read CU")}
      <h3>Each column, cold</h3>
      ${scatter({ points: ctx.columns.filter((c) => c.mb > 0 && c.cold > 0).map((c) => ({ x: c.mb, y: c.cold, family: c.family, filled: c.reorders,
        tip: `${c.column} · ${c.writerLabel} · ${fmtMb(c.mb)} · cold +${fmtMs(c.cold)}` })),
        x: { label: "column compressed size", log: true, fmt: fmtMb }, y: { label: "extra cold time to load it", log: true, fmt: fmtMs }, w: 640, h: 360, trend: true })}
      <p class="note">Table size is measured where it varied enough within a dataset to fit a slope; a dataset where every engine wrote a similar size does not vote. A column's cost is its probe query minus the row-count probe in the same pass; read CU is billed per session, not per column, so the column chart is cold time.</p>` });
}
export function propSort(ctx) {
  const s = { mb: columnSlope(ctx, (c) => c.runsPer1k, (c) => c.mb), cold: columnSlope(ctx, (c) => c.mb, (c) => c.cold) };
  return propertyCard({ id: "sort", title: "Sort order", evidence: "observational", status: verdictOf(s.mb),
    head: [s.mb.med !== null ? `${pct(s.mb)} size` : "—", "column size when equal values are split into twice as many runs"],
    card: "Sorting puts equal values together, so columns shrink, and smaller columns are cheaper to read.",
    claim: s.mb.med !== null ? `Within the same column, twice as many runs of repeated values meant <b>${pct(s.mb)} more bytes</b> (${agree(s.mb)}), and each doubling of a column's bytes cost ${pct(s.cold)} cold time (${agree(s.cold)}).` : "Not measurable yet.",
    body: `${effectTable([["column size, per doubling of runs", s.mb], ["column cold, per doubling of column size", s.cold]])}
      ${legend(familiesIn(ctx.columns))}
      ${scatter({ points: ctx.columns.filter((c) => c.runsPer1k > 0 && c.mb > 0).map((c) => ({ x: c.runsPer1k, y: c.mb, family: c.family, filled: c.reorders,
        tip: `${c.column} · ${c.writerLabel} · ${c.runsPer1k.toFixed(1)} runs per 1,000 rows · ${fmtMb(c.mb)}` })),
        x: { label: "runs of repeated values per 1,000 rows", log: true, fmt: (v) => (v >= 10 ? fmtInt(v) : v >= 1 ? v.toFixed(0) : String(+v.toPrecision(1))) },
        y: { label: "column compressed size", log: true, fmt: fmtMb }, w: 640, h: 360, trend: true })}` });
}

/** The one piece of engine advice on the page: in Spark, V-Order on. Controlled — same engine, one switch. */
export function propSparkVOrder(ctx) {
  const vo = (r) => r.writer === "spark_vorder", plain = (r) => r.writer === "spark_plain";
  const s = { cu: ratioEffect(ctx, vo, plain, M.cu.get), cold: ratioEffect(ctx, vo, plain, M.cold.get),
    warm: ratioEffect(ctx, vo, plain, M.warm.get), mb: ratioEffect(ctx, vo, plain, M.mb.get), dq: ratioEffect(ctx, vo, plain, M.dq.get) };
  const warmWorse = s.warm.med !== null && s.warm.med > 1 + EFFECT_MIN;
  return propertyCard({ id: "spark-vorder", title: "Spark: turn V-Order on", evidence: "controlled", status: verdictOf(s.cu),
    head: [s.cu.med !== null ? `${pct(s.cu)} CU` : "—", "Direct Lake read CU with V-Order on (readHeavyForPBI) against the default"],
    card: "If Spark writes your tables, use the readHeavyForPBI profile: cheaper to read, faster cold, smaller files.",
    claim: s.cu.med !== null ? `Same engine, same data, one switch: with V-Order on, Spark's tables cost <b>${pct(s.cu)} Direct Lake read CU</b> (${agree(s.cu)}), loaded ${pct(s.cold)} cold (${agree(s.cold)}) and took ${pct(s.mb)} on disk. DirectQuery's bill did not change (${pct(s.dq)}).${warmWorse ? ` The one cost: warm queries were ${pct(s.warm)} (${agree(s.warm)}), mixed across datasets, and warm is fast either way.` : ""}` : "Not measurable yet.",
    body: `${effectTable([["Direct Lake read CU", s.cu], ["cold", s.cold], ["warm", s.warm], ["table size", s.mb], ["DirectQuery read CU", s.dq]])}
      ${groupStrips(ctx, [
        { label: "Spark, V-Order", keep: vo, family: "spark", filled: true },
        { label: "Spark, no V-Order", keep: plain, family: "spark", filled: false }], M.cu.get, "Direct Lake read CU")}
      <p>In Fabric Spark, V-Order comes with the <code>readHeavyForPBI</code> resource profile; the workspace default, <code>writeHeavy</code>, leaves it off, and <code>readHeavyForSpark</code> does not turn it on despite its name. The profile also changes optimizeWrite's bin size, so file geometry moves with it. Build cost is not on this page; the repository's findings measured about +8% build CU.</p>` });
}

/** The controlled row-group pair: duckrun on aemo, same versions, same `date, time, price` sort, only
 *  `row_group_size` moved (auto, ~3.9M rows, 37 groups -> 131,072 rows, 1,099 groups). */
const RG_PAIR = { before: ["38998148550", "39024032584"], after: "39029139553" };

function rgControlled(ctx) {
  const before = RG_PAIR.before.map((id) => ctx.runs.find((r) => r.id === id)).filter(Boolean);
  const after = ctx.runs.find((r) => r.id === RG_PAIR.after);
  if (!before.length || !after) return null;
  const mid = (f) => median(before.map(f).filter((v) => v > 0));
  const row = (lab, f, fmt) => {
    const b = mid(f), a = f(after);
    return b > 0 && a > 0 ? [lab, fmt(b), fmt(a), `<b>${a >= b ? fmtX(a / b) : `÷${Math.round(b / a)}`}</b>`] : null;
  };
  const rows = [
    row("rows per row group", (r) => r.rowsPerRg, fmtRows),
    row("row groups", (r) => r.rgs, fmtInt),
    row("size on disk", (r) => r.mb, fmtMb),
    row("cold, whole suite", (r) => r.dl && r.dl.cold, fmtMs),
    row("warm, whole suite", (r) => r.dl && r.dl.warm, fmtMs),
    row("Direct Lake read CU", (r) => r.cu && r.cu.dl, fmtCu),
  ];
  const colCold = (id, col) => { const c = ctx.columns.find((x) => x.runId === id && x.column === col); return c ? c.cold : null; };
  const cols = [...new Set(ctx.columns.filter((c) => c.runId === after.id).map((c) => c.column))];
  const colRatios = [];
  for (const col of cols) {
    const b = median(RG_PAIR.before.map((id) => colCold(id, col)).filter((v) => v > 0)), a = colCold(after.id, col);
    if (b > 0 && a > 0) { rows.push([`<code>${esc(col)}</code> cold`, fmtMs(b), fmtMs(a), `<b>${fmtX(a / b)}</b>`]); colRatios.push(a / b); }
  }
  return { rows: rows.filter(Boolean), colMed: colRatios.length ? median(colRatios) : null,
    cold: after.dl && mid((r) => r.dl && r.dl.cold) ? after.dl.cold / mid((r) => r.dl && r.dl.cold) : null,
    rgX: after.rgs / mid((r) => r.rgs) };
}

export function propRowGroups(ctx) {
  const rpr = (r) => r.rowsPerRg;
  const rg = Object.fromEntries(ctx.runs.map((r) => [r.id, r.rowsPerRg]));
  const s = { cu: runSlope(ctx, rpr, M.cu.get), cold: runSlope(ctx, rpr, M.cold.get), warm: runSlope(ctx, rpr, M.warm.get),
    col: columnSlope(ctx, (c) => rg[c.runId], (c) => c.cold), drs: runSlope(ctx, rpr, M.cu.get, (r) => r.family === "delta-rs") };
  const bandPts = bandPoints(ctx), bandT = bandTrend(bandPts, 2);
  const ctl = rgControlled(ctx);
  const ctlClaim = ctl && ctl.colMed ? ` One controlled run, same writer and sort, only the row-group size moved: ${fmtX(ctl.rgX)} the row groups (131,072 rows each) made a column's cold load <b>${fmtX(ctl.colMed)}</b> (median over columns) and the whole cold suite ${fmtX(ctl.cold)}; warm did not move.` : "";
  return propertyCard({ id: "row-groups", title: "Row-group size", evidence: "observational", status: verdictOf(s.col),
    head: [s.col.med !== null ? `${pct(s.col)} cold` : "—", "a column's cold cost each time row groups double in rows"],
    card: "Bigger row groups make the cold load cheaper, and below about a million rows per group the cost climbs fast. Warm barely moves.",
    claim: s.col.med !== null ? `Twice the rows per row group: a column's cold cost <b>${pct(s.col)}</b> (${agree(s.col)}), Direct Lake read CU ${pct(s.cu)} (${agree(s.cu)}), warm ${pct(s.warm)}. ${bandT && bandT.band ? `Both cold and warm stay near the median from ${esc(bandText(bandT))} rows per group.` : "No single band of row-group sizes stands out across datasets."}${ctlClaim}` : "Not measurable yet.",
    body: `${effectTable([["column cold, per doubling of rows per group", s.col], ["Direct Lake read CU, per doubling", s.cu], ["cold suite, per doubling", s.cold], ["warm suite, per doubling", s.warm], ["Direct Lake read CU, delta-rs only", s.drs]])}
      ${ctl ? `<h3>Controlled: one switch</h3>
      <p>duckrun on aemo, the same writer versions and the same <code>date, time, price</code> sort; only <code>row_group_size</code> moved. Before is the median of two runs, after is one run, so read it as one sample per arm.</p>
      ${table(["aemo, duckrun", "default", "131,072 rows per group", "change"], ctl.rows, "num")}
      <p class="note">Every chunk has a dictionary page in both arms. <code>DUID</code> carries a separate, larger cold penalty on both arms of this pair that row-group size does not touch: it loaded in under a second on the September runs.</p>` : ""}
      ${bandPanels(bandPts, bandT, false)}
      <p class="note">The lines weigh every dataset equally: each dataset's own value per octave of rows per group, then the median across datasets.</p>` });
}

/** Run ids of the controlled declaration pair: iceberg on aemo, DuckDB dev365 then dev379. */
const DECLARE_PAIR = { before: "32444969823", after: "33700004854" };

export function propDictionary(ctx) {
  const base = findDictionary(ctx);
  // Within the same column: chunks missing a dictionary page against fully dictionary-encoded.
  const per = {};
  for (const ds of dsList(ctx)) {
    const C = ctx.columns.filter((c) => c.ds === ds && c.cold > 0 && c.dictFull !== null);
    const rs = [];
    for (const g of C.filter((c) => c.dictFull === false)) {
      const full = C.filter((c) => c.column === g.column && c.dictFull);
      if (full.length >= 2) rs.push(g.cold / median(full.map((c) => c.cold)));
    }
    per[ds] = rs.length ? median(rs) : null;
  }
  const s = acrossDatasets(per);
  // THE HEADLINE IS THE CONTROLLED PAIR. The DuckDB wheel on the iceberg leg moved from 1.6.0.dev365
  // (no footer encoding_stats) to 1.6.0.dev379 (duckdb#24957) between these two aemo runs, which
  // wrote the same files, row groups, bytes and dictionary pages: only the declaration differs.
  const run = (id) => ctx.runs.find((r) => r.id === id);
  const before = run(DECLARE_PAIR.before), after = run(DECLARE_PAIR.after);
  const ratio = (m) => (before && after && m(before) > 0 && m(after) > 0 ? m(after) / m(before) : null);
  const cu = ratio((r) => r.cu && r.cu.dl), cold = ratio((r) => r.dl && r.dl.cold);
  const drop = (x) => `−${Math.round((1 - x) * 100)}%`;
  const paired = cu !== null && cold !== null;
  return propertyCard({ id: "dictionary", title: "Dictionary encoding", evidence: paired ? "controlled" : "observational",
    status: paired ? "clear" : base.status === "clear" ? "clear" : "partial",
    head: paired ? [`${drop(cu)} CU`, "Direct Lake read CU once the file declares its dictionary: same table, same pages, one footer list"]
      : [s.med !== null ? `${pct(s)} cold` : "—", "a column whose chunks lack a dictionary page, against the same column fully encoded"],
    card: "Keep every column dictionary-encoded, and have the file declare it so Power BI can reuse the dictionary instead of rebuilding it.",
    claim: `${paired ? `Same DuckDB-written table, same files, row groups and dictionary pages; the only change is the footer declaring the dictionary (duckdb#24957): Direct Lake read CU <b>${drop(cu)}</b>, cold load <b>${drop(cold)}</b>. ` : ""}A column with chunks that have no dictionary page at all cost ${pct(s)} cold against the same column fully encoded (${agree(s)}).`,
    body: `${effectTable([["column cold, dictionary gaps ÷ full", s]])}${base.html}` });
}

export function propFileCount(ctx) {
  const files = (r) => r.files;
  const s = { cu: runSlope(ctx, files, M.cu.get), cold: runSlope(ctx, files, M.cold.get), warm: runSlope(ctx, files, M.warm.get) };
  return propertyCard({ id: "file-count", title: "File count", evidence: "observational", status: counts(s.cu) ? "clear" : "none",
    head: [s.cu.med !== null ? pct(s.cu) : "—", "Direct Lake read CU each time the file count doubles"],
    card: "How many files the table is split into made no measurable difference.",
    claim: `Twice the files: Direct Lake read CU ${pct(s.cu)}, cold ${pct(s.cold)}, warm ${pct(s.warm)}. ${counts(s.cu) ? "" : "Inside the ±10% noise floor: no measurable effect."}`,
    body: `${effectTable([["Direct Lake read CU, per doubling of files", s.cu], ["cold, per doubling", s.cold], ["warm, per doubling", s.warm]])}
      ${legend(familiesIn(ctx.runs))}
      ${normalisedScatter(ctx, files, M.cu.get, "file count", "Direct Lake read CU")}` });
}

export function propDirectQuery(ctx) {
  const ratio = {}, tot = {};
  for (const ds of dsList(ctx)) {
    const R = ctx.runs.filter((r) => r.ds === ds && r.cu.dl && r.cu.dq);
    ratio[ds] = R.length >= 2 ? median(R.map((r) => r.cu.dq / r.cu.dl)) : null;
  }
  const s = acrossDatasets(ratio);
  const sens = [["table size", runSlope(ctx, M.mb.get, M.dq.get)], ["rows per row group", runSlope(ctx, (r) => r.rowsPerRg, M.dq.get)],
    ["V-Order on ÷ off", ratioEffect(ctx, (r) => r.writer === "spark_vorder", (r) => r.writer === "spark_plain", M.dq.get)]];
  return propertyCard({ id: "directquery", title: "DirectQuery", evidence: "observational", status: verdictOf(s),
    head: [s.med !== null ? `${fmtX(s.med)} CU` : "—", "read CU in DirectQuery against Direct Lake, same queries"],
    card: "DirectQuery costs several times the capacity of Direct Lake, and layout barely changes that.",
    claim: s.med !== null ? `The same queries cost <b>${fmtX(s.med)} the read CU</b> through DirectQuery as through Direct Lake (${agree(s)}). Layout properties move DirectQuery's bill by little: ${sens.map(([n, x]) => `${n} ${pct(x)}`).join(", ")}.` : "Not measurable yet.",
    body: `${effectTable([["DirectQuery ÷ Direct Lake, read CU", s], ...sens.map(([n, x]) => [`DirectQuery read CU, ${n}`, x])])}
      ${groupStrips(ctx, [{ label: "Direct Lake", keep: (r) => r.cu.dl > 0, family: "delta-rs", filled: true }], M.cu.get, "Direct Lake read CU")}
      <p>DirectQuery is not a layout property; it is the price of not loading the data into VertiPaq at all.</p>` });
}

// The presentation's order: the three levers any writer controls (sort, dictionary encoding, row-group
// size), then Spark V-Order, then DirectQuery, then the rest.
export const PROPERTIES = [propSort, propDictionary, propRowGroups, propSparkVOrder, propDirectQuery,
  propTableSize, propFileCount];

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
    <p><b>How every number is made.</b> A continuous property (size, rows per row group, file count, sort runs) is fitted per dataset as the change in the measure each time the property doubles. An on/off property (V-Order, compression, sorted) is the ratio of medians per dataset. The number on a card is the median of those per-dataset values, so a dataset with many runs counts no more than one with few, and each card says how many datasets agree on the direction. An effect counts when it is beyond ±10% and most datasets agree; otherwise the card says there is no measurable effect.</p>
    <p>A total is the sum over the queries a dataset's runs have in common. Runs are compared only within a dataset and only at the same source row count. A difference is called <b>clear</b> only when every run of the winner beat every run of the loser, with at least ${MIN_RUNS} runs each. Otherwise it is <b>inside the noise</b> or <b>too few runs</b>, because one Fabric capacity runs everything and its load varies from run to run.</p>
    <p>A writer changes several things at once: encoding, sort, row-group size, compression. Two findings hold the writer fixed (row-group size, and sorted against unsorted delta-rs) and one holds it to a single switch (Spark with V-Order on and off). Everything else is a correlation across writers, labelled as one.</p>
    ${table(["dataset", "rows", "runs", "timed runs", "queries", "writers"], rows)}`;
}

// ------------------------------------------------------------------------------- performance tab
//
// `?page=perf`: which writer's output is cheapest to READ, every dataset at once. ONE measure, read CU:
// it is billed per session, not per pass, so it is never split into cold and warm. One writer x
// dataset grid; each cell is the writer's median session as a multiple of the CHEAPEST writer on that
// dataset (1.0x = cheapest, green), with the CU under it. Absolute numbers cannot share a chart across datasets — a
// bigger table costs more whoever wrote it — so each dataset is read against itself. One storage mode
// at a time: Direct Lake and DirectQuery are never in the same table. Deliberately plain.
//
// EACH CELL IS THE MEDIAN OF THE WRITER'S LAST `PERF_LAST_RUNS` RUNS ON THAT DATASET, newest by start
// time, counting only runs with a read CU in that storage mode. Older runs stay in `history/runs/`
// and on every other page, so an improvement over time remains visible there, while the grid
// describes the writer as it is now. That replaces parking superseded runs in `legacy/`.

export const PERF_PHASES = { dl: "Direct Lake", dq: "DirectQuery" };
export const PERF_LAST_RUNS = 3;

// Writers left off this tab by the user's call (2026-10-18): unsorted delta-rs is not a layout anyone
// would ship, and V-Order is ON by default on a warehouse, so "Warehouse, V-Order off" is a setting
// someone has to go and turn off. readHeavyForSpark enables no V-Order (Microsoft's profile reference)
// and is neither side of the Spark comparison. Their runs stay in the data and on every other page.
export const PERF_HIDDEN = ["deltars_unsorted", "dwh_novorder", "spark_readHeavyForSpark"];

/** One entry per (dataset, writer) measured in `phase`, datasets in page order, writers in `WRITERS` order. */
export function perfGroups(ctx, phase = "dl") {
  const order = WRITERS.map((w) => w.key);
  const rank = (k) => (order.includes(k) ? order.indexOf(k) : order.length);
  return dsList(ctx).flatMap((ds) => {
    const by = new Map();
    for (const r of ctx.runs) {
      if (r.ds !== ds || PERF_HIDDEN.includes(r.writer) || !r[phase] || !r[phase].cold || !r[phase].warm) continue;
      if (!((r.cu || {})[phase] > 0)) continue;
      (by.get(r.writer) || by.set(r.writer, []).get(r.writer)).push(r);
    }
    return [...by].sort((a, b) => rank(a[0]) - rank(b[0])).map(([key, all]) => {
      const runs = [...all].sort((a, b) => String(b.started).localeCompare(String(a.started))).slice(0, PERF_LAST_RUNS);
      return {
      ds, dsLabel: label(ds), key, label: runs[0].writerLabel, runs: runs.length, total: all.length,
      ids: runs.map((r) => r.id),
      cold: median(runs.map((r) => r[phase].cold)), warm: median(runs.map((r) => r[phase].warm)),
      cu: median(runs.map((r) => (r.cu || {})[phase]).filter((v) => typeof v === "number" && v > 0)) };
    });
  });
}

/** Per dataset: the best (lowest) writer on `measure`, and the worst as a multiple of it. */
export function perfBest(groups, measure) {
  const out = [];
  for (const ds of [...new Set(groups.map((g) => g.ds))]) {
    const G = groups.filter((g) => g.ds === ds && g[measure] > 0).sort((a, b) => a[measure] - b[measure]);
    if (G.length < 2) continue;
    out.push({ ds, best: G[0], worst: G[G.length - 1], spread: G[G.length - 1][measure] / G[0][measure] });
  }
  return out;
}

/** Writer x dataset: each cell `v / best on that dataset`, the best cell marked. */
export function perfGrid(groups, measure, fmt) {
  const datasets = [...new Set(groups.map((g) => g.ds))];
  const writers = [...new Map(groups.map((g) => [g.key, g.label]))];
  const best = Object.fromEntries(datasets.map((ds) => {
    const v = groups.filter((g) => g.ds === ds && g[measure] > 0).map((g) => g[measure]);
    return [ds, v.length ? Math.min(...v) : null];
  }));
  const head = `<tr><th>writer</th>${datasets.map((ds) => `<th>${esc(label(ds))}</th>`).join("")}</tr>`;
  const body = writers.map(([key, name]) => `<tr><th scope="row">${esc(name)}</th>${datasets.map((ds) => {
    const g = groups.find((x) => x.ds === ds && x.key === key);
    if (!g || !(g[measure] > 0)) return `<td class="na">—</td>`;
    const x = g[measure] / best[ds];
    const cls = x < 1.0005 ? "best" : x >= 2 ? "slow" : "";
    return `<td class="${cls}" title="${esc(`${g.label} on ${g.dsLabel}: ${fmt(g[measure])}, median of the last ${g.runs} of ${g.total} run${g.total === 1 ? "" : "s"} (${g.ids.join(", ")})`)}"><b>${x.toFixed(1)}×</b><span>${esc(fmt(g[measure]))}</span></td>`;
  }).join("")}</tr>`).join("");
  return `<div class="tablewrap"><table class="grid"><thead>${head}</thead><tbody>${body}</tbody></table></div>`;
}

/** One plain sentence: who is best most often, and how far apart best and worst are. */
function perfHeadline(groups, measure, word) {
  const B = perfBest(groups, measure);
  if (!B.length) return "";
  const wins = new Map();
  for (const b of B) wins.set(b.best.label, (wins.get(b.best.label) || 0) + 1);
  const who = [...wins].sort((a, b) => b[1] - a[1])
    .map(([w, n], i) => `<b>${esc(w)}</b> on ${n}${i === 0 ? ` of ${B.length} datasets` : ""}`);
  const lo = Math.min(...B.map((b) => b.spread)), hi = Math.max(...B.map((b) => b.spread));
  const spread = lo.toFixed(1) === hi.toFixed(1) ? `${hi.toFixed(1)}×` : `${lo.toFixed(1)}× to ${hi.toFixed(1)}×`;
  return `<p class="headline">${word[0].toUpperCase()}${word.slice(1)}: ${who.join(", ")}. On the same dataset, the most expensive writer costs ${spread} as much.</p>`;
}

export function perfSection(ctx, opts = {}) {
  const phase = PERF_PHASES[opts.phase] ? opts.phase : "dl";
  const pill = (on, url, text) => `<a class="pill${on ? " on" : ""}" href="${url}"${on ? ' aria-current="true"' : ""}>${text}</a>`;
  const phPills = Object.entries(PERF_PHASES).map(([k, v]) =>
    pill(k === phase, href({ page: "perf", phase: k === "dl" ? "" : k }), esc(v))).join("");
  const groups = perfGroups(ctx, phase);
  if (!groups.length) return `<div class="filters"><div class="pills">${phPills}</div></div><p class="empty">No timed runs yet.</p>`;
  const mode = esc(PERF_PHASES[phase]);
  if (!groups.some((g) => g.cu)) return `<div class="filters"><div class="pills">${phPills}</div></div><p class="empty">No read CU measured yet.</p>`;
  return `
    <div class="filters"><div class="pills" aria-label="storage mode">${phPills}</div></div>
    ${perfHeadline(groups, "cu", "cheapest to read")}
    <p class="dim">Read CU is what the whole ${mode} session billed: the semantic model and the lakehouse it reads through, for the full query suite. Each cell is the median of a writer's last ${PERF_LAST_RUNS} sessions on that dataset, as a multiple of the cheapest writer on the same dataset: <b>1.0×</b> is the cheapest (green), 2.0× costs twice as much. The CU is under it.</p>
    ${perfGrid(groups, "cu", fmtCu)}
    <h3>Read CU over time</h3>
    <p class="dim">Every ${mode} session, one panel per dataset, one line per writer. A step in a line is that writer's read cost changing, whether from the writer, its settings or the service. Hover a dot for the run.</p>
    ${perfHistory(ctx, phase)}`;
}

/** A run's start as epoch ms: `run.started`, or the `2026-10-17T1526Z-…` prefix of its record file. */
export function runTime(started) {
  const s = String(started || "");
  const iso = Date.parse(s);
  if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/.test(s) && Number.isFinite(iso)) return iso;
  const m = s.match(/^(\d{4}-\d{2}-\d{2})T(\d{2})(\d{2})Z/);
  return m ? Date.parse(`${m[1]}T${m[2]}:${m[3]}:00Z`) : null;
}

/** Per dataset, per shown writer: every session with a read CU in `phase`, oldest first. */
export function perfHistoryPoints(ctx, phase = "dl") {
  const order = WRITERS.map((w) => w.key);
  const rank = (k) => (order.includes(k) ? order.indexOf(k) : order.length);
  return dsList(ctx).map((ds) => {
    const by = new Map();
    for (const r of ctx.runs) {
      const cu = (r.cu || {})[phase], t = runTime(r.started);
      if (r.ds !== ds || PERF_HIDDEN.includes(r.writer) || !(cu > 0) || t === null) continue;
      if (!r[phase] || !r[phase].cold) continue;
      (by.get(r.writer) || by.set(r.writer, { key: r.writer, label: r.writerLabel, family: r.family,
        filled: r.reorders, pts: [] }).get(r.writer)).pts.push({ t, cu, id: r.id, when: r.when, rg: r.rgSetting });
    }
    const lines = [...by.values()].sort((a, b) => rank(a.key) - rank(b.key));
    for (const l of lines) l.pts.sort((a, b) => a.t - b.t);
    return { ds, lines };
  }).filter((p) => p.lines.length);
}

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

/** One dataset's panel: time across, read CU up (from zero), a line and dots per writer. */
function historyPanel(p, [t0, t1], { w = 470, h = 250 } = {}) {
  const m = { l: 52, r: 12, t: 26, b: 30 };
  const all = p.lines.flatMap((l) => l.pts.map((q) => q.cu));
  const sy = scale([0, Math.max(...all)], false, h - m.b, m.t);
  const fx = (t) => m.l + ((t - t0) / (t1 - t0 || 1)) * (w - m.l - m.r);
  const parts = [`<svg viewBox="0 0 ${w} ${h}" class="chart" role="img" aria-label="${esc(`read CU over time on ${label(p.ds)}`)}">`,
    `<text x="${m.l}" y="15" class="panel-title">${esc(label(p.ds))}</text>`];
  for (const v of sy.ticks) {
    const y = sy.f(v);
    parts.push(`<line x1="${m.l}" x2="${w - m.r}" y1="${y.toFixed(1)}" y2="${y.toFixed(1)}" class="grid"/>`,
      `<text x="${m.l - 6}" y="${(y + 3.5).toFixed(1)}" class="tick" text-anchor="end">${esc(fmtCu(v).replace(" CU", ""))}</text>`);
  }
  // One tick per month boundary inside the window.
  const d = new Date(t0); d.setUTCDate(1); d.setUTCHours(0, 0, 0, 0);
  for (d.setUTCMonth(d.getUTCMonth() + 1); d.getTime() <= t1; d.setUTCMonth(d.getUTCMonth() + 1)) {
    const x = fx(d.getTime());
    parts.push(`<line x1="${x.toFixed(1)}" x2="${x.toFixed(1)}" y1="${m.t}" y2="${h - m.b}" class="grid"/>`,
      `<text x="${x.toFixed(1)}" y="${h - m.b + 15}" class="tick" text-anchor="middle">${MONTHS[d.getUTCMonth()]}</text>`);
  }
  for (const l of p.lines) {
    const css = fam(l.family).css;
    if (l.pts.length > 1) {
      parts.push(`<polyline points="${l.pts.map((q) => `${fx(q.t).toFixed(1)},${sy.f(q.cu).toFixed(1)}`).join(" ")}" class="hline${l.filled ? "" : " dashed"}" style="stroke:${css}"/>`);
    }
  }
  for (const l of p.lines) {
    for (const q of l.pts) {
      parts.push(mark(fx(q.t), sy.f(q.cu), { family: l.family, filled: l.filled,
        tip: `${l.label} · ${label(p.ds)} · run ${q.id} (${q.when}) · ${fmtCu(q.cu)}${q.rg ? ` · row groups ${q.rg}` : ""}` }, 3.6));
    }
  }
  parts.push("</svg>");
  return parts.join("");
}

/** The history: one panel per dataset on a shared time axis, with a legend naming each writer's mark. */
export function perfHistory(ctx, phase = "dl") {
  const panels = perfHistoryPoints(ctx, phase);
  if (!panels.length) return `<p class="empty">No sessions with a read CU yet.</p>`;
  const ts = panels.flatMap((p) => p.lines.flatMap((l) => l.pts.map((q) => q.t)));
  const pad = 2 * 86400000, span = [Math.min(...ts) - pad, Math.max(...ts) + pad];
  const seen = new Map();
  for (const p of panels) for (const l of p.lines) if (!seen.has(l.key)) seen.set(l.key, l);
  const key = [...seen.values()].map((l) => `<span class="key"><svg viewBox="0 0 30 12" width="30" height="12"><line x1="1" x2="29" y1="6" y2="6" class="hline${l.filled ? "" : " dashed"}" style="stroke:${fam(l.family).css}"/>${mark(15, 6, l, 4).replace(/<circle[^>]*fill="transparent"\/>/, "")}</svg>${esc(l.label)}</span>`).join("");
  return `<div class="legend">${key}</div><div class="facets two">${panels.map((p) =>
    `<figure class="facet">${historyPanel(p, span)}</figure>`).join("")}</div>`;
}

// ------------------------------------------------------------------------------------- the page

// The home page: one card per LAYOUT PROPERTY, in PROPERTIES order.
export const FINDINGS = PROPERTIES;
export const EXTRA_FINDINGS = [];

export function context(records, opts = {}, ledger = null) {
  const b = build(records, PROBE_COLUMNS, ledger);
  // GLOBAL ONLY: every view aggregates every dataset, each weighing the same (see the property cards).
  const datasets = Object.keys(b.perDataset);
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
  return `<a class="card" href="${href({ finding: f.id })}">
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
  const switcher = "";
  const back = `<a class="back" href="?">← All properties</a>`;
  const frame = `<p class="framelinks">${Object.entries(PAGES).map(([k, v]) =>
    `<a href="${href({ page: k })}">${esc(v)}</a>`).join(" · ")}</p>`;
  const nds = ctx.datasets.length;
  const intro = `<p class="about">Each card is one layout property: <b>which layout properties matter, and by how much</b>. One number per card, the median across ${nds} datasets, so each dataset weighs the same.</p>`;
  const idx = found.findIndex((f) => f.id === opts.finding);
  const side = extra.find((f) => f.id === opts.finding);
  let body;
  if (side) {
    body = `${back}${switcher}<section id="${side.id}" class="finding"><h2>${esc(side.title)} ${badge(side.status)}</h2>${side.html}</section>`;
  } else if (idx >= 0) {
    const f = found[idx];
    const prev = found[idx - 1], next = found[idx + 1];
    const pager = `<nav class="pager">${prev ? `<a href="${href({ finding: prev.id })}">← ${esc(prev.title)}</a>` : "<span></span>"}${next ? `<a href="${href({ finding: next.id })}">${esc(next.title)} →</a>` : ""}</nav>`;
    body = `${back}${switcher}<section id="${f.id}" class="finding"><h2><span class="num">${idx + 1}</span>${esc(f.title)} ${badge(f.status)}</h2>${f.html}</section>${pager}`;
  } else if (opts.page === "perf") {
    body = `<section id="perf" class="perf"><h2>Which writer is cheapest to read</h2>${perfSection(ctx, opts)}</section>`;
  } else if (PAGES[opts.page]) {
    const inner = opts.page === "method" ? methodSection(ctx) : opts.page === "how" ? HOW : dataSection(ctx);
    body = `${back}${switcher}<section id="${opts.page}"><h2>${esc(PAGES[opts.page])}</h2>${inner}</section>`;
  } else {
    body = `<div class="cards">${found.map((f, i) => card(f, i, opts)).join("")}</div>${frame}${intro}`;
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
    finding: q.get("finding") || "", page: q.get("page") || "", phase: q.get("phase") || "",
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
    // The tabs are static HTML; the Performance tab is a view of this same page, so mark it here.
    if (opts.page === "perf") {
      for (const t of doc.querySelectorAll(".tabs a")) {
        if (t.getAttribute("href") === "./?page=perf") t.setAttribute("aria-current", "page");
        else t.removeAttribute("aria-current");
      }
    }
    if (status) status.textContent = note;
    wire(app, doc, ctx);
  } catch (ex) {
    app.innerHTML = `<p class="empty"><b>Could not read the run records.</b> ${esc(ex && ex.message || ex)}. ` +
      `The page reads <code>history/runs/</code> from raw.githubusercontent.com at view time.</p>`;
    if (status) status.textContent = "";
  }
}

if (typeof document !== "undefined" && document.getElementById("app")) boot();
