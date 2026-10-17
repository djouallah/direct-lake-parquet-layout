// The report: what in a parquet file decides how fast Power BI reads it. Every sentence that states a
// number is generated from `history/runs/*.json` at view time, so the page rewrites itself after
// every Benchmark run and can never quote a number the records no longer support.
//
// Shape follows the write-up it was modelled on: the short version, the method, how Direct Lake
// reads parquet, one finding per variable, the takeaway, then the data itself.
import { DATASETS, MIN_RUNS, build, median, spearman, strength, verdict, fmtInt, fmtMs, fmtMb, fmtX, fmtRows, loadRemote, DEFAULTS } from "./data.js";
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
export function scatter({ points, x, y, w = 520, h = 320, guides = [] }) {
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

function facets(items) {
  const live = items.filter((i) => i.html);
  if (!live.length) return `<p class="empty">Not enough measured points yet.</p>`;
  return `<div class="facets${live.length === 1 ? " one" : ""}">${live.map((i) =>
    `<figure class="facet"><figcaption>${esc(i.title)}</figcaption>${i.html}</figure>`).join("")}</div>`;
}

export function table(head, rows, cls = "") {
  return `<div class="tablewrap"><table class="${cls}"><thead><tr>${head.map((h) =>
    `<th>${h}</th>`).join("")}</tr></thead><tbody>${rows.map((r) =>
    `<tr>${r.map((c) => `<td>${c ?? "—"}</td>`).join("")}</tr>`).join("")}</tbody></table></div>`;
}

const STATUS = { clear: "clear", noise: "inside the noise", few: "too few runs" };
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
// Each finding returns `{id, title, short, status, html}`. `short` is the one line the short version
// and the takeaway print; it is generated, so a rule the data stops supporting says so there too.

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
    ? `The first query after a refresh costs ${range(ratios, fmtX)} the second in Direct Lake; the parquet layout matters for that first one.`
    : "No run carries both a cold and a warm Direct Lake time yet.";
  const chart = facets(per.map((p) => ({
    title: `${label(p.ds)} · cold ${fmtX(p.ratio)} warm`,
    html: scatter({
      points: p.R.map((r) => ({ x: r.dl.warm, y: r.dl.cold, family: r.family, filled: r.reorders,
        tip: runTip(r, ` · cold ${fmtMs(r.dl.cold)} · warm ${fmtMs(r.dl.warm)}`) })),
      x: { label: "warm: second visit, data in memory", log: true, fmt: fmtMs },
      y: { label: "cold: first visit after deploy", log: true, fmt: fmtMs },
      guides: [1, 10], w: 420, h: 290 }) })));
  const rows = per.map((p) => [label(p.ds), fmtInt(p.R.length), fmtX(p.ratio), rhoText(p.rho),
    p.dqRatio ? fmtX(p.dqRatio) : "—"]);
  const html = `
    <p class="claim">${per.length ? `In Direct Lake the first visit cost <b>${range(ratios, fmtX)}</b> the second, median per dataset. Within a dataset a run's cold time says ${rhos.length ? `${strength(median(rhos)) === "strong" ? "a lot" : "little"} about its warm time (ρ ${range(rhos, rhoText)})` : "nothing measurable yet about its warm time"}. ${dqr.length ? `DirectQuery has no transcode step and its first visit cost only ${range(dqr, fmtX)} its second.` : ""}` : short}</p>
    <p>Cold is the transcode: VertiPaq reading each column out of parquet and building its own segments. Warm is a second visit with those segments already in memory. They answer different questions, so every finding below reports them separately.</p>
    ${legend(familiesIn(per.flatMap((p) => p.R)))}
    ${chart}
    ${table(["dataset", "runs", "cold ÷ warm (Direct Lake)", "ρ cold vs warm", "cold ÷ warm (DirectQuery)"], rows, "num")}`;
  return { id: "cold-warm", title: "Cold and warm are different questions", short, status: per.length ? "clear" : "few", html };
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
      w: 420, h: 290 }) })));
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
  return { id: "column-size", title: "What makes a column cheap to load: its size", short, status: rc.length ? "clear" : "few", html };
}

/** Per (dataset, column): every writer's median MB and cold, plus how far apart they land. */
export function columnSpread(ctx, ds) {
  const C = ctx.columns.filter((c) => c.ds === ds && c.mb > 0);
  const byCol = {};
  for (const c of C) ((byCol[c.column] = byCol[c.column] || {})[c.writer] = byCol[c.column][c.writer] || []).push(c);
  return Object.entries(byCol).map(([column, ws]) => {
    const cells = Object.fromEntries(Object.entries(ws).map(([w, cs]) => [w, {
      mb: median(cs.map((c) => c.mb)), cold: median(cs.map((c) => c.cold)), n: cs.length,
      label: cs[0].writerLabel, family: cs[0].family, reorders: cs[0].reorders,
      enc: cs[cs.length - 1].encodings, dictFull: cs.every((c) => c.dictFull !== false) }]));
    const mbs = Object.values(cells).map((c) => c.mb);
    return { column, cells, mbSpread: Math.max(...mbs) / Math.min(...mbs) };
  }).sort((a, b) => b.mbSpread - a.mbSpread);
}

export function findSameColumn(ctx) {
  const blocks = [];
  let best = null;
  for (const ds of dsList(ctx)) {
    const spread = columnSpread(ctx, ds);
    if (!spread.length) continue;
    const writers = [...new Set(spread.flatMap((s) => Object.keys(s.cells)))];
    const labels = Object.fromEntries(spread.flatMap((s) => Object.entries(s.cells).map(([w, c]) => [w, c.label])));
    // The example the claim quotes: the biggest COLD gap between two writers on one column, among
    // columns timed under at least two of them. A size gap with no timing behind it proves nothing
    // about speed.
    for (const s of spread) {
      const cs = Object.values(s.cells).filter((c) => c.cold > 0 && c.mb >= 0.1);
      if (cs.length < 2) continue;
      const fast = cs.reduce((a, b) => (b.cold < a.cold ? b : a)), slow = cs.reduce((a, b) => (b.cold > a.cold ? b : a));
      const gap = slow.cold / fast.cold;
      if (!best || gap > best.gap) best = { ds, column: s.column, small: fast, big: slow, gap };
    }
    const rows = spread.map((s) => {
      const minCold = Math.min(...Object.values(s.cells).map((c) => c.cold).filter((v) => v !== null));
      return [`<code>${esc(s.column)}</code>`, ...writers.map((w) => {
        const c = s.cells[w];
        if (!c) return "—";
        const isBest = c.cold === minCold;
        return `${fmtMb(c.mb)} · ${isBest ? "<b>" : ""}${fmtMs(c.cold)}${isBest ? "</b>" : ""}${c.dictFull ? "" : ' <span class="warn" title="some row groups have no dictionary">no dict</span>'}`;
      }), fmtX(s.mbSpread)];
    });
    blocks.push(`<details${blocks.length ? "" : " open"}><summary>${esc(label(ds))}</summary>
      ${table(["column", ...writers.map((w) => esc(labels[w])), "biggest ÷ smallest"], rows, "num")}</details>`);
  }
  const short = best
    ? `The writer alone changes how long one column takes to load cold by up to ${fmtX(best.gap)}: \`${best.column}\` on ${label(best.ds)} is ${fmtMb(best.small.mb)} and ${fmtMs(best.small.cold)} from ${best.small.label}, ${fmtMb(best.big.mb)} and ${fmtMs(best.big.cold)} from ${best.big.label}.`
    : "No column measured under two writers yet.";
  const html = `
    <p class="claim">${best ? `Same data, same column, a different writer: <code>${esc(best.column)}</code> on ${esc(label(best.ds))} is <b>${fmtMb(best.small.mb)}</b> written by ${esc(best.small.label)} and <b>${fmtMb(best.big.mb)}</b> by ${esc(best.big.label)}, and its cold load is ${fmtMs(best.small.cold)} against ${fmtMs(best.big.cold)}.` : short}</p>
    <p>Each cell is the median compressed size and the median extra cold time for that column under that writer. The fastest cold load in each row is bold. Columns are ordered by how much the writer changed their size.</p>
    ${blocks.join("")}`;
  return { id: "same-column", title: "Same column, different writers", short, status: best ? "clear" : "few", html };
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
    const comp = {};
    for (const r of ctx.runs.filter((r) => r.family === f && dsList(ctx).includes(r.ds))) if (r.compression) comp[r.compression] = (comp[r.compression] || 0) + 1;
    return [esc(fam(f).label), top || "—", chunks ? `${((pages / chunks) * 100).toFixed(chunks - pages ? 1 : 0)}%` : "—",
      esc(Object.keys(comp).sort((a, b) => comp[b] - comp[a])[0] || "—")];
  });
  // The effect of a gap: the same (dataset, column) with and without full dictionary coverage.
  const gaps = C.filter((c) => c.dictFull === false);
  const ratios = [], mbRatios = [];
  for (const g of gaps) {
    const full = C.filter((c) => c.ds === g.ds && c.column === g.column && c.dictFull === true);
    if (!full.length) continue;
    const fc = median(full.map((c) => c.cold)), fm = median(full.map((c) => c.mb));
    if (g.cold > 0 && fc > 0) ratios.push(g.cold / fc);
    if (g.mb > 0 && fm > 0) mbRatios.push(g.mb / fm);
  }
  const gapFams = [...new Set(gaps.map((g) => fam(g.family).label))];
  const short = gaps.length
    ? `${fmtInt(gaps.length)} column-runs (${gapFams.join(", ")}) had row groups without a dictionary${ratios.length ? `; they cost a median ${fmtX(median(ratios))} the cold time of the same column fully encoded` : ""}.`
    : "Every writer here put a dictionary in every row group of every probed column.";
  const html = `
    <p class="claim">${gaps.length ? `${fmtInt(gaps.length)} column-runs, all from ${esc(gapFams.join(" and "))}, had row groups with no dictionary page. ${ratios.length ? `Against the same column fully dictionary-encoded by another writer they were a median <b>${fmtX(median(mbRatios))}</b> the size and cost <b>${fmtX(median(ratios))}</b> the cold time.` : ""}` : short}</p>
    <p>VertiPaq stores every column as a dictionary plus indexes into it. A parquet column that already carries a dictionary can be remapped instead of re-encoded value by value. When a writer's dictionary outgrows its limit it falls back to <code>PLAIN</code> for the rest of that row group, usually on the high-cardinality columns, and that is where the gaps sit.</p>
    ${table(["writer", "encodings it writes (most common)", "row groups with a dictionary", "compression"], rows)}
    ${table(["column-run with a gap", "dictionary pages", "size", "extra cold time"],
      gaps.slice(0, 12).map((g) => [`<code>${esc(g.column)}</code> · ${esc(label(g.ds))} · ${esc(g.writerLabel)}`, `${g.dictPages} of ${g.chunks}`, fmtMb(g.mb), fmtMs(g.cold)]), "num")}`;
  // No run-against-run verdict exists for a gap (each is one column in one run), so the label is
  // earned by size: a median penalty under 1.5× on a capacity whose runs swing more than that is
  // not something to call.
  const st = ratios.length < MIN_RUNS ? "few" : median(ratios) >= 1.5 ? "clear" : "noise";
  return { id: "dictionary", title: "Dictionary encoding", short, status: st, html };
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
    ? `Sort the rows: within the same column, fewer runs of repeated values meant a smaller column on ${pos} of ${within.length} columns, and smaller columns load faster cold.`
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
      `${fmtMb(median(p.s.map((r) => r.mb)))} vs ${fmtMb(median(p.u.map((r) => r.mb)))}`];
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
      ${table(["dataset", "runs sorted / unsorted", "cold", "warm", "table size"], pairTable, "num")}` : ""}`;
  return { id: "sort", title: "Global sort: fewer runs, smaller columns", short, status: within.length ? "clear" : "few", html };
}

export function findRowGroups(ctx) {
  // delta-rs only, where the row-group size is a dispatched setting and nothing else about the writer
  // moves. Across writers the row-group count travels with encoding, sort and compression.
  const per = dsList(ctx).map((ds) => {
    const R = ctx.runs.filter((r) => r.ds === ds && r.family === "delta-rs" && r.rowsPerRg && r.dl.cold && r.dl.warm);
    const distinct = new Set(R.map((r) => r.rgs)).size;
    const groups = {};
    for (const r of R) (groups[r.rgSetting || "?"] = groups[r.rgSetting || "?"] || []).push(r);
    const big = Object.fromEntries(Object.entries(groups).filter(([, v]) => v.length >= MIN_RUNS));
    const vc = verdict(Object.fromEntries(Object.entries(big).map(([k, v]) => [k, v.map((r) => r.dl.cold)])));
    const vw = verdict(Object.fromEntries(Object.entries(big).map(([k, v]) => [k, v.map((r) => r.dl.warm)])));
    return { ds, R, distinct, groups, vc, vw,
      rhoCold: spearman(R.map((r) => r.rowsPerRg), R.map((r) => r.dl.cold)),
      rhoWarm: spearman(R.map((r) => r.rowsPerRg), R.map((r) => r.dl.warm)),
      rhoHot: spearman(R.map((r) => r.rowsPerRg), R.map((r) => r.dl.hot)) };
  }).filter((p) => p.distinct >= 3);
  const all = dsList(ctx).map((ds) => {
    const R = ctx.runs.filter((r) => r.ds === ds && r.rowsPerRg && r.dl.cold);
    return [label(ds), fmtInt(R.length), range(R.map((r) => r.rowsPerRg), fmtRows),
      rhoText(spearman(R.map((r) => r.rowsPerRg), R.map((r) => r.dl.cold))),
      rhoText(spearman(R.map((r) => r.rowsPerRg), R.map((r) => r.dl.warm)))];
  });
  const lead = per[0];
  const short = lead
    ? `Row-group size: from ${range(lead.R.map((r) => r.rowsPerRg), fmtRows)} rows per group, delta-rs on ${label(lead.ds)} showed ${linkWord(lead.rhoCold)} to cold (ρ ${rhoText(lead.rhoCold)}) or warm (ρ ${rhoText(lead.rhoWarm)}); the settings were ${STATUS[lead.vc.status]} on cold.`
    : "No dataset has delta-rs runs at three or more row-group sizes yet.";
  const charts = per.flatMap((p) => ["cold", "warm"].map((tier) => ({
    title: `${label(p.ds)} · ${tier} · ρ ${rhoText(tier === "cold" ? p.rhoCold : p.rhoWarm)}`,
    html: scatter({
      points: p.R.map((r) => ({ x: r.rowsPerRg, y: r.dl[tier], family: r.family, filled: r.reorders,
        tip: runTip(r, ` · ${r.rgs} row groups · ${fmtRows(r.rowsPerRg)} rows each · ${tier} ${fmtMs(r.dl[tier])}`) })),
      x: { label: "rows per row group", log: true, fmt: fmtRows },
      y: { label: `${tier} query suite total`, log: false, fmt: fmtMs }, w: 420, h: 270 }) })));
  const settingRows = per.flatMap((p) => Object.entries(p.groups)
    .sort((a, b) => median(a[1].map((r) => r.rowsPerRg)) - median(b[1].map((r) => r.rowsPerRg)))
    .map(([k, v]) => [label(p.ds), k === "16000000" ? "16M (old default)" : k === "auto" ? "auto" : fmtRows(Number(k)),
      fmtInt(v.length), range(v.map((r) => r.rgs), fmtInt), fmtMs(median(v.map((r) => r.dl.cold))),
      fmtMs(median(v.map((r) => r.dl.warm))), fmtMs(median(v.map((r) => r.dl.hot)))]));
  const html = `
    <p class="claim">${lead ? `delta-rs on ${esc(label(lead.ds))} was written at ${lead.distinct} different row-group counts (${range(lead.R.map((r) => r.rowsPerRg), fmtRows)} rows per group). Cold time shows ${linkWord(lead.rhoCold)} to it (ρ ${rhoText(lead.rhoCold)}), warm ${linkWord(lead.rhoWarm)} (ρ ${rhoText(lead.rhoWarm)}). Comparing the dispatched settings directly, cold is ${badge(lead.vc.status)} and warm is ${badge(lead.vw.status)}.` : short}</p>
    <p>A row group becomes one VertiPaq segment, so its size sets how many segments a column is split into. Fewer, bigger segments mean less per-segment overhead; more, smaller ones mean more parallelism and finer skipping. This chart holds the writer fixed: only delta-rs, where the row-group size is a setting someone chose.</p>
    ${facets(charts)}
    ${table(["dataset", "row_group_size setting", "runs", "row groups", "cold", "warm", "hot"], settingRows, "num")}
    <h4>Across every writer</h4>
    <p>Here the row-group count moves together with encoding, sort and compression, so a correlation cannot be read as an effect of row groups alone.</p>
    ${table(["dataset", "runs", "rows per row group", "ρ vs cold", "ρ vs warm"], all, "num")}`;
  return { id: "row-groups", title: "Row-group size", short, status: lead ? lead.vc.status : "few", html };
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
  const short = pairs.length
    ? `Spark with V-Order (readHeavyForPBI) against without: cold was faster on ${coldFaster} of ${pairs.length} datasets (${coldWins ? `${coldWins} clearly` : "none clearly on its own"}), the table smaller on ${smaller}, and warm faster on only ${warmFaster}.`
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
    <p class="claim">${pairs.length ? `With V-Order on, Spark's cold time was lower on <b>${coldFaster} of ${pairs.length}</b> datasets, ${coldWins ? `${coldWins} of them clearly (every V-Order run beat every run without it)` : "none of them clearly on its own (some runs overlap)"}, and its table was smaller on ${smaller}. ${signNote(coldFaster, pairs.length)}Warm was lower on ${warmFaster} of ${pairs.length}: once the columns are in memory, V-Order made no consistent difference.` : short}</p>
    <p>V-Order is a Fabric write-time pass: it sorts rows within each row group, sets the row-group geometry and dictionary-encodes, all aimed at VertiPaq. In Spark it comes with the <code>readHeavyForPBI</code> resource profile. The workspace default, <code>writeHeavy</code>, leaves it off. Fabric Warehouse has it on by default.</p>
    ${strips({ rows, x: { label: "Direct Lake query suite total", fmt: fmtMs } })}
    ${table(["dataset", "runs on / off", "cold (on vs off)", "warm (on vs off)", "DirectQuery warm", "table size"], tbl, "num")}
    ${dwh.length ? `<h4>Fabric Warehouse with V-Order turned off</h4>${table(["dataset", "runs on / off", "cold (on vs off)", "warm (on vs off)", "table size"],
      dwh.map((x) => [label(x.ds), `${x.v.length} / ${x.p.length}`,
        `${fmtMs(median(x.v.map((r) => r.dl.cold)))} vs ${fmtMs(median(x.p.map((r) => r.dl.cold)))} ${badge(verdict({ a: x.v.map((r) => r.dl.cold), b: x.p.map((r) => r.dl.cold) }).status)}`,
        `${fmtMs(median(x.v.map((r) => r.dl.warm)))} vs ${fmtMs(median(x.p.map((r) => r.dl.warm)))} ${badge(verdict({ a: x.v.map((r) => r.dl.warm), b: x.p.map((r) => r.dl.warm) }).status)}`,
        `${fmtMb(median(x.v.map((r) => r.mb)))} vs ${fmtMb(median(x.p.map((r) => r.mb)))}`]), "num")}
      <p class="note">Turning Warehouse V-Order off is irreversible on a warehouse, so it has been measured rarely.</p>` : ""}`;
  return { id: "vorder", title: "V-Order", short, status: coldWins ? "clear" : pairs.length ? "noise" : "few", html };
}

export function findTableSize(ctx) {
  const per = dsList(ctx).map((ds) => {
    const R = ctx.runs.filter((r) => r.ds === ds && r.mb && r.dl.cold && r.dl.warm);
    return { ds, R, rc: spearman(R.map((r) => r.mb), R.map((r) => r.dl.cold)),
      rw: spearman(R.map((r) => r.mb), R.map((r) => r.dl.warm)) };
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
      y: { label: "cold query suite total", log: false, fmt: fmtMs }, w: 420, h: 270 }) })))}
    ${table(["median table size", ...fams.map((f) => esc(fam(f).label))], sizeRows, "num")}`;
  const st = rc.length ? (["strong", "moderate"].includes(strength(median(rc))) ? "clear" : "noise") : "few";
  return { id: "table-size", title: "Table size and compression", short, status: st, html };
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
  return { id: "directquery", title: "Direct Lake against DirectQuery", short, status: per.length ? "clear" : "few", html };
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
    <p>A total is the sum over the queries a dataset's runs have in common. Runs are compared only within a dataset and only at the same source row count. A difference is called <b>clear</b> only when every run of the winner beat every run of the loser, with at least ${MIN_RUNS} runs each. Otherwise it is <b>inside the noise</b> or <b>too few runs</b>, because one Fabric capacity runs everything and its load varies from run to run.</p>
    <p>A writer changes several things at once: encoding, sort, row-group size, compression. Two findings hold the writer fixed (row-group size, and sorted against unsorted delta-rs) and one holds it to a single switch (Spark with V-Order on and off). Everything else is a correlation across writers, labelled as one.</p>
    ${table(["dataset", "rows", "runs", "timed runs", "queries", "writers"], rows)}`;
}

// ------------------------------------------------------------------------------------- the page

export const FINDINGS = [findColdWarm, findColumnSize, findSameColumn, findDictionary, findSort,
  findRowGroups, findVOrder, findTableSize, findDirectQuery];

export function context(records, opts = {}) {
  const b = build(records, PROBE_COLUMNS);
  const datasets = Object.keys(b.perDataset).filter((ds) => !opts.dataset || ds === opts.dataset);
  return { ...b, datasets };
}

function dataSection(ctx) {
  const R = ctx.runs.filter((r) => ctx.datasets.includes(r.ds));
  const head = ["dataset", "writer", "run", "date", "files", "row groups", "rows / RG", "size", "compression",
    "DL cold", "DL warm", "DL hot", "DQ cold", "DQ warm"];
  const rows = R.map((r) => [label(r.ds), esc(r.writerLabel),
    r.url ? `<a href="${esc(r.url)}">${esc(r.id)}</a>` : esc(r.id), esc(r.when), fmtInt(r.files), fmtInt(r.rgs),
    fmtRows(r.rowsPerRg), fmtMb(r.mb), esc(r.compression || "—"),
    fmtMs(r.dl.cold), fmtMs(r.dl.warm), fmtMs(r.dl.hot), fmtMs(r.dq.cold), fmtMs(r.dq.warm)]);
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
  "files", "rgs", "rowsPerRg", "mb", "compression", "dl.cold", "dl.warm", "dl.hot", "dq.cold", "dq.warm", "dq.hot"];
export const COLUMN_KEYS = ["ds", "writerLabel", "runId", "column", "type", "mb", "encodings", "dictPages",
  "chunks", "runsPer1k", "cold", "warm", "coldRaw", "warmRaw"];

export function render(records, opts = {}) {
  const ctx = context(records, opts);
  if (!ctx.runs.length) return { html: `<p class="empty">No run records found.</p>`, ctx };
  const found = FINDINGS.map((f) => f(ctx));
  const live = found.filter((f) => f.status !== "few" || f.id === "row-groups");
  const switcher = `<nav class="switch">Dataset: ${[["", "all"], ...Object.keys(ctx.perDataset).map((d) => [d, label(d)])]
    .map(([d, l]) => (d === (opts.dataset || "") ? `<b>${esc(l)}</b>` : `<a href="?${d ? `dataset=${d}` : ""}">${esc(l)}</a>`)).join(" · ")}</nav>`;
  const shortList = `<ol class="short">${live.map((f) => `<li><a href="#${f.id}">${esc(f.title)}</a>: ${esc(f.short).replace(/`([^`]+)`/g, "<code>$1</code>")} ${badge(f.status)}</li>`).join("")}</ol>`;
  const html = `
    ${switcher}
    <section id="short"><h2>The short version</h2>${shortList}</section>
    <section id="method"><h2>Method</h2>${methodSection(ctx)}</section>
    <section id="how"><h2>How Power BI reads parquet</h2>
      <p>Direct Lake does not import data. On a column's first use it reads that column's parquet and transcodes it into VertiPaq's own segments, one per row group, and keeps them in memory. Everything about the file that makes that read cheaper (smaller columns, dictionaries it can reuse, long runs of equal values) pays off at that moment. After it, queries run against VertiPaq's copy. DirectQuery keeps nothing and goes back to the parquet through the SQL endpoint for every query.</p>
      ${DIAGRAM}</section>
    ${found.map((f, i) => `<section id="${f.id}" class="finding"><h2><span class="num">${i + 1}</span>${esc(f.title)} ${badge(f.status)}</h2>${f.html}</section>`).join("")}
    <section id="takeaway"><h2>Takeaway</h2>
      <p>What the measurements say, each with its live evidence:</p>
      ${shortList}
      <p>What moved, moved the <b>cold</b> query: the first one after a refresh or a model reload. Small columns, full dictionaries and sorted rows all make that transcode cheaper, and V-Order is Fabric's way of doing all three at write time. None of it measurably moved the warm query once the columns were in memory.</p></section>
    <section id="data"><h2>The data</h2>${dataSection(ctx)}</section>`;
  return { html, ctx };
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
    repo: q.get("repo") || DEFAULTS.repo, ref: q.get("ref") || DEFAULTS.ref };
  try {
    let records, note;
    const snap = doc.getElementById("snapshot");
    if (snap && snap.textContent.trim()) {
      const s = JSON.parse(snap.textContent);
      records = s.records;
      note = `Offline copy, frozen at ${s.built || "?"}.`;
    } else {
      if (status) status.textContent = "Reading the run records from GitHub…";
      records = await loadRemote(opts);
      note = `Live: ${records.length} run records read from ${opts.repo}@${opts.ref}. Reload for new runs.`;
    }
    const { html, ctx } = render(records, opts);
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
