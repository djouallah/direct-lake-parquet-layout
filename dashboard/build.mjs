/**
 * One HTML file out of `index.html` + the report modules, twice — the live page and the frozen copy.
 *
 *     node dashboard/build.mjs --out site/index.html               # live: fetches history/ at load time
 *     node dashboard/build.mjs --out dashboard.html --snapshot     # offline: history/ inlined
 *     node dashboard/build.mjs --out site/ja/index.html --lang ja  # the Japanese live page, served at /ja/
 *
 * The live page carries no data: it reads `history/runs/` from GitHub on every load, so a Benchmark
 * run reaches readers with no redeploy. The snapshot copy inlines the records so it opens off a disk.
 *
 * No bundler. `data.js`, `probe_columns.js` and `report.js` are concatenated into ONE module script,
 * in dependency order, with their relative `import` lines removed — which is why those imports are
 * each written on a single line.
 */
import { readFileSync, writeFileSync, mkdirSync, readdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(HERE, "..");
/** Where the live page is published — only the offline copy needs it, to reach `dag.html`. */
const PAGES = "https://djouallah.github.io/direct-lake-parquet-layout";

const argv = process.argv.slice(2);
const flag = (name, dflt = null) => {
  const i = argv.indexOf(`--${name}`);
  return i < 0 ? dflt : (argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[i + 1] : true);
};

/** `</script>` anywhere in an inlined payload ends the script element early, whatever the quoting
 *  around it — so it is refused rather than escaped. Neither input has ever contained one; this is
 *  here so that if one ever does, the build fails instead of the page silently truncating. */
function refuseScriptClose(what, text) {
  if (/<\/script/i.test(text)) throw new Error(`${what} contains </script — cannot be inlined`);
  return text;
}

/** Every whole run record plus the ledger, exactly as the live loader would have fetched them. The
 *  `legacy/` subdirectory is skipped for the same reason the loader skips it: those records predate
 *  the item GUIDs and cannot be joined to a ledger at all. */
function snapshot() {
  const runsDir = join(ROOT, "history", "runs");
  const records = [];
  let names = [];
  try {
    names = readdirSync(runsDir, { withFileTypes: true })
      .filter((e) => e.isFile() && e.name.endsWith(".json")).map((e) => e.name).sort();
  } catch { names = []; }
  for (const n of names) {
    // `index.json` is the page's directory listing — a JSON ARRAY of record filenames, written by
    // record.py so the live page can list this directory over raw instead of the rate-limited
    // contents API. Skipped by name AND by shape: assigning `_file` to an array is legal in JS, so
    // without the shape check a phantom "record" would reach the snapshot silently.
    if (n === "index.json") continue;
    try {
      const rec = JSON.parse(readFileSync(join(runsDir, n), "utf8"));
      if (!rec || typeof rec !== "object" || Array.isArray(rec)) {
        process.stderr.write(`  skipping ${n}: not a record\n`);
        continue;
      }
      rec._file = n;
      records.push(rec);
    } catch (ex) {
      process.stderr.write(`  skipping ${n}: unreadable (${ex.message})\n`);
    }
  }
  let ledger = null;
  try { ledger = JSON.parse(readFileSync(join(ROOT, "history", "cu.json"), "utf8")); } catch { }
  return { built: new Date().toISOString().slice(0, 16).replace("T", " ") + " UTC", records, ledger };
}

const out = flag("out");
if (!out || out === true) {
  process.stderr.write("usage: node dashboard/build.mjs --out <file.html> [--snapshot]\n");
  process.exit(2);
}

let html = readFileSync(join(HERE, "index.html"), "utf8");
const MODULES = ["i18n.js", "data.js", "probe_columns.js", "report.js"];
const LOCAL_IMPORT = /^import [^\n]* from "\.\/[\w.]+";\r?\n/gm;
const app = refuseScriptClose("report modules", MODULES.map((m) =>
  readFileSync(join(HERE, m), "utf8").replace(LOCAL_IMPORT, "")).join("\n"));
if (/^import /m.test(app)) throw new Error("a module still carries an import the build cannot inline");

const before = html;
html = html.replace('<script type="module" src="report.js"></script>',
  `<script type="module">\n${app}\n</script>`);
if (html === before) throw new Error("index.html no longer carries the report.js script tag");

if (flag("snapshot")) {
  const data = snapshot();
  // `<` is escaped rather than the payload being trusted: JSON inside a `<script>` element only has to
  // avoid the closing tag, and escaping every `<` is the one rule that cannot be got subtly wrong.
  const json = JSON.stringify(data).replace(/</g, "\\u003c");
  const swapped = html.replace('<script id="snapshot" type="application/json"></script>',
    `<script id="snapshot" type="application/json">${json}</script>`);
  if (swapped === html) throw new Error("index.html no longer carries the snapshot script tag");
  html = swapped;
  // Read it back out of the finished document. The offline copy is the one output nobody looks at
  // until they need it — years later, off a disk, with no network — so the escaping is verified here
  // rather than trusted. A truncated or unparseable snapshot renders as "no run records", which is
  // indistinguishable from a repo that has never been measured.
  const back = html.match(/<script id="snapshot" type="application\/json">([\s\S]*?)<\/script>/);
  const parsed = JSON.parse(back[1]);
  if (parsed.records.length !== data.records.length) {
    throw new Error("the inlined snapshot did not survive the round trip");
  }
  process.stderr.write(`  snapshot: ${data.records.length} record(s), ` +
    `${data.ledger ? Object.keys(data.ledger.items || {}).length : 0} ledger item(s)\n`);

  // The DAG link is RELATIVE on the live page, where `dag.html` sits beside `index.html` in the
  // Pages artifact. The offline copy is one loose file with no sibling, so the same href would
  // dangle — silently, since a 404 off a disk looks like nothing happened. Point it at the published
  // copy instead: it needs a network, which is exactly what the GitHub link beside it already needs.
  const linked = html.replace('<a id="daglink" href="dag.html">',
    `<a id="daglink" href="${PAGES}/dag.html">`);
  if (linked === html) throw new Error("index.html no longer carries the relative dag.html link");
  // The static learning pages are siblings on the live site too; same reasoning.
  // A link may name a section (`transcode.html#last`, which the pager uses), so the hash is kept.
  html = linked.replace(/href="(intro|parquet|transcode|writers)\.html(#[\w/-]*)?"/g,
    (_, page, hash) => `href="${PAGES}/${page}.html${hash || ""}"`);
}

// THE JAPANESE PAGE (`--lang ja`): the same shell, served from `/ja/` beside the translated learning
// pages, so every relative link stays inside the Japanese site. `<html lang="ja">` is what report.js
// reads to pick the language. Each swap must hit exactly as counted, or the build fails rather than
// shipping a half-English shell.
const LANG = flag("lang", "en");
if (LANG === "ja") {
  if (flag("snapshot")) throw new Error("--lang ja builds the live page only; the offline copy stays English");
  const JA = [
    ['<html lang="en">', '<html lang="ja">', 1],
    ["<title>Parquet for Power BI</title>", "<title>Power BI のための parquet</title>", 1],
    ['<nav class="tabs" aria-label="pages">', '<nav class="tabs" aria-label="ページ">', 1],
    ["</svg>Introduction</a>", "</svg>はじめに</a>", 1],
    ["</svg>Transcoding</a>", "</svg>トランスコード</a>", 1],
    ["</svg>Recap</a>", "</svg>まとめ</a>", 1],
    ["</svg>Writers compared</a>", "</svg>ライターの比較</a>", 1],
    ["</svg>Third-party writers</a>", "</svg>サードパーティのライター</a>", 1],
    ['<a class="lang" id="langlink" href="ja/" lang="ja">日本語</a>', '<a class="lang" id="langlink" href="../" lang="en">English</a>', 1],
    ['title="Source on GitHub" aria-label="Source on GitHub"', 'title="GitHub のソース" aria-label="GitHub のソース"', 1],
    ["<h1>Writing parquet that Power BI likes</h1>", "<h1>Power BI が好む parquet の書き方</h1>", 1],
    ['<p class="note">Loading…</p>', '<p class="note">読み込み中…</p>', 1],
    [`<p>The same data written by four engines, then read by Power BI in Direct Lake.
      Every number on this page is computed in your browser from the run records under
      <code>history/runs/</code>.</p>`, `<p>同じデータを 4 つのエンジンで書き出し、Power BI の Direct Lake で読み取っています。
      このページの数値はすべて、<code>history/runs/</code> の実行記録からブラウザー上で計算されています。</p>`, 1],
    ['<a href="intro.html">how it works</a>', '<a href="intro.html">仕組み</a>', 1],
    ['<a id="daglink" href="dag.html">the dbt DAG</a>', '<a id="daglink" href="../dag.html">dbt の DAG</a>', 1],
  ];
  for (const [en, ja, n] of JA) {
    const hits = html.split(en).length - 1;
    if (hits !== n) throw new Error(`--lang ja: expected ${n} of ${JSON.stringify(en.slice(0, 60))} in index.html, found ${hits}`);
    html = html.split(en).join(ja);
  }
} else if (LANG !== "en") throw new Error(`--lang ${LANG}: only en and ja exist`);

mkdirSync(dirname(resolve(out)), { recursive: true });
writeFileSync(resolve(out), html, "utf8");
process.stderr.write(`  wrote ${out} (${(html.length / 1024).toFixed(0)} KB)\n`);
