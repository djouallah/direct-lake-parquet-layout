// The Japanese switch, offline: the `ja/` learning pages keep the English pages' structure, and the
// report rendered in Japanese over the real `history/` leaks no English prose. Neither test judges the
// WORDING; they catch a section added in English only, and a sentence nobody translated.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import * as r from "./report.js";
import { setLang } from "./i18n.js";

const HERE = new URL("./", import.meta.url);
const read = (p) => readFileSync(new URL(p, HERE), "utf8");
const PAGES = ["intro.html", "parquet.html", "transcode.html", "writers.html"];

/** Every value of one attribute, in document order. */
const attrs = (html, name) => [...html.matchAll(new RegExp(`\\s${name}="([^"]*)"`, "g"))].map((m) => m[1]);

for (const page of PAGES) {
  test(`ja/${page} keeps the structure of ${page}`, () => {
    const en = read(page), ja = read(`ja/${page}`);
    assert.match(ja, /<html lang="ja">/);
    for (const a of ["id", "data-name", "data-pane", "data-tab", "class"]) {
      assert.deepEqual(attrs(ja, a), attrs(en, a), `${a} differs: update ja/${page} to follow ${page}`);
    }
    assert.equal((ja.match(/<script/g) || []).length, (en.match(/<script/g) || []).length, "script count");
    assert.match(ja, /href="\.\.\/learn\.css"/);
    assert.match(ja, /src="\.\.\/learn\.js"/);
  });
}

// Words that stay English in Japanese text: product names, code, units, data values.
const KEEP = new Set(`power bi direct lake directquery vertipaq v-order order fabric spark warehouse delta-rs delta duckdb
  databricks iceberg parquet cu dax onelake sql endpoint lakehouse duckrun readheavyforpbi writeheavy readheavyforspark
  aemo nyc bts cms green open payments thrift pyarrow plain rle dictionary rle_dictionary encoding_stats encodings
  pagestats pageencodingstats optimize optimizewrite cluster by zorder dl dq rg mb gb kb ms s k m b id ids csv json
  github raw githubusercontent com history runs dev365 dev379 auto t-sql x of the and modern architecture choices
  reporting on azure for capacity metrics app card cash`.split(/\s+/).filter(Boolean));

/** Runs of 4+ consecutive English words (not in KEEP) in the visible text of an HTML string. */
function leaks(html) {
  const text = html.replace(/<script[\s\S]*?<\/script>/g, " ").replace(/<style[\s\S]*?<\/style>/g, " ")
    .replace(/<(pre|code)[\s\S]*?<\/\1>/g, " ").replace(/<!--[\s\S]*?-->/g, " ")
    .replace(/<[^>]+>/g, " ").replace(/&[a-z#0-9]+;/g, " ");
  const out = [];
  for (const chunk of text.split(/[^A-Za-z'’-]*[^\x00-\x7F][^A-Za-z'’-]*|[.;:!?()·,|/=+×–—\n\d%]/)) {
    const words = chunk.trim().split(/\s+/).filter((w) => /[a-z]/i.test(w) && !KEEP.has(w.toLowerCase()));
    if (words.length >= 4) out.push(chunk.trim());
  }
  return out;
}
/** Same, for the strings an inline demo script writes into the page. */
function scriptLeaks(html) {
  const out = [];
  for (const [, body] of html.matchAll(/<script>([\s\S]*?)<\/script>/g)) {
    for (const [, s] of body.matchAll(/(["'`])((?:(?!\1)[^\\\n]|\\.)*)\1/g)) out.push(...leaks(s.replace(/\$\{[^}]*\}/g, " ")));
  }
  return out;
}

for (const page of PAGES) {
  test(`ja/${page} has no untranslated English prose`, () => {
    const ja = read(`ja/${page}`);
    assert.deepEqual([...leaks(ja), ...scriptLeaks(ja)], []);
  });
}

function records() {
  const dir = new URL("../history/runs/", HERE);
  const recs = readdirSync(dir, { withFileTypes: true })
    .filter((e) => e.isFile() && e.name.endsWith(".json") && e.name !== "index.json")
    .map((e) => ({ ...JSON.parse(readFileSync(new URL(e.name, dir), "utf8")), _file: e.name }));
  let ledger = null;
  try { ledger = JSON.parse(readFileSync(new URL("../history/cu.json", HERE), "utf8")); } catch { /* none */ }
  return { recs, ledger };
}

test("the report in Japanese leaks no English prose, on every view", () => {
  const { recs, ledger } = records();
  const views = [{}, { page: "perf" }, { page: "perf", phase: "dq" }, { page: "method" }, { page: "data" }, { page: "how" },
    ...r.PROPERTIES.map((p) => ({ finding: p(r.context(recs, {}, ledger)).id }))];
  const found = [];
  for (const v of views) {
    const { html } = r.render(recs, { ...v, lang: "ja" }, ledger);
    // Tooltips and attribute text count too: they are what a reader hovers.
    const attrText = [...html.matchAll(/(?:data-tip|title|aria-label)="([^"]*)"/g)].map((m) => m[1]).join(" . ");
    for (const l of [...leaks(html), ...leaks(attrText)]) found.push(`${JSON.stringify(v)}: ${l}`);
  }
  setLang("en");
  assert.deepEqual(found, []);
});

test("the report stays English by default", () => {
  const { recs, ledger } = records();
  const { html } = r.render(recs, {}, ledger);
  assert.doesNotMatch(html, /[぀-ヿ]/, "Japanese text on the English page");
});

test("the language switch keeps the view", () => {
  assert.equal(r.otherLangHref("/direct-lake-parquet-layout/", "?finding=sort", ""), "ja/?finding=sort");
  assert.equal(r.otherLangHref("/direct-lake-parquet-layout/ja/", "?page=perf", "", true), "../?page=perf");
  assert.equal(r.otherLangHref("/x/index.html", "", ""), "ja/index.html");
});
