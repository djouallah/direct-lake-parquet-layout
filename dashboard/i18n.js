/**
 * The page's two languages. English is the default, always: the language is decided by the PATH alone
 * (`/ja/…` is Japanese, everything else English) — never by `navigator.language`, never remembered in
 * storage — so a bare link opens in English and a shared `/ja/` link opens in Japanese.
 *
 * Strings are bilingual INLINE at each call site, `tr(english, japanese)`, so a sentence built around
 * live numbers interpolates them natively in both languages and an edit to one sits beside the other.
 *
 * GLOSSARY — use these everywhere (report.js, data.js, ja/*.html, learn.js):
 *   kept as-is: Power BI, Direct Lake, DirectQuery, VertiPaq, V-Order, Fabric, Spark, Warehouse,
 *               delta-rs, DuckDB, Databricks, Iceberg, parquet, Delta, CU, DAX, profile names
 *               (readHeavyForPBI, writeHeavy, readHeavyForSpark), column and table names, code.
 *   row group → 行グループ        dictionary encoding → 辞書エンコーディング   dictionary → 辞書
 *   segment → セグメント          transcoding / transcode → トランスコード     footer → フッター
 *   cold / warm / hot → コールド / ウォーム / ホット                         read CU → 読み取り CU
 *   sort order → ソート順         sort → ソート     run (of equal values) → ラン
 *   writer → ライター             dataset → データセット   query → クエリ    file → ファイル
 *   column → 列                   table → テーブル          encoding → エンコーディング
 */
let LANG = "en";

/** `"ja"` or `"en"`; anything else is English. */
export function setLang(l) { LANG = l === "ja" ? "ja" : "en"; return LANG; }
export function lang() { return LANG; }
/** The string for the current language. A missing Japanese falls back to the English. */
export function tr(en, ja) { return LANG === "ja" && ja != null ? ja : en; }
/** `{ en, ja }` → the current language's value. */
export function pick(o) { return LANG === "ja" && o.ja != null ? o.ja : o.en; }
