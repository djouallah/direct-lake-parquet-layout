# `dashboard/` — the report

**A live write-up of what in a parquet file decides how fast, and how cheaply, Power BI reads it.**
`report.js` fetches `history/runs/*.json` and `history/cu.json` from `raw.githubusercontent.com` on
every load.

The home page is one card per finding: a headline number, the claim and a status. A card opens that
finding's evidence (`?finding=<id>`, which keeps `?dataset=`). Method, how Power BI reads parquet and
the raw data are pages of their own (`?page=method|how|data`). Every finding reports query time and
**read CU**: the capacity units each benchmark session's own Direct Lake or DirectQuery items billed.
Build CU is deliberately left out.

The findings themselves are made of:

- the short version;
- the method;
- how Direct Lake and DirectQuery read parquet;
- nine findings, one variable each;
- the takeaway;
- the data itself.

Every sentence that states a number is generated from the records, so the page rewrites itself after
each Benchmark run. It cannot quote a number the records no longer support.

The shape follows
[Writing parquet that VertiPaq likes](https://datamonkeysite.com/2026/08/29/writing-parquet-that-vertipaq-likes/).

```
index.html         the shell: styles, a header, three empty elements
data.js            records -> flat runs[] and columns[]; generation filter, query set, statistics
report.js          the findings, the SVG charts, the page, boot
probe_columns.js   GENERATED: which column each probe_<name> query touches (from benchmark/xmla_compare.py)
probe_columns.mjs  regenerates it: node dashboard/probe_columns.mjs
build.mjs          index.html + the three modules -> one file, twice (live, and offline with data inlined)
data.test.mjs      offline tests, no browser, no network
dag.html           the dbt DAG, `dbt docs generate --static` output
app.js             the PREVIOUS page, no longer served; kept because profile_table.mjs reuses its
app.test.mjs         join to regenerate the README's spark-profile table (capacity.yml)
profile_table.mjs
```

## When it publishes

**On a push to `dashboard/**` on `main`, and on dispatch. Nothing else.** Push a change to the page
and it deploys itself; that is the only automation, and you start nothing by hand in the normal case.

This is a reversal of the repo's "nothing runs on push" rule, and it is safe for a reason that does
**not** generalise — do not copy the trigger to another workflow. That rule exists because the
workflows that COMMIT would otherwise pay for their own commits. `Dashboard` commits nothing (it
deploys to Pages), and `dashboard/**` never matches the `history/` paths that `Benchmark` and
`Capacity units` write. So no commit can trigger a publish and no publish can make a commit: the loop
is not reachable. Two things must stay true —

- **`history/` must never appear in the path filter.** That single edit builds the loop.
- **`Benchmark` and `Capacity units` must never gain a `push:` trigger.** They commit, and one of
  them spends capacity.

The filter is the whole directory even though only `report.js`, `index.html` and `build.mjs` reach the
published bytes. A narrower one is tempting, but if someone later adds `dashboard/theme.css` and
forgets to extend it, the page **silently** stops updating; the broad filter's worst case is a free
no-op deploy for a README edit. `paths:` is also evaluated per PUSH, not per file, so a mixed commit
fires it anyway — which is the same reason and the same cost.

One wrinkle when this trigger is first added, or if it is ever changed: **a commit that edits
`dashboard.yml` but nothing under `dashboard/` does not fire it.** Dispatch once by hand to bootstrap.

## Why raw.githubusercontent, and why the contents API

`raw.githubusercontent.com` serves this repo's own files with `Access-Control-Allow-Origin: *` and a
~5 minute CDN TTL, which is what lets a page hosted on `djouallah.github.io` read them at all. The
repo is public and `history/` has always been committed, so nothing here is a new disclosure.

**It must NOT be served from the Pages origin.** Copying `history/` into `site/` would put the data
back inside the published artifact and make every commit a republish again — the exact thing this
removes.

Raw serves files, not directory indexes, so the listing of `history/runs/` comes from the GitHub
contents API — also CORS-open, and rate-limited to **60 requests per hour per IP** without a token.
One call per page load. When it refuses, the page says so and names the limit, because an empty page
and a rate-limited API look identical to a reader and only one of them means "nothing has ever been
measured".

**DuckDB-WASM was considered and rejected.** The whole dataset is ~300 KB of JSON, already in the
shape the page wants; ~30 MB of wasm from a CDN to query it would be a cost with no matching
benefit. If an ad-hoc SQL explorer over the records is ever wanted, that is a different page.


## Two builds, one implementation

```
node dashboard/build.mjs --out site/index.html              # live: fetches history/ at load time
node dashboard/build.mjs --out dashboard.html --snapshot    # offline: history/ inlined
```

The build concatenates `data.js`, `probe_columns.js` and `report.js` into one module script, in that
order, and strips their relative `import` lines. **Keep each of those imports on a single line**:
the build refuses to emit a page that still carries one. The offline copy uses the same code with
the records inlined in `#snapshot`, so it opens off a disk with no network. The charts are
hand-written SVG with no CDN, so that copy still draws them.

To work on it, build the snapshot copy and open it in a browser:

```
node dashboard/build.mjs --out dashboard.html --snapshot
```

## How the numbers are made comparable

- **One source generation per dataset.** Only runs at the biggest mart row count are kept. A run on
  a smaller slice is a different workload, not a faster one. The data section names every run
  left out.
- **Read CU is per session, per phase.** It is the semantic model, its shortcut lakehouse and that
  lakehouse's SQL endpoint, matched by role and endpoint name (`readCu` in `data.js`). It covers all
  six passes together, so it has no cold/warm split of its own.
- **One query set per dataset.** A total sums the queries at least 60% of the dataset's runs carry
  (`QUERY_SHARE`). A run missing one of them gets no total but keeps its layout row. DirectQuery
  is voted on only by runs that measured DirectQuery.
- **A column's cost is its probe minus `probe_rowcount` in the same pass.** This is the benchmark's
  own decomposition, and it isolates loading that column from the fixed cost of any query.
- **`clear` means every run of the winner beat every run of the loser, with at least 3 each.**
  Anything weaker reads `inside the noise` or `too few runs`. Correlations are Spearman's ρ, and a
  claim says "strong / moderate / only a weak / no link" rather than printing a bare coefficient.
- **The writer is held fixed where it can be.** Row-group size uses delta-rs runs only, where it
  is a dispatched setting. The sort finding compares delta-rs sorted against unsorted, and V-Order
  compares the two Spark profiles. Everything across writers is labelled as a correlation.

## Marks

Colour is the writer family. Three hues are validated all-pairs in both modes with the dataviz
palette validator; a fourth family would fail that check, so Iceberg is grey with a square marker.
A filled mark means the writer reordered the rows (V-Order or a global sort); a hollow one means it
did not. A triangle in the column chart marks a column with row groups that have no dictionary.

## Things that will bite

- **`probe_columns.js` is generated.** Change a probe in `benchmark/xmla_compare.py`, then rerun
  `node dashboard/probe_columns.mjs`. `data.test.mjs` fails until you do.
- **`report.js` boots only when a `#app` element exists**, so `node --test` can import it. Every
  renderer returns a string.
- **Adding a writer** means a case in `writerOf` (`data.js`) and, if it is a new family, an entry
  in `FAMILIES` (`report.js`). A fourth hue fails the all-pairs palette check, so fold it into grey
  or use a shape.

## Isolation

No imports from `benchmark/` or `cu/` at run time, and no third-party package: no bundler, no
framework, no CDN. It is built to be deleted by removing one directory.
