# CI operations

Operational notes for running and maintaining this repo's CI, moved out of the README so that
file can stay about the findings. Deeper rules for contributors — gating, incremental
strategies, the run record — live in [CLAUDE.md](../CLAUDE.md).

## The workflows

There are three, sharing nothing but the JSON in `history/`:

| workflow | file | does | triggered by |
|---|---|---|---|
| `Benchmark` | `.github/workflows/benchmark.yml` | offline checks, plan, land, build, layout, benchmark, teardown, record | nightly cron · dispatch — the only one that spends capacity |
| `Capacity units` | `.github/workflows/capacity.yml` | reads the Capacity Metrics model, commits `history/cu.json` | `workflow_run` after Benchmark · cron · dispatch |
| `Dashboard` | `.github/workflows/dashboard.yml` | builds and deploys the page | `push` to `dashboard/**` · dispatch |

Never run two `Benchmark` runs at once — they share one Fabric capacity (throttling silently
inflates both runs' numbers) and the output item names are fixed strings, so two concurrent runs
build into the same lakehouse and the first teardown deletes the item the second is still using.

## The build, per engine

`Benchmark` runs the pipeline on the selected engines against Microsoft Fabric / OneLake. In the
workspace it:

1. provisions the engine's Fabric item(s) — a lakehouse for duckrun/iceberg/spark, a lakehouse +
   warehouse for dwh (`.github/scripts/provision.py`);
2. lands the raw source files into the landing lakehouse's `Files`;
3. runs `dbt build` against OneLake for that target — each engine writes and tests its own output
   in one DAG walk. The `layout` job then reads every item back through Delta (`stats.py`) and
   puts every shared table side by side — the staging view, the facts, then the mart — so a
   disagreement in the mart can be traced to its inputs; that parity table is the only thing
   comparing engines to each other. The singular tests are written per dialect
   (`tests/<dataset>/{duckdb,dwh,spark}`, gated per folder in `dbt_project.yml`), so all four
   targets run the same assertions against the output they just wrote.
4. runs the query benchmark (see [benchmark/README.md](../benchmark/README.md)), then the
   teardown deletes everything the run created except the landing lakehouse, and the `record`
   job commits `history/runs/<ts>-<run id>.json`.

## Auth: OIDC, and the rename trap

Auth is **OIDC only** (the `fabric-github-deploy` app is Admin in the workspace) — the repo
needs just `AZURE_CLIENT_ID` / `AZURE_TENANT_ID` secrets and a federated credential.

**This account presents the IMMUTABLE subject form, and it embeds the repo NAME**, so the credential
this repo actually authenticates with is:

```
repo:djouallah@12554469/direct-lake-parquet-layout@1310610554:ref:refs/heads/main
       ^owner  ^owner id  ^repo name               ^repo id
```

(The repo has been renamed twice — `djouallah/dbt` → `fabric-dbt-benchmark` on 2026-08-01,
→ `direct-lake-parquet-layout` on 2026-08-12 — and each rename means a new credential with the
new name in the subject, created the same day.)

Two things follow, both learned the expensive way on the first rename:

- **A rename breaks OIDC even though the repo ID does not change.** The name sits in the middle of
  that string, so "immutable" means the ids are pinned, not that the subject survives a rename. The
  old `dbt_main_immutable` credential carries the *same* owner id and the *same* repo id
  (`1310610554`) and still stopped matching.
- **Adding the plain `repo:<owner>/<repo>:ref:...` form does not help**, because with immutable
  identifiers enabled that form is never presented. A credential for it sits there looking correct
  and matching nothing. `direct_lake_parquet_layout_main` is exactly that, kept only as a fallback
  if the account setting is ever turned off.

The failure is `AADSTS700213: No matching federated identity record found for presented assertion
subject '…'` at the `azure/login` step. **Read the subject in that error and create a credential for
it verbatim** — it is the authoritative statement of what GitHub is sending, and it reads like a
missing secret if you skip it:

```bash
az ad app federated-credential create --id <appId> --parameters @fic.json
```

The old `dbt_main` and `dbt_main_immutable` credentials were **deleted** on 2026-08-01, right after
the new ones were verified green. `dbt_main` in particular was a standing trust for a repo name that
could be created again, and the app is Admin in the workspace — so the trust list should carry
nothing that does not currently authenticate something. Keep it that way: when a repo is renamed or
retired, remove its credential in the same pass that adds the replacement.

## Where the DuckDB-family build actually runs

In a Fabric notebook, always. `duckrun` and `iceberg` are both just DuckDB in a Python process, so
that process *could* live on the GitHub runner — but CI no longer tries to decide. `fabric_run.py`
zips the project into a throwaway notebook via `duckrun.run_python` and runs `fabric_build.py`
there, data-local to OneLake, so a backlog drain never pulls the corpus over the public internet.
`dwh` and `spark` never had the choice: their compute *is* Fabric's server, and the runner only
ever holds the dbt client.

Two placement heuristics were tried and removed. The first asked whether `land` had downloaded a
new `PUBLIC_DAILY` file, which describes the download rather than the backlog — a from-scratch
lakehouse has the whole ~3000-file archive waiting with nothing new landed, and that reads as
"small". The second counted each engine's genuinely pending files, but had to read that count
through the very tables the build was about to write, so an unreadable table collapsed the
estimate to its fail-safe sentinel regardless.

The saving on offer was one Fabric session start-up on quiet intraday runs. The cost of being
wrong was a 7GB runner thrashing through a full archive fold. One path is worth more than the
saving.
