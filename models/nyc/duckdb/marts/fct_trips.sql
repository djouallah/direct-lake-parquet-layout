-- The wide raw fact, and the table under layout test on this dataset — stats.py's MART for
-- DATASET=nyc, the way mart.fct_summary is for aemo.
--
-- WHY A WIDE RAW FACT AND NOT AN AGGREGATE. The whole reason this dataset exists is that
-- fct_summary is five narrow columns on a regular 5-minute grid, which is close to the worst
-- possible surface for V-Order: an encoding pass acts on column count x categorical skew, and that
-- table has neither. Here there are 17 columns, of which RatecodeID, store_and_fwd_flag,
-- payment_type and VendorID sit at 97-99% single-value and the two LocationIDs are Zipfian on
-- Manhattan and the airports. Summarising this into a mart would throw away the only property
-- being measured.
--
-- THIS IS THE ONE MODEL IN THE PROJECT THAT WRITES WITH `append`, AND IT IS A KNOWING EXCEPTION
-- TO A STANDING RULE. CLAUDE.md says nothing writes with append any more and nothing should go
-- back to it. Here nothing else is expressible, and the reason is worth stating precisely so this
-- is not "fixed" back into a keyed write that cannot run:
--
--   * TLC trip records have NO natural unique key. Duplicate trips are a documented feature of the
--     source, so no combination of the 17 columns is a key.
--   * The only candidate is the FILE, and the source is 3M rows per file -- not unique on it.
--   * duckrun REFUSES a unique_key its source is not unique on, and the refusal covers BOTH write
--     paths: engine.assert_source_unique() guards the delta-rs merge AND the routed insert-only
--     anti-join ("MERGE source is not unique on the join key (file)"). Verified against both
--     `merge` + do_nothing and `insert`; each raises on the first real incremental file. So a
--     file-keyed write does not degrade gracefully on duckrun -- it does not run at all.
--   * A surrogate row id would fix that and was rejected: DuckDB gets one free from parquet
--     metadata (file_row_number) and Spark from _metadata.row_index, but Fabric Warehouse has no
--     equivalent, so dwh would have to invent one with an arbitrary ORDER BY. That is a stored
--     column on ~1.5B rows in a benchmark about write cost, whose VALUES would differ on one
--     engine -- in a project whose whole claim is that the four outputs are the same table.
--   * delete+insert is not available either: on duckrun it is a fenced FULL-TABLE OVERWRITE, i.e.
--     a rewrite of the entire fact on every incremental run. That already killed the process at
--     143M rows.
--
-- WHAT GUARDS THE WRITE INSTEAD. The pre_hook's file list is computed from {{ this }}, so a file
-- already in the table is never read: this is the file-selection guard CLAUDE.md correctly calls
-- weaker than a key, because the list is computed BEFORE the write and two overlapping runs could
-- both see a file as new. Two things bound that exposure here. Every dispatch tears down its own
-- output and rebuilds from nothing, so there is no cross-run incremental state to race over -- the
-- window left is a `dbt retry` racing its own run. And assert_fct_trips_matches_archive_log is the
-- detector: it compares each file's stored row count against the count the downloader read from
-- that file's parquet footer, so a doubled or truncated month fails the leg that wrote it.
--
-- The other three engines DO keep a write-time guard, and the asymmetry is deliberate rather than
-- an oversight: spark's merge + skip_matched_step and dwh's delete+insert on [file] are both
-- idempotent per file with a non-unique key, so only the DuckDB pair gives it up. iceberg follows
-- duckrun rather than keeping a merge of its own, because the standing rule for this tree is that
-- the two run byte-identical model code -- a `target.name` branch here would cost more than the
-- guard is worth on the one engine that could still have it.
--
-- No incremental_predicates: they narrow a keyed write's target read, and there is no keyed write
-- left to narrow. `append` never reads the target at all, which is also why this model is cheaper
-- per run than any AEMO fact.
{%- set pending_files_query -%}
SELECT file_stem FROM {{ ref('stg_parquet_archive_log') }}
WHERE source_type = 'yellow'
{%- if is_incremental() %}
AND file_stem NOT IN (SELECT DISTINCT file FROM {{ this }})
{%- endif -%}
{%- endset -%}

{%- if execute and flags.WHICH in ('run', 'build', 'retry') -%}
  {%- set files_result = run_query(pending_files_query) -%}
  {%- set pending_files = files_result.columns[0].values() | list if files_result else [] -%}
{%- else -%}
  {#-- Parse time: unknowable. `none` means "assume there is work", so the model renders its real
       body rather than the no-op branch — the compiled SQL a reviewer reads is the one that runs. --#}
  {%- set pending_files = none -%}
{%- endif -%}
{%- set has_files = pending_files is none or pending_files | length > 0 -%}

{%- set cols = nyc_trip_columns() -%}

{#-- The sort and geometry knobs are the SAME dispatch inputs the AEMO mart reads, so a layout
     question can be asked of either dataset with one workflow. The env default below is
     NYC-shaped: pickup_date first (every composite query groups or filters THROUGH the date
     relationship, and a date is far lower cardinality than the timestamp, so it RLEs where the
     timestamp would not), then PULocationID (the widest skewed categorical, and what the
     selectivity ladder filters on). The plan job REFUSES a sort_by naming columns this dataset's
     mart does not have -- which is what a dispatch gets by leaving the field at the other
     dataset's key -- rather than substituting, because a run that quietly measured a layout other
     than the one the form described is the failure that reshaped that field. --#}
-- pickup_date IS A STORED COLUMN AND IT IS NOT THE month_key MISTAKE. Direct Lake cannot relate a
-- DATETIME column to a DATE dimension key, and it has no calculated columns to bridge one, so a
-- date dimension is only reachable if the fact carries a DATE. month_key was rejected because
-- NOTHING read it — no model, no test, no macro, no semantic model; this one is read by the
-- relationship every date-grouped query in the suite traverses, and it is one narrow column whose
-- values are near-contiguous under the default sort, so it costs little and RLEs well.
{#-- `auto` is duckrun's own picker: it profiles the data and chooses the key, and it is the
     dispatch default. It must be passed as a SCALAR — duckrun raises on `['auto']`, because a list
     means "these columns are the key". Any other value is a comma-separated column list, and blank
     means no sort at all.

     The comment sits ABOVE the tag, not inside it: dbt parses `config(...)` as an expression, and
     a Jinja comment between its arguments is `invalid syntax for function call expression` — an
     error that points at the `{{ config(` line and says nothing about comments. --#}
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    sort_by=(('auto' if env_var('DUCKDB_SORT_BY', 'auto').lower() == 'auto'
              else env_var('DUCKDB_SORT_BY', 'auto').split(','))
             if env_var('DUCKDB_SORTED', 'false') == 'true' else none),
    max_row_group_size=(env_var('DUCKDB_ROW_GROUP_SIZE', '16000000') | int
                        if env_var('DUCKDB_SORTED', 'false') == 'true' else none),
    target_file_size_mb=(env_var('DUCKDB_FILE_SIZE_MB', '1024') | int
                         if env_var('DUCKDB_SORTED', 'false') == 'true' else none),
    pre_hook="SET VARIABLE nyc_yellow_paths = (SELECT COALESCE(NULLIF(list('{{ get_parquet_archive_path() }}' || archive_path), []), ['']) FROM (SELECT archive_path FROM {{ ref('stg_parquet_archive_log') }} WHERE source_type = 'yellow'{% if is_incremental() %} AND file_stem NOT IN (SELECT DISTINCT file FROM {{ this }}){% endif %} ORDER BY archive_path))"
) }}

{% if has_files %}
{#-- A plain read, with no union_by_name and no schema merging, because the archive is HOMOGENEOUS
     by construction: download_nyc_taxi.py rewrites every month to the canonical 17-column schema
     before uploading it. That normalisation exists for Spark, which refuses a scan whose files
     disagree on a column's type and which TLC's own files do — but the benefit lands here too, as
     one plain statement. The CASTs below are therefore no-ops over already-canonical data, kept as
     the explicit declaration that all four engines store the same types. --#}
WITH trips AS (
  SELECT *
  FROM read_parquet(
    getvariable('nyc_yellow_paths'),
    filename = 1,
    hive_partitioning = false
  )
)

SELECT
  {%- for name in cols %}
  CAST({{ name }} AS {{ nyc_trip_type(name, 'duckdb') }}) AS {{ name }},
  {%- endfor %}
  CAST(tpep_pickup_datetime AS DATE) AS pickup_date,
  {{ parse_filename('filename') }} AS file
FROM trips
{% else %}
SELECT * FROM {{ this }} WHERE FALSE
{% endif %}
