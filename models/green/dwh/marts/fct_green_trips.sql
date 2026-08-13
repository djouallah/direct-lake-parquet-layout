{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['[file]'],
    on_schema_change='sync_all_columns',
    schema='mart'
) }}

{#-- pickup_date IS A STORED COLUMN AND IT IS NOT THE month_key MISTAKE. Direct Lake cannot
     relate a DATETIME column to a DATE dimension key, and it has no calculated columns to bridge
     one, so a date dimension is only reachable if the fact carries a DATE. This one is read by
     the relationship every date-grouped query in the suite traverses, and it is one narrow column
     whose values are near-contiguous under the default sort, so it costs little and RLEs well.

     A JINJA comment, and BELOW the config, like every other note in this tree: dbt-fabric wraps a
     model in EXEC('create view ... as <sql>') and a leading `--` block collapses onto the SELECT.
     The duckdb and spark copies carry the same note as plain `--`, which is safe there. --#}

{#-- The wide raw fact — this dataset's MART, the table under layout test. See the duckdb copy's
     header for why it is a raw fact and not an aggregate.

     THIS IS THE ONE PLACE dwh DELIBERATELY DIVERGES FROM THE OTHER THREE, and the reason is a hard
     adapter limit rather than a preference. TLC trip records have NO natural unique key (duplicate
     trips are a documented feature of the source) and no stable row identifier is free across the
     three dialects, so the incremental key is the FILE. On duckrun, iceberg and spark that is exact:
     none of them ever executes a matched branch, so a non-unique key degenerates to "insert the rows
     of files not already present". dbt-fabric CANNOT express that — fabric__get_merge_sql delegates
     to default__get_merge_sql, which always emits WHEN MATCHED THEN UPDATE SET <every column> — so a
     merge on [file] is a many-to-many match and raises error 8672, "attempted to UPDATE or DELETE
     the same row more than once", the first time a file's rows are re-offered.

     delete+insert on [file] is CLAUDE.md's own sanctioned fallback, and it is safe HERE in a way it
     was not for fct_summary. There the strategy retracted rows a recomputation no longer produced,
     because the model's output for a date could change. A landed month's parquet is IMMUTABLE:
     replacing a whole file writes back exactly the rows that file contains, so nothing that should
     survive is retracted. The unit of replacement is the unit of landing.

     Bracket every key column — dbt interpolates them raw and `file` is reserved.

     No month_key, no partitioning, no surrogate key. A stored column nothing reads is the
     month_key mistake in a benchmark whose subject is write cost. --#}

{%- set cols = green_trip_columns() -%}

{#-- The set of files is resolved from the archive log and passed to OPENROWSET as an EXPLICIT
     BULK (...) list, not a folder glob — a glob re-reads the whole archive every run and discards
     all but the newest files. With monthly files the 1024-path statement limit is unreachable in
     practice, so the wildcard fallback in new_parquet_files() is theory here. --#}
{%- set new_files = new_parquet_files('green', this if is_incremental() else none) -%}
{%- if is_incremental() and new_files | length == 0 -%}
{#-- No new months this run: compile to a zero-row no-op. --#}
SELECT * FROM {{ this }} WHERE 1 = 0
{%- else -%}
SELECT
  {%- for name in cols %}
  TRY_CAST([{{ name }}] AS {{ green_trip_type(name, 'fabric') }}) AS [{{ name }}],
  {%- endfor %}
  TRY_CAST([lpep_pickup_datetime] AS DATE) AS [pickup_date],
  {{ parse_filename('src.filepath()') }} AS [file]
FROM {{ openrowset_parquet_files(new_files) }} AS src
{#-- Dedup guard, rendered ONLY on the wildcard fallback: T-SQL rejects filepath(N) when the BULK
     path carries no wildcard to index, so it must not appear when the file list is explicit — and
     an explicit list already excludes ingested files. --#}
{%- if is_incremental() and new_files | length == 1 and '*' in new_files[0] %}
WHERE {{ parse_filename('src.filepath(1)') }} NOT IN (SELECT [file] FROM {{ this }})
{%- endif %}
{%- endif %}
