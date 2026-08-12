{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['[file]'],
    on_schema_change='sync_all_columns',
    schema='mart'
) }}

{#-- The wide raw fact — this dataset's MART, the table under layout test. See the duckdb copy's
     header for why it is a raw fact and why the incremental key is the file.

     Same divergence as nyc's dwh fct_trips, same hard adapter limit: BTS publishes no primary key
     and duplicate rows exist in the source, so the incremental key is the FILE, which is not
     unique. dbt-fabric's merge always emits WHEN MATCHED THEN UPDATE, which a many-to-many match
     on [file] cannot survive (error 8672), so delete+insert on [file] is the sanctioned fallback —
     and it is safe here for the same reason it is there: a landed month's parquet is IMMUTABLE, so
     replacing a whole file writes back exactly the rows that file contains. The unit of
     replacement is the unit of landing.

     Bracket every key column — dbt interpolates them raw and `file` is reserved.

     A JINJA comment, and BELOW the config, like every other note in this tree: dbt-fabric wraps a
     model in EXEC('create view ... as <sql>') and a leading `--` block collapses onto the
     SELECT. --#}

{%- set cols = bts_flight_columns() -%}

{#-- The set of files is resolved from the archive log and passed to OPENROWSET as an EXPLICIT
     BULK (...) list, not a folder glob — a glob re-reads the whole archive every run and discards
     all but the newest files. new_parquet_files() serves both parquet datasets verbatim: the log
     filename and columns are byte-identical, only the lakehouse differs, and FILES_PATH already
     points there. --#}
{%- set new_files = new_parquet_files('flights', this if is_incremental() else none) -%}
{%- if is_incremental() and new_files | length == 0 -%}
{#-- No new months this run: compile to a zero-row no-op. --#}
SELECT * FROM {{ this }} WHERE 1 = 0
{%- else -%}
SELECT
  {%- for name in cols %}
  TRY_CAST([{{ name }}] AS {{ bts_flight_type(name, 'fabric') }}) AS [{{ name }}],
  {%- endfor %}
  {{ parse_filename('src.filepath()') }} AS [file]
FROM {{ openrowset_parquet_files(new_files) }} AS src
{#-- Dedup guard, rendered ONLY on the wildcard fallback: T-SQL rejects filepath(N) when the BULK
     path carries no wildcard to index, so it must not appear when the file list is explicit — and
     an explicit list already excludes ingested files. --#}
{%- if is_incremental() and new_files | length == 1 and '*' in new_files[0] %}
WHERE {{ parse_filename('src.filepath(1)') }} NOT IN (SELECT [file] FROM {{ this }})
{%- endif %}
{%- endif %}
