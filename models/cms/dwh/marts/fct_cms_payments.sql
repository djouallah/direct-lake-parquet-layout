{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['[file]', '[Record_ID]'],
    on_schema_change='sync_all_columns',
    schema='mart'
) }}

{#-- A JINJA comment, and BELOW the config, like every other note in this tree: dbt-fabric wraps a
     model in EXEC('create view ... as <sql>') and a leading `--` block collapses onto the SELECT.
     The duckdb and spark copies carry the same notes as plain `--`, which is safe there. --#}

{#-- The wide raw fact — this dataset's MART, the table under layout test. See the duckdb copy's
     header for why 91 columns, and for the sparsity and dual skew regime this dataset was added
     to measure.

     ⚠️ THIS IS A REAL MERGE, NOT green's delete+insert, AND THE DIFFERENCE IS THE KEY. green's dwh
     fact falls back to delete+insert on [file] because TLC publishes no unique key, so a merge on
     [file] alone is a many-to-many match and dbt-fabric — whose fabric__get_merge_sql delegates to
     default__get_merge_sql, always emitting WHEN MATCHED THEN UPDATE SET <every column> — raises
     error 8672, "attempted to UPDATE or DELETE the same row more than once". CMS publishes
     Record_ID, so ([file], [Record_ID]) is genuinely unique and the match is one-to-one. The forced
     UPDATE branch is then a semantic no-op on append-only data (a matched row is rewritten with its
     own values), which is the same accepted cost the AEMO facts carry on this engine.

     That also makes dwh's guard here STRONGER than green's rather than weaker: Fabric Warehouse is
     the one engine whose write path can genuinely duplicate — snapshot isolation with no commit
     check — and on this dataset it has a keyed write to shrink that window with, where on green it
     had only the file list.

     Bracket every key column: dbt interpolates them raw into the ON clause and `file` is reserved.

     on_schema_change='sync_all_columns' is not decoration. With dbt's default `ignore`,
     dest_columns comes from the EXISTING relation, so a column added or removed in the macro would
     have the merge select a column the temp relation no longer has ("Invalid column name") — the
     exact failure the month_key removal hit. With 91 columns the odds of that are higher, not
     lower.

     No surrogate key, no month_key, no partitioning. A stored column nothing reads is the month_key
     mistake in a benchmark whose subject is write cost. --#}

{%- set cols = cms_payment_columns() -%}

{#-- The set of files is resolved from the archive log and passed to OPENROWSET as an EXPLICIT
     BULK (...) list, not a folder glob — a glob re-reads the whole archive every run and discards
     all but the newest files. ~84 monthly files across seven program years, so the 1024-path
     statement limit is unreachable in practice and the wildcard fallback in new_parquet_files() is
     theory here. --#}
{%- set new_files = new_parquet_files('cms', this if is_incremental() else none) -%}
{%- if is_incremental() and new_files | length == 0 -%}
{#-- No new months this run: compile to a zero-row no-op. --#}
SELECT * FROM {{ this }} WHERE 1 = 0
{%- else -%}
SELECT
  {%- for name in cols %}
  TRY_CAST({{ cms_payment_value('[' ~ name ~ ']', name, 'fabric') }} AS {{ cms_payment_type(name, 'fabric') }}) AS [{{ name }}],
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
