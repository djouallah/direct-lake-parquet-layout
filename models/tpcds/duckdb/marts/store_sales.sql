-- TPC-DS `store_sales` -- the LARGEST fact table of the white paper's subset, and this
-- dataset's MART: the table stats.py profiles deeply and the layout tables rank.
--
-- A PASS-THROUGH. The white paper's section 4.5 customisation happens at LAND time, in
-- download_tpcds.py, so the parquet under parquet_raw/store_sales/ already IS the paper's table: the facts
-- have had every any-null row dropped and carry `cache_buster`, and date_dim carries `d_date_sk_1`
-- and only the 2021-2026 rows. This model selects the columns and nothing else -- no CAST, no
-- derived column, no filter. dsdgen emits one canonical schema at every scale factor, so unlike
-- nyc/green/cms there is no drift to normalise and no source pathology to guard against.
--
-- Columns come from macros/tpcds_columns.sql so all four engines store the same columns in the same
-- order. `.github/scripts/test_tpcds_columns.py` pins that list against the generator's.
--
-- FULL REBUILD, NOT A FILE-DRIVEN INCREMENTAL, and this is the only dataset here that works that
-- way. Every other fact grows a file at a time, so it carries a `file` column, resolves the pending
-- files in a pre_hook and merges on `file` first. dsdgen emits a whole scale factor in one go: there
-- is no arrival order, no watermark and nothing to top up, so there is no `file` column and no file
-- list. The paper writes these tables with a single CTAS and so does this.
{%- set cols = tpcds_columns('store_sales') -%}
--
-- THE `incremental` SPELLING IS THE ICEBERG CATALOG'S REQUIREMENT, NOT AN INTENT TO TOP UP. This
-- tree renders for BOTH DuckDB targets, and the OneLake Iceberg REST catalog supports neither
-- CREATE VIEW nor dbt-duckdb's table materialization (which RENAMEs a temp table) -- the same
-- constraint that makes every staging model in this project incremental. It does do CREATE TABLE AS
-- + INSERT. So the write is spelled as an insert-only merge and, because the teardown deletes the
-- output item at the end of every dispatch, EVERY CI run is a first run: duckrun takes the overwrite
-- branch, which is the CTAS the paper describes, and applies the dispatched sort and geometry to it
-- (delta_plugin.py `_store_overwrite`). The merge key only does work on a by-hand re-run.
--
-- The key is the TPC-DS primary key and it is genuinely unique after the null drop, verified at SF1.
-- Do NOT copy nyc's `append`: the reason that model has no usable key does not hold here.
--
-- The geometry knobs are the same dispatch inputs every other mart here reads, so a layout
-- question can be asked of this dataset with the same workflow. `auto` must be a SCALAR --
-- duckrun raises on `['auto']`, because a list means "these columns are the key". The comment
-- sits ABOVE the tag: dbt parses config(...) as an expression, and a Jinja comment between its
-- arguments is a syntax error that points at the config line and never mentions comments.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    merge_clauses={'when_matched': [{'action': 'do_nothing'}]},
    unique_key=['ss_item_sk', 'ss_ticket_number'],
    sort_by=('auto' if env_var('DUCKDB_SORTED', 'false') == 'true' else none),
    max_row_group_size=(none if env_var('DUCKDB_ROW_GROUP_SIZE', 'auto').lower() == 'auto'
                        else env_var('DUCKDB_ROW_GROUP_SIZE', 'auto') | int),
    target_file_size_mb=(none if env_var('DUCKDB_FILE_SIZE_MB', 'auto').lower() == 'auto'
                         else env_var('DUCKDB_FILE_SIZE_MB', 'auto') | int)
) }}

SELECT
{%- for name in cols %}
  {{ name }}{{ "," if not loop.last }}
{%- endfor %}
FROM read_parquet('{{ get_parquet_archive_path() }}/store_sales/*.parquet')
