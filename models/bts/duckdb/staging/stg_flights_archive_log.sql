-- Staging table over the archive log download_bts_flights.py writes to the landing lakehouse
-- (Files/parquet_raw_archive_log.parquet — same filename and columns as the nyc log, in bts's own
-- landing lakehouse). One log; every engine reads it with SQL.
-- INCREMENTAL, not table/view, for the same reason as the other datasets' staging models: the
-- DuckDB Iceberg catalog supports neither CREATE VIEW nor the table materialization's temp-table
-- RENAME, but it does CREATE TABLE AS + INSERT. Insert-only, keyed on
-- (source_type, source_filename), spelled the way both DuckDB targets accept verbatim — see
-- models/aemo/duckdb/marts/fct_price.sql for the full reasoning behind one config serving duckrun
-- and iceberg alike.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    merge_clauses={'when_matched': [{'action': 'do_nothing'}]},
    unique_key=['source_type', 'source_filename'],
    schema='landing'
) }}

SELECT
    source_type,
    source_filename,
    archive_path,
    archived_at,
    row_count,
    source_url,
    etag,
    file_stem,
    columns
FROM read_parquet('{{ get_root_path() }}/parquet_raw_archive_log.parquet')
{% if is_incremental() %}
WHERE (source_type, source_filename) NOT IN (SELECT source_type, source_filename FROM {{ this }})
{% endif %}
