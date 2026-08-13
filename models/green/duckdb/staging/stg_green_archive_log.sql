-- Staging table over the archive log download_green_taxi.py writes to the landing lakehouse
-- (Files/parquet_raw_archive_log.parquet). One log; every engine reads it with SQL.
-- INCREMENTAL, not table/view, for the same reason as the AEMO staging model: the DuckDB Iceberg
-- catalog supports neither CREATE VIEW nor the table materialization's temp-table RENAME, but it
-- does CREATE TABLE AS + INSERT. Insert-only, keyed on (source_type, source_filename), spelled the
-- way both DuckDB targets accept verbatim — see models/aemo/duckdb/marts/fct_price.sql for the
-- full reasoning behind that one config serving duckrun and iceberg alike.
-- The WHERE keeps the merge source to just the unlogged rows, and reading {{ this }} in the body
-- puts the read and the commit on one snapshot.
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
