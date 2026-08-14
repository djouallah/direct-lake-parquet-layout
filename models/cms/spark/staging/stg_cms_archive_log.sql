-- Spark table over the archive log download_cms_payments.py writes to the landing lakehouse.
-- Fabric Spark 3.5 has no read_files(), so read parquet with the path datasource syntax
-- (parquet.`path`). Materialized as a TABLE (not a view) so it is a physical OneLake Delta table a
-- neutral Delta reader can see -- same as every engine's fct_*/dim_* output.
{{ config(materialized='table', schema='landing') }}

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
FROM parquet.`{{ get_root_path() }}/parquet_raw_archive_log.parquet`
