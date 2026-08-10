-- Table over the archive log download_nyc_taxi.py writes to the landing lakehouse
-- (Files/parquet_raw_archive_log.parquet). Materialized as a TABLE, not a view, so it is a
-- physical OneLake Delta table a neutral Delta reader can see — same as every engine's
-- fct_*/dim_* output, and what lets stats.py put the row counts side by side.
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
FROM {{ openrowset_parquet(get_root_path() ~ '/parquet_raw_archive_log.parquet') }} AS log
