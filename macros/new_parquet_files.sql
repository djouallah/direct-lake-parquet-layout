{#-- The OneLake parquet paths the NYC fact reads THIS RUN, resolved from the archive log
     download_nyc_taxi.py writes (parquet_raw_archive_log.parquet). Sibling of new_source_files();
     see get_parquet_archive_path.sql for why these are siblings rather than one parameterised macro.

     Same selection rule as the AEMO macro, so both datasets fold files the same way:

     - first run / --full-refresh (this_relation none): the WHOLE source folder as one wildcard
       path. Fabric OPENROWSET rejects >1024 explicit BULK paths per statement (and chunking with
       UNION ALL does not help — the limit is per statement), so a full archive cannot be
       enumerated; the wildcard reads it all in one statement instead.
     - incremental: the EXPLICIT list of files NOT already ingested into {{ this }}.

     The explicit path is built from the log's `archive_path` ('/<subfolder>/<name>.parquet'), which
     carries the real on-disk filename WITH extension — prefix it with the parquet_raw root. The
     NOT IN dedup is on `file_stem` (extension stripped), which is exactly what the models store as
     [file], since parse_filename() takes everything before the first '.'.

     `file_stem`, not `csv_filename`: the AEMO log calls that column csv_filename and the NYC log
     does not, because naming a parquet file's stem "csv" is the kind of small lie that survives for
     a year. That difference is the whole reason this macro is not the CSV one with a flag.

     `source_type` is also the subfolder under parquet_raw/ ('yellow'). Returns a list of full abfss
     paths; an empty list is valid (the model compiles to a no-op). --#}
{% macro new_parquet_files(source_type, this_relation) %}
  {%- if not execute -%}{{ return([]) }}{%- endif -%}
  {%- set root = get_parquet_archive_path() -%}
  {%- if this_relation is none -%}
    {{ return([root ~ '/' ~ source_type ~ '/*.parquet']) }}
  {%- endif -%}
  {%- set log_path = get_root_path() ~ '/parquet_raw_archive_log.parquet' -%}
  {%- set q -%}
    SELECT l.archive_path
    FROM OPENROWSET(BULK '{{ log_path }}', FORMAT = 'PARQUET') AS l
    WHERE l.source_type = '{{ source_type }}'
      AND l.file_stem NOT IN (SELECT DISTINCT [file] FROM {{ this_relation }})
    ORDER BY l.archive_path
  {%- endset -%}
  {%- set archive_paths = run_query(q).columns[0].values() -%}
  {%- set paths = [] -%}
  {%- for ap in archive_paths %}{% do paths.append(root ~ ap) %}{% endfor -%}
  {#-- More pending files than one OPENROWSET statement can take as an explicit list (hard Fabric
       limit, 1024 paths/statement): fall back to the folder wildcard. The model's incremental
       guard dedups the re-read rows, so this is slower, never wrong. Only happens while a large
       backlog drains — and with monthly files that is ~1024 months, i.e. never in practice. --#}
  {%- if paths | length > 1024 -%}
    {{ return([root ~ '/' ~ source_type ~ '/*.parquet']) }}
  {%- endif -%}
  {{ return(paths) }}
{% endmacro %}
