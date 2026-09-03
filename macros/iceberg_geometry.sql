{# The write geometry for the ICEBERG leg, as Iceberg table properties.

   ONE DISPATCH DRIVES BOTH DuckDB WRITERS, and that is the point of this macro. duckrun takes
   `max_row_group_size` (ROWS) and `target_file_size_mb`; duckdb-iceberg takes
   `write.parquet.row-group-size` (ROWS — DuckDB's own extension to the spec, which is what makes
   the units line up) and `write.target-file-size-bytes`. So the SAME `row_group_size` /
   `file_size_mb` dispatch inputs now describe both, with no conversion on the row count and a
   plain MiB multiply on the file size. Two writers, one geometry, one number in the form — which
   is the comparison the duckrun/iceberg pair exists to make.

   INTEGERS, NEVER '128MB'. duckdb-iceberg's `ParseByteSizeOptionallyFormatted` accepts human
   units, and duckdb-iceberg#1150 records that it then writes the human STRING into the table
   metadata, where another engine reading the table may not parse it. The property is metadata
   other readers see, so it is spelled in bytes.

   `auto` on either input yields no property for it, so a default dispatch emits an EMPTY dict and
   `duckdb__create_table_as` falls back to the adapter's own SQL byte for byte. Absence is the
   writer's default (128 MB row groups / 512 MB files since duckdb-iceberg#1202), not zero.

   ⚠️ MART ONLY, matching where duckrun's geometry keys sit. Putting it on the facts would give
   iceberg a knob on tables duckrun writes at its default, which is the asymmetry this macro
   exists to remove. #}
{% macro iceberg_geometry() %}
  {%- set rg = env_var('DUCKDB_ROW_GROUP_SIZE', 'auto') -%}
  {%- set mb = env_var('DUCKDB_FILE_SIZE_MB', 'auto') -%}
  {%- set out = {} -%}
  {%- if rg | lower != 'auto' -%}
    {%- do out.update({'write.parquet.row-group-size': rg | int}) -%}
  {%- endif -%}
  {%- if mb | lower != 'auto' -%}
    {%- do out.update({'write.target-file-size-bytes': (mb | int) * 1048576}) -%}
  {%- endif -%}
  {{ return(out) }}
{% endmacro %}
