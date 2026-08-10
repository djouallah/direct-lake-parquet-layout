{#-- Root of the NYC TLC parquet archive in the landing lakehouse Files section.

     A SIBLING of get_csv_archive_path(), not a parameterisation of it. The two datasets land
     different formats in different folders and nothing about one should be able to move the other:
     the AEMO models' compiled SQL must stay byte-identical to what 40+ committed run records were
     measured against, and the cheapest guarantee of that is that they call a macro this change
     never touched. Same reasoning as the three per-dialect model trees. --#}
{%- macro get_parquet_archive_path() -%}
{{ get_root_path() ~ '/parquet_raw' }}
{%- endmacro -%}
