{#-- Spark counterpart of new_parquet_files(): the parquet FILENAMES (with extension) the NYC fact
     may ingest THIS RUN, resolved from the archive log at COMPILE TIME via run_query and inlined
     into the model as an explicit Hadoop brace glob (parquet.`<root>/<type>/{A.parquet,B.parquet}`).

     Only called on INCREMENTAL runs: a first/full-refresh build reads the bare source folder,
     because everything is new then and a folder scan IS the explicit list.

     Explicit list on incremental, NOT a folder scan, for the same reason as the AEMO leg: reading
     the whole folder and filtering on _metadata.file_name afterwards re-reads the entire archive
     per model per run.

     The selection rule stays IDENTICAL to the other two dialects — files minus whatever
     {{ this }} already holds, oldest first — so every engine folds the SAME files.
     Returns [] while parsing (execute=false). --#}
{% macro spark_new_parquet_files(source_type, this_relation) %}
  {%- if not execute -%}{{ return([]) }}{%- endif -%}
  {%- set q -%}
    SELECT archive_path
    FROM {{ ref('stg_parquet_archive_log') }}
    WHERE source_type = '{{ source_type }}'
      AND file_stem NOT IN (SELECT DISTINCT file FROM {{ this_relation }})
    ORDER BY archive_path
  {%- endset -%}
  {%- set names = [] -%}
  {#-- archive_path is '/<subfolder>/<name>.parquet'; the glob needs just the real filename. --#}
  {%- for ap in run_query(q).columns[0].values() %}{% do names.append(ap.split('/')[-1]) %}{% endfor -%}
  {{ return(names) }}
{% endmacro %}
