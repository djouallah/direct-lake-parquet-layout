{#-- OPENROWSET(BULK ..., FORMAT = 'PARQUET') over OneLake Files, for the NYC dwh models.

     Sibling of openrowset_csv.sql, and deliberately much smaller, because parquet is self-describing:
     there is no FIELDTERMINATOR, no ROWTERMINATOR, no FIRSTROW and — the important one — no
     `WITH (...)` ordinal block. The CSV macro types every column as VARCHAR by position because the
     landed AEMO reports are ragged multi-record text; a parquet file carries its own schema, so
     OPENROWSET infers it and the model selects columns BY NAME.

     That name-based read is what makes the archive's schema drift survivable. TLC has published
     yellow trip data since 2009 and the column set moved over the years (zone ids replacing lat/lon,
     surcharges appearing, one month shipping `Airport_fee` where its neighbours ship `airport_fee`).
     The guard is at LAND time, not read time: download_nyc_taxi.py reads each file's footer and
     refuses to archive one that does not carry the core column list, so everything under
     parquet_raw/ is readable by one statement. A file that slipped through would fail here with
     `Invalid column name`, loudly, rather than silently returning NULLs.

     Two entry points, mirroring the CSV macro:
       openrowset_parquet(path_glob)      -- single path / wildcard
       openrowset_parquet_files(paths)    -- EXPLICIT list -> BULK ('a','b',...)

     Prefer the explicit list for incremental ingestion: a folder wildcard re-reads the whole archive
     every run and discards all but the newest files. Fabric caps an explicit BULK list at 1024 paths
     per statement, which is why new_parquet_files() falls back to the wildcard above that.
     Alias the result and call <alias>.filepath(1) in the SELECT to recover the source file path. --#}

{%- macro _openrowset_parquet_bulk(bulk_expr) -%}
OPENROWSET(
    BULK {{ bulk_expr }},
    FORMAT = 'PARQUET'
)
{%- endmacro -%}

{% macro openrowset_parquet(path_glob) -%}
{{ _openrowset_parquet_bulk("'" ~ path_glob ~ "'") }}
{%- endmacro %}

{% macro openrowset_parquet_files(paths) -%}
{%- set quoted = [] -%}
{%- for p in paths %}{% do quoted.append("'" ~ p ~ "'") %}{% endfor -%}
{{ _openrowset_parquet_bulk("(" ~ quoted | join(",") ~ ")") }}
{%- endmacro %}
