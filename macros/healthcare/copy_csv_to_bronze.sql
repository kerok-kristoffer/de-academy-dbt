-- macros/copy_csv_to_bronze.sql
{% macro copy_csv_to_bronze(target_table, stage_name, file_pattern, columns, file_format='healthcare_db.bronze.ff_csv') %}

-- 1) Create the table (all STRING + metadata)
{% call statement('create_bronze_tbl', fetch_result=False) %}
create table if not exists {{ target_table }} (
  {% for c in columns %}
    {{ adapter.quote(c|lower|replace(' ', '_')) }} string{% if not loop.last %},{% endif %}
  {% endfor %},
  _src_file_name string,
  _src_row_number number,
  _ingested_at timestamp_ntz
);
{% endcall %}

-- 2) Copy from stage → table
{% call statement('copy_into_bronze', fetch_result=True) %}
copy into {{ target_table }} (
  {% for c in columns %}
    {{ adapter.quote(c|lower|replace(' ', '_')) }}{% if not loop.last %},{% endif %}
  {% endfor %},
  _src_file_name, _src_row_number, _ingested_at
)
from (
  select
    {% for c in columns %}
      t.${{ loop.index }}::string{% if not loop.last %},{% endif %}
    {% endfor %},
    metadata$filename as _src_file_name,
    metadata$file_row_number as _src_row_number,
    current_timestamp() as _ingested_at
  from @{{ stage_name }} (file_format => '{{ file_format }}', pattern => '{{ file_pattern }}') t
)
on_error = 'continue';
{% endcall %}

{# Log COPY results to the dbt console #}
{% set res = load_result('copy_into_bronze') %}
{% if res and res['data'] %}
  {% set t = res['data'] %}
  {% for row in t.rows %}
    {% do log("COPY: file=" ~ row[0] ~ ", status=" ~ row[1] ~ ", rows_loaded=" ~ row[3], info=True) %}
  {% endfor %}
{% else %}
  {% do log("COPY returned no result rows (pattern may have matched 0 files).", info=True) %}
{% endif %}

{% endmacro %}
