{%- set items = ['first', 'second', 'third', 'fourth']  -%}

SELECT
{%- for item in items  %}
    {{ item }}
    {%- if loop.last -%}

    {%- else -%}
    ,
    {%- endif %}
{%- endfor %}
FROM employee_raw
LIMIT 5