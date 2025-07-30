SELECT

{% set items = ['first', 'second', 'third', 'fourth']  %}

{% for item in items  %}
    a {{ item }}
{% endfor %}

FROM employee_raw
LIMIT 5