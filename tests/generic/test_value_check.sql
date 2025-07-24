{% test value_check(model, column_name) %}

SELECT * FROM
{{ model }}
WHERE {{ column_name }} < 10000

{% endtest %}