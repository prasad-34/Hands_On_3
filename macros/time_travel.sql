{% macro time_travel(column_name, number=30, date_part='day') %}
    DATEADD({{ date_part }}, {{ number }}, {{ column_name }})
{% endmacro %}
