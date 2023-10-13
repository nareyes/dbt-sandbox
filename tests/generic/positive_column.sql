{{
    config(
        severity = 'warn'
    )
}}


{% test positive_column(model, column_name) %}

select
    {{ column_name }}

from {{ model }}
where {{ column_name }} < 0


{% endtest %}