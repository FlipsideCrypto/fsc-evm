{% test accepted_values_recent_row(
    model,
    column_name,
    context_column,
    value,
    timestamp_column
) %}

SELECT
    {{ column_name }},
    {{ context_column }}
FROM
    {{ model }}
WHERE
    {{ timestamp_column }} = (
        SELECT
            MAX(
                {{ timestamp_column }}
            )
        FROM
            {{ model }}
    )
    AND {{ column_name }} <> {{ value }}

{% endtest %}
