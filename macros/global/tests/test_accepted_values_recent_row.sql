{% test accepted_values_recent_row(
    model,
    test_column,
    context_column,
    value,
    timestamp_column
) %}

SELECT
    {{ test_column }},
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
    AND {{ test_column }} <> {{ value }}

{% endtest %}
