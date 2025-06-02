{% test sequence_gaps(
    model,
    partition_by,
    column_name
) %}
{%- set partition_sql = partition_by | join(", ") -%}
{%- set previous_column = "prev_" ~ column_name -%}
WITH source AS (
    SELECT
        {{ partition_sql + "," if partition_sql }}
        {{ column_name }},
        LAG(
            {{ column_name }},
            1
        ) over (
            {{ "PARTITION BY " ~ partition_sql if partition_sql }}
            ORDER BY
                {{ column_name }} ASC
        ) AS {{ previous_column }}
    FROM
        {{ model }}
)
SELECT
    {{ partition_sql + "," if partition_sql }}
    {{ previous_column }},
    {{ column_name }},
    {{ column_name }} - {{ previous_column }}
    - 1 AS gap
FROM
    source
WHERE
    {{ column_name }} - {{ previous_column }} <> 1
{% if vars.MAIN_OBSERV_EXCLUSION_LIST_ENABLED and {{column_name}} = 'block_number' %}
    AND {{column_name}} NOT IN (
        SELECT
            block_number :: INT
        FROM
            observability.exclusion_list
    )
{% endif %}
ORDER BY
    gap DESC 
{% endtest %}