{{ config(
    materialized = 'view',
    tags = ['silver_vars']
) }}

{%- set master_vars = master_keys_config() -%}

WITH flattened_data AS (
    {% for key, config in master_vars.items() %}
        SELECT
            SPLIT_PART('{{ key }}', '_', 1) AS package,
            SPLIT_PART('{{ key }}', '_', 2) AS category,
            '{{ key }}' AS key,
            '{{ config.data_type }}' AS data_type,

            {% set default_value = config.default.replace("'", "''") %}
            '{{ default_value }}' AS default_value
        
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
)

SELECT
    package,
    category,
    key,
    data_type,
    default_value,
    {{ dbt_utils.generate_surrogate_key(
        ['key']
    ) }} AS dim_variables_id
FROM
    flattened_data