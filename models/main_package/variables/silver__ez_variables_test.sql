{{ config(
    materialized = 'view'
) }}

{%- set vars_data = vars_config() -%}
{%- set database = target.database.lower() | replace('_dev', '') -%}

WITH flattened_data AS (
    {% for chain,
        chain_config in vars_data.items() %}
        {% for key,
            value in chain_config.items() %}
            {% if value is mapping %}
                {% for nested_key,
                    nested_value in value.items() %}

                    SELECT
                        '{{ chain }}' AS chain,
                        '{{ nested_key }}' AS key,
                        '{{ nested_value }}' AS VALUE,
                        '{{ key }}' AS parent_key

                        {% if not loop.last %}
                    UNION ALL
                    {% endif %}
                {% endfor %}
            {% else %}
            SELECT
                '{{ chain }}' AS chain,
                '{{ key }}' AS key,
                '{{ value }}' AS VALUE,
                NULL AS parent_key
            {% endif %}

            {% if not loop.last %}
            UNION ALL
            {% endif %}
        {% endfor %}

        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
)
SELECT
    *
FROM
    flattened_data
WHERE
    chain = '{{ database }}'
