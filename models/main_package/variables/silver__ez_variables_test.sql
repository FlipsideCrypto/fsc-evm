{{ config(
    materialized = 'view'
) }}

{%- set vars_data = vars_config() -%}

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
                        {% if nested_value is string %}
                        '{{ nested_value }}' AS VALUE,
                        {% elif nested_value is iterable and nested_value is not string %}
                        '{{ nested_value | tojson }}' AS VALUE,
                        {% else %}
                        '{{ nested_value }}' AS VALUE,
                        {% endif %}
                        '{{ key }}' AS parent_key

                        {% if not loop.last %}
                    UNION ALL
                    {% endif %}
                {% endfor %}
            {% else %}
            SELECT
                '{{ chain }}' AS chain,
                '{{ key }}' AS key,
                {% if value is string %}
                '{{ value }}' AS VALUE,
                {% elif value is iterable and value is not string %}
                '{{ value | tojson }}' AS VALUE,
                {% else %}
                '{{ value }}' AS VALUE,
                {% endif %}
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
    chain,
    key,
    value,
    parent_key
FROM
    flattened_data