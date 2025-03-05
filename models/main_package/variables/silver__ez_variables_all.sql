{{ config(
    materialized = 'view',
    tags = ['project_vars']
) }}

{%- set vars_data = vars_config(all_projects=true) -%}

WITH flattened_data AS (
    {% for project, project_config in vars_data.items() %}
        {% for key, value in project_config.items() %}
            {% if value is mapping %}
                {% for nested_key, nested_value in value.items() %}
                    SELECT
                        '{{ project }}' AS project,
                        '{{ nested_key }}' AS key,
                        {% if nested_value is string %}
                            '{{ nested_value }}' AS VALUE,
                        {% elif nested_value is iterable and nested_value is not string %}
                            '{{ nested_value | tojson }}' AS VALUE,
                        {% else %}
                            '{{ nested_value }}' AS VALUE,
                        {% endif %}
                        '{{ key }}' AS parent_key
                    
                    {% if not loop.last %}UNION ALL{% endif %}
                {% endfor %}
                
                {% if not loop.last %}UNION ALL{% endif %}
            {% else %}
                SELECT
                    '{{ project }}' AS project,
                    '{{ key }}' AS key,
                    {% if value is string %}
                        '{{ value }}' AS VALUE,
                    {% elif value is iterable and value is not string %}
                        '{{ value | tojson }}' AS VALUE,
                    {% else %}
                        '{{ value }}' AS VALUE,
                    {% endif %}
                    NULL AS parent_key
                
                {% if not loop.last %}UNION ALL{% endif %}
            {% endif %}
        {% endfor %}
        
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

SELECT
    project,
    key,
    VALUE,
    parent_key
FROM
    flattened_data
