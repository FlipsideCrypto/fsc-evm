{% macro load_all_variables() %}
    {% if var('_all_variables_cache', none) is not none %}
        {{ return(var('_all_variables_cache')) }}
    {% endif %}

    {%- set database = target.database.lower() | replace('_dev', '') -%}

    {# First pass: Load all simple variables (without expressions in default values) #}
    {% set query_simple %}
        SELECT 
            key, 
            data_type, 
            value, 
            default_value,
            parent_key
        FROM {{ ref('silver__variables_with_defaults') }}
        WHERE chain = '{{ database }}'
          AND parent_key IS NULL
          AND (default_value NOT LIKE '{{%}}' OR default_value IS NULL)
          AND is_enabled = TRUE
    {% endset %}
    
    {% set results_simple = run_query(query_simple) %}
    {% set vars_dict = {} %}
    
    {# Load simple variables first #}
    {% for row in results_simple.rows %}
        {% set key = row[0] %}
        {% set data_type = row[1] %}
        {% set value = row[2] %}
        {% set default_value = row[3] %}
        
        {% if value is not none %}
            {# Convert value based on data_type #}
            {% do vars_dict.update({key: convert_value(value, data_type)}) %}
        {% elif default_value is not none %}
            {% do vars_dict.update({key: convert_value(default_value, data_type)}) %}
        {% endif %}
    {% endfor %}
    
    {# Second pass: Load variables with expressions in default values #}
    {% set query_expressions %}
        SELECT 
            key, 
            data_type, 
            value, 
            default_value,
            parent_key
        FROM {{ ref('silver__variables_with_defaults') }}
        WHERE chain = '{{ database }}'
          AND parent_key IS NULL
          AND default_value LIKE '{{%}}'
          AND is_enabled = TRUE
    {% endset %}
    
    {% set results_expressions = run_query(query_expressions) %}
    
    {# Process variables with expressions #}
    {% for row in results_expressions.rows %}
        {% set key = row[0] %}
        {% set data_type = row[1] %}
        {% set value = row[2] %}
        {% set default_value = row[3] %}
        
        {% if value is not none %}
            {% do vars_dict.update({key: convert_value(value, data_type)}) %}
        {% elif default_value is not none %}
            {% set evaluated_value = evaluate_expression(default_value, vars_dict) %}
            {% do vars_dict.update({key: convert_value(evaluated_value, data_type)}) %}
        {% endif %}
    {% endfor %}
    
    {# Finally, load mapping variables (parent_key is not null) #}
    {% set query_mappings %}
        SELECT 
            key, 
            parent_key, 
            data_type, 
            value, 
            default_value
        FROM {{ ref('silver__variables_with_defaults') }}
        WHERE chain = '{{ database }}'
          AND parent_key IS NOT NULL
          AND is_enabled = TRUE
    {% endset %}
    
    {% set results_mappings = run_query(query_mappings) %}
    
    {# Process mapping variables #}
    {% set mappings = {} %}
    {% for row in results_mappings.rows %}
        {% set key = row[0] %}
        {% set parent_key = row[1] %}
        {% set data_type = row[2] %}
        {% set value = row[3] %}
        {% set default_value = row[4] %}
        
        {# Initialize the mapping dictionary if it doesn't exist #}
        {% if mappings.get(parent_key) is none %}
            {% do mappings.update({parent_key: {}}) %}
        {% endif %}
        
        {# Add the key-value pair to the mapping #}
        {% if value is not none %}
            {% do mappings[parent_key].update({key: convert_value(value, data_type)}) %}
        {% elif default_value is not none %}
            {% set evaluated_value = evaluate_expression(default_value, vars_dict) %}
            {% do mappings[parent_key].update({key: convert_value(evaluated_value, data_type)}) %}
        {% endif %}
    {% endfor %}
    
    {# Add mappings to the main dictionary #}
    {% for parent_key, mapping in mappings.items() %}
        {% do vars_dict.update({parent_key: mapping}) %}
    {% endfor %}
    
    {# Cache the result #}
    {% do var('_all_variables_cache', vars_dict) %}
    {{ return(vars_dict) }}
{% endmacro %} 