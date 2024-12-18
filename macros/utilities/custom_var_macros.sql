{% macro get_var(var_name, default=none) %}
    {# Check if variable exists in dbt's built-in var() function. If it does, return the value. #}
    {% if var(var_name, none) is not none %}
        {{ return(var(var_name)) }}
    {% endif %}

    {# Query to get variable values from custom variables table #}
    {% set query %}
        SELECT 
            category,
            data_type,
            parent_key,
            key,
            value
        FROM {{ ref('silver__variables_seed_test') }}
        WHERE key = '{{ var_name }}'
           OR parent_key = '{{ var_name }}'
        ORDER BY key
    {% endset %}
    
    {% if execute %}
        {% set results = run_query(query) %}
        {% set category = results.rows[0][0].lower() %}
        {% set data_type = results.rows[0][1].lower() %}
        {% set parent_key = results.rows[0][2] %}
        {% set value = results.rows[0][4] %}
        
        {# If no results found, return the default value #}
        {% if results.rows | length == 0 %}
            {{ return(default) }}
        {% endif %}
        
        {# Check if this is a simple variable (no parent key) or a mapping (has parent key) #}
        {% if parent_key is none or parent_key == '' %}
            {% if data_type == 'array' %}
                {# For array type, parse and convert values to appropriate types #}
                {% set array_values = value.split(',') %}
                {% set converted_array = [] %}
                {% for val in array_values %}
                    {% set stripped_val = val.strip() %}
                    {% if stripped_val.isdigit() %}
                        {% do converted_array.append(stripped_val | int) %}
                    {% elif stripped_val.replace('.','',1).isdigit() %}
                        {% do converted_array.append(stripped_val | float) %}
                    {% elif stripped_val.lower() in ['true', 'false'] %}
                        {% do converted_array.append(stripped_val.lower() == 'true') %}
                    {% else %}
                        {% do converted_array.append(stripped_val) %}
                    {% endif %}
                {% endfor %}
                {{ return(converted_array) }}
            {# For other types, convert appropriately #}
            {% elif data_type in ['json', 'variant', 'object'] %}
                {{ return(fromjson(value)) }}
            {% elif data_type in ['number', 'integer', 'fixed', 'float', 'decimal'] %}
                {% if '.' in value %}
                    {{ return(value | float) }}
                {% else %}
                    {{ return(value | int) }}
                {% endif %}
            {% elif data_type in ['boolean', 'bool'] %}
                {{ return(value | lower == 'true') }}
            {% else %}
                {{ return(value) }}
            {% endif %}
        {% else %}
            {# For variables with a parent_key, build a dictionary of all child values #}
            {% set mapping = {} %}
            {% for row in results.rows %}
                {# key: value pairings based on parent_key #}
                {% do mapping.update({row[3]: row[4]}) %} 
            {% endfor %}
            {{ return(mapping) }}
        {% endif %}
    {% else %}
        {{ return(default) }}
    {% endif %}
{% endmacro %}