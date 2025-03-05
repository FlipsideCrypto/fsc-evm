{% macro flatten_vars() %}
    {# Get the nested structure from vars_config() #}
    {% set nested_vars = vars_config() %}
    
    {# Convert the nested structure to the flat format expected by get_var() #}
    {% set flat_vars = [] %}
    
    {% for chain, vars in nested_vars.items() %}
        {% for key, value in vars.items() %}
            {% if value is mapping %}
                {# Handle nested mappings (where parent_key is not none) #}
                {% for subkey, subvalue in value.items() %}
                    {% do flat_vars.append({
                        'chain': chain,
                        'key': subkey,
                        'parent_key': key,
                        'value': subvalue,
                        'is_enabled': true
                    }) %}
                {% endfor %}
            {% else %}
                {% do flat_vars.append({
                    'chain': chain,
                    'key': key,
                    'parent_key': none,
                    'value': value,
                    'is_enabled': true
                }) %}
            {% endif %}
        {% endfor %}
    {% endfor %}
    
    {{ return(flat_vars) }}
{% endmacro %}

{% macro get_var(variable_key, default=none) %}
    {# Check if variable exists in dbt's built-in var() function. If it does, return the value. #}
    {% if var(variable_key, none) is not none %}
        {{ return(var(variable_key)) }}
    {% endif %}

    {# Get flattened variables from the config file #}
    {% set all_vars = flatten_vars() %}
    
    {% if execute %}
        {# Filter variables based on the requested key #}
        {% set filtered_vars = [] %}
        {% for var_item in all_vars %}
            {% if (var_item.key == variable_key or var_item.parent_key == variable_key) and var_item.is_enabled %}
                {% do filtered_vars.append(var_item) %}
            {% endif %}
        {% endfor %}
        
        {# If no results found, return the default value #}
        {% if filtered_vars | length == 0 %}
            {{ return(default) }}
        {% endif %}

        {% set first_var = filtered_vars[0] %}
        {% set parent_key = first_var.parent_key %}
        {% set value = first_var.value %}
        {% set is_enabled = first_var.is_enabled %}
        
        {# Check if this is a simple variable (no parent key) or a mapping (has parent key) #}
        {% if parent_key is none or parent_key == '' %}
            {# Infer data type from value #}
            {% if value is string %}
                {% if value.startswith('[') and value.endswith(']') %}
                    {# For array type, parse and convert values to appropriate types #}
                    {% set array_values = value.strip('[]').split(',') %}
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
                {% elif value.startswith('{') and value.endswith('}') %}
                    {# For JSON, VARIANT, OBJECT #}
                    {{ return(fromjson(value)) }}
                {% elif value.isdigit() %}
                    {{ return(value | int) }}
                {% elif value.replace('.','',1).isdigit() %}
                    {{ return(value | float) }}
                {% elif value.lower() in ['true', 'false'] %}
                    {{ return(value.lower() == 'true') }}
                {% else %}
                    {{ return(value) }}
                {% endif %}
            {% else %}
                {# If value is already a non-string type (int, bool, etc.) #}
                {{ return(value) }}
            {% endif %}
        {% else %}
            {# For variables with a parent_key, build a dictionary of all child values #}
            {% set mapping = {} %}
            {% for var_item in filtered_vars %}
                {# key: value pairings based on parent_key #}
                {% do mapping.update({var_item.key: var_item.value}) %} 
            {% endfor %}
            {{ return(mapping) }}
        {% endif %}
    {% else %}
        {{ return(default) }}
    {% endif %}
{% endmacro %}