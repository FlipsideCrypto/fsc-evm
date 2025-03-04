{% macro return_var(key, default='') %}
    {# 
    Enhanced version that:
    1. Uses the project name from dbt_project.yml
    2. Supports nested configs under chain-specific names
    3. Falls back to default_values when a specific chain config isn't available
    4. Supports expressions in the default parameter
    5. Supports referencing other variables with special syntax
    6. Uses DBT vars for configuration
    #}
    
    {# Extract chain name from the project name #}
    {% set project_name = project_name %}
    {% set chain_name = project_name.split('_')[0] if '_' in project_name else project_name %}
    
    {# Get config from dbt vars #}
    {% set config = var('chain_config', {}) %}
    
    {# Get the value for the key from the appropriate config #}
    {% if chain_name in config and key in config[chain_name] %}
        {% set value = config[chain_name][key] %}
    {% elif 'default_values' in config and key in config['default_values'] %}
        {% set value = config['default_values'][key] %}
    {% else %}
        {{ return(default) }}
    {% endif %}
    
    {# Resolve variable references if needed #}
    {% if value is string and ' * ' in value %}
        {% set parts = value.split(' * ') %}
        {% set multiplier = parts[0].strip() | int %}
        {% set var_name = parts[1].strip() %}
        
        {# Get the referenced variable value #}
        {% if chain_name in config and var_name in config[chain_name] %}
            {% set referenced_value = config[chain_name][var_name] %}
        {% elif 'default_values' in config and var_name in config['default_values'] %}
            {% set referenced_value = config['default_values'][var_name] %}
        {% else %}
            {% set referenced_value = 0 %}
        {% endif %}
        
        {# Calculate the final value #}
        {% if referenced_value is number %}
            {{ return(multiplier * referenced_value) }}
        {% else %}
            {{ return(value) }}
        {% endif %}
    {% else %}
        {{ return(value) }}
    {% endif %}
{% endmacro %}