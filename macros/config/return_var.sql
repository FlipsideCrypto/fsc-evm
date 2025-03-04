{% macro return_var(key, default='') %}
    {# 
    Enhanced version that:
    1. Checks for command-line variable overrides first
    2. Uses the project name from dbt_project.yml
    3. Supports nested configs under chain-specific names
    4. Falls back to default_values when a specific chain config isn't available
    5. Supports expressions in the default parameter
    6. Supports referencing other variables with special syntax
    7. Uses DBT vars for configuration
    8. Supports nested values with dot notation (e.g., "VERTEX_CONTRACTS.ABI")
    #}
    
    {# Check for direct command-line override first #}
    {% if var(key, none) is not none %}
        {% set value = var(key) %}
        {{ return(value) }}
    {% endif %}
    
    {# Extract chain name from the project name #}
    {% set project_name = project_name %}
    {% set chain_name = project_name.split('_')[0] if '_' in project_name else project_name %}
    
    {# Get config from dbt vars #}
    {% set config = var('chain_config', {}) %}
    
    {# Check if key has dot notation for nested value #}
    {% if '.' in key %}
        {% set key_parts = key.split('.') %}
        {% set parent_key = key_parts[0] %}
        {% set child_key = key_parts[1] %}
        
        {# Get the parent value from the appropriate config #}
        {% if chain_name in config and parent_key in config[chain_name] %}
            {% set parent_value = config[chain_name][parent_key] %}
            {% if parent_value is mapping and child_key in parent_value %}
                {% set value = parent_value[child_key] %}
            {% else %}
                {# Try default values if chain-specific nested value not found #}
                {% if 'default_values' in config and parent_key in config['default_values'] %}
                    {% set parent_value = config['default_values'][parent_key] %}
                    {% if parent_value is mapping and child_key in parent_value %}
                        {% set value = parent_value[child_key] %}
                    {% else %}
                        {{ return(default) }}
                    {% endif %}
                {% else %}
                    {{ return(default) }}
                {% endif %}
            {% endif %}
        {% elif 'default_values' in config and parent_key in config['default_values'] %}
            {% set parent_value = config['default_values'][parent_key] %}
            {% if parent_value is mapping and child_key in parent_value %}
                {% set value = parent_value[child_key] %}
            {% else %}
                {{ return(default) }}
            {% endif %}
        {% else %}
            {{ return(default) }}
        {% endif %}
    {% else %}
        {# Regular non-nested key handling #}
        {% if chain_name in config and key in config[chain_name] %}
            {% set value = config[chain_name][key] %}
        {% elif 'default_values' in config and key in config['default_values'] %}
            {% set value = config['default_values'][key] %}
        {% else %}
            {{ return(default) }}
        {% endif %}
    {% endif %}
    
    {# Resolve variable references if needed #}
    {% if value is string and ' * ' in value %}
        {% set parts = value.split(' * ') %}
        {% set multiplier = parts[0].strip() | int %}
        {% set var_name = parts[1].strip() %}
        
        {# Check for command-line override of the referenced variable #}
        {% if var(var_name, none) is not none %}
            {% set referenced_value = var(var_name) %}
        {# Otherwise, check in the config using recursive call to handle possible nested values #}
        {% else %}
            {% set referenced_value = return_var(var_name, 0) %}
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