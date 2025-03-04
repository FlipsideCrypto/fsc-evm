{% macro return_var(key, default='') %}
    {# 
    Enhanced version that:
    1. Uses the project name from dbt_project.yml
    2. Supports nested configs under chain-specific names
    3. Falls back to global defaults when a specific chain config isn't available
    4. Supports expressions in the default parameter
    5. Supports referencing other variables with special syntax
    6. Loads configuration from an external JSON file
    #}
    
    {# Extract chain name from the project name #}
    {% set project_name = project_name %}
    {% set chain_name = project_name.split('_')[0] if '_' in project_name else project_name %}
    
    {# Load configuration from JSON file using dbt's get_file_contents utility #}
    {% set config_path = 'chain_config.json' %}
    {% set config = fromjson(get_file_contents(config_path)) %}
    
    {# Get the value for the key from the appropriate config #}
    {% if chain_name in config and key in config[chain_name] %}
        {% set value = config[chain_name][key] %}
    {% elif key in config['global'] %}
        {% set value = config['global'][key] %}
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
        {% elif var_name in config['global'] %}
            {% set referenced_value = config['global'][var_name] %}
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