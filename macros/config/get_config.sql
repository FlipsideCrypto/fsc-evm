{% macro get_config(key, default='') %}
    {#
        Retrieve a configuration value by key.
        
        Args:
            key (str): The configuration key to retrieve
            default: The default value to return if key not found
            
        Returns:
            The configuration value, or default if not found
    #}
    
    {% if execute %}
        -- Try to look it up in the cached config file
        {% set project_name = project_name %}
        {% set fsc_evm_dir = modules.os.path.dirname(project_path) %}
        {% set config_path = modules.os.path.join(fsc_evm_dir, 'logs', 'config_cache', 'config_' ~ project_name ~ '.json') %}
        
        {% if modules.os.path.exists(config_path) %}
            {% set config_file = open(config_path, 'r') %}
            {% set config = modules.json.loads(config_file.read()) %}
            {% do config_file.close() %}
            
            {% if key in config %}
                {{ return(config[key]) }}
            {% endif %}
        {% endif %}
        
        -- If not found, return the default
        {{ return(default) }}
    {% else %}
        {{ return(default) }}
    {% endif %}
{% endmacro %}