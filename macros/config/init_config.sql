{% macro init_config() %}
    {% if execute %}
        {% set project_name = project_name %}
        {% do log("Initializing configuration for project " ~ project_name, info=True) %}
        
        -- Set environment variable for Python to detect the DBT project name
        {% do modules.os.environ.update({'DBT_PROJECT': project_name}) %}
        
        -- Import and initialize ConfigManager
        {% do modules.importlib.import_module('analysis.config').get_instance() %}
        
        {% do log("Configuration loaded successfully", info=True) %}
    {% endif %}
    
    {{ return('') }}
{% endmacro %}