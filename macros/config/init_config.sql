{% macro init_config() %}
    {% if execute %}
        {% set project_name = project_name %}
        {% do log("Initializing configuration for project " ~ project_name, info=True) %}
        
        -- Create a database-friendly value for the chain info
        {% if run_started_at %}
            {% set run_id = run_started_at.strftime('%Y%m%d%H%M%S') %}
        {% else %}
            {% set run_id = modules.datetime.datetime.now().strftime('%Y%m%d%H%M%S') %}
        {% endif %}
        
        -- Make a simple global variable that can be accessed without modules
        -- This will persist for the duration of the dbt invocation
        {% do exceptions.warn("Setting config run ID: " ~ run_id) %}
        {{ return('') }}
    {% endif %}
    
    {{ return('') }}
{% endmacro %}