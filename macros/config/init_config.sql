{% macro init_config() %}
    {% if execute %}
        {% set project_name = project_name %}
        {% do log("Initializing configuration for project " ~ project_name, info=True) %}
        
        -- Check if we need to regenerate the config cache
        {% set config_path %}{{project_path}}/../logs/config_cache/config_chain_{{target.name}}.json{% endset %}
        {% set config_exists = modules.os.path.exists(config_path) %}
        
        {% if not config_exists %}
            {% do log("Config cache not found, regenerating...", info=True) %}
            
            -- Run the Python config processor
            {% set processor_path %}{{project_path}}/../analysis/config/process_config.py{% endset %}
            {% set py_cmd %}python "{{processor_path}}"{% endset %}
            
            {% do log("Executing: " ~ py_cmd, info=True) %}
            {% do run_shell_command(py_cmd) %}
        {% endif %}
        
        {% do log("Configuration initialized successfully", info=True) %}
    {% endif %}
    
    {{ return('') }}
{% endmacro %}

{% macro run_shell_command(command) %}
    {% if execute %}
        {% do log("Running shell command: " ~ command, info=True) %}
        {% set result = modules.subprocess.run(
            command, 
            shell=True, 
            capture_output=True, 
            text=True,
            check=False
        ) %}
        
        {% if result.returncode != 0 %}
            {% do exceptions.raise_compiler_error("Shell command failed: " ~ result.stderr) %}
        {% else %}
            {% do log("Shell command output: " ~ result.stdout, info=True) %}
        {% endif %}
    {% endif %}
{% endmacro %}