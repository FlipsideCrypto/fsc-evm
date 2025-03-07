{% macro vars_config(all_projects=false) %}
    {# Retrieves variable configurations and values from project-specific macros.
       When all_projects=True, gets variables from all projects.
       Otherwise, gets variables only for the current project based on target database. #}
    
    {# Initialize empty dictionary for all variables #}
    {% set target_vars = {} %}
    
    {# Determine current project based on database name #}
    {% set target_db = target.database.lower() | replace('_dev', '') %}
    
    {% if all_projects %}
        {# Get all macro names in the context #}
        {% set all_macros = context.keys() %}
        
        {# Filter for project variable macros (those ending with _vars) #}
        {% for macro_name in all_macros %}
            {% if macro_name.endswith('_vars') %}
                {# Extract project name from macro name #}
                {% set project_name = macro_name.replace('_vars', '') %}
                
                {# Call the project macro and add to target_vars #}
                {% set project_config = context[macro_name]() %}
                
                {# Only include if the result is a mapping #}
                {% if project_config is mapping %}
                    {% do target_vars.update({project_name: project_config}) %}
                {% endif %}
            {% endif %}
        {% endfor %}
    {% else %}
        {# Construct the macro name for this project #}
        {% set project_macro = target_db ~ '_vars' %}
        
        {# Try to call the macro directly #}
        {% if context.get(project_macro) is not none %}
            {% set project_config = context[project_macro]() %}
            {% do target_vars.update({target_db: project_config}) %}
        {% endif %}
    {% endif %}
    
    {{ return(target_vars) }}
{% endmacro %}

{% macro flatten_vars() %}
    {# Converts the nested variable structure from vars_config() into a flat list of dictionaries.
       Each dictionary contains project, key, parent_key, value properties. #}
    
    {# Get the nested structure from vars_config() #}
    {% set nested_vars = vars_config() %}
    
    {# Convert the nested structure to the flat format expected by get_var() #}
    {% set flat_vars = [] %}
    
    {% for project, vars in nested_vars.items() %}
        {% for key, value in vars.items() %}
            {% if value is mapping %}
                {# Handle nested mappings (where parent_key is not none) #}
                {% for subkey, subvalue in value.items() %}
                    {% do flat_vars.append({
                        'project': project,
                        'key': subkey,
                        'parent_key': key,
                        'value': subvalue
                    }) %}
                {% endfor %}
            {% else %}
                {% do flat_vars.append({
                    'project': project,
                    'key': key,
                    'parent_key': none,
                    'value': value
                }) %}
            {% endif %}
        {% endfor %}
    {% endfor %}
    
    {{ return(flat_vars) }}
{% endmacro %}

{% macro get_var_logs(variable_key, default) %}
    {# Logs variable information to the terminal and a table in the database.
       Dependent on GET_VAR_LOGS_ENABLED and execute flags. #}

    {% if var('GET_VAR_LOGS_ENABLED', false) and execute %}
        {% set package = variable_key.split('_')[0] %}
        {% set category = variable_key.split('_')[1] %}
        
        {# Determine the data type of the default value #}       
        {% if default is not none %}
            {% if default is string %}
                {% set default_type = 'STRING' %}
                {% set default_value = '\'\'' ~ default ~ '\'\'' %}
            {% elif default is number %}
                {% set default_type = 'NUMBER' %}
                {% set default_value = default %}
            {% elif default is boolean %}
                {% set default_type = 'BOOLEAN' %}
                {% set default_value = default %}
            {% elif default is mapping %}
                {% set default_type = 'OBJECT' %}
                {% set default_value = default | tojson %}
            {% elif default is sequence and default is not string %}
                {% set default_type = 'ARRAY' %}
                {% set default_value = default | tojson %}
            {% elif default is iterable and default is not string %}
                {% set default_type = 'ITERABLE' %}
                {% set default_value = 'ITERABLE' %}
            {% else %}
                {% set default_type = 'UNKNOWN' %}
                {% set default_value = 'UNKNOWN' %}
            {% endif %}
        {% else %}
            {% set default_type = none %}
            {% set default_value = none %}
        {% endif %}
        
        {% set log_query %}
            CREATE TABLE IF NOT EXISTS {{target.database}}.bronze._master_keys (
                package STRING,
                category STRING,
                variable_key STRING,
                default_value STRING,
                default_type STRING,
                _inserted_timestamp TIMESTAMP_NTZ DEFAULT SYSDATE()
            );
            
            INSERT INTO {{target.database}}.bronze._master_keys (
                package, 
                category, 
                variable_key, 
                default_value,
                default_type
            )
            VALUES (
                '{{ package }}', 
                '{{ category }}', 
                '{{ variable_key }}', 
                '{{ default_value }}',
                '{{ default_type }}'
            );
        {% endset %}
        {% do run_query(log_query) %}
        
        {# Update terminal logs to include type information #}
        {% do log(package ~ "|" ~ category ~ "|" ~ variable_key ~ "|" ~ default_value ~ "|" ~ default_type, info=True) %}
    {% endif %}
{% endmacro %}

{% macro get_var(variable_key, default=none) %}
    {# Retrieves a variable by key from either dbt's built-in var() function or from project configs.
       Handles type conversion for strings, numbers, booleans, arrays, and JSON objects.
       Returns the default value if the variable is not found. #}

    {# Log variable info if enabled #}
    {% do get_var_logs(variable_key, default) %}

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
            {% if (var_item.key == variable_key or var_item.parent_key == variable_key) %}
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