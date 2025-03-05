{% macro vars_config(all_projects=false) %}
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