{% macro vars_config() %}
    {# Initialize empty dictionary for all variables #}
    {% set all_vars = {} %}
    
    {# Determine current project based on database name #}
    {% set target_db = target.database.lower() | replace('_dev', '') %}
    
    {# Construct the macro name for this project #}
    {% set project_macro = target_db ~ '_vars' %}
    
    {# Try to call the macro directly #}
    {% if context.get(project_macro) is not none %}
        {% set project_config = context[project_macro]() %}
        {% do all_vars.update({target_db: project_config}) %}
    {% endif %}
    
    {{ return(all_vars) }}
{% endmacro %}