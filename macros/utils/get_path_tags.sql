{%- macro get_path_tags(model_name, additional_tags=[]) -%}
    {% set tags = [] %}
    
    {# Get the full path from the model name #}
    {% set path = model_name.split('/') %}
    
    {# Skip 'models' directory if it exists #}
    {% set start_index = 1 if path[0] == 'models' else 0 %}
    
    {# Process each directory in the path #}
    {% for part in path[start_index:-1] %}  {# -1 to exclude the filename #}
        {% do tags.append(part) %}
    {% endfor %}
    
    {# Process the filename without extension #}
    {% set filename = path[-1] | replace('.sql', '') | replace('.yml', '') %}
    
    {# Remove prefixes like bronze__, silver__, gold__, core__ #}
    {% set clean_filename = filename | replace('bronze__', '') | replace('silver__', '') | replace('gold__', '') | replace('core__', '') %}
    
    {% do tags.append(clean_filename) %}
    
    {# Add any additional tags provided #}
    {% if additional_tags is not none %}
        {% do tags.extend(additional_tags) %}
    {% endif %}
    
    {# Return unique tags to avoid duplicates #}
    {{ return(tags | unique | list) }}
{%- endmacro -%} 