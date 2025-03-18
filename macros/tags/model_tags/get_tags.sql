{%- macro load_tag_mapping() -%}
    {% set tag_mapping = get_tag_dictionary() %}
    {{ return(tag_mapping) }}
{%- endmacro -%}

{%- macro get_path_tags(model) -%}
    {% set tags = [] %}

    {% set path_str = model.original_file_path | string %}
    {% set path = path_str.split('/') %}

    {# Skip 'models' directory if it exists #}
    {% set start_index = 1 if path[0] == 'models' else 0 %}

    {# Process each directory in the path #}
    {% for part in path[start_index:-1] %}
        {% do tags.append(part) %}
    {% endfor %}

    {# Process the filename without extension #}
    {% set filename = path[-1] | replace('.sql', '') | replace('.yml', '') %}

    {# Add the full filename as a tag #}
    {% do tags.append(filename) %}

    {# Load tag mapping from YAML #}
    {% set tag_mapping = load_tag_mapping() %}

    {# Apply tag mapping rules #}
    {% set final_tags = tags.copy() %}
    {% for tag in tags %}
        {% if tag in tag_mapping %}
            {% do final_tags.extend(tag_mapping[tag]) %}
        {% endif %}
    {% endfor %}

    {# Add tags as a comment in the compiled SQL #}
    {{ "-- Auto-generated tags: " ~ (final_tags | unique | list | join(', ')) }}

    {# Return unique tags to avoid duplicates #}
    {{ return(final_tags | unique | list) }}
{%- endmacro -%}