{% macro generate_schema_name(custom_schema_name=none, node=none) -%}
    {% set vars = return_vars() %}
    {% set node_name = node.name %}
    {% set split_name = node_name.split('__') %}
    
    {% if split_name[0] == 'core' and vars.GOLD_CORE_SCHEMA_NAME is not none %}
        {{ vars.GOLD_CORE_SCHEMA_NAME | trim }}
    {% else %}
        {{ split_name[0] | trim }}
    {% endif %}
{%- endmacro %}

{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {% set node_name = node.name %}
    {% set split_name = node_name.split('__') %}
    {{ split_name[1] | trim }}
{%- endmacro %}