{#
    Sets folder level tags, which will be inherited by all models in that folder
#}
{%- macro get_tag_dictionary() -%}
    {% set tag_mapping = {
        
        'defi': ['curated', 'reorg'],
        'protocols': ['curated', 'reorg']
    } %}
    
    {{ return(tag_mapping) }}
{%- endmacro -%}