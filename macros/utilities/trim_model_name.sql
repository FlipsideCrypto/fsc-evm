{% macro trim_model_name(model_name, trim_suffix='') %}
    {% if trim_suffix and model_name.endswith(trim_suffix) %}
        {{ return(model_name[:-len(trim_suffix)]) }}
    {% else %}
        {{ return(model_name) }}
    {% endif %}
{% endmacro %}