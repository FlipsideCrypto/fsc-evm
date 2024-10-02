{% macro extract_model_info() %}
    {% set identifier = this.identifier %}
    {% set identifier_parts = identifier.split('__') %}
    {% if '__' in identifier %}
        {% set model_parts = identifier_parts[1].split('_') %}
    {% else %}
        {% set model_parts = identifier.split('_') %}
    {% endif %}
    
    {% set model_type = model_parts[-1] %}
    {% set model = '_'.join(model_parts[:-1]) %}
    {% set view_source = identifier_parts[1] if identifier_parts|length > 1 else identifier %}
    
    {% do return {
        'model': model,
        'model_type': model_type,
        'view_source': view_source
    } %}
{% endmacro %}