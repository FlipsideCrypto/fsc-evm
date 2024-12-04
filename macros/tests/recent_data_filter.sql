{% macro recent_data_filter() %}
    {% if 'curated' in model.config.get('tags', []) %}
        {% set original_sql = model.compiled_sql %}
        {% if 'where' in original_sql.lower() %}
            {% set modified_sql = original_sql | replace('where', 'where BLOCK_TIMESTAMP >= dateadd(day, -3, current_timestamp()) and') %}
        {% else %}
            {% set modified_sql = original_sql ~ ' where BLOCK_TIMESTAMP >= dateadd(day, -3, current_timestamp())' %}
        {% endif %}
        {{ return(modified_sql) }}
    {% else %}
        {{ return(original_sql) }}
    {% endif %}
{% endmacro %}

{% on_run_start %}
    {{ config.set_test_compile_hook(recent_data_filter) }}
{% endon_run_start %}