{% macro add_days_filter(model) %}
    {% set days = var('days', none) %}
    {% if days is not none %}
        {# For CTE-based filtering #}
        {% set filtered_model %}
            with filtered_data as (
                select *
                from {{ model }}
                where BLOCK_TIMESTAMP >= dateadd(day, -{{ days }}, sysdate())
            )
            select * from filtered_data
        {% endset %}
        
        {# For row_condition-based filtering #}
        {% set row_condition = "BLOCK_TIMESTAMP >= dateadd(day, -" ~ days ~ ", sysdate())" %}
        
        {# Return both options #}
        {{ return({'filtered_model': filtered_model, 'row_condition': row_condition}) }}
    {% else %}
        {{ return({'filtered_model': model, 'row_condition': none}) }}
    {% endif %}
{% endmacro %}