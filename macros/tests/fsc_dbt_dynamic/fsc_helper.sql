{% macro add_days_filter(model, timestamp_column=none) %}
    {% set timestamp_column = timestamp_column if timestamp_column is not none else 'MODIFIED_TIMESTAMP' %}
    
    {% set intervals = {
        'minutes': var('minutes', none),
        'hours': var('hours', none),
        'days': var('days', none),
        'weeks': var('weeks', none),
        'months': var('months', none),
        'years': var('years', none)
    } %}

    {% for interval_type, interval_value in intervals.items() %}
        {% if interval_value is not none %}
            {% set row_condition = timestamp_column ~ " >= dateadd(" ~ interval_type[:-1] ~ ", -" ~ interval_value ~ ", sysdate())" %}
            {{ return({'row_condition': row_condition}) }}
        {% endif %}
    {% endfor %}

    {{ return({'row_condition': none}) }}
{% endmacro %}