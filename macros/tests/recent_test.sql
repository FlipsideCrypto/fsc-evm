{% macro recent_test(model, test_sql) %}
    {% if var('RECENT_TEST_DAYS', 3) is none %}
        {# Run test on all data #}
        {{ test_sql }}
    {% else %}
        {# Add timestamp filter to the original test SQL #}
        {% set date_filter %}
            WHERE block_timestamp >= DATEADD('day', -{{ var('RECENT_TEST_DAYS', 3) }}, CURRENT_TIMESTAMP())
        {% endset %}

        {# Insert the WHERE clause after the model reference #}
        {% set final_sql = test_sql | replace(model, model ~ ' ' ~ date_filter) %}
        
        {{ final_sql }}
    {% endif %}
{% endmacro %}
