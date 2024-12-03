{% macro recent_test(model, test_sql) %}
    {% if var('RECENT_TEST_DAYS', 3) is none %}
        {# Run test on all data #}
        {{ test_sql }}
    {% else %}
        {# Add timestamp filter to the original test SQL #}
        {% set date_filter %}
            AND block_timestamp >= DATEADD('day', -{{ var('RECENT_TEST_DAYS', 3) }}, CURRENT_TIMESTAMP())
        {% endset %}

        {# Insert the date filter before the first GROUP BY or HAVING or at the end if neither exists #}
        {% if test_sql is match('.*GROUP BY.*', flags='is') %}
            {% set final_sql = test_sql | replace('GROUP BY', date_filter ~ ' GROUP BY') %}
        {% elif test_sql is match('.*HAVING.*', flags='is') %}
            {% set final_sql = test_sql | replace('HAVING', date_filter ~ ' HAVING') %}
        {% else %}
            {% set final_sql = test_sql ~ ' ' ~ date_filter %}
        {% endif %}
        
        {{ final_sql }}
    {% endif %}
{% endmacro %}
