{% macro get_vars(var_name, default=none) %}
    {% set query %}
        SELECT value 
        FROM {{ ref('silver__variables_seed_test') }}
        WHERE variable_name = '{{ var_name }}'
        LIMIT 1
    {% endset %}
    
    {% set results = run_query(query) %}
    
    {% if execute %}
        {% if results.rows | length == 0 %}
            {{ return(default) }}
        {% endif %}
        {{ return(results.columns[0].values()[0]) }}
    {% else %}
        {{ return('') }}
    {% endif %}
{% endmacro %}