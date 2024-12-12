{% macro get_vars(var_name, default=none) %}
    {% if var(var_name, none) is not none %}
        {{ return(var(var_name)) }}
    {% endif %}

    {% set query %}
        SELECT 
            variable_name,
            parent_key,
            variable_name as key,
            value
        FROM {{ ref('silver__variables_seed_test') }}
        WHERE variable_name = '{{ var_name }}'
           OR parent_key = '{{ var_name }}'
        ORDER BY variable_name
    {% endset %}
    
    {% if execute %}
        {% set results = run_query(query) %}
        
        {% if results.rows | length == 0 %}
            {{ return(default) }}
        {% endif %}

        {# Check if this is a mapping type (has child rows) #}
        {% if results.rows[0][1] is none or results.rows[0][1] == '' %}
            {{ return(results.rows[0][3]) }}  {# Return value #}
        {% else %}
            {% set mapping = {} %}
            {% for row in results.rows %}
                {% do mapping.update({row[0]: row[3]}) %}  {# variable_name: value #}
            {% endfor %}
            {{ return(mapping) }}
        {% endif %}
    {% else %}
        {{ return('') }}
    {% endif %}
{% endmacro %}