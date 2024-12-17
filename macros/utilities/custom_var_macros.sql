{% macro get_var(var_name, default=none) %}
    {# Check if variable exists in dbt's built-in var() function. If it does, return the value. #}
    {% if var(var_name, none) is not none %}
        {{ return(var(var_name)) }}
    {% endif %}

    {# Query to get variable value from custom variables table #}
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
    
    {# Only execute query during actual dbt run, not during compile #}
    {% if execute %}
        {% set results = run_query(query) %}
        
        {# If no results found, return the default value #}
        {% if results.rows | length == 0 %}
            {{ return(default) }}
        {% endif %}

        {# Check if this is a simple variable (no parent key) or a mapping (has parent key) #}
        {% if results.rows[0][1] is none or results.rows[0][1] == '' %}
            {# For simple variable, return the value directly #}
            {{ return(results.rows[0][3]) }}
        {% else %}
            {# For mapping type, build a dictionary of all child values #}
            {% set mapping = {} %}
            {% for row in results.rows %}
                {% do mapping.update({row[0]: row[3]}) %}
            {% endfor %}
            {{ return(mapping) }}
        {% endif %}
    {% else %}
        {# During compile phase, return empty string #}
        {{ return('') }}
    {% endif %}
{% endmacro %}