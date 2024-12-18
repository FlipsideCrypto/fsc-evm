{% macro get_where_subquery(relation) -%}
    {%- set where = config.get('where') -%}
    
    {# Declare variables at the top level of the macro #}
    {%- set interval_vars = namespace(
        interval_type = none,
        interval_value = none
    ) -%}
    
    {# Check if any interval variables are set #}
    {% set intervals = {
        'minutes': var('minutes', none),
        'hours': var('hours', none), 
        'days': var('days', none),
        'weeks': var('weeks', none),
        'months': var('months', none),
        'years': var('years', none)
    } %}
    
    {% for type, value in intervals.items() %}
        {% if value is not none %}
            {% set interval_vars.interval_type = type[:-1] %}
            {% set interval_vars.interval_value = value %}
            {% break %}
        {% endif %}
    {% endfor %}
    
    {% if 'dbt_expectations_expect_column_values_to_be_in_type_list' in this | string %}
        {% do return(relation) %}
    {% endif %}

    {# Initialize namespaces at the top of the macro #}
    {%- set ts_vars = namespace(
        timestamp_column = none,
        filter_condition = none
    ) -%}

    {% if where %}
        {% if "__timestamp_filter__" in where %}
            {# Get the appropriate timestamp column if none provided #}
            {% set columns = adapter.get_columns_in_relation(relation) %}
            {% set column_names = columns | map(attribute='name') | list %}

            {# Check for MODIFIED_TIMESTAMP first #}
            {% for column in columns %}
                {% if column.name == 'MODIFIED_TIMESTAMP' %}
                    {% set ts_vars.timestamp_column = 'MODIFIED_TIMESTAMP' %}
                    {% break %}
                {% endif %}
            {% endfor %}

            {# If no MODIFIED_TIMESTAMP, check for _INSERTED_TIMESTAMP #}
            {% if not ts_vars.timestamp_column %}
                {% for column in columns %}
                    {% if column.name == '_INSERTED_TIMESTAMP' %}
                        {% set ts_vars.timestamp_column = '_INSERTED_TIMESTAMP' %}
                        {% break %}
                    {% endif %}
                {% endfor %}
            {% endif %}

            {# If still no timestamp, check for BLOCK_TIMESTAMP #}
            {% if not ts_vars.timestamp_column %}
                {% for column in columns %}
                    {% if column.name == 'BLOCK_TIMESTAMP' %}
                        {% set ts_vars.timestamp_column = 'BLOCK_TIMESTAMP' %}
                        {% break %}
                    {% endif %}
                {% endfor %}
            {% endif %}

            {% if ts_vars.timestamp_column is not none %}
                {% set ts_vars.filter_condition = ts_vars.timestamp_column ~ " >= dateadd(" ~ 
                    interval_vars.interval_type ~ ", -" ~ 
                    interval_vars.interval_value ~ ", current_timestamp())" %}
                {% set where = where | replace("__timestamp_filter__", ts_vars.filter_condition) %}
            {% endif %}
        {% endif %}
        
        {%- set filtered -%}
            (select * from {{ relation }} where {{ where }}) dbt_subquery
        {%- endset -%}
        {% do return(filtered) %}
    {%- else -%}
        {% do return(relation) %}
    {%- endif -%}
{%- endmacro %}