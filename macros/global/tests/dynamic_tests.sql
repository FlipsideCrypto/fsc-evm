{% macro get_where_subquery(relation) -%}
    {%- set where = config.get('where') -%}
    
    {%- set interval_vars = namespace(
        interval_type = none,
        interval_value = none
    ) -%}
    
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

    {%- set ts_vars = namespace(
        timestamp_column = none,
        filter_condition = none
    ) -%}

    {% if where %}
        {% if "__timestamp_filter__" in where and interval_vars.interval_type is not none and interval_vars.interval_value is not none %}
            {% set columns = adapter.get_columns_in_relation(relation) %}
            {% set column_names = columns | map(attribute='name') | list %}
            
            {# Define common timestamp patterns #}
            {% set timestamp_patterns = [
                'modified_timestamp',
                '_inserted_timestamp',
                'block_timestamp',
                'created_timestamp'
            ] %}

            {# First pass: Try to find exact matches (case-insensitive) #}
            {% for pattern in timestamp_patterns %}
                {% for column in columns %}
                    {% if column.name | lower == pattern | lower %}
                        {% set ts_vars.timestamp_column = column.name %}
                        {% break %}
                    {% endif %}
                {% endfor %}
                {% if ts_vars.timestamp_column is not none %}
                    {% break %}
                {% endif %}
            {% endfor %}

            {# Second pass: Try to find any column containing '_timestamp' or 'timestamp_' (case-insensitive) #}
            {% if ts_vars.timestamp_column is none %}
                {% for column in columns %}
                    {% if '_timestamp' in column.name | lower or 'timestamp_' in column.name | lower %}
                        {% set ts_vars.timestamp_column = column.name %}
                        {% break %}
                    {% endif %}
                {% endfor %}
            {% endif %}

            {% if ts_vars.timestamp_column is not none %}
                {% set ts_vars.filter_condition = ts_vars.timestamp_column ~ " >= dateadd(" ~ 
                    interval_vars.interval_type ~ ", -" ~ 
                    interval_vars.interval_value ~ ", current_timestamp())" %}
                {% set where = where | replace("__timestamp_filter__", ts_vars.filter_condition) %}
            {% else %}
                {# If no timestamp column is found, remove the timestamp filter #}
                {% set where = where | replace("__timestamp_filter__", "1=1") %}
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