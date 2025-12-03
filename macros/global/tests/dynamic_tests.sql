{% macro get_where_subquery(relation, where_override=none) -%}
    {# 
        Get where clause from config or override parameter.
        This macro is namespace-agnostic and can be imported into any dbt project.
        
        Args:
            relation: The relation (model/table) to query
            where_override: Optional override for the where clause (useful when config context may vary)
    #}
    
    {# Get where clause - prefer override, then config, then none #}
    {%- if where_override is not none -%}
        {%- set where = where_override -%}
    {%- else -%}
        {%- set where = config.get('where', none) -%}
    {%- endif -%}
    
    {%- set interval_vars = namespace(
        interval_type = none,
        interval_value = none
    ) -%}
    
    {# Check for interval variables - these work across all namespaces #}
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

    {# Skip timestamp filtering for dbt_expectations type list tests #}
    {%- set skip_timestamp_filter = false -%}
    {%- if this is not none -%}
        {%- set this_string = this | string | lower -%}
        {%- if 'dbt_expectations_expect_column_values_to_be_in_type_list' in this_string -%}
            {%- set skip_timestamp_filter = true -%}
        {%- endif -%}
    {%- endif -%}
    
    {%- if skip_timestamp_filter -%}
        {% do return(relation) %}
    {%- endif -%}

    {%- set ts_vars = namespace(
        timestamp_column = none,
        filter_condition = none
    ) -%}

    {# Build timestamp filter if interval vars are set #}
    {% if interval_vars.interval_type is not none and interval_vars.interval_value is not none %}
        {% set columns = adapter.get_columns_in_relation(relation) %}

        {# Search for common timestamp column names in priority order #}
        {% for column in columns %}
            {% if column.name == 'MODIFIED_TIMESTAMP' %}
                {% set ts_vars.timestamp_column = 'MODIFIED_TIMESTAMP' %}
                {% break %}
            {% endif %}
        {% endfor %}

        {% if not ts_vars.timestamp_column %}
            {% for column in columns %}
                {% if column.name == '_INSERTED_TIMESTAMP' %}
                    {% set ts_vars.timestamp_column = '_INSERTED_TIMESTAMP' %}
                    {% break %}
                {% endif %}
            {% endfor %}
        {% endif %}

        {% if not ts_vars.timestamp_column %}
            {% for column in columns %}
                {% if column.name == 'BLOCK_TIMESTAMP' %}
                    {% set ts_vars.timestamp_column = 'BLOCK_TIMESTAMP' %}
                    {% break %}
                {% endif %}
            {% endfor %}
        {% endif %}

        {% if not ts_vars.timestamp_column %}
            {% for column in columns %}
                {% if column.name == 'BLOCK_DATE' %}
                    {% set ts_vars.timestamp_column = 'BLOCK_DATE' %}
                    {% break %}
                {% endif %}
            {% endfor %}
        {% endif %}

        {# Build timestamp filter condition if we found a timestamp column #}
        {% if ts_vars.timestamp_column is not none %}
            {% set ts_vars.filter_condition = ts_vars.timestamp_column ~ " >= dateadd(" ~ 
                interval_vars.interval_type ~ ", -" ~ 
                interval_vars.interval_value ~ ", SYSDATE())" %}
        {% endif %}
    {% endif %}

    {# Handle where clause with timestamp filtering #}
    {% if ts_vars.filter_condition is not none %}
        {# We have a timestamp filter to apply #}
        {% if where %}
            {# Combine timestamp filter with existing where using AND #}
            {% set where = ts_vars.filter_condition ~ " AND (" ~ where ~ ")" %}
        {% else %}
            {# No existing where clause, use just the timestamp filter #}
            {% set where = ts_vars.filter_condition %}
        {% endif %}
    {% endif %}

    {# Return filtered relation - always as a subquery for consistency #}
    {% if where %}
        {%- set filtered -%}
            (select * from {{ relation }} where {{ where }})
        {%- endset -%}
        {% do return(filtered) %}
    {%- else -%}
        {# Return relation as a subquery even when no where clause for consistent behavior #}
        {%- set filtered -%}
            (select * from {{ relation }})
        {%- endset -%}
        {% do return(filtered) %}
    {%- endif -%}
{%- endmacro %}