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
    
    {# Skip timestamp filtering for specific test types - made namespace-agnostic #}
    {# 'this' is a standard dbt context variable available in all namespaces when called from model/test context #}
    {%- set skip_timestamp_filter = false -%}
    {%- if this is not none -%}
        {%- set this_string = this | string | lower -%}
        
        {# Skip dbt_expectations type list tests #}
        {%- if 'dbt_expectations_expect_column_values_to_be_in_type_list' in this_string -%}
            {%- set skip_timestamp_filter = true -%}
        {%- endif -%}
        
        {# Skip custom macro tests - these are defined with {% test %} and don't have package prefixes #}
        {# Standard dbt tests: not_null, unique, relationships, accepted_values, etc. #}
        {# Package tests have dots: dbt_utils.*, dbt_expectations.*, etc. #}
        {# Custom macro tests: txs_have_traces, sequence_gaps, curated_recency_defi, etc. #}
        {%- set standard_tests = ['not_null', 'unique', 'relationships', 'accepted_values', 'expression_is_true'] -%}
        {%- set is_standard_test = false -%}
        {%- set has_package_prefix = '.' in this_string -%}
        
        {# Check if it's a standard dbt test #}
        {%- for test_pattern in standard_tests -%}
            {%- if test_pattern in this_string -%}
                {%- set is_standard_test = true -%}
                {%- break -%}
            {%- endif -%}
        {%- endfor -%}
        
        {# If it's not a standard test and doesn't have a package prefix (dot), it's a custom macro test - skip filtering #}
        {%- if not is_standard_test and not has_package_prefix -%}
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

    {% if where %}
        {% if "__timestamp_filter__" in where %}
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

            {% if ts_vars.timestamp_column is not none and interval_vars.interval_type is not none and interval_vars.interval_value is not none %}
                {% set ts_vars.filter_condition = ts_vars.timestamp_column ~ " >= dateadd(" ~ 
                    interval_vars.interval_type ~ ", -" ~ 
                    interval_vars.interval_value ~ ", SYSDATE())" %}
                {% set where = where | replace("__timestamp_filter__", ts_vars.filter_condition) %}
            {% elif "__timestamp_filter__" in where %}
                {% set where = where | replace("__timestamp_filter__", "1=1") %}
            {% endif %}
        {% endif %}
        
        {%- set filtered -%}
            (select * from {{ relation }} where {{ where }})
        {%- endset -%}
        {% do return(filtered) %}
    {%- else -%}
        {% do return(relation) %}
    {%- endif -%}
{%- endmacro %}