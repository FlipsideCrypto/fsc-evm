{% macro add_days_filter(model) %}
    {% set days = var('days', none) %}
    {% if days is not none %}
        with filtered_data as (
            select *
            from {{ model }}
            where BLOCK_TIMESTAMP >= dateadd(day, -{{ days }}, current_timestamp())
        )
        select * from filtered_data
    {% else %}
        select * from {{ model }}
    {% endif %}
{% endmacro %}

{# dbt_utils wrapper #}
{% macro dbt_utils__test_unique_combination_of_columns(model, combination_of_columns) %}
    {% if execute %}
        {% do print("==================== RUNNING test_unique_combination_of_columns ====================") %}
        {% do print("Model: " ~ model) %}
        {% do print("Columns: " ~ combination_of_columns) %}
    {% endif %}
    {% set filtered_model = add_days_filter(model) %}
    {{ dbt_utils.test_unique_combination_of_columns(filtered_model, combination_of_columns) }}
{% endmacro %}

{# dbt_expectations wrappers #}
{% macro dbt_expectations_test_expect_column_values_to_be_in_type_list(model, column_name, column_type_list) %}
    {% if execute %}
        {% do print("==================== RUNNING test_expect_column_values_to_be_in_type_list ====================") %}
        {% do print("Model: " ~ model) %}
        {% do print("Column: " ~ column_name) %}
        {% do print("Type list: " ~ column_type_list) %}
    {% endif %}
    {% set filtered_model = add_days_filter(model) %}
    {{ dbt_expectations.test_expect_column_values_to_be_in_type_list(filtered_model, column_name, column_type_list) }}
{% endmacro %}

{% macro dbt_expectations_test_expect_row_values_to_have_recent_data(model, datepart, interval) %}
    {% if execute %}
        {% do print("==================== RUNNING test_expect_row_values_to_have_recent_data ====================") %}
        {% do print("Model: " ~ model) %}
        {% do print("Datepart: " ~ datepart) %}
        {% do print("Interval: " ~ interval) %}
    {% endif %}
    {% set filtered_model = add_days_filter(model) %}
    {{ dbt_expectations.test_expect_row_values_to_have_recent_data(filtered_model, datepart, interval) }}
{% endmacro %}

{% macro dbt_expectations_test_expect_column_values_to_match_regex(model, column_name, regex) %}
    {% if execute %}
        {% do print("==================== RUNNING test_expect_column_values_to_match_regex ====================") %}
        {% do print("Model: " ~ model) %}
        {% do print("Column: " ~ column_name) %}
        {% do print("Regex: " ~ regex) %}
    {% endif %}
    {% set filtered_model = add_days_filter(model) %}
    {{ dbt_expectations.test_expect_column_values_to_match_regex(filtered_model, column_name, regex) }}
{% endmacro %}

{% macro snowflake__test_not_null(model, column_name) %}
    {% set days = var('days', none) %}
    {% if execute %}
        {% do print("==================== CUSTOM TEST DISPATCH STARTING ====================") %}
        {% do print("Model: " ~ model) %}
        {% do print("Days filter: " ~ var('days', none)) %}
    {% endif %}

    {% if days is not none %}
        with filtered_data as (
            select *
            from {{ model }}
            where BLOCK_TIMESTAMP >= dateadd(day, -{{ days }}, current_timestamp())
        )
        select *
        from filtered_data
        where {{ column_name }} is null
    {% else %}
        select *
        from {{ model }}
        where {{ column_name }} is null
    {% endif %}
{% endmacro %}

{% macro snowflake__test_unique(model, column_name) %}
    {% set days = var('days', none) %}
    {% if days is not none %}
        with filtered_data as (
            select *
            from {{ model }}
            where BLOCK_TIMESTAMP >= dateadd(day, -{{ days }}, current_timestamp())
        )
        select
            {{ column_name }}
            ,count(*) as n_records
        from filtered_data
        group by {{ column_name }}
        having count(*) > 1
    {% else %}
        select
            {{ column_name }}
            ,count(*) as n_records
        from {{ model }}
        group by {{ column_name }}
        having count(*) > 1
    {% endif %}
{% endmacro %}

{% macro snowflake__test_accepted_values(model, column_name, values, quote=True) %}
    {% set days = var('days', none) %}
    {% if days is not none %}
        with filtered_data as (
            select *
            from {{ model }}
            where BLOCK_TIMESTAMP >= dateadd(day, -{{ days }}, current_timestamp())
        )
        select *
        from filtered_data
        where {{ column_name }} not in (
            {% for value in values -%}
                {% if quote -%}
                    '{{ value }}'
                {%- else -%}
                    {{ value }}
                {%- endif -%}
                {%- if not loop.last -%},{%- endif -%}
            {%- endfor -%}
        )
    {% else %}
        select *
        from {{ model }}
        where {{ column_name }} not in (
            {% for value in values -%}
                {% if quote -%}
                    '{{ value }}'
                {%- else -%}
                    {{ value }}
                {%- endif -%}
                {%- if not loop.last -%},{%- endif -%}
            {%- endfor -%}
        )
    {% endif %}
{% endmacro %}

{% macro snowflake__test_relationships(model, column_name, to, field) %}
    {% set days = var('days', none) %}
    {% if days is not none %}
        with filtered_data as (
            select *
            from {{ model }}
            where BLOCK_TIMESTAMP >= dateadd(day, -{{ days }}, current_timestamp())
        )
        select *
        from filtered_data as child
        left join {{ to }} as parent on child.{{ column_name }} = parent.{{ field }}
        where parent.{{ field }} is null
        and child.{{ column_name }} is not null
    {% else %}
        select *
        from {{ model }} as child
        left join {{ to }} as parent on child.{{ column_name }} = parent.{{ field }}
        where parent.{{ field }} is null
        and child.{{ column_name }} is not null
    {% endif %}
{% endmacro %}

{# Register all implementations #}
{% macro test_not_null(model, column_name) %}
    {{ return(adapter.dispatch('test_not_null')(model, column_name)) }}
{% endmacro %}

{% macro test_unique(model, column_name) %}
    {{ return(adapter.dispatch('test_unique')(model, column_name)) }}
{% endmacro %}

{% macro test_accepted_values(model, column_name, values, quote=True) %}
    {{ return(adapter.dispatch('test_accepted_values')(model, column_name, values, quote)) }}
{% endmacro %}

{% macro test_relationships(model, column_name, to, field) %}
    {{ return(adapter.dispatch('test_relationships')(model, column_name, to, field)) }}
{% endmacro %}