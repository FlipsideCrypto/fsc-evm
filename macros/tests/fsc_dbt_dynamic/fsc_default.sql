{% macro snowflake__test_not_null(model, column_name, timestamp_column=none) %}
    {% set filter = add_days_filter(model, timestamp_column=timestamp_column) %}
    
    {% if filter.row_condition is not none %}
        with filtered_data as (
            select *
            from {{ model }}
            where {{ filter.row_condition }}
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

{% macro snowflake__test_unique(model, column_name, timestamp_column=none) %}
    {% set filter = add_days_filter(model, timestamp_column=timestamp_column) %}
    
    {% if filter.row_condition is not none %}
        with filtered_data as (
            select *
            from {{ model }}
            where {{ filter.row_condition }}
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

{% macro snowflake__test_accepted_values(model, column_name, values, timestamp_column=none, quote=True) %}
    {% set filter = add_days_filter(model, timestamp_column=timestamp_column) %}
    
    {% if filter.row_condition is not none %}
        with filtered_data as (
            select *
            from {{ model }}
            where {{ filter.row_condition }}
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

{% macro snowflake__test_relationships(model, column_name, to, field, timestamp_column=none) %}
    {% set filter = add_days_filter(model, timestamp_column=timestamp_column) %}
    
    {% if filter.row_condition is not none %}
        with filtered_data as (
            select *
            from {{ model }}
            where {{ filter.row_condition }}
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

{# ============================================================= #}
{#                      DEFAULT DBT TEST REGISTRATION            #}
{# ============================================================= #}
{% macro test_not_null(model, column_name, timestamp_column=none) %}
    {{ return(adapter.dispatch('test_not_null')(model, column_name, timestamp_column)) }}
{% endmacro %}

{% macro test_unique(model, column_name, timestamp_column=none) %}
    {{ return(adapter.dispatch('test_unique')(model, column_name, timestamp_column)) }}
{% endmacro %}

{% macro test_accepted_values(model, column_name, values, timestamp_column=none, quote=True) %}
    {{ return(adapter.dispatch('test_accepted_values')(model, column_name, values, timestamp_column, quote)) }}
{% endmacro %}

{% macro test_relationships(model, column_name, to, field, timestamp_column=none) %}
    {{ return(adapter.dispatch('test_relationships')(model, column_name, to, field, timestamp_column)) }}
{% endmacro %}