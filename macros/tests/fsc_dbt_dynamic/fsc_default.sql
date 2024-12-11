{% macro snowflake__test_not_null(model, column_name, timestamp_column) %}

    {% set timestamp_column = timestamp_column if timestamp_column is not none else 'BLOCK_TIMESTAMP' %}
    {% set days = var('days', none) %}

    {% if days is not none %}
        with filtered_data as (
            select *
            from {{ model }}
            where {{ timestamp_column }} >= dateadd(day, -{{ days }}, sysdate())
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

{% macro snowflake__test_unique(model, column_name, timestamp_column) %}
    
    {% set timestamp_column = timestamp_column if timestamp_column is not none else 'BLOCK_TIMESTAMP' %}
    {% set days = var('days', none) %}
    {% if days is not none %}
        with filtered_data as (
            select *
            from {{ model }}
            where {{ timestamp_column }} >= dateadd(day, -{{ days }}, sysdate())
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

{% macro snowflake__test_accepted_values(model, column_name, values, quote=True, timestamp_column) %}
    
    {% set timestamp_column = timestamp_column if timestamp_column is not none else 'BLOCK_TIMESTAMP' %}
    {% set days = var('days', none) %}
    {% if days is not none %}
        with filtered_data as (
            select *
            from {{ model }}
            where {{ timestamp_column }} >= dateadd(day, -{{ days }}, sysdate())
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

{% macro snowflake__test_relationships(model, column_name, to, field, timestamp_column) %}

    {% set timestamp_column = timestamp_column if timestamp_column is not none else 'BLOCK_TIMESTAMP' %}
    {% set days = var('days', none) %}
    {% if days is not none %}
        with filtered_data as (
            select *
            from {{ model }}
            where {{ timestamp_column }} >= dateadd(day, -{{ days }}, sysdate())
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
{% macro test_not_null(model, column_name, timestamp_column) %}
    {{ return(adapter.dispatch('test_not_null')(model, column_name, timestamp_column)) }}
{% endmacro %}

{% macro test_unique(model, column_name, timestamp_column) %}
    {{ return(adapter.dispatch('test_unique')(model, column_name, timestamp_column)) }}
{% endmacro %}

{% macro test_accepted_values(model, column_name, values, quote=True, timestamp_column) %}
    {{ return(adapter.dispatch('test_accepted_values')(model, column_name, values, quote, timestamp_column)) }}
{% endmacro %}

{% macro test_relationships(model, column_name, to, field, timestamp_column) %}
    {{ return(adapter.dispatch('test_relationships')(model, column_name, to, field, timestamp_column)) }}
{% endmacro %}