{% test fsc_evm_unique_combination_of_columns(model, combination_of_columns, quote_columns=false) %}

    {% set days = var('days', none) %}

    {% if not quote_columns %}
        {%- set column_list=combination_of_columns %}
    {% elif quote_columns %}
        {%- set column_list=[] %}
        {% for column in combination_of_columns -%}
            {% set column_list = column_list.append( adapter.quote(column) ) %}
        {%- endfor %}
    {% else %}
        {{ exceptions.raise_compiler_error(
            "`quote_columns` argument for unique_combination_of_columns test must be one of [True, False] Got: '" ~ quote ~"'.'"
        ) }}
    {% endif %}

    {%- set columns_csv=column_list | join(', ') %}

    {% if days is not none %}
        with filtered_data as (
            select *
            from {{ model }}
            where BLOCK_TIMESTAMP >= dateadd(day, -{{ days }}, sysdate())
        ),
        validation_errors as (
            select
                {{ columns_csv }}
            from filtered_data
            group by {{ columns_csv }}
            having count(*) > 1
        )
    {% else %}
        with validation_errors as (
            select
                {{ columns_csv }}
            from {{ model }}
            group by {{ columns_csv }}
            having count(*) > 1
        )
    {% endif %}

    select *
    from validation_errors

{% endtest %}

{% test fsc_evm_equality(model, compare_model, compare_columns=None) %}

    {% set days = var('days', none) %}

    {% set set_diff %}
        count(*) + coalesce(abs(
            sum(case when which_diff = 'a_minus_b' then 1 else 0 end) -
            sum(case when which_diff = 'b_minus_a' then 1 else 0 end)
        ), 0)
    {% endset %}

    {{ config(fail_calc = set_diff) }}

    {%- if not execute -%}
        {{ return('') }}
    {% endif %}

    -- setup
    {%- do dbt_utils._is_relation(model, 'test_equality') -%}

    {%- if not compare_columns -%}
        {%- do dbt_utils._is_ephemeral(model, 'test_equality') -%}
        {%- set compare_columns = adapter.get_columns_in_relation(model) | map(attribute='quoted') -%}
    {%- endif -%}

    {% set compare_cols_csv = compare_columns | join(', ') %}

    {% if days is not none %}
        with filtered_a as (
            select * 
            from {{ model }}
            where BLOCK_TIMESTAMP >= dateadd(day, -{{ days }}, current_timestamp())
        ),
        filtered_b as (
            select * 
            from {{ compare_model }}
            where BLOCK_TIMESTAMP >= dateadd(day, -{{ days }}, current_timestamp())
        ),
        a_minus_b as (
            select {{compare_cols_csv}} from filtered_a
            {{ dbt.except() }}
            select {{compare_cols_csv}} from filtered_b
        ),
        b_minus_a as (
            select {{compare_cols_csv}} from filtered_b
            {{ dbt.except() }}
            select {{compare_cols_csv}} from filtered_a
        )
    {% else %}
        with a_minus_b as (
            select {{compare_cols_csv}} from {{ model }}
            {{ dbt.except() }}
            select {{compare_cols_csv}} from {{ compare_model }}
        ),
        b_minus_a as (
            select {{compare_cols_csv}} from {{ compare_model }}
            {{ dbt.except() }}
            select {{compare_cols_csv}} from {{ model }}
        )
    {% endif %}

    , unioned as (
        select 'a_minus_b' as which_diff, a_minus_b.* from a_minus_b
        union all
        select 'b_minus_a' as which_diff, b_minus_a.* from b_minus_a
    )

    select * from unioned

{% endtest %}