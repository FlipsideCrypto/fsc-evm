{# ============================================================= #}
{#                      VARIABLE INPUTS                          #}
{# ============================================================= #}
    {% macro add_days_filter(model) %}
        {% set days = var('days', none) %}
        {% if days is not none %}
            {# For CTE-based filtering #}
            {% set filtered_model %}
                with filtered_data as (
                    select *
                    from {{ model }}
                    where BLOCK_TIMESTAMP >= dateadd(day, -{{ days }}, sysdate())
                )
                select * from filtered_data
            {% endset %}
            
            {# For row_condition-based filtering #}
            {% set row_condition = "BLOCK_TIMESTAMP >= dateadd(day, -" ~ days ~ ", sysdate())" %}
            
            {# Return both options #}
            {{ return({'filtered_model': filtered_model, 'row_condition': row_condition}) }}
        {% else %}
            {{ return({'filtered_model': model, 'row_condition': none}) }}
        {% endif %}
    {% endmacro %}

{# ============================================================= #}
{#                      DBT UTILS TESTS                          #}
{# ============================================================= #}
    {% test fsc_evm_unique_combination_of_columns(model, combination_of_columns, quote_columns=false) %}
        {% if execute %}
            {% do print("Model: " ~ model) %}
            {% do print("Columns: " ~ combination_of_columns) %}
        {% endif %}

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

        {% if execute %}
            {% do print("Model: " ~ model) %}
            {% do print("Compare Model: " ~ compare_model) %}
            {% do print("Compare Columns: " ~ compare_columns) %}
        {% endif %}

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

{# ============================================================= #}
{#                      DBT EXPECTATIONS TESTS                   #}
{# ============================================================= #}
    {% test fsc_evm_expect_column_values_to_match_regex(model, column_name, regex) %}
        {% if execute %}
            {% do print("Model: " ~ model) %}
            {% do print("Column: " ~ column_name) %}
            {% do print("Regex: " ~ regex) %}
        {% endif %}
        
        {% set filter_config = add_days_filter(model) %}
        
        {{ return(dbt_expectations.test_expect_column_values_to_match_regex(
            model,
            column_name,
            regex,
            row_condition=filter_config.row_condition
        )) }}
    {% endtest %}

    {% test fsc_evm_expect_column_values_to_be_between(model, column_name, min_value=None, max_value=None) %}
        {% if execute %}
            {% do print("Model: " ~ model) %}
            {% do print("Column: " ~ column_name) %}
            {% do print("Min value: " ~ min_value) %}
            {% do print("Max value: " ~ max_value) %}
        {% endif %}
        
        {% set row_condition = add_days_filter_to_row_condition() %}
        
        {{ return(dbt_expectations.test_expect_column_values_to_be_between(
            model,
            column_name,
            min_value,
            max_value,
            row_condition=row_condition
        )) }}
    {% endtest %}

    {% test fsc_evm_expect_column_values_to_be_in_set(model, column_name, value_set, quote_values=True) %}
        {% if execute %}
            {% do print("Model: " ~ model) %}
            {% do print("Column: " ~ column_name) %}
            {% do print("Value Set: " ~ value_set) %}
            {% do print("Quote Values: " ~ quote_values) %}
        {% endif %}

        {% set days = var('days', none) %}
        {% set row_condition = "BLOCK_TIMESTAMP >= dateadd(day, -" ~ days ~ ", current_timestamp())" if days is not none else None %}

        with all_values as (
            select
                {{ column_name }} as value_field
            from {{ model }}
            {% if row_condition %}
            where {{ row_condition }}
            {% endif %}
        ),
        
        set_values as (
            {% for value in value_set -%}
            select
                {% if quote_values -%}
                cast('{{ value }}' as {{ dbt.type_string() }})
                {%- else -%}
                {{ value }}
                {%- endif %} as value_field
            {% if not loop.last %}union all{% endif %}
            {% endfor %}
        ),
        
        validation_errors as (
            -- values from the model that are not in the set
            select
                v.value_field
            from
                all_values v
                left join
                set_values s on v.value_field = s.value_field
            where
                s.value_field is null
        )

        select *
        from validation_errors

    {% endtest %}

    --These tests have no changes as a date filter is not needed, just here for prefix and if we want to change in the future
    {% test fsc_evm_expect_column_values_to_be_in_type_list(model, column_name, column_type_list) %}
        {{ return(dbt_expectations.test_expect_column_values_to_be_in_type_list(
            model,
            column_name,
            column_type_list
        )) }}
    {% endtest %}

    {% test fsc_evm_expect_row_values_to_have_recent_data(model,
                                                    column_name,
                                                    datepart,
                                                    interval,
                                                    row_condition=None) %}
        {{ return(dbt_expectations.test_expect_row_values_to_have_recent_data(
            model,
            column_name,
            datepart,
            interval,
            row_condition
        )) }}
    {% endtest %}
{# ============================================================= #}
{#                      DEFAULT DBT TESTS                        #}
{# ============================================================= #}
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
                where BLOCK_TIMESTAMP >= dateadd(day, -{{ days }}, sysdate())
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
                where BLOCK_TIMESTAMP >= dateadd(day, -{{ days }}, sysdate())
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
                where BLOCK_TIMESTAMP >= dateadd(day, -{{ days }}, sysdate())
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
                where BLOCK_TIMESTAMP >= dateadd(day, -{{ days }}, sysdate())
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