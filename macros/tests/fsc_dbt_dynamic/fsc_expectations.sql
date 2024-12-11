{% test fsc_evm_expect_column_values_to_match_regex(model, column_name, regex) %}
    
    {% set filter_config = add_days_filter(model) %}
    
    {{ return(dbt_expectations.test_expect_column_values_to_match_regex(
        model,
        column_name,
        regex,
        row_condition=filter_config.row_condition
    )) }}
{% endtest %}

{% test fsc_evm_expect_column_values_to_be_between(model, column_name, min_value=None, max_value=None) %}
    
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