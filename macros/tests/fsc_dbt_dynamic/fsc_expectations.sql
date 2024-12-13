{% test fsc_evm_expect_column_values_to_match_regex(model, column_name, regex, timestamp_column=none) %}
    {% set filter = add_days_filter(model, timestamp_column=timestamp_column) %}
    {{ return(dbt_expectations.test_expect_column_values_to_match_regex(
        model,
        column_name,
        regex,
        row_condition=filter.row_condition
    )) }}
{% endtest %}

{% test fsc_evm_expect_column_values_to_be_between(model, column_name, min_value=None, max_value=None, timestamp_column=none) %}
    {% set filter = add_days_filter(model, timestamp_column=timestamp_column) %}
    
    {{ return(dbt_expectations.test_expect_column_values_to_be_between(
        model,
        column_name,
        min_value,
        max_value,
        row_condition=filter.row_condition
    )) }}
{% endtest %}

{% test fsc_evm_expect_column_values_to_be_in_set(model, column_name, value_set, timestamp_column=none, quote_values=True) %}
    {% set filter = add_days_filter(model, timestamp_column=timestamp_column) %}

    with all_values as (
        select
            {{ column_name }} as value_field
        from {{ model }}
        {% if filter.row_condition %}
        where {{ filter.row_condition }}
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

--These tests have no changes as a date filter is not needed
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