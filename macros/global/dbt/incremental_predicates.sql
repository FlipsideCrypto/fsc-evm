{% macro standard_predicate(
        input_column = 'block_number'
    ) -%}
    {#
        NOTE: This macro returns a signal value that get_merge_sql will detect and strip.

        In dbt-snowflake 1.10+, the old fsc_utils.dynamic_range_predicate() approach
        doesn't work because run_query() can't access the tmp view at compile time.

        The get_merge_sql override detects this signal and strips the predicate,
        allowing the merge to proceed without partition pruning.

        See: https://github.com/dbt-labs/dbt-snowflake/pull/93
    #}
    standard_predicate:{{ input_column }}
{%- endmacro %}
