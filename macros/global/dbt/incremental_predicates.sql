{% macro standard_predicate(
        input_column = 'block_number'
    ) -%}
    {#
        NOTE: This macro returns a signal value that get_merge_sql will process.

        In dbt-snowflake 1.10+, subqueries referencing the tmp table don't work because:
        1. The tmp view doesn't exist at compile time (run_query fails)
        2. Subqueries at execution time trigger SYSTEM_TABLE_SCAN constant errors

        The fix is handled in get_merge_sql which:
        1. Detects the 'standard_predicate' signal
        2. Wraps the source in a subquery with window functions for min/max
        3. Adds predicates using those computed columns

        See: https://github.com/dbt-labs/dbt-snowflake/pull/93
    #}
    standard_predicate:{{ input_column }}
{%- endmacro %}
