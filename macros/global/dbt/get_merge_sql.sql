{% macro get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates=none) -%}
    {#
        Custom get_merge_sql override for fsc-evm.

        In dbt-snowflake 1.10+, the default tmp_relation_type changed from 'table' to 'view'.
        This breaks the old fsc_utils.dynamic_range_predicate() approach which used run_query()
        to query the tmp table during macro compilation - the tmp view doesn't exist as a
        standalone queryable object.

        This override handles the 'dynamic_range' predicate keyword by stripping it and
        passing through to dbt's default merge behavior. The dynamic range optimization
        is no longer supported - models will use standard merge without partition pruning.

        For better performance on large tables, consider:
        1. Adding appropriate cluster_by keys
        2. Using search optimization on the target table
        3. Ensuring the incremental WHERE clause is selective

        See: https://github.com/dbt-labs/dbt-snowflake/pull/93
    #}
    {% set predicates = incremental_predicates %}

    {% if incremental_predicates and incremental_predicates|length > 0 %}
        {% if incremental_predicates[0] == "dynamic_range" or incremental_predicates[0] == "dynamic_range_predicate" %}
            {#
                Strip the dynamic_range predicate since it's no longer supported in dbt 1.10+.
                Pass no predicates to use dbt's default merge behavior.
            #}
            {% set predicates = none %}
        {% endif %}
    {% endif %}

    {# Dispatch to dbt's built-in get_merge_sql #}
    {% set merge_sql = adapter.dispatch('get_merge_sql', 'dbt')(target, source, unique_key, dest_columns, predicates) %}
    {{ return(merge_sql) }}

{% endmacro %}
