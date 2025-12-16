{% macro get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates=none) -%}
    {#
        Custom get_merge_sql override for fsc-evm.

        In dbt-snowflake 1.10+, the default tmp_relation_type changed from 'table' to 'view'.
        This breaks the old fsc_utils.dynamic_range_predicate() approach which used run_query()
        to query the tmp table during macro compilation.

        This override intercepts the special predicate formats and strips them, allowing
        the merge to proceed without partition pruning predicates. The merge will still
        work correctly, just without the optimization.

        Supported predicate formats that get stripped:
        - ["dynamic_range", "column_name"]
        - ["dynamic_range_predicate", "column_name"]
        - ["standard_predicate:column_name"]

        See: https://github.com/dbt-labs/dbt-snowflake/pull/93
    #}
    {% set predicates = incremental_predicates %}

    {% if incremental_predicates and incremental_predicates|length > 0 %}
        {% set first_pred = incremental_predicates[0] | string | trim %}

        {# Strip dynamic_range format: ["dynamic_range", "column_name"] #}
        {% if first_pred == "dynamic_range" or first_pred == "dynamic_range_predicate" %}
            {% set predicates = none %}

        {# Strip standard_predicate format: ["standard_predicate:column_name"] #}
        {% elif first_pred.startswith('standard_predicate:') %}
            {% set predicates = none %}
        {% endif %}
    {% endif %}

    {# Get the base merge SQL from dbt - predicates are either passed through or stripped #}
    {{ return(adapter.dispatch('get_merge_sql', 'dbt')(target, source, unique_key, dest_columns, predicates)) }}

{% endmacro %}
