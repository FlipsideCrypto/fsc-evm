{% macro get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates=none) -%}
    {#
        Custom get_merge_sql override for fsc-evm.

        In dbt-snowflake 1.10+, the default tmp_relation_type changed from 'table' to 'view'.
        This breaks the old fsc_utils.dynamic_range_predicate() approach which used run_query()
        to query the tmp table during macro compilation.

        This override handles special predicate keywords by:
        1. Generating the merge with no predicates
        2. Post-processing to inject predicates that use window functions in the USING clause

        Supported predicate formats:
        - ["dynamic_range", "column_name"]
        - ["dynamic_range_predicate", "column_name"]
        - ["standard_predicate:column_name"]

        See: https://github.com/dbt-labs/dbt-snowflake/pull/93
    #}
    {% set predicates = incremental_predicates %}
    {% set use_dynamic_predicate = false %}
    {% set predicate_column = none %}

    {% if incremental_predicates and incremental_predicates|length > 0 %}
        {% set first_pred = incremental_predicates[0] | string | trim %}

        {# Handle dynamic_range format: ["dynamic_range", "column_name"] #}
        {% if first_pred == "dynamic_range" or first_pred == "dynamic_range_predicate" %}
            {% set use_dynamic_predicate = true %}
            {% set predicate_column = incremental_predicates[1] %}
            {% set predicates = none %}

        {# Handle standard_predicate format: ["standard_predicate:column_name"] #}
        {% elif first_pred.startswith('standard_predicate:') %}
            {% set use_dynamic_predicate = true %}
            {% set predicate_column = first_pred.split(':')[1] | trim %}
            {% set predicates = none %}
        {% endif %}
    {% endif %}

    {# Get the base merge SQL from dbt #}
    {% set merge_sql = adapter.dispatch('get_merge_sql', 'dbt')(target, source, unique_key, dest_columns, predicates) %}

    {% if use_dynamic_predicate and predicate_column %}
        {#
            Modify the merge SQL to:
            1. Wrap the source in a subquery that adds min/max as window functions
            2. Add a predicate using those computed values

            Original: USING source AS DBT_INTERNAL_SOURCE
            Modified: USING (SELECT *, MIN(col) OVER() as _pr_min, MAX(col) OVER() as _pr_max FROM source) AS DBT_INTERNAL_SOURCE

            Then add: AND DBT_INTERNAL_DEST.col >= DBT_INTERNAL_SOURCE._pr_min
                      AND DBT_INTERNAL_DEST.col <= DBT_INTERNAL_SOURCE._pr_max

            This avoids subqueries that trigger SYSTEM_TABLE_SCAN constant errors.
        #}
        {% set source_str = source | string %}
        {% set new_using = '(SELECT *, MIN(' ~ predicate_column ~ ') OVER() as _pr_min, MAX(' ~ predicate_column ~ ') OVER() as _pr_max FROM ' ~ source_str ~ ')' %}
        {% set modified_sql = merge_sql | replace('using ' ~ source_str ~ ' as DBT_INTERNAL_SOURCE', 'using ' ~ new_using ~ ' as DBT_INTERNAL_SOURCE') %}
        {% set modified_sql = modified_sql | replace('USING ' ~ source_str ~ ' as DBT_INTERNAL_SOURCE', 'USING ' ~ new_using ~ ' as DBT_INTERNAL_SOURCE') %}

        {# Add the predicate to the ON clause #}
        {% set predicate_sql = 'DBT_INTERNAL_DEST.' ~ predicate_column ~ ' >= DBT_INTERNAL_SOURCE._pr_min AND DBT_INTERNAL_DEST.' ~ predicate_column ~ ' <= DBT_INTERNAL_SOURCE._pr_max' %}

        {# Find the ON clause and add our predicate #}
        {% set modified_sql = modified_sql | replace(') and (', ') and (' ~ predicate_sql ~ ') and (') %}
        {% set modified_sql = modified_sql | replace(') AND (', ') AND (' ~ predicate_sql ~ ') AND (') %}

        {# If no existing predicates, we need to add after the ON keyword differently #}
        {% if ') and (' not in merge_sql and ') AND (' not in merge_sql %}
            {% set modified_sql = modified_sql | replace('on (', 'on (' ~ predicate_sql ~ ') and (') %}
            {% set modified_sql = modified_sql | replace('ON (', 'ON (' ~ predicate_sql ~ ') AND (') %}
        {% endif %}

        {{ return(modified_sql) }}
    {% else %}
        {{ return(merge_sql) }}
    {% endif %}

{% endmacro %}
