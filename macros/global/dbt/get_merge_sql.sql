{% macro get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates=none) -%}
    {% set predicate_override = "" %}
    {% if incremental_predicates and incremental_predicates|length > 0 and incremental_predicates[0] == "dynamic_range" %}
        {#
            Generate a runtime predicate using subqueries instead of run_query().
            This avoids the dbt-snowflake 1.10 issue where the tmp table doesn't exist
            at macro compilation time.

            The predicate limits the destination table scan to only rows where the
            predicate column falls within the min/max range of the source data.
        #}
        {% set predicate_column = incremental_predicates[1] %}
        {% set predicate_override = 'DBT_INTERNAL_DEST.' ~ predicate_column ~ ' >= (SELECT MIN(' ~ predicate_column ~ ') FROM ' ~ source ~ ') AND DBT_INTERNAL_DEST.' ~ predicate_column ~ ' <= (SELECT MAX(' ~ predicate_column ~ ') FROM ' ~ source ~ ')' %}
    {% endif %}
    {% set predicates = [predicate_override] if predicate_override else incremental_predicates %}
    -- standard merge from here - dispatch to adapter's get_merge_sql
    {% set merge_sql = adapter.dispatch('get_merge_sql', 'dbt')(target, source, unique_key, dest_columns, predicates) %}
    {{ return(merge_sql) }}

{% endmacro %}
