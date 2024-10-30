{% macro drop_github_actions_schema() %}

    {% set sql %}
        DROP ALL TASKS IN SCHEMA {{ target.database }}.github_actions;
    {% endset %}

    {% do run_query(sql) %}
    {% do log("Dropped all tasks in schema '" ~ target.database ~ ".github_actions'", info=true) %}

{% endmacro %}