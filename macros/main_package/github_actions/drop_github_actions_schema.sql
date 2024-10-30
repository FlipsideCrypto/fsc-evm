{% macro drop_github_actions_schema() %}

    {% set show_tasks_query %}
        SHOW TASKS IN SCHEMA {{ target.database }}.github_actions;
    {% endset %}
    
    {% set results = run_query(show_tasks_query) %}
    
    {% if execute %}
        {% for task in results %}
            {% set drop_task_sql %}
                DROP TASK IF EXISTS {{ target.database }}.github_actions.{{ task[1] }};
            {% endset %}
            {% do run_query(drop_task_sql) %}
            {% do log("Dropped task '" ~ task[1] ~ "'", info=true) %}
        {% endfor %}
    {% endif %}

{% endmacro %}