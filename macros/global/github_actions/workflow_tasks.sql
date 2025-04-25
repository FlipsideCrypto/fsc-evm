{% macro update_workflow_table(workflow_values) %}
    -- Create schema if it doesn't exist
    {% set create_schema_sql %}
    CREATE SCHEMA IF NOT EXISTS github_actions;
    {% endset %}
    {% do run_query(create_schema_sql) %}
    
    -- Create or replace the workflows table
    {% set update_table_sql %}
    CREATE OR REPLACE TABLE github_actions.workflows AS 
    WITH source_data AS (
        SELECT column1 as workflow_name 
        FROM VALUES 
        {{ workflow_values }}
    ) 
    SELECT 
        workflow_name, 
        SYSDATE() as inserted_at 
    FROM source_data;
    {% endset %}
    {% do run_query(update_table_sql) %}
    
    -- Return success message
    {% do log("Table github_actions.workflows updated successfully.", info=True) %}
{% endmacro %}

{% macro create_gha_tasks() %}
    -- Get the list of tasks to create
    {% set query %}
    SELECT
        task_name,
        workflow_name,
        cron_schedule
    FROM
        {{ ref('github_actions__workflow_schedule') }}
    {% endset %}
    {% set results = run_query(query) %}
    {% if execute and results is not none %}
        {% set results_list = results.rows %}
    {% else %}
        {% set results_list = [] %}
    {% endif %}
    
    -- Normalize database name
    {% set prod_db = target.database.lower().replace('_dev', '') %}
    
    -- Create tasks
    {% for result in results_list %}
        {% set task_name = result[0] %}
        {% set workflow_name = result[1] %}
        {% set cron_schedule = result[2] %}
        
        -- Create the task (always in suspended state)
        {% set create_task_sql %}
        CREATE OR REPLACE TASK github_actions.{{ task_name }} 
        WAREHOUSE = DBT_CLOUD 
        SCHEDULE = '{{ cron_schedule }}' 
        COMMENT = 'Task to trigger {{ workflow_name }}.yml workflow according to {{ cron_schedule }}' 
        AS 
        DECLARE 
            rs resultset; 
            output string; 
        BEGIN 
            rs := (SELECT github_actions.workflow_dispatches('FlipsideCrypto', '{{ prod_db }}-models', '{{ workflow_name }}.yml', NULL):status_code::int AS status_code); 
            SELECT LISTAGG($1, ';') INTO :output FROM TABLE(result_scan(LAST_QUERY_ID())) LIMIT 1; 
            CALL SYSTEM$SET_RETURN_VALUE(:output); 
        END;
        {% endset %}
        {% do run_query(create_task_sql) %}
    {% endfor %}
{% endmacro %}

{% macro alter_gha_tasks(
        task_names,
        task_action
    ) %}
    {% set task_list = task_names.split(',') %}
    {% for task_name in task_list %}
        {% set task_name = task_name.strip() %}
        {% set sql %}
        EXECUTE IMMEDIATE 'ALTER TASK IF EXISTS github_actions.{{ task_name }} {{ task_action }};' 
        {% endset %}
        {% do run_query(sql) %}
    {% endfor %}
{% endmacro %}

{% macro alter_all_gha_tasks(task_action) %}
    {% set query %}
    SELECT
        task_name
    FROM
        {{ ref('github_actions__workflow_schedule') }}
    {% endset %}
    {% set results = run_query(query) %}
    {% if execute and results is not none %}
        {% set results_list = results.rows %}
    {% else %}
        {% set results_list = [] %}
    {% endif %}

    {% for result in results_list %}
        {% set task_name = result[0] %}
        {% set sql %}
        EXECUTE IMMEDIATE 'ALTER TASK IF EXISTS github_actions.{{ task_name }} {{ task_action }};' 
        {% endset %}
        {% do run_query(sql) %}
    {% endfor %}
{% endmacro %}

{% macro get_task_status() %}

    {% set create_view_sql %}
    CREATE OR REPLACE VIEW github_actions.task_status AS
    SELECT 
        "name" AS task_name,
        "schedule" AS schedule,
        "state" AS state,
        "database_name" AS database_name,
        "schema_name" AS schema_name,
        "warehouse" AS warehouse,
        "owner" AS owner,
        "created_on" AS created_on,
        "last_committed_on" AS last_committed_on,
        "last_suspended_on" AS last_suspended_on,
        "comment" AS comment
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    {% endset %}
    
    {% set show_tasks_sql %}
    SHOW TASKS IN SCHEMA {{ target.database }}.github_actions;
    {% endset %}

    {% do run_query(show_tasks_sql) %}
    {% do run_query(create_view_sql) %}
{% endmacro %}