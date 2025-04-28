{% macro create_workflow_table(workflow_values) %}
    {# Intended to be called via the make deploy_gha_workflows_table command in the Makefile #}
    {% set create_schema_sql %}
    CREATE SCHEMA IF NOT EXISTS github_actions;
    {% endset %}
    {% do run_query(create_schema_sql) %}
    
    {% set update_table_sql %}
    CREATE OR REPLACE TABLE {{target.database}}.github_actions.workflows AS 
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
    
    {% set prod_db = target.database.lower().replace('_dev', '') %}
    {% set grant_sql %}
    GRANT USAGE ON SCHEMA {{target.database}}.github_actions TO ROLE INTERNAL_DEV;
    GRANT USAGE ON SCHEMA {{target.database}}.github_actions TO ROLE DBT_CLOUD_{{ prod_db }};
    
    GRANT SELECT ON TABLE {{target.database}}.github_actions.workflows TO ROLE INTERNAL_DEV;
    GRANT SELECT ON TABLE {{target.database}}.github_actions.workflows TO ROLE DBT_CLOUD_{{ prod_db }};
    {% endset %}
    {% do run_query(grant_sql) %}
    
    {% do log("Table github_actions.workflows updated successfully with grants applied.", info=True) %}
{% endmacro %}

{% macro create_gha_tasks() %}
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

    {% set prod_db = target.database.lower().replace('_dev', '') %}
    
    {% set created_tasks = [] %}
    
    {% for result in results_list %}
        {% set task_name = result[0] %}
        {% set workflow_name = result[1] %}
        {% set cron_schedule = result[2] %}
        
        {% do log("Task: " ~ task_name ~ ", Schedule: " ~ cron_schedule, info=true) %}
        
        {% set sql %}
        EXECUTE IMMEDIATE 'CREATE OR REPLACE TASK github_actions.{{ task_name }} WAREHOUSE = DBT_CLOUD SCHEDULE = ''USING CRON {{ cron_schedule }} UTC'' COMMENT = ''Task to trigger {{ workflow_name }}.yml workflow according to {{ cron_schedule }}'' AS DECLARE rs resultset; output string; BEGIN rs := (SELECT github_actions.workflow_dispatches(''FlipsideCrypto'', ''{{ prod_db }}-models'', ''{{ workflow_name }}.yml'', NULL):status_code::int AS status_code); SELECT LISTAGG($1, '';'') INTO :output FROM TABLE(result_scan(LAST_QUERY_ID())) LIMIT 1; CALL SYSTEM$SET_RETURN_VALUE(:output); END;'
        {% endset %}
        
        {% do run_query(sql) %}
        {% do created_tasks.append(task_name) %}
    {% endfor %}
    
    {# Optionally, resume tasks if the variable is set #}
    {% if var('RESUME_GHA_TASKS', false) %}
        {% do log("Tasks created in RESUME state. Use var RESUME_GHA_TASKS: false to automatically suspend them.", info=true) %}
        {% for task_name in created_tasks %}
            {% set resume_task_sql %}
            ALTER TASK github_actions.{{ task_name }} RESUME;
            {% endset %}
            {% do run_query(resume_task_sql) %}
            {% do log("Resumed task: " ~ task_name, info=true) %}
        {% endfor %}
    {% else %}
        {% do log("Tasks created in SUSPENDED state. Use var RESUME_GHA_TASKS: true to automatically resume them.", info=true) %}
    {% endif %}
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