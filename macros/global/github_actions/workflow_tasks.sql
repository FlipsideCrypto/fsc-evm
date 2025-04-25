{% macro sp_update_workflow_table() %}
CREATE OR REPLACE PROCEDURE github_actions.update_workflow_table(workflow_list VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
try {
    // Parse the comma-separated list of workflow names
    var workflows = WORKFLOW_LIST.split(',').map(w => w.trim());
    
    // Prepare values for SQL statement
    var values = workflows.map(w => `('${w}')`).join(',');
    
    // Create or replace the workflows table
    var sql = `
    CREATE OR REPLACE TABLE github_actions.workflows AS
    WITH source_data AS (
      SELECT column1 as workflow_name
      FROM VALUES
      ${values}
    )
    SELECT 
      workflow_name,
      current_timestamp() as inserted_at
    FROM 
      source_data;`;
    
    snowflake.execute({sqlText: sql});
    
    return "Successfully updated workflows table with " + workflows.length + " workflows";
} catch (err) {
    return "Error updating workflows table: " + err;
}
$$;
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
    {% set results_list = execute and results is not none ? results.rows : [] %}
    
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

{% macro sp_create_task_status() %}
CREATE OR REPLACE PROCEDURE github_actions.get_task_status()
RETURNS TABLE (
    task_name STRING,
    schedule STRING,
    state STRING,
    database_name STRING,
    schema_name STRING,
    warehouse STRING,
    owner STRING,
    created_on TIMESTAMP_NTZ,
    last_committed_on TIMESTAMP_NTZ,
    last_suspended_on TIMESTAMP_NTZ,
    comment STRING
)
LANGUAGE SQL
AS
$$
BEGIN
    SHOW TASKS IN SCHEMA {{ target.database }}.github_actions;
    RETURN TABLE(
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
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
    );
END;
$$;
{% endmacro %}