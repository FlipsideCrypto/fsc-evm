{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'view',
    tags = ['silver','gha_tasks','phase_1']
) }}

WITH task_status AS (
{{ get_task_status() }}
)

SELECT
    task_name,
    schedule,
    state,
    database_name,
    schema_name,
    warehouse,
    owner,
    created_on,
    last_committed_on,
    last_suspended_on,
    comment
FROM 
    task_status