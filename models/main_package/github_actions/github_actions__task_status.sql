{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'view',
    tags = ['silver','gha_tasks']
) }}

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
    TABLE(github_actions.get_task_status())