{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'view',
    tags = ['silver','gha_tasks','phase_1']
) }}

SELECT 
    *
FROM 
    TABLE(information_schema.current_task_graphs())
WHERE
    database_name = UPPER('{{ target.database }}')
    AND schema_name = 'GITHUB_ACTIONS'